"""Launch CS2 with AfxHookSource.dll injected — without HLAE.exe GUI.

HLAE's own Custom Loader does the same thing we do here: CreateProcess with
CREATE_SUSPENDED, LoadLibrary the hook in the target, resume. By replicating
the pattern we can launch CS2 headlessly from Python and chain everything
from a single `python hlae_runner.py --plan X`.

Two remote threads run in sequence:
  1. `SetDllDirectoryW(<hlae_dir>)` — so LoadLibrary can resolve the hook's
     dependencies (msvcp140, Imath, OpenEXR, ucrt shims, …) all sitting next
     to `AfxHookSource.dll`.
  2. `LoadLibraryW(<hook_dll>)` — the actual injection.

Safety note: this does not bypass VAC, does not connect to any server, and
mirrors exactly what HLAE has been doing for years. Intended for demo
playback (`-insecure +playdemo …`) only. Do not use while a VAC-protected
server session is active.
"""

from __future__ import annotations

import ctypes
import logging
import subprocess
import sys
import time
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


log = logging.getLogger(__name__)


# --- Win32 constants -------------------------------------------------------

CREATE_SUSPENDED = 0x00000004
# Suppress the flash of a cmd/PowerShell window when we shell out to things
# like `tasklist` / `taskkill`. Windows-only; on other platforms this ends up
# as 0 (harmless). Same constant is defined in hlae_runner.py for the ffmpeg
# invocations — keeping two local copies (vs a shared utils module) to keep
# cs2_launcher.py importable without pulling half the client into scope.
_NO_WINDOW = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
PROCESS_ALL_ACCESS = 0x001F0FFF
MEM_COMMIT = 0x1000
MEM_RESERVE = 0x2000
MEM_RELEASE = 0x8000
PAGE_READWRITE = 0x04
INFINITE = 0xFFFFFFFF
STILL_ACTIVE = 259

# ShowWindow commands
SW_MINIMIZE = 6
SW_SHOWMINNOACTIVE = 7
SW_SHOWNOACTIVATE = 4

# STARTUPINFO flags
STARTF_USESHOWWINDOW = 0x00000001

# SetWindowPos flags
SWP_NOSIZE = 0x0001
SWP_NOZORDER = 0x0004
SWP_NOACTIVATE = 0x0010
SWP_NOOWNERZORDER = 0x0200

# Coordinates far off every reasonable virtual desktop. The window exists,
# Windows still tells D3D it's visible (so Source 2 keeps rendering and
# mirv_streams keeps capturing), but the user sees nothing on their screens.
OFFSCREEN_X = -32000
OFFSCREEN_Y = -32000


# --- Win32 structs ---------------------------------------------------------


class _STARTUPINFO(ctypes.Structure):
    _fields_ = [
        ("cb", wintypes.DWORD),
        ("lpReserved", wintypes.LPWSTR),
        ("lpDesktop", wintypes.LPWSTR),
        ("lpTitle", wintypes.LPWSTR),
        ("dwX", wintypes.DWORD),
        ("dwY", wintypes.DWORD),
        ("dwXSize", wintypes.DWORD),
        ("dwYSize", wintypes.DWORD),
        ("dwXCountChars", wintypes.DWORD),
        ("dwYCountChars", wintypes.DWORD),
        ("dwFillAttribute", wintypes.DWORD),
        ("dwFlags", wintypes.DWORD),
        ("wShowWindow", wintypes.WORD),
        ("cbReserved2", wintypes.WORD),
        ("lpReserved2", ctypes.POINTER(ctypes.c_ubyte)),
        ("hStdInput", wintypes.HANDLE),
        ("hStdOutput", wintypes.HANDLE),
        ("hStdError", wintypes.HANDLE),
    ]


class _PROCESS_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("hProcess", wintypes.HANDLE),
        ("hThread", wintypes.HANDLE),
        ("dwProcessId", wintypes.DWORD),
        ("dwThreadId", wintypes.DWORD),
    ]


# --- Win32 API bindings ----------------------------------------------------

_k32 = ctypes.WinDLL("kernel32", use_last_error=True)

_CreateProcessW = _k32.CreateProcessW
_CreateProcessW.argtypes = [
    wintypes.LPCWSTR,      # lpApplicationName
    wintypes.LPWSTR,       # lpCommandLine (mutable!)
    ctypes.c_void_p,       # lpProcessAttributes
    ctypes.c_void_p,       # lpThreadAttributes
    wintypes.BOOL,         # bInheritHandles
    wintypes.DWORD,        # dwCreationFlags
    ctypes.c_void_p,       # lpEnvironment
    wintypes.LPCWSTR,      # lpCurrentDirectory
    ctypes.POINTER(_STARTUPINFO),
    ctypes.POINTER(_PROCESS_INFORMATION),
]
_CreateProcessW.restype = wintypes.BOOL

_VirtualAllocEx = _k32.VirtualAllocEx
_VirtualAllocEx.argtypes = [wintypes.HANDLE, ctypes.c_void_p, ctypes.c_size_t, wintypes.DWORD, wintypes.DWORD]
_VirtualAllocEx.restype = ctypes.c_void_p

_VirtualFreeEx = _k32.VirtualFreeEx
_VirtualFreeEx.argtypes = [wintypes.HANDLE, ctypes.c_void_p, ctypes.c_size_t, wintypes.DWORD]
_VirtualFreeEx.restype = wintypes.BOOL

_WriteProcessMemory = _k32.WriteProcessMemory
_WriteProcessMemory.argtypes = [wintypes.HANDLE, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)]
_WriteProcessMemory.restype = wintypes.BOOL

_GetModuleHandleW = _k32.GetModuleHandleW
_GetModuleHandleW.argtypes = [wintypes.LPCWSTR]
_GetModuleHandleW.restype = wintypes.HMODULE

_GetProcAddress = _k32.GetProcAddress
_GetProcAddress.argtypes = [wintypes.HMODULE, wintypes.LPCSTR]
_GetProcAddress.restype = ctypes.c_void_p

_CreateRemoteThread = _k32.CreateRemoteThread
_CreateRemoteThread.argtypes = [wintypes.HANDLE, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
_CreateRemoteThread.restype = wintypes.HANDLE

_WaitForSingleObject = _k32.WaitForSingleObject
_WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
_WaitForSingleObject.restype = wintypes.DWORD

_GetExitCodeThread = _k32.GetExitCodeThread
_GetExitCodeThread.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
_GetExitCodeThread.restype = wintypes.BOOL

_ResumeThread = _k32.ResumeThread
_ResumeThread.argtypes = [wintypes.HANDLE]
_ResumeThread.restype = wintypes.DWORD

_TerminateProcess = _k32.TerminateProcess
_TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
_TerminateProcess.restype = wintypes.BOOL

_CloseHandle = _k32.CloseHandle
_CloseHandle.argtypes = [wintypes.HANDLE]
_CloseHandle.restype = wintypes.BOOL

_GetExitCodeProcess = _k32.GetExitCodeProcess
_GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
_GetExitCodeProcess.restype = wintypes.BOOL

_OpenProcess = _k32.OpenProcess
_OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
_OpenProcess.restype = wintypes.HANDLE

# --- user32 for window control --------------------------------------------

_u32 = ctypes.WinDLL("user32", use_last_error=True)

_ShowWindow = _u32.ShowWindow
_ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
_ShowWindow.restype = wintypes.BOOL

_GetWindowThreadProcessId = _u32.GetWindowThreadProcessId
_GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
_GetWindowThreadProcessId.restype = wintypes.DWORD

_IsWindowVisible = _u32.IsWindowVisible
_IsWindowVisible.argtypes = [wintypes.HWND]
_IsWindowVisible.restype = wintypes.BOOL

_EnumWindowsProc = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

_EnumWindows = _u32.EnumWindows
_EnumWindows.argtypes = [_EnumWindowsProc, wintypes.LPARAM]
_EnumWindows.restype = wintypes.BOOL

_SetWindowPos = _u32.SetWindowPos
_SetWindowPos.argtypes = [
    wintypes.HWND, wintypes.HWND,
    ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
    wintypes.UINT,
]
_SetWindowPos.restype = wintypes.BOOL

_GetWindow = _u32.GetWindow
_GetWindow.argtypes = [wintypes.HWND, wintypes.UINT]
_GetWindow.restype = wintypes.HWND

_EnableWindow = _u32.EnableWindow
_EnableWindow.argtypes = [wintypes.HWND, wintypes.BOOL]
_EnableWindow.restype = wintypes.BOOL

# SetWindowLongPtrW for 64-bit pointer-safe window-style edits
if ctypes.sizeof(ctypes.c_void_p) == 8:
    _SetWindowLongPtr = _u32.SetWindowLongPtrW
else:
    _SetWindowLongPtr = _u32.SetWindowLongW
_SetWindowLongPtr.argtypes = [wintypes.HWND, ctypes.c_int, ctypes.c_void_p]
_SetWindowLongPtr.restype = ctypes.c_void_p

if ctypes.sizeof(ctypes.c_void_p) == 8:
    _GetWindowLongPtr = _u32.GetWindowLongPtrW
else:
    _GetWindowLongPtr = _u32.GetWindowLongW
_GetWindowLongPtr.argtypes = [wintypes.HWND, ctypes.c_int]
_GetWindowLongPtr.restype = ctypes.c_void_p

GWL_EXSTYLE = -20
WS_EX_TOOLWINDOW = 0x00000080
WS_EX_NOACTIVATE = 0x08000000


# --- Errors ----------------------------------------------------------------


class InjectionError(RuntimeError):
    pass


def _raise_last(op: str) -> None:
    err = ctypes.get_last_error()
    raise InjectionError(f"{op} failed: Win32 error {err} ({ctypes.WinError(err).strerror})")


# --- Main API --------------------------------------------------------------


@dataclass
class InjectedProcess:
    """A suspended-then-resumed CS2 process with the hook injected."""

    pid: int
    process_handle: int
    main_thread_handle: int

    def wait(self, timeout_sec: float | None = None) -> int:
        """Block until CS2 exits; return exit code."""
        ms = INFINITE if timeout_sec is None else int(timeout_sec * 1000)
        _WaitForSingleObject(self.process_handle, ms)
        code = wintypes.DWORD(0)
        if not _GetExitCodeProcess(self.process_handle, ctypes.byref(code)):
            _raise_last("GetExitCodeProcess")
        return code.value

    def is_alive(self) -> bool:
        code = wintypes.DWORD(0)
        if not _GetExitCodeProcess(self.process_handle, ctypes.byref(code)):
            return False
        return code.value == STILL_ACTIVE

    def terminate(self, exit_code: int = 0) -> None:
        _TerminateProcess(self.process_handle, exit_code)

    def close(self) -> None:
        if self.main_thread_handle:
            _CloseHandle(self.main_thread_handle)
        if self.process_handle:
            _CloseHandle(self.process_handle)
        self.process_handle = 0
        self.main_thread_handle = 0


# --- Window / process management helpers -----------------------------------


def find_windows_for_pid(pid: int, *, only_visible: bool = False) -> list[int]:
    """Return HWNDs of top-level windows owned by the given process.

    `only_visible=False` (default): return EVERY top-level window owned by
    `pid`, even if `IsWindowVisible` returns false. This matters because
    CS2's splash/D3D window can be enumerable for a few ms *before* it
    gets WS_VISIBLE set — if we only find visible windows we end up moving
    it too late, after the user already saw it flash on their primary
    monitor (regression reported on v0.2.10 testing: "logo gigante da Valve").

    `only_visible=True`: legacy behaviour, kept for callers that explicitly
    want to wait until the window is on-screen (e.g. diagnostics).
    """
    found: list[int] = []

    def callback(hwnd: int, lparam: int) -> bool:
        wnd_pid = wintypes.DWORD(0)
        _GetWindowThreadProcessId(hwnd, ctypes.byref(wnd_pid))
        if wnd_pid.value != pid:
            return True
        if only_visible and not _IsWindowVisible(hwnd):
            return True
        found.append(hwnd)
        return True

    _EnumWindows(_EnumWindowsProc(callback), 0)
    return found


def minimize_process_windows(pid: int, *, timeout_sec: float = 10.0, poll_sec: float = 0.25) -> bool:
    """Minimize all top-level windows of `pid`. Deprecated for capture —
    Source 2 (CS2) skips `Present()` calls when the window is minimized,
    which means `mirv_streams` records 0 frames. Use
    `move_process_windows_offscreen()` instead for background renders.
    """
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        windows = find_windows_for_pid(pid)
        if windows:
            for hwnd in windows:
                _ShowWindow(hwnd, SW_MINIMIZE)
            log.info("minimized %d window(s) for pid %d", len(windows), pid)
            return True
        time.sleep(poll_sec)
    log.warning("no visible window appeared for pid %d within %.1fs", pid, timeout_sec)
    return False


def _hide_and_disable(hwnd: int) -> None:
    """Move hwnd offscreen, disable input, remove from Alt+Tab.

    Order matters: we flip the ex-style FIRST (removing the window from the
    taskbar / Alt+Tab catalogue) before moving it, because
    WS_EX_TOOLWINDOW + WS_EX_NOACTIVATE have to be set *before* the window
    paints itself into those shell surfaces. Then we move it offscreen,
    then disable input. If we moved first, the shell would have already
    indexed a "Counter-Strike 2" entry for this HWND and the taskbar chip
    lingered briefly (user report: "só fica no topo da esquerda da tela
    escrito 'Counter-Strike 2'").

    - WS_EX_TOOLWINDOW → removes the window from Alt+Tab & taskbar.
    - WS_EX_NOACTIVATE → keeps it from taking focus.
    - SetWindowPos → off the visible desktop (D3D still sees it as visible
      so Source 2 keeps rendering).
    - EnableWindow(False) → keyboard/mouse bounce off this window even if
      somehow focused. Stops the user from hitting the demo-playback arrows
      and disrupting the capture.
    """
    current = _GetWindowLongPtr(hwnd, GWL_EXSTYLE) or 0
    new_style = current | WS_EX_TOOLWINDOW | WS_EX_NOACTIVATE
    if new_style != current:
        _SetWindowLongPtr(hwnd, GWL_EXSTYLE, ctypes.c_void_p(new_style))
    flags = SWP_NOSIZE | SWP_NOZORDER | SWP_NOACTIVATE | SWP_NOOWNERZORDER
    _SetWindowPos(hwnd, 0, OFFSCREEN_X, OFFSCREEN_Y, 0, 0, flags)
    _EnableWindow(hwnd, False)


def move_process_windows_offscreen(
    pid: int,
    *,
    timeout_sec: float = 20.0,
    hot_poll_sec: float = 0.005,
    hot_poll_duration_sec: float = 3.0,
    cold_poll_sec: float = 0.05,
    watch_for_sec: float = 8.0,
) -> bool:
    """Hide + disable every top-level window of `pid`.

    Moves windows offscreen **the instant** they appear (no settle delay —
    a visible-but-focused CS2 window during setup was scary for users and
    let them press keys that disrupted demo playback). Keeps watching for
    `watch_for_sec` after the first sighting because Source 2 may swap
    splash → D3D main window, and we want to catch both.

    Two poll regimes to balance CPU vs responsiveness:
      • HOT (5ms, first 3s): catch the CS2 splash/D3D window in the <100ms
        window between CreateWindow and first Present. Without this the
        user sees the Valve logo flash on their primary monitor.
      • COLD (50ms, after): keep watching for late-created windows (rare
        but happens on display-mode change or driver overlays like
        NVIDIA Shadow Play).

    `find_windows_for_pid(only_visible=False)` returns windows **before**
    WS_VISIBLE is set, letting us reposition them while they're still
    invisible — so even a paint-before-first-present flash never shows up
    on the user's desktop.

    Note: v0.2.10 tried `-x -32000 -y -32000` launch args but CS2 ignored
    them in many configs. v0.2.11 relies entirely on this watcher and
    drops the SDL positioning flags (kept in the cmdline for belt-and-
    braces but they're no-ops on most CS2 builds).
    """
    deadline = time.monotonic() + timeout_sec
    hot_end = time.monotonic() + hot_poll_duration_sec
    handled: set[int] = set()
    seen_first: float | None = None
    total_hidden = 0
    while time.monotonic() < deadline:
        # Include invisible windows — we want to pre-hide them before
        # they ever paint. This is what fixes the "Valve logo dominou o
        # desktop" regression from v0.2.10.
        windows = find_windows_for_pid(pid, only_visible=False)
        new_hwnds = [w for w in windows if w not in handled]
        if new_hwnds:
            for hwnd in new_hwnds:
                _hide_and_disable(hwnd)
                handled.add(hwnd)
                total_hidden += 1
            log.info("hid %d CS2 window(s) offscreen (total %d)", len(new_hwnds), total_hidden)
            if seen_first is None:
                seen_first = time.monotonic()
        if seen_first is not None and time.monotonic() - seen_first >= watch_for_sec:
            return total_hidden > 0
        # Tight loop right after launch (splash can appear in <100ms),
        # relaxed after the hot-poll budget expires.
        now = time.monotonic()
        time.sleep(hot_poll_sec if now < hot_end else cold_poll_sec)
    if total_hidden == 0:
        log.warning("no visible window appeared for pid %d within %.1fs", pid, timeout_sec)
    return total_hidden > 0


def get_desktop_resolution() -> tuple[int, int]:
    """Primary monitor's current resolution via Win32 `GetSystemMetrics`.

    Used so we can launch CS2 at exactly the desktop's resolution —
    mismatches can cause Source 2 to change the display mode, which is
    scary if CS2 dies before restoring it.
    """
    SM_CXSCREEN = 0
    SM_CYSCREEN = 1
    w = _u32.GetSystemMetrics(SM_CXSCREEN)
    h = _u32.GetSystemMetrics(SM_CYSCREEN)
    return int(w), int(h)


def find_running_cs2_pids() -> list[int]:
    """Return PIDs of all running cs2.exe processes (via `tasklist`)."""
    try:
        out = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq cs2.exe", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, check=True, timeout=10,
            creationflags=_NO_WINDOW,
        ).stdout
    except Exception as e:
        log.warning("tasklist failed: %s", e)
        return []
    pids: list[int] = []
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith('"INFO'):
            continue
        parts = [p.strip('"') for p in line.split(",")]
        if len(parts) >= 2 and parts[0].lower() == "cs2.exe":
            try:
                pids.append(int(parts[1]))
            except ValueError:
                continue
    return pids


def kill_running_cs2(*, wait_sec: float = 5.0) -> int:
    """Terminate all running cs2.exe processes. Returns how many were killed."""
    pids = find_running_cs2_pids()
    if not pids:
        return 0
    log.info("killing %d existing cs2.exe process(es): %s", len(pids), pids)
    subprocess.run(
        ["taskkill", "/F", "/IM", "cs2.exe"],
        capture_output=True, timeout=10,
        creationflags=_NO_WINDOW,
    )
    # Give Windows a moment to actually release the process + its ports/files.
    deadline = time.monotonic() + wait_sec
    while time.monotonic() < deadline:
        if not find_running_cs2_pids():
            return len(pids)
        time.sleep(0.2)
    log.warning("some cs2.exe processes did not exit within %.1fs", wait_sec)
    return len(pids)


def _write_wstring(process: int, text: str) -> int:
    """Allocate + write a UTF-16 null-terminated string in the target. Return its remote address."""
    payload = (text + "\0").encode("utf-16-le")
    addr = _VirtualAllocEx(process, None, len(payload), MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE)
    if not addr:
        _raise_last("VirtualAllocEx")
    written = ctypes.c_size_t(0)
    if not _WriteProcessMemory(process, addr, payload, len(payload), ctypes.byref(written)):
        _VirtualFreeEx(process, addr, 0, MEM_RELEASE)
        _raise_last("WriteProcessMemory")
    return addr


def _remote_call_with_wstring(process: int, func_addr: int, arg_addr: int, op_name: str) -> int:
    """CreateRemoteThread calling func(arg_addr). Wait and return exit code."""
    thread = _CreateRemoteThread(process, None, 0, func_addr, arg_addr, 0, None)
    if not thread:
        _raise_last(f"CreateRemoteThread({op_name})")
    try:
        _WaitForSingleObject(thread, INFINITE)
        code = wintypes.DWORD(0)
        if not _GetExitCodeThread(thread, ctypes.byref(code)):
            _raise_last(f"GetExitCodeThread({op_name})")
        return code.value
    finally:
        _CloseHandle(thread)


def launch_cs2_injected(
    cs2_exe: Path,
    hook_dll: Path,
    dll_search_dir: Path,
    extra_args: Sequence[str] = (),
    cwd: Path | None = None,
    *,
    hide_offscreen: bool = True,
    kill_existing: bool = False,
) -> InjectedProcess:
    """Spawn CS2 with hook_dll loaded before the main thread runs.

    `dll_search_dir` is added to the target's DLL search path via
    `SetDllDirectoryW` so the hook's dependencies resolve without
    copying them into the CS2 install.

    `hide_offscreen=True` (default): after resume, move CS2's window to
    (-32000, -32000). The user never sees it and it can't steal focus, but
    Source 2 still renders — crucial, because a *minimized* window skips
    `Present()` and mirv_streams captures 0 frames (learned the hard way).

    `kill_existing=False` (default): DO NOT kill the user's existing CS2.
    The caller should have pre-flighted (see `find_running_cs2_pids`) and
    refused if the user has a live game running — killing it would lose
    their match. Set True only for dev/debug or when the user explicitly
    opts in via a 'force' flag.
    """
    cs2_exe = Path(cs2_exe)
    hook_dll = Path(hook_dll)
    dll_search_dir = Path(dll_search_dir)

    for p, label in [(cs2_exe, "cs2_exe"), (hook_dll, "hook_dll"), (dll_search_dir, "dll_search_dir")]:
        if not p.exists():
            raise FileNotFoundError(f"{label} not found at {p}")

    if kill_existing:
        killed = kill_running_cs2()
        if killed:
            log.info("killed %d existing cs2.exe before launch", killed)

    # CommandLine must be mutable (CreateProcessW may modify). Build as:
    #   "<cs2_exe>" arg1 arg2 ...
    # Quoting the exe is required if its path has spaces (Steam does).
    parts = [f'"{cs2_exe}"'] + list(extra_args)
    command_line = " ".join(parts)
    command_line_buf = ctypes.create_unicode_buffer(command_line)

    startup = _STARTUPINFO()
    startup.cb = ctypes.sizeof(_STARTUPINFO)
    if hide_offscreen:
        # Ask the Win32 subsystem to show the initial window without
        # activating it — so CS2 doesn't steal focus from the user's
        # browser. The actual off-screen move happens after the D3D
        # window appears.
        startup.dwFlags = STARTF_USESHOWWINDOW
        startup.wShowWindow = SW_SHOWNOACTIVATE
    pi = _PROCESS_INFORMATION()

    log.info("CreateProcess (suspended): %s", command_line)
    ok = _CreateProcessW(
        None,
        command_line_buf,
        None,
        None,
        False,
        CREATE_SUSPENDED,
        None,
        str(cwd) if cwd else None,
        ctypes.byref(startup),
        ctypes.byref(pi),
    )
    if not ok:
        _raise_last("CreateProcessW")

    log.info("CS2 pid=%d (suspended)", pi.dwProcessId)

    try:
        # LoadLibraryW and SetDllDirectoryW are in kernel32, loaded at the
        # same address in every process on the same OS session — we can use
        # our own addresses as the remote addresses.
        k32 = _GetModuleHandleW("kernel32.dll")
        if not k32:
            _raise_last("GetModuleHandleW(kernel32)")
        set_dll_directory = _GetProcAddress(k32, b"SetDllDirectoryW")
        load_library = _GetProcAddress(k32, b"LoadLibraryW")
        if not set_dll_directory or not load_library:
            raise InjectionError("resolving SetDllDirectoryW / LoadLibraryW returned NULL")

        # 1) SetDllDirectoryW(<hlae_dir>) in target
        dir_addr = _write_wstring(pi.hProcess, str(dll_search_dir))
        rc = _remote_call_with_wstring(pi.hProcess, set_dll_directory, dir_addr, "SetDllDirectoryW")
        _VirtualFreeEx(pi.hProcess, dir_addr, 0, MEM_RELEASE)
        if rc == 0:
            raise InjectionError("SetDllDirectoryW returned 0 in target")

        # 2) LoadLibraryW(<hook_dll>) in target — hModule returned as thread exit code
        dll_addr = _write_wstring(pi.hProcess, str(hook_dll))
        module = _remote_call_with_wstring(pi.hProcess, load_library, dll_addr, "LoadLibraryW")
        _VirtualFreeEx(pi.hProcess, dll_addr, 0, MEM_RELEASE)
        if module == 0:
            raise InjectionError(
                f"LoadLibraryW returned NULL — dependency of {hook_dll.name} probably failed to load"
            )
        log.info("injected %s → hModule=0x%x", hook_dll.name, module)

        proc = InjectedProcess(
            pid=pi.dwProcessId,
            process_handle=pi.hProcess,
            main_thread_handle=pi.hThread,
        )

        # Spawn the offscreen watcher BEFORE resume — this is the critical
        # fix for v0.2.10's "CS2 dominou o desktop" regression. If we
        # started the watcher AFTER resume, Source 2 had already created
        # + painted its splash window by the time our first poll ran
        # (~10-30ms race window that consistently lost on fast PCs).
        # Starting it first means the watcher is already polling at 5ms
        # when CS2 starts creating windows.
        if hide_offscreen:
            import threading as _th
            _th.Thread(
                target=move_process_windows_offscreen,
                args=(proc.pid,),
                kwargs={"timeout_sec": 30.0},
                daemon=True,
                name="cs2-offscreen",
            ).start()

        # 3) Resume — CS2 now runs with the hook active from frame 0.
        #    The offscreen watcher is already polling above.
        _ResumeThread(pi.hThread)

        return proc
    except Exception:
        _TerminateProcess(pi.hProcess, 1)
        _CloseHandle(pi.hThread)
        _CloseHandle(pi.hProcess)
        raise


# --- CLI (for manual testing) ---------------------------------------------


if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    ap = argparse.ArgumentParser(description="Launch CS2 with AfxHookSource injected")
    ap.add_argument("--cs2-exe", type=Path, required=True)
    ap.add_argument("--hlae-dir", type=Path, required=True)
    ap.add_argument("--hook-dll", type=Path, default=None, help="defaults to <hlae-dir>/x64/AfxHookSource2.dll (CS2 / Source 2)")
    ap.add_argument("--demo-basename", default=None, help="replays/<basename> for +playdemo")
    ap.add_argument("--exec", dest="exec_cfg", default="fragreel/capture", help="+exec token")
    ap.add_argument("--insecure", action="store_true", default=True)
    ap.add_argument("--wait", action="store_true", help="block until CS2 exits")
    args = ap.parse_args()

    hook = args.hook_dll or (args.hlae_dir / "x64" / "AfxHookSource2.dll")
    search_dir = hook.parent
    extra: list[str] = []
    if args.insecure:
        extra += ["-insecure", "-novid"]
    if args.demo_basename:
        extra += ["+playdemo", f"replays/{args.demo_basename}"]
    if args.exec_cfg:
        extra += ["+exec", args.exec_cfg]

    proc = launch_cs2_injected(args.cs2_exe, hook, search_dir, extra_args=extra)
    print(f"CS2 running pid={proc.pid}")
    if args.wait:
        code = proc.wait()
        print(f"CS2 exited with code {code}")
    proc.close()
