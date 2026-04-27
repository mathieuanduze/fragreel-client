"""HLAE capture runner.

Orchestrates the PC side of FragReel rendering:

    render_plan (from server /matches/{id}/render-plan)
      ↓
    [stage_capture_cfg]   writes <CS2>/game/csgo/cfg/fragreel/capture.cfg
      ↓
    [launch_cs2]          (MVP: instructs user; 4d: will auto-launch via HLAE)
      ↓
    [wait_for_capture]    polls <CS2>/game/bin/win64/<record_name>/takeNNNN
                          for a stable, non-growing take directory
      ↓
    [convert_tga_to_prores]   (optional: ffmpeg → ProRes 4444 for Remotion)
      ↓
    CaptureResult                 → handed to Remotion by caller

This file is a skeleton / MVP. The CS2 launch automation is a TODO — see
`LaunchStrategy.MANUAL` below. All other stages work headlessly.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable


# ffmpeg emits progress lines on stderr like:
#   frame=  123 fps= 45 q=-0.0 size=    1234kB time=00:00:02.05 bitrate=...
# We grep the `frame=` number and forward it to a caller-supplied callback
# so the UI's render-pipeline bar can show live per-segment progress
# (v0.2.14: previous versions only updated when ffmpeg exited, which felt
# frozen for ~4 min per 3964-frame 1080p ProRes 4444 segment).
_FFMPEG_FRAME_RE = re.compile(r"frame=\s*(\d+)")


class RenderCancelled(Exception):
    """Clean abort signal raised by the conversion pipeline when a
    caller-supplied `is_cancelled()` callable returns True mid-encode.

    Distinct from `RuntimeError` so the coordinator can treat cancel as
    `state='cancelled'` instead of `state='error'`. Introduced in v0.2.15
    after PC test confirmed v0.2.14's cancel-from-web-thread flow left
    ffmpeg orphaned (Bug #4): the `except BaseException` in
    `_convert_one_take` only fires on signals delivered to the encoding
    thread, but web-triggered cancel just flips a `threading.Event` on
    another thread — the stderr loop kept reading until ffmpeg finished.
    """
    pass

from cs2_launcher import InjectedProcess, get_desktop_resolution, launch_cs2_injected
from scripts.capture_script import (
    CaptureSegment,
    generate_capture_cfg,
    steamid64_to_account_id,
)


log = logging.getLogger(__name__)


# Windows-only: supress the cmd/PowerShell console window that normally pops
# up when we call a .exe like ffmpeg.exe via subprocess. Users reported (v0.2.9
# testing, Apr 23) that a PowerShell window blinked open showing the MEIPASS
# ffmpeg path — scary from a non-technical user's POV. CREATE_NO_WINDOW is
# 0x08000000. On non-Windows this ends up as 0, a harmless flag.
_NO_WINDOW = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0

# Round 4c Fase 1.11 — thermal safeguard. Mathieu reportou que o PC desligou
# durante render de TGA→ProRes (CPU 100% sustained → thermal shutdown). ffmpeg
# com prores_ks satura todos os cores por ~1-2min seguidos. Mitigação:
#   1. -threads 4: limita ffmpeg a 4 threads (em vez de all-cores). Trade-off:
#      ~30% mais lento, mas user pode usar PC durante render sem travar.
#   2. BELOW_NORMAL_PRIORITY_CLASS (Windows): scheduler dá menos CPU slice quando
#      outros processos pedem. Sistema fica responsivo, ffmpeg cede prioridade.
#      0x4000 é a constante; combina com CREATE_NO_WINDOW via OR.
# Em conjunto: PC fica usável + thermal stress reduzido sem perder muito tempo
# total (render move de ~60s pra ~80s no setup do Mathieu — aceitável).
_BELOW_NORMAL_PRIORITY = 0x00004000 if sys.platform == "win32" else 0
_FFMPEG_CREATIONFLAGS = _NO_WINDOW | _BELOW_NORMAL_PRIORITY
_FFMPEG_THREAD_LIMIT = "4"  # cap ffmpeg threads pra deixar PC responsivo


def _resolve_bundled_npx() -> str | None:
    """Round 4c Fase 2 — resolve npx do Node portable bundlado.

    Frozen mode (.exe), Node 20 LTS Windows x64 vive em
    `_MEIPASS/vendor/node/npx.cmd`. Source/dev mode, busca
    `<client>/vendor/node/npx.cmd` (criado por setup_node.py).

    Retorna str do path se achado, senão None (caller fallback pra
    `shutil.which("npx")`).
    """
    candidates: list[Path] = []

    # Frozen: PyInstaller _MEIPASS extraction
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            candidates.append(Path(meipass) / "vendor" / "node" / "npx.cmd")
            candidates.append(Path(meipass) / "vendor" / "node" / "npx")  # non-Windows fallback

    # Source/dev: <client>/vendor/node/
    client_root = Path(__file__).parent
    candidates.append(client_root / "vendor" / "node" / "npx.cmd")
    candidates.append(client_root / "vendor" / "node" / "npx")

    for c in candidates:
        if c.exists():
            log.info("npx bundled resolved: %s", c)
            return str(c)
    return None


# ---------------------------------------------------------------------------
# Path conventions inside the CS2 install
# ---------------------------------------------------------------------------

CS2_EXE_REL = Path("game/bin/win64/cs2.exe")
CS2_CFG_REL = Path("game/csgo/cfg/fragreel/capture.cfg")
CS2_CFG_EXEC_TOKEN = "fragreel/capture"  # argument to pass to `exec`
CS2_RECORDING_PARENT_REL = Path("game/bin/win64")  # mirv_streams writes <record_name>/takeNNNN/ here
CS2_REPLAYS_REL = Path("game/csgo/replays")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RenderPlan:
    """Everything the runner needs to know to produce a FragReel from one demo.

    - `demo_path`: absolute path to the `.dem` on disk. Runner assumes it's
      already under `<CS2>/game/csgo/replays/` (CS2 only plays from there via
      `playdemo replays/<basename>`).
    - `segments`: one `(start_tick, end_tick)` per highlight to capture. A
      single .cfg handles all segments; HLAE schedules them all upfront.
    - `user_steamid64`: SteamID64 of the player whose kills should be pinned
      in the killfeed. Converted to SteamID3 for `mirv_deathmsg localPlayer`.
    - `user_player_name`: in-game CS2 name (string shown in scoreboard) of
      the same player. Required for `spec_player "<name>"` to lock the
      spectator camera. CS2 has no `*_by_accountid` variant — the killfeed
      pin uses the SteamID3, but the camera lock needs the name.
    """

    demo_path: Path
    segments: tuple[tuple[int, int], ...]
    user_steamid64: str | None = None
    user_player_name: str | None = None
    record_name: str = "fragreel"
    stream_name: str = "default"
    # Round 4c Fase 1.21 — x-ray opt-in. Web envia `show_xray` no payload
    # /render. Default False (sem x-ray). Quando True, capture.cfg emite
    # `spec_show_xray 1` em vez de `spec_show_xray 0` (Fase 1.19 #7).
    show_xray: bool = False

    @classmethod
    def from_json(cls, payload: dict) -> "RenderPlan":
        return cls(
            demo_path=Path(payload["demo_path"]),
            segments=tuple((int(s["start_tick"]), int(s["end_tick"])) for s in payload["segments"]),
            user_steamid64=payload.get("user_steamid64"),
            user_player_name=payload.get("user_player_name"),
            record_name=payload.get("record_name", "fragreel"),
            stream_name=payload.get("stream_name", "default"),
            show_xray=bool(payload.get("show_xray", False)),
        )

    @property
    def demo_basename(self) -> str:
        """The name used in `playdemo replays/<basename>` (without extension)."""
        return self.demo_path.stem


@dataclass(frozen=True)
class TakeOutput:
    """One HLAE take = one capture segment (= one highlight).

    HLAE creates a fresh `takeNNNN/` dir each time `mirv_streams record start`
    fires. With one start/stop cycle per segment, segment_index N maps to
    the (pre_take_index + 1 + N)-th take dir under <record_name>/.

    `mov_path` is None until `convert_takes_to_prores()` fills it.
    """

    segment_index: int
    take_dir: Path  # <CS2>/game/bin/win64/<record_name>/takeNNNN
    stream_dir: Path  # take_dir / <stream_name> — contains the TGAs
    frame_count: int
    audio_path: Path | None  # take_dir / audio.wav (when present)
    mov_path: Path | None = None  # filled by convert_takes_to_prores


@dataclass(frozen=True)
class CaptureResult:
    """All takes produced by one render plan, in segment order (0..N-1)."""

    takes: tuple[TakeOutput, ...]

    @property
    def total_frames(self) -> int:
        return sum(t.frame_count for t in self.takes)

    def with_movs(self, movs: dict[int, Path]) -> "CaptureResult":
        """Return a new CaptureResult where takes have mov_path filled in
        from the {segment_index: mov_path} mapping."""
        new_takes = tuple(
            dataclasses.replace(t, mov_path=movs.get(t.segment_index, t.mov_path))
            for t in self.takes
        )
        return CaptureResult(takes=new_takes)


@dataclass(frozen=True)
class HlaeRunnerConfig:
    cs2_install: Path  # ...\Counter-Strike Global Offensive
    hlae_dir: Path  # ...\vendor\hlae
    ffmpeg_exe: Path | None = None  # defaults to <hlae_dir>/ffmpeg/ffmpeg.exe

    @property
    def cs2_exe(self) -> Path:
        return self.cs2_install / CS2_EXE_REL

    @property
    def hook_dll(self) -> Path:
        """Source 2 hook (CS2). The root-level AfxHookSource.dll is 32-bit
        legacy for CS:GO — we use the 64-bit Source 2 build under x64/."""
        return self.hlae_dir / "x64" / "AfxHookSource2.dll"

    @property
    def hook_search_dir(self) -> Path:
        """Directory added to target's DLL search path via SetDllDirectoryW.
        Holds the hook plus all its 64-bit dependencies (msvcp140, ucrt, …)."""
        return self.hlae_dir / "x64"

    @property
    def recording_parent(self) -> Path:
        return self.cs2_install / CS2_RECORDING_PARENT_REL

    @property
    def cfg_target(self) -> Path:
        return self.cs2_install / CS2_CFG_REL

    def resolved_ffmpeg(self) -> Path | None:
        """Find an ffmpeg.exe for TGA→ProRes conversion.

        Search order:
          1. Explicit override via `ffmpeg_exe`.
          2. HLAE canonical location `<hlae>/ffmpeg/bin/ffmpeg.exe` — HLAE's
             own ffmpeg folder is shipped empty; dropping a static build
             there is the documented install path.
          3. `<hlae>/ffmpeg/ffmpeg.exe` — flatter layout some HLAE users ship.
          4. System `ffmpeg` on $PATH.
        """
        if self.ffmpeg_exe and self.ffmpeg_exe.exists():
            return self.ffmpeg_exe
        for candidate in (
            self.hlae_dir / "ffmpeg" / "bin" / "ffmpeg.exe",
            self.hlae_dir / "ffmpeg" / "ffmpeg.exe",
        ):
            if candidate.exists():
                return candidate
        from_path = shutil.which("ffmpeg")
        return Path(from_path) if from_path else None


class LaunchStrategy(Enum):
    """How the runner starts CS2."""

    INJECT = "inject"  # default — Python ctypes CreateProcess+LoadLibrary; no HLAE GUI
    MANUAL = "manual"  # fallback — print instructions for user to launch HLAE manually


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------


class HlaeRunner:
    def __init__(self, config: HlaeRunnerConfig):
        if not config.cs2_exe.exists():
            raise FileNotFoundError(f"CS2 not found at {config.cs2_exe}")
        if not config.hlae_dir.exists():
            raise FileNotFoundError(f"HLAE dir not found at {config.hlae_dir}")
        self.config = config
        self._launched: InjectedProcess | None = None

    # -- stage 1 ------------------------------------------------------------

    def stage_capture_cfg(self, plan: RenderPlan) -> Path:
        """Write the capture .cfg under <CS2>/game/csgo/cfg/fragreel/."""
        return generate_capture_cfg(
            self.config.cfg_target,
            plan.segments,
            user_steamid64=plan.user_steamid64,
            user_player_name=plan.user_player_name,
            record_name=plan.record_name,
            stream_name=plan.stream_name,
            show_xray=plan.show_xray,
        )

    # -- stage 2 ------------------------------------------------------------

    def launch_cs2(
        self,
        plan: RenderPlan,
        strategy: LaunchStrategy = LaunchStrategy.INJECT,
    ) -> InjectedProcess | None:
        """Launch CS2 with AfxHookSource injected + demo + capture cfg.

        `INJECT` (default): Python spawns CS2 directly, injects the hook, and
        passes `+playdemo … +exec fragreel/capture` as launch args. Fully
        automated — no HLAE GUI, no console typing by the user. Returns an
        `InjectedProcess` you can `.wait()` / `.terminate()`.

        `MANUAL`: prints instructions for the user to launch HLAE by hand.
        Returns None. Fallback only.
        """
        if strategy is LaunchStrategy.MANUAL:
            self._print_manual_launch(plan)
            return None

        if strategy is LaunchStrategy.INJECT:
            return self._launch_injected(plan)

        raise NotImplementedError(f"launch strategy {strategy!r}")

    def _print_manual_launch(self, plan: RenderPlan) -> None:
        args = self._cs2_launch_args(plan)
        log.info("---- MANUAL LAUNCH INSTRUCTIONS ----")
        log.info("1. Open HLAE: %s\\HLAE.exe", self.config.hlae_dir)
        log.info("2. Launcher → Counter-Strike 2")
        log.info("   Program:   %s", self.config.cs2_exe)
        log.info("   Arguments: %s", " ".join(args))
        log.info("3. Run. When CS2 opens, capture is auto-scheduled.")
        log.info("-------------------------------------")

    def _cs2_launch_args(self, plan: RenderPlan) -> list[str]:
        # Match the user's desktop resolution so Source 2 never has a
        # reason to switch display modes (the scary "everything became
        # huge" bug when CS2 was killed mid-transition). Capped at 1920x1080
        # because bigger captures explode disk usage (6.2 MB/frame at 1080p
        # = ~9 GB for a 5s segment already).
        desktop_w, desktop_h = get_desktop_resolution()
        w = min(desktop_w, 1920) if desktop_w > 0 else 1920
        h = min(desktop_h, 1080) if desktop_h > 0 else 1080
        log.info("CS2 launch resolution: %dx%d (desktop %dx%d)", w, h, desktop_w, desktop_h)

        # -windowed forces non-exclusive windowed mode (no display mode
        # change even if CS2's saved config had fullscreen).
        # +mat_fullscreen 0 belts-and-braces it once the game is running.
        # -insecure: no VAC — required for injected hook + demo playback.
        # +mat_queue_mode 2: multi-thread renderer.
        # +fps_max 0: uncap render rate; host_framerate drives pacing.
        # -noborder + -x/-y: CS2 uses SDL for windowing and respects these
        # flags. Positioning the window at (-32000, -32000) from frame 0
        # means the user never sees it on their desktop — previously we
        # relied on a polling watcher to move it *after* it appeared, which
        # left a 100–500ms visible flash (regression reported on v0.2.9).
        # -noborder strips the titlebar so even during that flash there's
        # no "CS2" window chrome, just black pixels.
        return [
            "-insecure",
            "-novid",
            "-windowed",
            "-noborder",
            "-x", "-32000",
            "-y", "-32000",
            "-w", str(w),
            "-h", str(h),
            "+mat_fullscreen", "0",
            "+fps_max", "0",
            "+mat_queue_mode", "2",
            "+playdemo", f"replays/{plan.demo_basename}",
            "+exec", CS2_CFG_EXEC_TOKEN,
        ]

    def _launch_injected(self, plan: RenderPlan) -> InjectedProcess:
        args = self._cs2_launch_args(plan)
        log.info("launching CS2 with %s injected (no HLAE GUI)", self.config.hook_dll.name)
        proc = launch_cs2_injected(
            cs2_exe=self.config.cs2_exe,
            hook_dll=self.config.hook_dll,
            dll_search_dir=self.config.hook_search_dir,
            extra_args=args,
        )
        self._launched = proc
        log.info("CS2 running, pid=%d", proc.pid)
        return proc

    def terminate_cs2(self, *, graceful_wait_sec: float = 8.0) -> None:
        """Close the CS2 process spawned by `_launch_injected`.

        The capture script now fires `quit` via mirv_cmd at end_tick+1, so
        CS2 should exit cleanly on its own and restore the user's display
        mode. This method first waits `graceful_wait_sec` for that to
        happen and only force-terminates as a safety net — because
        TerminateProcess skips the engine's display-mode restoration and
        can leave the desktop at the wrong resolution.
        """
        if self._launched is None:
            return
        if self._launched.is_alive():
            import time as _t
            deadline = _t.monotonic() + graceful_wait_sec
            while _t.monotonic() < deadline and self._launched.is_alive():
                _t.sleep(0.25)
            if self._launched.is_alive():
                log.warning(
                    "CS2 pid=%d still alive after %.1fs grace — force terminating",
                    self._launched.pid, graceful_wait_sec,
                )
                self._launched.terminate()
            else:
                log.info("CS2 pid=%d exited gracefully", self._launched.pid)
        self._launched.close()
        self._launched = None

    # -- stage 3 ------------------------------------------------------------

    def wait_for_capture(
        self,
        plan: RenderPlan,
        *,
        pre_take_index: int | None = None,
        timeout_sec: float = 600.0,
        poll_sec: float = 2.0,
        on_progress: Callable[[int, int], None] | None = None,
        on_take_finalized: Callable[["TakeOutput"], None] | None = None,
    ) -> CaptureResult:
        """Poll the recording dir until ALL N takes (one per segment) are stable.

        HLAE creates a new `takeNNNN/` each time `mirv_streams record start`
        fires. With one start/stop per segment, we expect exactly
        `len(plan.segments)` new take dirs above `pre_take_index`. Returns
        a CaptureResult whose `takes` are sorted by take index ASC and
        labelled with the matching segment_index (0..N-1).

        Stability rule (per take): TGA count must be unchanged across two
        consecutive polls (>= poll_sec * 2 quiescence). The last take to
        stabilise gates completion. Earlier takes are considered final once
        the *next* take dir appears (HLAE has moved on).

        `pre_take_index` = highest take number seen BEFORE launching CS2,
        so leftovers from previous renders aren't picked up. If None,
        `_snapshot_take_index` is called now.

        `on_take_finalized` (v0.2.6+): fires ONCE per take the moment we know
        it's done — either because a higher-numbered take has appeared
        (HLAE moved on) or because the final stability check passed for the
        last take. Lets the caller stream-convert (TGA→ProRes) and free
        disk while CS2 is still capturing the next segment, capping peak
        disk usage at ~1 segment instead of N. The callback runs SYNCHRONOUSLY
        on the polling thread — long callbacks pause progress polling. If
        the callback raises, the render is aborted with that exception.
        """
        record_root = self.config.recording_parent / plan.record_name
        if pre_take_index is None:
            pre_take_index = self._snapshot_take_index(record_root)

        expected = len(plan.segments)
        if expected == 0:
            raise ValueError("plan has no segments")

        deadline = time.monotonic() + timeout_sec
        # Per-take state: take_number -> (last_frames, stable_since)
        take_state: dict[int, tuple[int, float | None]] = {}
        # Finalized takes (callback already fired). Their TGA dirs may be
        # gone (cleanup_tgas), so their frames are remembered separately
        # to keep the progress total monotonic.
        finalized: dict[int, "TakeOutput"] = {}
        finalized_frames_total = 0
        last_total = -1

        def _finalize(n: int, take_dir: Path, frame_count: int) -> None:
            """Build a TakeOutput for take n and fire the callback once."""
            nonlocal finalized_frames_total
            if n in finalized:
                return
            stream_dir = take_dir / plan.stream_name
            audio = take_dir / "audio.wav"
            # segment_index assigned in finalization order (= take_n order
            # because we sort takes ascending and finalize the lowest first).
            take_output = TakeOutput(
                segment_index=len(finalized),
                take_dir=take_dir,
                stream_dir=stream_dir,
                frame_count=frame_count,
                audio_path=audio if audio.exists() else None,
            )
            finalized[n] = take_output
            finalized_frames_total += frame_count
            if on_take_finalized is not None:
                # Caller may convert + delete TGAs here. If they raise we
                # abort the whole capture — there's no graceful partial
                # success when one segment is missing from the reel.
                on_take_finalized(take_output)

        while time.monotonic() < deadline:
            takes_disk = self._list_new_takes(record_root, skip_up_to=pre_take_index)
            # Discard takes already finalized — their dirs may be deleted by
            # the streaming converter. They contribute via finalized_frames_total.
            live_takes = [(n, p) for n, p in takes_disk if n not in finalized]
            # Highest take ever seen (live OR finalized) — gates "is this
            # take definitively done" check.
            seen_n = set(finalized.keys()) | {n for n, _ in takes_disk}
            highest_seen = max(seen_n) if seen_n else -1

            live_total = 0
            for n, take_dir in live_takes:
                stream_dir = take_dir / plan.stream_name
                frames = self._count_tgas(stream_dir) if stream_dir.is_dir() else 0
                live_total += frames
                prev_frames, prev_stable = take_state.get(n, (-1, None))
                # A take below the current highest is definitively stable
                # — finalize immediately so the streaming converter starts.
                if n < highest_seen:
                    _finalize(n, take_dir, frames)
                    continue
                if frames > 0 and frames == prev_frames:
                    take_state[n] = (frames, prev_stable or time.monotonic())
                else:
                    take_state[n] = (frames, None)

            current_total = finalized_frames_total + live_total
            if on_progress is not None and current_total != last_total:
                on_progress(current_total, last_total)
                last_total = current_total

            total_takes_seen = len(finalized) + len(live_takes)
            if total_takes_seen >= expected:
                # Got every expected take. Either they're all finalized
                # (return immediately) or the last live one needs to pass
                # the stability gate before we finalize it too.
                if not live_takes:
                    return self._build_capture_result_streaming(plan, finalized, pre_take_index)
                last_n, last_dir = live_takes[-1]
                _, stable_since = take_state.get(last_n, (0, None))
                if stable_since is not None and (
                    time.monotonic() - stable_since >= poll_sec * 2
                ):
                    stream_dir = last_dir / plan.stream_name
                    frames = self._count_tgas(stream_dir) if stream_dir.is_dir() else 0
                    _finalize(last_n, last_dir, frames)
                    return self._build_capture_result_streaming(plan, finalized, pre_take_index)

            time.sleep(poll_sec)

        raise TimeoutError(
            f"capture did not produce {expected} stable takes within "
            f"{timeout_sec}s under {record_root} (got {len(take_state) + len(finalized)} takes)"
        )

    @staticmethod
    def _build_capture_result_streaming(
        plan: RenderPlan,
        finalized: dict[int, "TakeOutput"],
        pre_take_index: int,
    ) -> CaptureResult:
        """Assemble the final CaptureResult from streaming-finalized takes.

        Iteration order matters for segment_index assignment, but _finalize
        already set segment_index in finalization order (= take_n order),
        so we just sort by take_n and return.
        """
        sorted_n = sorted(finalized.keys())
        outputs = tuple(finalized[n] for n in sorted_n)
        if outputs:
            log.info(
                "capture complete: %d takes (pre_index=%d, range take%04d..take%04d)",
                len(outputs),
                pre_take_index,
                sorted_n[0],
                sorted_n[-1],
            )
        return CaptureResult(takes=outputs)

    @staticmethod
    def _snapshot_take_index(record_root: Path) -> int:
        """Largest `takeNNNN` number currently present; -1 if none / dir missing."""
        if not record_root.is_dir():
            return -1
        highest = -1
        for child in record_root.iterdir():
            if child.is_dir() and child.name.startswith("take"):
                try:
                    n = int(child.name[4:])
                    highest = max(highest, n)
                except ValueError:
                    continue
        return highest

    @staticmethod
    def _list_new_takes(
        record_root: Path, *, skip_up_to: int
    ) -> list[tuple[int, Path]]:
        """All take dirs with index > skip_up_to, sorted by index ASC."""
        if not record_root.is_dir():
            return []
        out: list[tuple[int, Path]] = []
        for child in record_root.iterdir():
            if child.is_dir() and child.name.startswith("take"):
                try:
                    n = int(child.name[4:])
                except ValueError:
                    continue
                if n > skip_up_to:
                    out.append((n, child))
        out.sort(key=lambda x: x[0])
        return out

    @staticmethod
    def _count_tgas(stream_dir: Path) -> int:
        return sum(1 for p in stream_dir.iterdir() if p.suffix.lower() == ".tga")

    @staticmethod
    def cleanup_orphan_take_dirs(record_root: Path, *, keep_below_index: int = 0) -> tuple[int, int]:
        """Delete `takeNNNN/` subdirs left behind under `record_root`.

        Called when a render is cancelled mid-capture — HLAE does not
        clean up after itself, and a single aborted 60-fps capture can
        leave 10+ GB of orphan TGAs on disk. Returns
        `(dirs_deleted, bytes_freed)`.

        `keep_below_index`: leave takes with N < this alone. Typically
        set to the `pre_take` snapshot so we never touch takes that were
        already on disk before our session started (user's older renders
        from previous sessions, in theory — in practice FragReel always
        cleans up its own, but belt-and-braces).

        Non-`takeNNNN` children of `record_root` are also left alone.
        """
        if not record_root.exists() or not record_root.is_dir():
            return 0, 0
        dirs_deleted = 0
        bytes_freed = 0
        for child in record_root.iterdir():
            if not child.is_dir() or not child.name.startswith("take"):
                continue
            try:
                n = int(child.name[4:])
            except ValueError:
                continue
            if n < keep_below_index:
                continue
            try:
                # Sum before delete for the telemetry log; best-effort —
                # errors walking a dir mean we just skip the size total.
                try:
                    bytes_freed += sum(
                        p.stat().st_size for p in child.rglob("*") if p.is_file()
                    )
                except OSError:
                    pass
                import shutil as _shutil
                _shutil.rmtree(child, ignore_errors=True)
                dirs_deleted += 1
            except OSError as e:
                log.warning("could not remove orphan take %s: %s", child, e)
        return dirs_deleted, bytes_freed

    # -- stage 4 ------------------------------------------------------------

    def render_remotion(
        self,
        result: CaptureResult,
        output_mp4: Path,
        editor_dir: Path,
        *,
        composition: str = "HighlightsReel",
        base_props: dict | None = None,
        npm_exe: str | None = None,
        plan: "RenderPlan | None" = None,
    ) -> Path:
        """Run `npx remotion render` against the editor repo.

        Wires per-segment `.mov` outputs into the matching highlight via
        `match.highlights[i].gameplayVideoSrc`. `base_props` should be the
        full ReelProps payload from the server's render-plan endpoint —
        typically `{match, selectedRanks, mood, playerName, orientation}`.
        We mutate `match.highlights` in place to inject the absolute file://
        path that Remotion's `<OffthreadVideo>` will resolve.

        Mapping rule: `result.takes[k].segment_index` indexes into the
        SELECTED highlights array (sorted by rank, like the editor does).
        The server's render-plan must guarantee that the segment order
        matches the highlight selection order — which it does, because
        the cfg generator iterates segments in start_tick order which is
        rank-stable.

        Raises on missing inputs or remotion errors. Returns the MP4 path.
        """
        if not editor_dir.is_dir():
            raise FileNotFoundError(f"editor dir not found: {editor_dir}")
        if base_props is None:
            base_props = {}

        merged = dict(base_props)
        match = dict(merged.get("match", {}))
        highlights = list(match.get("highlights", []))

        if highlights:
            # Round 4c Fase 1.10 (PC catched 26/04): mapping segment_index →
            # highlight precisa estar em ordem CRONOLÓGICA (round_num), não
            # por rank.
            #
            # Bug original: capture takes saem em ordem cronológica do demo
            # (seg00=R7, seg01=R8, seg02=R14). Ordering por rank ASC dava
            # [rank1=R8, rank2=R7, rank3=R14] → seg00→R8, seg01→R7 (SWAPPED).
            # Highlight #1 (label R8 Defuse+Clutch) mostrava footage do R7,
            # highlight #2 (label R7 2K) mostrava footage do R8 com defuse
            # overlay. Mathieu validou visualmente.
            #
            # Fix: ordenar por round_num pra alinhar com ordem cronológica
            # de capture. Display final continua POR RANK na composition —
            # só a associação interna gameplayVideoSrc → highlight precisa
            # ser temporal pra unambiguous mapping.
            selected_ranks = merged.get("selectedRanks") or [h.get("rank") for h in highlights]
            selected_ranks_set = set(selected_ranks)
            ordered_selected = sorted(
                (h for h in highlights if h.get("rank") in selected_ranks_set),
                key=lambda h: h.get("round_num", 0),  # CHRONOLOGICAL, not by rank
            )

            # Round 4c Fase 1.7 — gameplayVideoSrc via HTTP local (não file://).
            # PC test (26/04 madrugada-2) catched: Remotion @remotion/renderer
            # rejeita file:// URIs em props que mapeiam pra <Video src=...>:
            #   "Can only download URLs starting with http:// or https://,
            #    got file:///C:/Users/.../seg00.mov"
            # Tenta downloadAsset via HTTP client e aborta em schemes não-http.
            #
            # Fix: HTTP server local thread-only sirve as .mov files no take_dir.
            # Spin up antes do subprocess, gameplayVideoSrc usa http://127.0.0.1:port,
            # shutdown no finally. Servidor é daemon thread, encerra com processo
            # se algo der errado.
            #
            # Round 4c Fase 1.26 (PC catched 27/04, root cause C):
            # Bug original — mapping `take.segment_index → ordered_selected
            # [segment_index]` assumia 1 take por highlight. Mas cluster v2
            # pode gerar 2+ windows por round (W3 kills + W4 plant separados
            # por gap > MERGE_GAP). R14 highlight tinha 1 entry em
            # ordered_selected mas takes incluía W3+W4 (2 takes pro mesmo
            # highlight). seg_idx=3 caía out-of-bounds → "no matching" warning
            # → W4 (plant) órfão → plant nunca chegava ao Remotion → "plant
            # não aparece" reportado pelo Mathieu múltiplas vezes.
            #
            # Fix: mapear takes → highlight via TICK RANGE OVERLAP (não por
            # segment_index). Múltiplos takes do mesmo highlight são CONCAT-
            # eted via ffmpeg em 1 .mov gameplay-contínuo antes do Remotion.
            #
            # Algoritmo:
            # 1. Pra cada take: descobrir start_tick = plan.segments[seg_idx][0]
            # 2. Pra cada highlight: achar TODOS takes cujo start_tick cai
            #    dentro de (highlight.start*tickrate, highlight.end*tickrate)
            # 3. Se 1 take: usa direto
            # 4. Se 2+: ffmpeg concat → highlight_<rank>_concat.mov
            # Fase 1.26 — tickrate hardcoded 64 (CS2 matchmaking padrão).
            # Tournaments usam 128 mas demos pessoais raramente. Pra demos
            # 128-tick, mapping pode falhar marginally em edge — TODO: passar
            # tickrate no payload do server (já temos no parser).
            tickrate = 64
            # Round 4c Fase 1.26.1 — defensive guard: plan is optional (caller
            # must pass it pra plant takes mapping funcionar). Sem plan, cai
            # no behavior pre-1.26: 1 take per highlight, plant fica órfão.
            # PC test catched (27/04 ~11h): commit 0e47f9b adicionou
            # `plan.segments` reference SEM atualizar signature (plan kwarg)
            # NEM o caller no render_coordinator.py:547. NameError → fallback
            # ffmpeg concat → MP4 cru sem editor. Fix: signature ganha
            # `plan: RenderPlan | None = None`, caller passa `plan=plan`.
            if plan is None:
                log.warning(
                    "render_remotion called without plan kwarg — Fase 1.26 plant "
                    "mapping disabled, falling back to legacy 1-take-per-highlight",
                )
                plan_segments = [(0, 0)] * len(result.takes)  # placeholder neutralizando o map
            else:
                plan_segments = list(plan.segments)  # [(start_tick, end_tick), ...]
            ffmpeg_resolved = self.config.resolved_ffmpeg()

            # Map highlight → list of mov paths (em ordem cronológica)
            highlight_takes: dict[int, list[Path]] = {}
            for take in result.takes:
                if take.mov_path is None:
                    log.warning(
                        "segment %d has no mov_path — Remotion will fall back "
                        "to placeholder gradient", take.segment_index,
                    )
                    continue
                if take.segment_index >= len(plan_segments):
                    log.warning(
                        "segment %d has no plan_segments entry (only %d) — skip",
                        take.segment_index, len(plan_segments),
                    )
                    continue
                seg_start_tick, seg_end_tick = plan_segments[take.segment_index]
                # Match contra ALL highlights (não só ordered_selected) por
                # tick range overlap. Highlights são em SEGUNDOS, segments em
                # TICKS. Convert e check overlap.
                matched_rank: int | None = None
                for h in ordered_selected:
                    h_start_tick = int(h.get("start", 0.0) * tickrate)
                    h_end_tick = int(h.get("end", 0.0) * tickrate)
                    # Round 4c Fase 1.29 (PC catched 27/04 night escalation):
                    # bomb_action_tick frequentemente está APÓS highlight.end
                    # (R14 example: highlight.end=92691 mas bomb_action_tick
                    # =94242, 24s depois). W4 plant capture seg start=93980
                    # caía FORA do range → órfão. Fix: extender range_end pra
                    # incluir bomb_action_tick + 10s cobertura defuse no-kit
                    # (worst case 10s, plant 3.2s — 10s cobre ambos).
                    bomb_tick = h.get("bomb_action_tick")
                    if bomb_tick is not None:
                        range_end = max(h_end_tick, int(bomb_tick) + 10 * tickrate)
                    else:
                        range_end = h_end_tick
                    # Take cai dentro do highlight se take overlap > 50% do
                    # take com highlight range. Usa midpoint do take pra
                    # decidir (robust a small jitter no cluster windows).
                    take_mid_tick = (seg_start_tick + seg_end_tick) // 2
                    if h_start_tick <= take_mid_tick <= range_end:
                        matched_rank = h.get("rank")
                        break
                if matched_rank is None:
                    log.warning(
                        "take seg %d (ticks %d-%d) sem highlight matching — orphan",
                        take.segment_index, seg_start_tick, seg_end_tick,
                    )
                    continue
                highlight_takes.setdefault(matched_rank, []).append(take.mov_path)
                log.info(
                    "take seg %d (ticks %d-%d) → highlight rank=%d",
                    take.segment_index, seg_start_tick, seg_end_tick, matched_rank,
                )

            # Pra cada highlight, descobrir gameplay_start_sec (demo time
            # do PRIMEIRO frame do .mov gameplay). Round 4c Fase 1.28 fix
            # pro killfeed atrasado: editor antes assumia gameplay começava
            # em highlight.start + frontSkip, mas cluster window pode
            # começar MUCH LATER (PAD_PRE 7s do first kill, não do round
            # start). killTimeInSceneSec calcula offset usando esse field.
            highlight_gameplay_start: dict[int, float] = {}
            for take in result.takes:
                if take.mov_path is None or take.segment_index >= len(plan_segments):
                    continue
                seg_start_tick, seg_end_tick = plan_segments[take.segment_index]
                take_mid_tick = (seg_start_tick + seg_end_tick) // 2
                for h in ordered_selected:
                    h_start_tick = int(h.get("start", 0.0) * tickrate)
                    h_end_tick = int(h.get("end", 0.0) * tickrate)
                    # Fase 1.29 — mesma extensão de bomb_action_tick que no
                    # primeiro loop pra consistency. Sem isso, gameplayStartSec
                    # não seria populado pra W4 plant takes (mesmo W4 sendo
                    # mapeado via primeiro loop).
                    bomb_tick = h.get("bomb_action_tick")
                    range_end = (
                        max(h_end_tick, int(bomb_tick) + 10 * tickrate)
                        if bomb_tick is not None
                        else h_end_tick
                    )
                    if h_start_tick <= take_mid_tick <= range_end:
                        rank = h.get("rank")
                        # Min start tick across multiple takes do mesmo highlight
                        cur = highlight_gameplay_start.get(rank, float("inf"))
                        highlight_gameplay_start[rank] = min(
                            cur, seg_start_tick / tickrate,
                        )
                        break

            # Concat múltiplos takes do mesmo highlight via ffmpeg.
            # Single take: usa direto. 2+: ffmpeg concat → 1 .mov.
            for rank, mov_paths in highlight_takes.items():
                if len(mov_paths) == 1:
                    final_mov = mov_paths[0]
                else:
                    log.info(
                        "highlight rank=%d tem %d takes — concat via ffmpeg",
                        rank, len(mov_paths),
                    )
                    final_mov = self._concat_movs_for_highlight(
                        mov_paths, rank, ffmpeg_path=ffmpeg_resolved,
                    )

                # Round 4c Fase 1.33 (Mathieu reportou pós-Fase 1.32 PC PASS:
                # freeze residual entre 1:00-1:16 do MP4). Diagnóstico Mac
                # via análise frame-by-frame: cluster v2 R14 gera W3 (kills,
                # ~15s) + W4 (plant, ~6s) com GAP de ~18s no demo time entre
                # eles. ffmpeg concat junta os 2 .movs (21s real total) MAS
                # Editor calc available baseado em SOURCE TIME (35s+ incluindo
                # gap) → roda 21s de video e HOLDS LAST FRAME por ~12s
                # (= gap não capturado).
                # Fix: probe mov duration REAL pós-concat e anexa ao payload.
                # Editor cap scene_end pelo mov real duration em vez de
                # source-time estimate. Single takes: probe ainda funciona
                # mas geralmente bate com source — não é problema lá.
                actual_mov_duration_sec: float | None = None
                try:
                    actual_mov_duration_sec = self._probe_mov_duration(
                        final_mov, ffmpeg_path=ffmpeg_resolved,
                    )
                    log.info(
                        "highlight rank=%d mov %.2fs (probed)",
                        rank, actual_mov_duration_sec,
                    )
                except Exception as exc:
                    log.warning(
                        "ffprobe duration failed pra rank %d (%s) — fallback editor scene_end",
                        rank, exc,
                    )

                # PLACEHOLDER URI — sobrescrito por http://127.0.0.1:PORT/<basename>
                # após HTTP server subir abaixo.
                for h in highlights:
                    if h.get("rank") == rank:
                        h["gameplayVideoSrc"] = f"__PLACEHOLDER__/{final_mov.name}"
                        gs = highlight_gameplay_start.get(rank)
                        if gs is not None:
                            h["gameplayStartSec"] = gs
                        if actual_mov_duration_sec is not None:
                            h["actualMovDurationSec"] = actual_mov_duration_sec
                        break

            match["highlights"] = highlights
            merged["match"] = match

        # v0.3.1 Round 4c Fase 1.5 — Windows subprocess shim resolution.
        # PC test catched WinError 2 com cmd=["npx",...]+shell=False no Win
        # (Node ships só .cmd shims, CreateProcess sem PATHEXT não acha).
        # shutil.which() faz lookup correto cross-platform.
        #
        # Round 4c Fase 2 — frozen mode usa Node bundled em _MEIPASS/vendor/
        # node/npx.cmd. Sem isso, user final que baixar .exe sem Node
        # instalado falha em "FileNotFoundError: 'npx'". Bundling Node 20
        # portable (~30MB) elimina dep externa.
        npm_exe_resolved = npm_exe or _resolve_bundled_npx() or shutil.which("npx") or "npx"

        # Round 4c Fase 1.7 — HTTP server local pros .mov files.
        # Round 4c Fase 1.8 (PC report 26/04 ~02:30) — UPGRADE pra _RangeAwareHandler:
        #   PC catched que Python's stdlib SimpleHTTPRequestHandler NÃO honra
        #   `Range: bytes=A-B` headers no Windows — sempre retorna o file
        #   inteiro com 200 OK ignorando Range. Pra .mov de 5 GB ProRes 4444
        #   isso é fatal: Remotion's <OffthreadVideo> usa ffmpeg que faz
        #   range requests pra seek frame-by-frame. Sem 206 Partial Content,
        #   ffmpeg recebe full file e abandona via timeout (28s delayRender).
        #   Fix: subclasse com parsing de Range + 206 + Content-Range.
        #   Plus debug log de cada GET pra confirmar Remotion HIT o server
        #   E veio com Range header (validar hipótese H1 do PC: proxy interno
        #   do Remotion CLI pode estar não-propagando Range).
        import http.server
        import socketserver
        import threading
        from functools import partial

        class _RangeAwareHandler(http.server.SimpleHTTPRequestHandler):
            """SimpleHTTPRequestHandler com suporte a HTTP Range requests.

            PC catched (26/04): Win stdlib não honra `Range:` em SimpleHTTP
            — sempre 200 + full file. Remotion's OffthreadVideo + ffmpeg
            espera 206 Partial Content pra seek/buffer eficiente em videos
            grandes. Sem 206 → 28s delayRender timeout → render falha.

            Fix: parseia `Range: bytes=START-END`, retorna 206 + slice.
            Suporta apenas single-range (Remotion's ffmpeg não usa multi-range).
            """

            def log_message(self, format, *args):
                """Override pra logar via fragreel logger (não stderr).
                Inclui Range header se presente — debug pra hipótese H1
                (Remotion proxy não-propaga Range)."""
                try:
                    range_hdr = self.headers.get("Range", "(no Range)")
                    log.info(
                        "remotion HTTP: %s | Range=%s | UA=%s",
                        format % args, range_hdr,
                        (self.headers.get("User-Agent") or "?")[:80],
                    )
                except Exception:
                    pass  # nunca deixa logging derrubar request

            def send_head(self):
                """Override pra suportar Range. Espelha SimpleHTTPRequestHandler.send_head
                mas adiciona path 206 quando Range header presente.
                """
                path = self.translate_path(self.path)
                f = None
                ctype = self.guess_type(path)
                try:
                    f = open(path, "rb")
                except OSError:
                    self.send_error(404, "File not found")
                    return None

                try:
                    fs = os.fstat(f.fileno())
                    file_size = fs[6]

                    # Parse Range header (single range only)
                    range_hdr = self.headers.get("Range")
                    if range_hdr and range_hdr.startswith("bytes="):
                        try:
                            range_spec = range_hdr[len("bytes="):].strip()
                            # "bytes=A-B" ou "bytes=A-" ou "bytes=-N" (suffix length)
                            if range_spec.startswith("-"):
                                # Suffix range: last N bytes
                                suffix_len = int(range_spec[1:])
                                start = max(0, file_size - suffix_len)
                                end = file_size - 1
                            else:
                                start_str, _, end_str = range_spec.partition("-")
                                start = int(start_str)
                                end = int(end_str) if end_str else file_size - 1
                                end = min(end, file_size - 1)

                            if start > end or start >= file_size:
                                self.send_error(416, "Range Not Satisfiable")
                                f.close()
                                return None

                            # 206 Partial Content
                            self.send_response(206, "Partial Content")
                            self.send_header("Content-Type", ctype)
                            self.send_header(
                                "Content-Range",
                                f"bytes {start}-{end}/{file_size}",
                            )
                            self.send_header("Content-Length", str(end - start + 1))
                            self.send_header("Accept-Ranges", "bytes")
                            self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
                            self.end_headers()
                            f.seek(start)
                            # copyfile() vai consumir o resto via shutil.copyfileobj —
                            # mas precisamos limitar a end-start+1 bytes. Wrap num
                            # iterator que para no end:
                            return _RangedFile(f, start, end)
                        except (ValueError, OSError) as e:
                            log.warning("invalid Range header %r: %s — falling back 200", range_hdr, e)
                            # Fallthrough pra 200 normal

                    # No Range → 200 OK full file (comportamento original)
                    self.send_response(200)
                    self.send_header("Content-Type", ctype)
                    self.send_header("Content-Length", str(file_size))
                    self.send_header("Accept-Ranges", "bytes")  # advertise support
                    self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
                    self.end_headers()
                    return f
                except Exception:
                    f.close()
                    raise

        class _RangedFile:
            """Wrapper que limita read pra range bytes — usado pelo
            SimpleHTTPRequestHandler.copyfile via shutil.copyfileobj."""

            def __init__(self, f, start: int, end: int):
                self._f = f
                self._remaining = end - start + 1

            def read(self, size=-1):
                if self._remaining <= 0:
                    return b""
                if size < 0 or size > self._remaining:
                    size = self._remaining
                data = self._f.read(size)
                self._remaining -= len(data)
                return data

            def close(self):
                self._f.close()

        mov_paths_with_take = [
            (t.segment_index, t.mov_path)
            for t in result.takes if t.mov_path is not None
        ]
        local_http_server = None
        srv_thread = None
        if mov_paths_with_take and highlights:
            # Assume todos os .movs no mesmo dir (caso geral em
            # convert_takes_to_prores). Se diferentes, server serve o pai
            # comum + URLs incluem subpath relativo.
            mov_dir = mov_paths_with_take[0][1].parent
            handler = partial(
                _RangeAwareHandler,
                directory=str(mov_dir),
            )
            # Port 0 = OS escolhe livre. Bind localhost-only pra não expor net.
            local_http_server = socketserver.ThreadingTCPServer(
                ("127.0.0.1", 0), handler,
            )
            local_http_server.allow_reuse_address = True
            port = local_http_server.server_address[1]
            srv_thread = threading.Thread(
                target=local_http_server.serve_forever, daemon=True,
            )
            srv_thread.start()
            log.info(
                "remotion: HTTP server local em http://127.0.0.1:%d serving %s (Range-aware)",
                port, mov_dir,
            )

            # Replace placeholder URIs com http://...
            base_url = f"http://127.0.0.1:{port}"
            for h in highlights:
                src = h.get("gameplayVideoSrc", "")
                if isinstance(src, str) and src.startswith("__PLACEHOLDER__/"):
                    mov_basename = src.removeprefix("__PLACEHOLDER__/")
                    h["gameplayVideoSrc"] = f"{base_url}/{mov_basename}"
            match["highlights"] = highlights
            merged["match"] = match

        # Round 4c Fase 1.7 — props via tempfile (não inline).
        # PC test catched: Windows CreateProcess command line limit (8191
        # chars). Props JSON com 10 highlights + kills + kill_ticks +
        # bomb_action_* + narrative em PT estoura o limite (6.5 KB+).
        # Determinístico em qualquer payload com >2-3 highlights ricos.
        # Fix: passar --props <path-to-json-file>.
        import tempfile
        props_file_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", prefix="fragreel_props_",
                delete=False, encoding="utf-8",
            ) as pf:
                json.dump(merged, pf)
                props_file_path = Path(pf.name)
            log.info(
                "remotion props serialized to tempfile (%d bytes): %s",
                props_file_path.stat().st_size, props_file_path,
            )

            # Round 4c Fase 1.8 — bump --timeout pra 120s (default delayRender
            # é 28s). PC catched que mesmo com Range support OK, render
            # estoura 28s tentando carregar 5GB ProRes via OffthreadVideo.
            # H4 do report PC: se 120s ainda falhar, é H2 (codec ProRes 4444
            # é simplesmente pesado demais pra OffthreadVideo) e próximo
            # passo é transcode ProRes→h264 antes de render_remotion.
            cmd: list[str] = [
                npm_exe_resolved,
                "remotion",
                "render",
                composition,
                str(output_mp4),
                "--props",
                str(props_file_path),
                "--timeout",
                "120000",  # 120s, vs default 28s
            ]
            log.info(
                "remotion render: composition=%s out=%s takes_with_mov=%d npx=%s",
                composition, output_mp4,
                len(mov_paths_with_take),
                npm_exe_resolved,
            )
            output_mp4.parent.mkdir(parents=True, exist_ok=True)

            # Round 4c Fase 1.7 — capture stderr pra debug. PC test reportou
            # "exit 1 sem stderr" pq usávamos check=True sem capture_output.
            # Agora capturamos + logamos last 2KB se falha.
            result_proc = subprocess.run(
                cmd,
                cwd=editor_dir,
                shell=False,
                # Round 4c Fase 1.11.1 — BELOW_NORMAL_PRIORITY removido daqui.
                # PC test (26/04 ~13h) catched: aplicar BelowNormal ao npx
                # subprocess CASCATEIA a priority pro Chromium headless que
                # @remotion/renderer spawna via Puppeteer. Chrome compositor
                # com BelowNormal não consegue keep up com frame rendering →
                # "browser crashed while rendering frame N" (target-closed
                # error) → retry loop infinito (3.5h sem progresso, ffmpeg
                # PID com 26s CPU em 12500s elapsed = 0.2% util = stuck).
                # Stderr: "The browser crashed while rendering frame 283,
                # retrying 1 more times" repetindo.
                # Fix: só _NO_WINDOW aqui. ffmpeg encoders pesados (TGA→ProRes
                # linha ~1184 e concat fallback linha ~1395) MANTÊM
                # _FFMPEG_CREATIONFLAGS — esses são puro CPU encode, sem
                # subprocess Chromium em jogo. Thermal protection segue
                # ativa onde realmente importa (encoding pesado).
                creationflags=_NO_WINDOW,
                capture_output=True,
                text=True,
            )
            if result_proc.returncode != 0:
                # Last 2KB pra não floodar log mas pegar a stack trace
                stderr_tail = (result_proc.stderr or "")[-2000:]
                stdout_tail = (result_proc.stdout or "")[-1000:]
                log.error(
                    "remotion render failed (exit %d) — stderr tail:\n%s\n"
                    "stdout tail:\n%s",
                    result_proc.returncode, stderr_tail, stdout_tail,
                )
                raise subprocess.CalledProcessError(
                    result_proc.returncode, cmd,
                    output=result_proc.stdout,
                    stderr=result_proc.stderr,
                )
            return output_mp4
        finally:
            # Cleanup: shutdown HTTP server + remove tempfile
            if local_http_server is not None:
                try:
                    local_http_server.shutdown()
                    local_http_server.server_close()
                    log.debug("remotion HTTP server shut down")
                except Exception as e:
                    log.warning("HTTP server shutdown raised: %s", e)
            if props_file_path is not None:
                try:
                    props_file_path.unlink(missing_ok=True)
                except Exception as e:
                    log.warning("props tempfile cleanup raised: %s", e)

    def convert_takes_to_prores(
        self,
        result: CaptureResult,
        output_dir: Path,
        *,
        basename: str = "highlight",
        source_framerate: int = 60,
        cleanup_tgas: bool = True,
        is_cancelled: Callable[[], bool] | None = None,
    ) -> CaptureResult:
        """ffmpeg every take in `result` → one ProRes 4444 .mov per segment.

        Outputs land at `<output_dir>/<basename>_seg<NN>.mov`. Returns a
        new CaptureResult with each TakeOutput.mov_path filled in.

        Per Edicao_FragReel_Best_Practices: ProRes 4444 preserves alpha
        and decodes fast in the Chromium build Remotion uses. Source
        framerate MUST match `host_framerate` that captured the TGAs —
        we ship 60 fps as default in v0.2.6+ (was 120 fps in v0.2.4-5,
        300 fps in v0.2.3; each step traded slow-mo for disk + speed).

        `cleanup_tgas=True` (default): once ffmpeg succeeds for a take,
        wipes that take's directory. Without this, each render leaves
        5–10 GB of single-use TGAs per segment on disk.

        `is_cancelled` (v0.2.15+): optional callable polled during the
        ffmpeg stderr loop. Returning True kills ffmpeg and raises
        `RenderCancelled` — used by the coordinator to abort cleanly
        when a user hits cancel mid-encode.
        """
        if not result.takes:
            raise ValueError("CaptureResult has no takes to convert")

        ffmpeg = self.config.resolved_ffmpeg()
        if ffmpeg is None:
            raise RuntimeError(
                "ffmpeg not found (checked --ffmpeg-exe, bundled, and $PATH)"
            )
        output_dir.mkdir(parents=True, exist_ok=True)

        movs: dict[int, Path] = {}
        for take in result.takes:
            # Between-take cancel check: spares us launching ffmpeg for a
            # segment we're going to abort anyway.
            if is_cancelled is not None and is_cancelled():
                raise RenderCancelled(
                    f"cancelled before seg={take.segment_index} (batch convert)"
                )
            mov_path = output_dir / f"{basename}_seg{take.segment_index:02d}.mov"
            self._convert_one_take(
                take=take,
                output_path=mov_path,
                ffmpeg=ffmpeg,
                source_framerate=source_framerate,
                cleanup_tgas=cleanup_tgas,
                is_cancelled=is_cancelled,
            )
            movs[take.segment_index] = mov_path

        return result.with_movs(movs)

    def convert_one_take(
        self,
        *,
        take: TakeOutput,
        output_path: Path,
        source_framerate: int = 60,
        cleanup_tgas: bool = True,
        ffmpeg: Path | None = None,
        on_frame_progress: Callable[[int], None] | None = None,
        is_cancelled: Callable[[], bool] | None = None,
    ) -> None:
        """Public wrapper around `_convert_one_take` — used by the streaming
        converter callback in the coordinator.

        `on_frame_progress(n)` fires as ffmpeg completes each batch of frames
        (throttled, not every frame). Lets the coordinator update
        `segments_done` as a float during a long encode instead of jumping
        from 0 → 1 only at the end.

        `is_cancelled` (v0.2.15+): optional callable polled inside the ffmpeg
        stderr loop. Returning True kills ffmpeg and raises `RenderCancelled`.
        Used by the coordinator to detect `threading.Event` cancels set from
        the web request thread — the previous `except BaseException` fallback
        only caught signals delivered to the encoding thread itself (Bug #4
        from v0.2.14 PC test: cancel from web left ffmpeg orphaned).
        """
        ff = ffmpeg or self.config.resolved_ffmpeg()
        if ff is None:
            raise RuntimeError(
                "ffmpeg not found (checked --ffmpeg-exe, bundled, and $PATH)"
            )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._convert_one_take(
            take=take,
            output_path=output_path,
            ffmpeg=ff,
            source_framerate=source_framerate,
            cleanup_tgas=cleanup_tgas,
            on_frame_progress=on_frame_progress,
            is_cancelled=is_cancelled,
        )

    def _convert_one_take(
        self,
        *,
        take: TakeOutput,
        output_path: Path,
        ffmpeg: Path,
        source_framerate: int,
        cleanup_tgas: bool,
        on_frame_progress: Callable[[int], None] | None = None,
        is_cancelled: Callable[[], bool] | None = None,
    ) -> None:
        """Single TGA-sequence → ProRes pass for one TakeOutput."""
        # Numbered TGA pattern (HLAE writes 00000.tga, 00001.tga, ...)
        input_pattern = str(take.stream_dir / "%05d.tga")

        cmd: list[str] = [
            str(ffmpeg),
            "-y",
            # Fase 1.11: thermal safeguard. Cap threads ANTES dos inputs (ffmpeg
            # arg ordering: opções globais primeiro). Ver _FFMPEG_THREAD_LIMIT.
            "-threads", _FFMPEG_THREAD_LIMIT,
            "-framerate", str(source_framerate),
            "-i", input_pattern,
        ]
        if take.audio_path is not None:
            cmd += ["-i", str(take.audio_path), "-c:a", "aac", "-b:a", "192k"]
        cmd += [
            # Round 4c Fase 1.21 — crop bottom 60px pra remover demo playback
            # bar do CS2 Source 2 (faixa preta + scrubber + "[G] ativar
            # controle..." + "1x" label). Fase 1.19 #7 tentou 6 cvars
            # defensive Panorama (`spec_show_xray 0`, `cl_show_observer_*`,
            # etc.) mas PC test confirmou demo bar AINDA visível em t=20s,
            # 26s, 38s, 50s, 56s, 65s. Esses widgets vivem em sistema
            # Panorama separado do HUD bag controlado por cl_drawhud, sem
            # cvar conhecido pra desligar (pesquisa community indecisa).
            #
            # Solution determinística: ffmpeg crop. Demo bar fica nos últimos
            # ~40-60px do bottom. Crop 60px com pad to keep aspect 1080×1920
            # (senão Remotion OffthreadVideo precisa stretch). pad cor preta
            # = invisível no fundo escuro do reel + tem o gradient overlay
            # bottom do FragReel Remotion na cena.
            #
            # Trade-off: perde 60px de gameplay no bottom (~3% da altura
            # 1920px). Aceito vs UX broken por demo bar visível. Pad bottom
            # de 60px preto fica invisível pq Remotion adiciona gradient
            # overlay bottom (HighlightScene line ~244-249).
            #
            # Pro futuro (Fase 2+): tentar HLAE `mirv_streams add world` em
            # vez de `normal` (captura framebuffer pré-Panorama compositing)
            # OU SDK Source 2 widget unhide via reverse-engineering.
            "-vf", "crop=iw:ih-60:0:0,pad=iw:ih+60:0:0:black",
            "-c:v", "prores_ks",
            # Round 4c Fase 1.9 (PC catched 26/04 03:25): trocar profile 4444
            # → 3 (422 HQ) E pix_fmt yuva444p10le → yuv422p10le.
            #
            # Bug catched: Remotion's @remotion/renderer Rust compositor
            # panics em frame 60/765 com:
            #   "range end index 6220804 out of range for slice of length 6220800"
            #   at scalable_frame.rs:343
            # 1080×1920×3 (RGB) = 6,220,800 bytes ← slice len. +4 bytes overflow
            # = exatamente 1 sample alpha em format 4-channel. Compositor
            # tentava read 4-channel (alpha) do ProRes 4444 onde esperava
            # 3-channel RGB. Off-by-alpha → panic.
            #
            # Fix: 422 HQ é canônico pra master video sem alpha. Trade-offs:
            #   ✅ Resolve Bug Remotion compositor (3-channel safe)
            #   ✅ File 60% menor (~120 Mbps vs ~330 Mbps) → menos disk write,
            #      menos pressão no HTTP server, mais rápido convert_takes
            #   ✅ Codec canônico de master video da indústria
            #   ❌ Perde alpha channel (não usamos workflow alpha overlay
            #      sobre gameplay anyway — Remotion adiciona overlays SOBRE
            #      o gameplay decoded, não embed alpha no source)
            #   ❌ Chroma 4:2:2 vs 4:4:4 (visualmente imperceptível em
            #      material que passa por h264 CRF 18 do Remotion final)
            "-profile:v", "3",          # ProRes 422 HQ (era "4444")
            "-pix_fmt", "yuv422p10le",  # 3-channel sem alpha (era yuva444p10le)
            # Round 4c Fase 1.8 (PC catched): faststart move moov atom pro
            # início. Helps render performance + reduces seeks. Mantido.
            "-movflags", "+faststart",
            str(output_path),
        ]

        log.info(
            "ffmpeg TGA→ProRes seg=%d frames=%d → %s",
            take.segment_index, take.frame_count, output_path.name,
        )
        # Live progress: stream stderr line-by-line and parse `frame=N` so
        # the UI can show a moving bar instead of a frozen "0/3 segments".
        # ffmpeg writes BOTH progress AND errors to stderr — we keep the
        # last 200 lines for error reporting if the process fails.
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            # Fase 1.11: BELOW_NORMAL_PRIORITY (Win) + NO_WINDOW. Mantém PC
            # responsivo durante render → previne CPU thermal saturation.
            creationflags=_FFMPEG_CREATIONFLAGS,
            bufsize=1,  # line-buffered so we see progress as it happens
        )

        stderr_tail: list[str] = []
        STDERR_TAIL_MAX = 200
        last_reported = 0
        # Throttle: call the progress callback at most every N frames, or
        # on completion. Avoids flooding the lock in render_coordinator.
        REPORT_EVERY = 30

        assert proc.stderr is not None  # bufsize=1 + stderr=PIPE guarantees it
        try:
            for line in proc.stderr:
                # Cancel check (v0.2.15+): poll the caller flag on every
                # stderr line. ffmpeg emits progress lines ~10–30 Hz, which
                # caps our cancel-detection latency at ~30–100 ms. That's
                # fast enough the user doesn't notice a delay, and we kill
                # ffmpeg BEFORE it consumes more TGAs. See Bug #4 analysis:
                # previous `except BaseException` only caught signals
                # delivered to THIS thread — web-thread cancel via
                # `threading.Event` would never trigger it.
                if is_cancelled is not None and is_cancelled():
                    log.info(
                        "cancel detected mid-encode for seg=%d (frame=%d/%d) "
                        "— killing ffmpeg pid=%d",
                        take.segment_index, last_reported,
                        take.frame_count, proc.pid,
                    )
                    try:
                        proc.kill()
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        log.warning(
                            "ffmpeg pid=%d did not exit within 5s of kill",
                            proc.pid,
                        )
                    except Exception as e:
                        log.warning("ffmpeg kill raised: %s", e)
                    raise RenderCancelled(
                        f"cancelled mid-encode seg={take.segment_index} "
                        f"at frame={last_reported}/{take.frame_count}"
                    )

                stderr_tail.append(line)
                if len(stderr_tail) > STDERR_TAIL_MAX:
                    stderr_tail.pop(0)
                m = _FFMPEG_FRAME_RE.search(line)
                if m and on_frame_progress is not None:
                    n = int(m.group(1))
                    if n - last_reported >= REPORT_EVERY:
                        last_reported = n
                        try:
                            on_frame_progress(n)
                        except Exception:
                            # Progress callback errors never kill the encode.
                            log.debug("on_frame_progress callback raised", exc_info=True)
            proc.wait()
            # Final flush — report the last frame count so the bar hits
            # 100% cleanly instead of stopping at the last REPORT_EVERY step.
            if on_frame_progress is not None and take.frame_count > last_reported:
                try:
                    on_frame_progress(take.frame_count)
                except Exception:
                    log.debug("on_frame_progress final flush raised", exc_info=True)
        except RenderCancelled:
            # Already killed ffmpeg above; just propagate the clean-abort
            # signal up to the coordinator for state='cancelled' handling.
            raise
        except BaseException:
            # Interrupted (Ctrl+C, SIGTERM, direct thread signal) — kill
            # the child so we don't orphan ffmpeg holding onto GB of TGA
            # file handles. Web-thread cancels flow through RenderCancelled
            # above (v0.2.15+); this remains a safety net for the rare
            # direct-signal case (e.g. user hits Ctrl+C in a dev console).
            try:
                proc.kill()
                proc.wait(timeout=5)
            except Exception:
                pass
            raise

        if proc.returncode != 0:
            tail = "".join(stderr_tail[-30:]) if stderr_tail else "(no stderr)"
            # Decode common Windows POSIX-style negative exit codes for the
            # error message — ffmpeg returns the underlying errno cast to
            # uint32, so e.g. -28 (ENOSPC) shows up as 4294967268.
            rc = proc.returncode
            decoded = ""
            if rc > 2**31:
                signed = rc - 2**32
                if signed == -28:
                    decoded = " (ENOSPC: no space left on device — check free disk)"
                elif signed == -12:
                    decoded = " (ENOMEM: out of memory)"
                else:
                    decoded = f" (signed: {signed})"
            raise RuntimeError(
                f"ffmpeg failed for seg={take.segment_index} (exit {rc}{decoded}).\n"
                f"command: {' '.join(cmd)}\n"
                f"stderr (last 30 lines):\n{tail}"
            )

        if cleanup_tgas and take.take_dir.exists():
            try:
                size_before = sum(
                    p.stat().st_size for p in take.take_dir.rglob("*") if p.is_file()
                )
                shutil.rmtree(take.take_dir, ignore_errors=True)
                log.info(
                    "cleaned %.1f GB of TGAs at %s",
                    size_before / (1024 ** 3), take.take_dir.name,
                )
            except OSError as e:
                # Non-fatal: the .mov is already written; user just has
                # leftover files they can clean manually.
                log.warning("could not clean take dir %s: %s", take.take_dir, e)

    # -- stage 5 fallback ----------------------------------------------------

    def _probe_mov_duration(
        self,
        mov_path: Path,
        *,
        ffmpeg_path: Path | None = None,
    ) -> float:
        """Round 4c Fase 1.33 — probe duração real do .mov via ffprobe.

        Pra concats W3+W4 (cluster v2 gera 2 windows pra mesmo highlight),
        a soma dos 2 .movs é a duração real, NÃO a janela source (que
        inclui gap entre as windows não capturadas). Editor precisa
        dessa info pra cap scene_end e evitar HOLD LAST FRAME residual.

        Resolve ffprobe na mesma dir do ffmpeg (vendor/hlae/ffmpeg + ffprobe
        no mesmo bin dir, pattern ffmpeg/ffprobe na maioria das distros).

        Returns: duração em segundos (float).
        Raises: RuntimeError se ffprobe não achar binary OU exit non-zero.
        """
        ff = ffmpeg_path or self.config.resolved_ffmpeg()
        if ff is None:
            raise RuntimeError("ffmpeg/ffprobe não encontrado pra probe duration")

        # ffprobe geralmente vive ao lado do ffmpeg
        ffprobe = ff.parent / ("ffprobe.exe" if ff.suffix.lower() == ".exe" else "ffprobe")
        if not ffprobe.exists():
            raise RuntimeError(f"ffprobe binary não achado em {ffprobe}")

        cmd = [
            str(ffprobe), "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(mov_path),
        ]
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            errors="replace",
            creationflags=_NO_WINDOW,
            timeout=10,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"ffprobe exit {proc.returncode}: {proc.stderr.strip()[:200]}"
            )
        out = proc.stdout.strip()
        if not out:
            raise RuntimeError("ffprobe duration vazio")
        return float(out)

    def _concat_movs_for_highlight(
        self,
        mov_paths: list[Path],
        rank: int,
        *,
        ffmpeg_path: Path | None = None,
    ) -> Path:
        """Round 4c Fase 1.26 — concat múltiplos takes do MESMO highlight em
        UM .mov contínuo. Usado quando cluster v2 gera windows separadas
        no mesmo round (kills cluster + plant separados por gap > MERGE_GAP).
        Output mantém formato ProRes 422 HQ (lossless concat via stream
        copy quando codecs match — fast, sem re-encode).

        Args:
            mov_paths: ordered list de .mov ProRes do mesmo highlight.
            rank: rank do highlight (usado pra naming output).
            ffmpeg_path: opcional; defaults pra config.resolved_ffmpeg().

        Returns: path do .mov concatenado (no mesmo dir do primeiro input).
        """
        if not mov_paths:
            raise ValueError("no movs to concat for highlight")
        if len(mov_paths) == 1:
            return mov_paths[0]  # nada a concatenar

        ff = ffmpeg_path or self.config.resolved_ffmpeg()
        if ff is None:
            raise RuntimeError(
                "ffmpeg not found pra concat takes do highlight"
            )

        # Output ao lado do primeiro take, naming explícito
        first = mov_paths[0]
        output = first.parent / f"highlight_rank{rank}_concat.mov"

        # Concat list file
        list_file = output.with_suffix(".concat.txt")
        with list_file.open("w", encoding="utf-8") as fh:
            for mov in mov_paths:
                escaped = str(mov).replace("\\", "/").replace("'", "'\\''")
                fh.write(f"file '{escaped}'\n")

        # Concat demuxer + stream copy (no re-encode — fast, lossless).
        # Funciona pra .movs com codecs idênticos (são todos prores_ks 422 HQ
        # gerados pelo mesmo _convert_one_take). Falha se headers divergem;
        # fallback raro = re-encode (não implementado, error explícito).
        cmd = [
            str(ff), "-y",
            "-f", "concat", "-safe", "0",
            "-i", str(list_file),
            "-c", "copy",
            "-movflags", "+faststart",
            str(output),
        ]
        log.info(
            "concat highlight rank=%d: %d takes → %s",
            rank, len(mov_paths), output.name,
        )
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            errors="replace",
            creationflags=_FFMPEG_CREATIONFLAGS,
        )
        try:
            list_file.unlink()
        except OSError:
            pass
        if proc.returncode != 0:
            tail = "\n".join((proc.stderr or "").splitlines()[-30:])
            raise RuntimeError(
                f"ffmpeg concat highlight rank={rank} failed (exit {proc.returncode}). "
                f"stderr tail:\n{tail}"
            )
        return output

    def concat_movs_to_mp4(
        self,
        mov_paths: list[Path],
        output_mp4: Path,
        *,
        ffmpeg: Path | None = None,
        cleanup_movs: bool = True,
    ) -> Path:
        """Concatenate ProRes .mov segments into a single h264 MP4.

        Used when Remotion isn't available (e.g. inside the PyInstaller
        .exe, where the editor/ folder isn't bundled). Without this
        fallback the user is left with ProRes 4444 .mov files whose
        codec_tag "ap4h" Windows Media Player refuses to decode (the
        "formato não suportado" symptom from v0.2.8 testing).

        We re-encode to h264 + yuv420p + aac so the result plays in
        every default OS player, social platforms, and Discord.

        Args:
            mov_paths: ordered list of .mov files (one per highlight).
                Empty list raises.
            output_mp4: destination path. Parent dir is created.
            cleanup_movs: delete the input .mov files after success.
                The user only needs the final MP4; the intermediate
                ProRes files are 5–10 GB each and serve no purpose
                once the MP4 is on disk. Survival of the source files
                on failure is guaranteed (cleanup runs only after
                ffmpeg returns 0).

        Returns the MP4 path on success. Raises RuntimeError otherwise.
        """
        if not mov_paths:
            raise ValueError("no .mov inputs to concat")

        ff = ffmpeg or self.config.resolved_ffmpeg()
        if ff is None:
            raise RuntimeError(
                "ffmpeg not found (checked --ffmpeg-exe, bundled, and $PATH)"
            )

        output_mp4.parent.mkdir(parents=True, exist_ok=True)

        # ffmpeg's concat *demuxer* needs a list file. We write it next
        # to the MP4 output so any debugging artifact is co-located.
        # Path quoting follows ffmpeg's concat protocol — single quotes
        # around the path, with embedded single-quotes escaped as
        # `'\''`. Windows backslashes need to become forward slashes
        # so ffmpeg doesn't choke on the escape sequences.
        list_file = output_mp4.with_suffix(".concat.txt")
        with list_file.open("w", encoding="utf-8") as fh:
            for mov in mov_paths:
                escaped = str(mov).replace("\\", "/").replace("'", "'\\''")
                fh.write(f"file '{escaped}'\n")

        cmd: list[str] = [
            str(ff),
            "-y",
            # Fase 1.11: thermal cap — mesma motivação do prores_ks pass.
            "-threads", _FFMPEG_THREAD_LIMIT,
            "-f", "concat",
            "-safe", "0",
            "-i", str(list_file),
            # h264 high profile + yuv420p = playable in WMP, QuickTime,
            # Discord, browsers.
            # Fase 1.11 (Mathieu reportou MP4 final 180MB): CRF 18 + preset fast
            # produzia bitrate ~16Mbps em 1080p → 180MB pra reel de 90s. Reels/
            # TikTok comprimem agressivamente upload → CRF 23 + preset medium
            # gera ~3Mbps (~35MB pra 90s) sem perda visual perceptível depois
            # do re-encode da plataforma. Sweet spot entre tamanho compartilhável
            # (Discord 25MB free / 50MB Nitro / WhatsApp 100MB) e qualidade.
            "-c:v", "libx264",
            "-preset", "medium",   # era "fast" — medium dá ~30% melhor compressão
            "-crf", "23",          # era 18 — 23 é "high quality" web standard
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-c:a", "aac",
            "-b:a", "128k",        # era 192k — 128k AAC é transparente pra game audio
            str(output_mp4),
        ]

        log.info(
            "ffmpeg concat: %d .mov(s) → %s",
            len(mov_paths), output_mp4.name,
        )
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            errors="replace",
            # Fase 1.11: BELOW_NORMAL_PRIORITY (Win) — h264 encode é CPU-heavy.
            creationflags=_FFMPEG_CREATIONFLAGS,
        )
        # Always remove the list file — it's just a manifest, not user data.
        try:
            list_file.unlink()
        except OSError:
            pass

        if proc.returncode != 0:
            tail = "\n".join(proc.stderr.splitlines()[-30:]) if proc.stderr else "(no stderr)"
            raise RuntimeError(
                f"ffmpeg concat failed (exit {proc.returncode}).\n"
                f"command: {' '.join(cmd)}\n"
                f"stderr (last 30 lines):\n{tail}"
            )

        if cleanup_movs:
            freed_bytes = 0
            for mov in mov_paths:
                try:
                    freed_bytes += mov.stat().st_size
                    mov.unlink()
                except OSError as e:
                    log.warning("could not delete %s: %s", mov, e)
            log.info(
                "freed %.1f GB of intermediate ProRes .mov(s)",
                freed_bytes / (1024 ** 3),
            )

        return output_mp4


# ---------------------------------------------------------------------------
# CLI (for manual testing / runner invocation)
# ---------------------------------------------------------------------------


def _load_plan(path: Path) -> RenderPlan:
    return RenderPlan.from_json(json.loads(path.read_text(encoding="utf-8")))


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    ap = argparse.ArgumentParser(description="FragReel HLAE runner")
    ap.add_argument("--plan", type=Path, required=True, help="Path to render plan JSON")
    ap.add_argument(
        "--cs2-install",
        type=Path,
        default=Path(r"C:\Program Files (x86)\Steam\steamapps\common\Counter-Strike Global Offensive"),
    )
    ap.add_argument(
        "--hlae-dir",
        type=Path,
        default=Path(__file__).parent / "vendor" / "hlae",
    )
    ap.add_argument(
        "--output-mov-dir",
        type=Path,
        default=None,
        help="If set, convert all takes to ProRes .mov files under this dir "
             "(<dir>/<basename>_seg00.mov, _seg01.mov, ...)",
    )
    ap.add_argument(
        "--mov-basename",
        default=None,
        help="Basename for output .mov files (default: demo basename)",
    )
    ap.add_argument(
        "--stage",
        choices=("cfg", "launch", "wait", "convert", "remotion", "all"),
        default="all",
    )
    ap.add_argument(
        "--editor-dir",
        type=Path,
        default=Path(__file__).parent.parent / "main" / "editor",
        help="Path to FragReel editor (the main repo's editor/ subdir)",
    )
    ap.add_argument(
        "--output-mp4",
        type=Path,
        default=None,
        help="If set + Remotion available, render final MP4 here from the .mov",
    )
    ap.add_argument(
        "--composition",
        default="HighlightsReel",
        help="Remotion composition id (default: HighlightsReel)",
    )
    ap.add_argument(
        "--strategy",
        choices=tuple(s.value for s in LaunchStrategy),
        default=LaunchStrategy.INJECT.value,
        help="How to launch CS2 (default: inject, headless)",
    )
    ap.add_argument("--timeout", type=float, default=600.0)
    ap.add_argument(
        "--keep-cs2",
        action="store_true",
        help="Leave CS2 running after capture (default terminates it)",
    )
    args = ap.parse_args()

    plan = _load_plan(args.plan)
    config = HlaeRunnerConfig(cs2_install=args.cs2_install, hlae_dir=args.hlae_dir)
    runner = HlaeRunner(config)
    strategy = LaunchStrategy(args.strategy)

    if args.stage in ("cfg", "all"):
        written = runner.stage_capture_cfg(plan)
        log.info("cfg written: %s", written)

    # Snapshot take index BEFORE launch so wait_for_capture ignores old takes.
    pre_take = runner._snapshot_take_index(config.recording_parent / plan.record_name)

    if args.stage in ("launch", "all"):
        runner.launch_cs2(plan, strategy=strategy)

    if args.stage in ("wait", "all"):
        def _progress(now: int, prev: int) -> None:
            if now != prev:
                log.info("captured frames: %d", now)
        try:
            result = runner.wait_for_capture(
                plan,
                pre_take_index=pre_take,
                timeout_sec=args.timeout,
                on_progress=_progress,
            )
            log.info(
                "capture done: %d takes, total frames=%d",
                len(result.takes),
                result.total_frames,
            )
            for t in result.takes:
                log.info(
                    "  seg=%d take=%s frames=%d audio=%s",
                    t.segment_index, t.take_dir.name, t.frame_count, t.audio_path,
                )

            if args.stage in ("convert", "remotion", "all") and args.output_mov_dir:
                basename = args.mov_basename or plan.demo_basename
                result = runner.convert_takes_to_prores(
                    result, args.output_mov_dir, basename=basename,
                )
                for t in result.takes:
                    log.info("ProRes written: %s", t.mov_path)

            if args.stage in ("remotion", "all") and args.output_mp4:
                if not any(t.mov_path for t in result.takes):
                    log.warning("no .mov outputs available; skipping Remotion stage")
                else:
                    mp4 = runner.render_remotion(
                        result=result,
                        output_mp4=args.output_mp4,
                        editor_dir=args.editor_dir,
                        composition=args.composition,
                    )
                    log.info("final MP4: %s", mp4)
        finally:
            if not args.keep_cs2 and strategy is LaunchStrategy.INJECT:
                runner.terminate_cs2()
