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

    @classmethod
    def from_json(cls, payload: dict) -> "RenderPlan":
        return cls(
            demo_path=Path(payload["demo_path"]),
            segments=tuple((int(s["start_tick"]), int(s["end_tick"])) for s in payload["segments"]),
            user_steamid64=payload.get("user_steamid64"),
            user_player_name=payload.get("user_player_name"),
            record_name=payload.get("record_name", "fragreel"),
            stream_name=payload.get("stream_name", "default"),
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
        npm_exe: str = "npx",
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
            # Selected highlights are the ones the segments map to. Editor
            # filters by selectedRanks then sorts by rank ASC; we replicate
            # that here so segment_index → highlight index is unambiguous.
            selected_ranks = merged.get("selectedRanks") or [h.get("rank") for h in highlights]
            selected_ranks_set = set(selected_ranks)
            ordered_selected = sorted(
                (h for h in highlights if h.get("rank") in selected_ranks_set),
                key=lambda h: h.get("rank", 0),
            )

            for take in result.takes:
                if take.mov_path is None:
                    log.warning(
                        "segment %d has no mov_path — Remotion will fall back "
                        "to placeholder gradient", take.segment_index,
                    )
                    continue
                if take.segment_index >= len(ordered_selected):
                    log.warning(
                        "segment %d has no matching highlight (only %d selected)",
                        take.segment_index, len(ordered_selected),
                    )
                    continue
                target_rank = ordered_selected[take.segment_index].get("rank")
                # file:// URI works on Windows (file:///C:/...) and POSIX.
                src_uri = take.mov_path.absolute().as_uri()
                for h in highlights:
                    if h.get("rank") == target_rank:
                        h["gameplayVideoSrc"] = src_uri
                        break

            match["highlights"] = highlights
            merged["match"] = match

        cmd: list[str] = [
            npm_exe,
            "remotion",
            "render",
            composition,
            str(output_mp4),
            "--props",
            json.dumps(merged),
        ]
        log.info(
            "remotion render: composition=%s out=%s takes_with_mov=%d",
            composition, output_mp4,
            sum(1 for t in result.takes if t.mov_path is not None),
        )
        output_mp4.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            cmd,
            cwd=editor_dir,
            check=True,
            shell=False,
            creationflags=_NO_WINDOW,
        )
        return output_mp4

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
            "-framerate", str(source_framerate),
            "-i", input_pattern,
        ]
        if take.audio_path is not None:
            cmd += ["-i", str(take.audio_path), "-c:a", "aac", "-b:a", "192k"]
        cmd += [
            "-c:v", "prores_ks",
            "-profile:v", "4444",
            "-pix_fmt", "yuva444p10le",
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
            creationflags=_NO_WINDOW,
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
            "-f", "concat",
            "-safe", "0",
            "-i", str(list_file),
            # h264 high profile + yuv420p = playable in WMP, QuickTime,
            # Discord, browsers. CRF 18 is "visually lossless" for the
            # 1080p ProRes source we just produced; preset fast keeps
            # encode time under ~30s for a typical 4-segment reel.
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "18",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-c:a", "aac",
            "-b:a", "192k",
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
            creationflags=_NO_WINDOW,
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
