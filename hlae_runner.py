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

import json
import logging
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable

from cs2_launcher import InjectedProcess, get_desktop_resolution, launch_cs2_injected
from scripts.capture_script import (
    CaptureSegment,
    generate_capture_cfg,
    steamid64_to_account_id,
)


log = logging.getLogger(__name__)


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
    """

    demo_path: Path
    segments: tuple[tuple[int, int], ...]
    user_steamid64: str | None = None
    record_name: str = "fragreel"
    stream_name: str = "default"

    @classmethod
    def from_json(cls, payload: dict) -> "RenderPlan":
        return cls(
            demo_path=Path(payload["demo_path"]),
            segments=tuple((int(s["start_tick"]), int(s["end_tick"])) for s in payload["segments"]),
            user_steamid64=payload.get("user_steamid64"),
            record_name=payload.get("record_name", "fragreel"),
            stream_name=payload.get("stream_name", "default"),
        )

    @property
    def demo_basename(self) -> str:
        """The name used in `playdemo replays/<basename>` (without extension)."""
        return self.demo_path.stem


@dataclass(frozen=True)
class CaptureResult:
    """What the HLAE capture produced on disk."""

    take_dir: Path  # <CS2>/game/bin/win64/<record_name>/takeNNNN
    stream_dir: Path  # take_dir / <stream_name> — contains TGAs
    frame_count: int
    audio_path: Path | None  # take_dir / audio.wav


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
        return [
            "-insecure",
            "-novid",
            "-windowed",
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
    ) -> CaptureResult:
        """Poll the recording dir for a new, stable take.

        Strategy: find the highest `takeNNNN` index under
        `<CS2>/game/bin/win64/<record_name>/`. Capture is done when that
        directory has at least one TGA AND its file count is stable across
        two polls (HLAE has stopped writing).

        `pre_take_index` = highest index seen BEFORE launching, so we can
        skip leftovers from previous captures. If None, `_snapshot_take_index`
        is called at the start of this method.
        """
        record_root = self.config.recording_parent / plan.record_name
        if pre_take_index is None:
            pre_take_index = self._snapshot_take_index(record_root)

        deadline = time.monotonic() + timeout_sec
        last_frames = -1
        stable_since: float | None = None

        while time.monotonic() < deadline:
            take_dir = self._find_latest_take(record_root, skip_up_to=pre_take_index)
            if take_dir is not None:
                stream_dir = take_dir / plan.stream_name
                if stream_dir.is_dir():
                    frames = self._count_tgas(stream_dir)
                    if on_progress is not None:
                        on_progress(frames, last_frames)
                    if frames > 0 and frames == last_frames:
                        stable_since = stable_since or time.monotonic()
                        if time.monotonic() - stable_since >= poll_sec * 2:
                            audio = take_dir / "audio.wav"
                            return CaptureResult(
                                take_dir=take_dir,
                                stream_dir=stream_dir,
                                frame_count=frames,
                                audio_path=audio if audio.exists() else None,
                            )
                    else:
                        stable_since = None
                    last_frames = frames
            time.sleep(poll_sec)

        raise TimeoutError(f"capture did not stabilize within {timeout_sec}s under {record_root}")

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
    def _find_latest_take(record_root: Path, *, skip_up_to: int) -> Path | None:
        """Return the take dir with index > skip_up_to, highest first."""
        if not record_root.is_dir():
            return None
        best: tuple[int, Path] | None = None
        for child in record_root.iterdir():
            if child.is_dir() and child.name.startswith("take"):
                try:
                    n = int(child.name[4:])
                except ValueError:
                    continue
                if n > skip_up_to and (best is None or n > best[0]):
                    best = (n, child)
        return best[1] if best else None

    @staticmethod
    def _count_tgas(stream_dir: Path) -> int:
        return sum(1 for p in stream_dir.iterdir() if p.suffix.lower() == ".tga")

    # -- stage 4 ------------------------------------------------------------

    def render_remotion(
        self,
        gameplay_mov: Path,
        output_mp4: Path,
        editor_dir: Path,
        composition: str = "HighlightsReel",
        props: dict | None = None,
        *,
        npm_exe: str = "npx",
    ) -> Path:
        """Run `npx remotion render` against the editor repo.

        The `editor_dir` is the `editor/` subdirectory of the main FragReel
        repo (not the client). Props typically include `gameplayVideoSrc`
        (pointing at `gameplay_mov`), the highlights array, kills, mood,
        music track, orientation. The server's `/matches/{id}/render-plan`
        endpoint is the source of truth for this shape.

        Raises on ffmpeg/remotion errors. Returns the output MP4 path.
        """
        if not editor_dir.is_dir():
            raise FileNotFoundError(f"editor dir not found: {editor_dir}")
        if not gameplay_mov.exists():
            raise FileNotFoundError(f"gameplay .mov not found: {gameplay_mov}")

        # Remotion reads props via --props '<json>' (single JSON arg). We
        # merge the caller's props with the gameplay video path it almost
        # certainly needs but might forget to set.
        merged: dict = {"gameplayVideoSrc": str(gameplay_mov)}
        if props:
            merged.update(props)

        cmd: list[str] = [
            npm_exe,
            "remotion",
            "render",
            composition,
            str(output_mp4),
            "--props",
            json.dumps(merged),
        ]
        log.info("remotion render: %s", " ".join(cmd))
        output_mp4.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(cmd, cwd=editor_dir, check=True, shell=False)
        return output_mp4

    def convert_tga_to_prores(
        self,
        result: CaptureResult,
        output_path: Path,
        *,
        source_framerate: int = 300,
        cleanup_tgas: bool = True,
    ) -> Path:
        """ffmpeg: TGA sequence + audio → ProRes 4444 .mov for Remotion.

        Per Edicao_FragReel_Best_Practices: ProRes 4444 preserves alpha and
        decodes fast in the Chromium build Remotion uses. Source framerate
        must match `host_framerate` that captured the TGAs.

        `cleanup_tgas=True` (default): once ffmpeg succeeds, wipes the
        take directory. Without this, each render leaves 5–10 GB of
        single-use TGAs on disk (1920×1080 × ~1400 frames ≈ 8 GB).
        """
        ffmpeg = self.config.resolved_ffmpeg()
        if ffmpeg is None:
            raise RuntimeError(
                "ffmpeg not found (checked --ffmpeg-exe, bundled, and $PATH)"
            )
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Numbered TGA pattern (HLAE writes 00000.tga, 00001.tga, ...)
        input_pattern = str(result.stream_dir / "%05d.tga")

        cmd: list[str] = [
            str(ffmpeg),
            "-y",
            "-framerate", str(source_framerate),
            "-i", input_pattern,
        ]
        if result.audio_path is not None:
            cmd += ["-i", str(result.audio_path), "-c:a", "aac", "-b:a", "192k"]
        cmd += [
            "-c:v", "prores_ks",
            "-profile:v", "4444",
            "-pix_fmt", "yuva444p10le",
            str(output_path),
        ]

        log.info("ffmpeg TGA→ProRes: %s", " ".join(cmd))
        subprocess.run(cmd, check=True)

        if cleanup_tgas and result.take_dir.exists():
            import shutil as _shutil
            try:
                size_before = sum(p.stat().st_size for p in result.take_dir.rglob("*") if p.is_file())
                _shutil.rmtree(result.take_dir, ignore_errors=True)
                log.info(
                    "cleaned up %.1f GB of source TGAs at %s",
                    size_before / (1024 ** 3), result.take_dir.name,
                )
            except OSError as e:
                # Non-fatal: the .mov is already written, user just has
                # leftover files to clean manually.
                log.warning("could not clean take dir %s: %s", result.take_dir, e)

        return output_path


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
        "--output-mov",
        type=Path,
        default=None,
        help="If set, convert captured TGAs to ProRes .mov at this path",
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
                "capture done: take=%s frames=%d audio=%s",
                result.take_dir.name,
                result.frame_count,
                result.audio_path,
            )
            mov_path: Path | None = None
            if args.stage in ("convert", "remotion", "all") and args.output_mov:
                mov_path = runner.convert_tga_to_prores(result, args.output_mov)
                log.info("ProRes written: %s", mov_path)

            if args.stage in ("remotion", "all") and args.output_mp4 and mov_path:
                mp4 = runner.render_remotion(
                    gameplay_mov=mov_path,
                    output_mp4=args.output_mp4,
                    editor_dir=args.editor_dir,
                    composition=args.composition,
                )
                log.info("final MP4: %s", mp4)
        finally:
            if not args.keep_cs2 and strategy is LaunchStrategy.INJECT:
                runner.terminate_cs2()
