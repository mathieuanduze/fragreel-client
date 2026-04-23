"""Background render coordinator — used by the local HTTP API.

When the website clicks "Generate", the web client POSTs to `/render` on the
Flask local_api. That handler calls `RenderCoordinator.start(plan)`, which
pushes the whole pipeline onto a background thread and returns immediately.

The web client then polls `GET /render/status` (or subscribes to it — the
existing AdModal already polls `serverStatus`) to get progress updates
while the user watches ads. Zero terminal interaction from the user, and
CS2 runs minimized in the background.

Lifecycle of a render session:

    state='idle'
        ↓ start(plan)
    state='staging'      // writing capture.cfg
        ↓
    state='launching'    // kill old CS2, inject hook, CS2 starts minimized
        ↓
    state='capturing'    // mirv_streams running inside CS2
                         // progress 0.05 → 0.80 as frames grow
        ↓
    state='converting'   // ffmpeg TGA sequence → ProRes .mov (0.80 → 0.92)
        ↓
    state='rendering'    // Remotion composes final .mp4 (0.92 → 0.99)
        ↓
    state='done'         // output_mp4 populated, progress 1.0

If any stage throws, state becomes 'error' with `error` holding the message.
`cancel()` terminates CS2 and sets state='cancelled'.
"""

from __future__ import annotations

import logging
import shutil
import threading
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from cs2_launcher import InjectedProcess, find_running_cs2_pids, kill_running_cs2
from hlae_runner import HlaeRunner, HlaeRunnerConfig, LaunchStrategy, RenderPlan, TakeOutput


log = logging.getLogger(__name__)


# Progress budget per stage — sum must be ~= 1.0.
PROGRESS_STAGING = 0.03
PROGRESS_LAUNCHING = 0.04
PROGRESS_CAPTURING = 0.70  # the bulk of the work
PROGRESS_CONVERTING = 0.12
PROGRESS_RENDERING = 0.10
PROGRESS_DONE = 1.00

# For capturing, we need a total-frame estimate to map frames → progress.
# At host_framerate=120 and tickrate=64, each tick ≈ 120/64 = 1.875 frames.
# IMPORTANT: must match DEFAULT_HOST_FRAMERATE in capture_script.py — wrong
# value here only affects progress %, not the capture itself, but a 4x
# mismatch makes the bar stay at 24% forever (the v0.2.3 → 0.2.4 lesson).
CS2_TICKRATE = 64
CAPTURE_FPS = 120
FRAMES_PER_TICK = CAPTURE_FPS / CS2_TICKRATE

# Wall-clock cap for the capture stage. Even on a modest PC, capturing 4
# segments at 120 fps + ProRes-bound TGA writes runs ~25 min for a typical
# 4-highlight reel. 60 min gives 2x margin. v0.2.3 had 15 min, which cut
# off mid-segment-0 on PCs that couldn't sustain >300 fps wall-clock.
CAPTURE_TIMEOUT_SEC = 3600.0

# Disk preflight: 1080p TGA from CS2 ≈ 6.2 MB / frame. Round to 7 MB for
# safety + filesystem overhead. With v0.2.6 streaming convert, peak TGA
# usage = the SINGLE largest segment plus the next segment being captured
# while the previous one converts. We size for ~2 segments worth of TGAs
# + 5 GB safety buffer (engine logs, audio.wav, ProRes scratch).
BYTES_PER_FRAME_TGA = 7_000_000
# ProRes 4444 1080p ≈ 105 Mbps = ~110 KB/frame at 120 fps. Round up.
BYTES_PER_FRAME_PRORES = 200_000
DISK_SAFETY_BUFFER_BYTES = 5 * 1024 ** 3  # 5 GB


class InsufficientDiskError(RuntimeError):
    """Not enough free disk on either the CS2 capture drive or the output drive."""

    def __init__(self, issues: list[dict]):
        self.issues = issues
        msg = "; ".join(
            f"{i['kind']} on {i['drive']}: need ~{i['needed_gb']:.1f} GB, have {i['free_gb']:.1f} GB"
            for i in issues
        )
        super().__init__(f"insufficient disk: {msg}")


@dataclass
class RenderSession:
    """Mutable state surfaced to the web client via /render/status."""

    render_id: str
    state: str = "idle"
    stage: str = "waiting"
    progress: float = 0.0
    frames_captured: int = 0
    frames_expected: int = 0
    segments_total: int = 0
    segments_done: int = 0  # how many takes have a finished .mov
    output_movs: tuple[Path, ...] = ()  # one per segment
    output_mp4: Path | None = None
    error: str | None = None
    started_at: float | None = None
    finished_at: float | None = None

    def to_dict(self) -> dict:
        return {
            "render_id": self.render_id,
            "state": self.state,
            "stage": self.stage,
            "progress": round(self.progress, 3),
            "frames_captured": self.frames_captured,
            "frames_expected": self.frames_expected,
            "segments_total": self.segments_total,
            "segments_done": self.segments_done,
            "output_movs": [str(p) for p in self.output_movs],
            "output_mp4": str(self.output_mp4) if self.output_mp4 else None,
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }


class RenderCoordinator:
    """Singleton-ish coordinator: one render at a time.

    The UX model is one-render-at-a-time per user (their PC, their CS2
    instance). Trying to start a second render while one is running
    returns the existing session unchanged.
    """

    def __init__(
        self,
        config: HlaeRunnerConfig,
        *,
        output_dir: Path,
        editor_dir: Path | None = None,
    ):
        self.config = config
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.editor_dir = editor_dir

        self._session: RenderSession | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._cancel_requested = threading.Event()
        self._runner: HlaeRunner | None = None

    # -- public API ---------------------------------------------------------

    def current(self) -> RenderSession | None:
        with self._lock:
            return self._session

    class CS2BusyError(RuntimeError):
        """Raised when the user has a live CS2 session we shouldn't kill."""

        def __init__(self, pids: list[int]):
            super().__init__(f"CS2 already running (pid={pids})")
            self.pids = pids

    def preflight(self) -> dict:
        """Check whether a render would succeed right now. Web calls this
        before showing the ad to avoid wasting the user's ad-watch on a
        render that'll be refused."""
        pids = find_running_cs2_pids()
        if pids:
            return {"ready": False, "reason": "cs2_running", "cs2_pids": pids}
        current = self.current()
        if current and current.state not in ("idle", "done", "error", "cancelled"):
            return {"ready": False, "reason": "render_in_progress",
                    "render_id": current.render_id}
        return {"ready": True}

    def preflight_disk(self, plan: RenderPlan) -> dict:
        """Estimate disk needed for `plan` and compare against free space.

        v0.2.5 produced an ENOSPC mid-conversion (`ffmpeg exit 4294967268`
        = -28) when the user had 3.4 GB free during a 2-segment 120-fps
        capture. v0.2.6 streaming-converts each take as soon as it
        finalizes and deletes its TGAs, so peak usage = the largest
        segment + ~2 GB scratch instead of sum of all segments.

        Returns:
            {ok: bool, issues: [{drive, needed_gb, free_gb, kind}], ...}
            ok=True means the render should fit. issues lists every drive
            that's short, with a precise "needed" estimate.
        """
        if not plan.segments:
            return {"ok": True, "issues": [], "note": "no segments"}

        seg_ticks = [e - s for s, e in plan.segments]
        max_seg_frames = int(max(seg_ticks) * FRAMES_PER_TICK)
        total_frames = int(sum(seg_ticks) * FRAMES_PER_TICK)

        # CS2 drive: streaming convert means peak = 1 active segment being
        # captured + at most 1 segment queued for conversion (synchronous
        # callback blocks polling but ffmpeg is fast enough vs CS2 capture
        # at host_framerate=120 that this rarely overlaps). Size for 2
        # segments worth as a safety margin.
        peak_tga_bytes = 2 * max_seg_frames * BYTES_PER_FRAME_TGA + DISK_SAFETY_BUFFER_BYTES
        # Output drive: all ProRes .mov files land here + the final MP4.
        # MP4 is much smaller (~30 MB) so we just round into the buffer.
        output_bytes = total_frames * BYTES_PER_FRAME_PRORES + DISK_SAFETY_BUFFER_BYTES

        cs2_drive = self.config.recording_parent
        try:
            cs2_drive.mkdir(parents=True, exist_ok=True)
            cs2_free = shutil.disk_usage(cs2_drive).free
        except OSError:
            cs2_free = 0
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            output_free = shutil.disk_usage(self.output_dir).free
        except OSError:
            output_free = 0

        issues: list[dict] = []
        if cs2_free < peak_tga_bytes:
            issues.append({
                "drive": str(cs2_drive),
                "needed_gb": peak_tga_bytes / 1e9,
                "free_gb": cs2_free / 1e9,
                "kind": "tga_capture",
            })
        # Skip the output check if the same drive is already in `issues`
        # — same physical disk, same problem.
        same_drive = (
            cs2_free
            and cs2_free == output_free
            and str(cs2_drive)[:3].lower() == str(self.output_dir)[:3].lower()
        )
        if not same_drive and output_free < output_bytes:
            issues.append({
                "drive": str(self.output_dir),
                "needed_gb": output_bytes / 1e9,
                "free_gb": output_free / 1e9,
                "kind": "prores_output",
            })

        return {
            "ok": not issues,
            "issues": issues,
            "estimated_total_frames": total_frames,
            "max_segment_frames": max_seg_frames,
            "peak_tga_gb": peak_tga_bytes / 1e9,
            "output_gb": output_bytes / 1e9,
        }

    def start(
        self,
        plan: RenderPlan,
        render_id: str,
        *,
        force_kill_cs2: bool = False,
        reel_props: dict | None = None,
    ) -> RenderSession:
        """Start a render if one isn't already active. Returns the session.

        Refuses with `CS2BusyError` if the user's CS2 is running and
        `force_kill_cs2` is False — killing a live game would lose their
        match. Web surfaces this as a "Close CS2 to render" prompt.
        """
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                log.info("render already running; returning existing session")
                assert self._session is not None
                return self._session

            pids = find_running_cs2_pids()
            if pids:
                if force_kill_cs2:
                    log.warning("force_kill_cs2=True — terminating pids %s", pids)
                    kill_running_cs2()
                else:
                    raise self.CS2BusyError(pids)

            # Disk preflight — fail fast with actionable info instead of
            # crashing mid-conversion with ENOSPC (the v0.2.5 failure mode).
            disk = self.preflight_disk(plan)
            if not disk["ok"]:
                log.error("disk preflight failed: %s", disk["issues"])
                raise InsufficientDiskError(disk["issues"])

            self._cancel_requested.clear()
            self._session = RenderSession(
                render_id=render_id,
                state="staging",
                stage="preparing capture script",
                started_at=time.time(),
                frames_expected=self._estimate_frames(plan),
                segments_total=len(plan.segments),
            )
            self._thread = threading.Thread(
                target=self._run,
                args=(plan, reel_props),
                daemon=True,
                name=f"fragreel-render-{render_id}",
            )
            self._thread.start()
            return self._session

    def cancel(self) -> None:
        """Abort the current render — kill CS2 and mark state='cancelled'."""
        self._cancel_requested.set()
        kill_running_cs2()
        with self._lock:
            if self._session and self._session.state not in ("done", "error", "cancelled"):
                self._session.state = "cancelled"
                self._session.stage = "cancelled by user"
                self._session.finished_at = time.time()

    # -- stage runner -------------------------------------------------------

    def _run(self, plan: RenderPlan, reel_props: dict | None = None) -> None:
        try:
            self._runner = HlaeRunner(self.config)

            # Stage 1: write capture.cfg
            self._update(state="staging", stage="writing capture.cfg", progress=0.01)
            self._runner.stage_capture_cfg(plan)
            self._update(progress=PROGRESS_STAGING)

            if self._cancel_requested.is_set():
                return self._mark_cancelled()

            # Snapshot existing takes so wait_for_capture ignores them
            record_root = self.config.recording_parent / plan.record_name
            pre_take = HlaeRunner._snapshot_take_index(record_root)

            # Stage 2: launch CS2 injected + minimized
            self._update(
                state="launching",
                stage="starting CS2 (minimized)",
                progress=PROGRESS_STAGING + PROGRESS_LAUNCHING / 2,
            )
            self._runner.launch_cs2(plan, strategy=LaunchStrategy.INJECT)
            self._update(progress=PROGRESS_STAGING + PROGRESS_LAUNCHING)

            if self._cancel_requested.is_set():
                return self._mark_cancelled()

            # Stage 3+4 fused (v0.2.6+): wait for capture WHILE converting
            # each take inline as soon as it finalizes. Caps peak disk at
            # ~1 segment of TGAs instead of N segments. The previous flow
            # (wait-all-then-convert-all) blew up with ENOSPC when the
            # user had < ~50 GB free for a 2-segment 120 fps capture.
            self._update(state="capturing", stage="capturing gameplay")

            mov_dir = self.output_dir / plan.demo_basename
            mov_dir.mkdir(parents=True, exist_ok=True)
            converted_movs: dict[int, Path] = {}

            def on_progress(now: int, prev: int) -> None:
                if self._cancel_requested.is_set():
                    return
                with self._lock:
                    if self._session is None:
                        return
                    self._session.frames_captured = now
                    if self._session.frames_expected > 0:
                        frac = min(1.0, now / self._session.frames_expected)
                    else:
                        frac = 0.5
                    # Capture+convert share the 0.07–0.92 budget. Convert
                    # gets a small slice because it overlaps capture.
                    self._session.progress = (
                        PROGRESS_STAGING
                        + PROGRESS_LAUNCHING
                        + (PROGRESS_CAPTURING + PROGRESS_CONVERTING) * frac
                    )

            def on_take_finalized(take: TakeOutput) -> None:
                """Stream-convert this take immediately so its TGAs free
                up while CS2 keeps capturing the next segment."""
                if self._cancel_requested.is_set():
                    return
                mov_path = mov_dir / f"{plan.demo_basename}_seg{take.segment_index:02d}.mov"
                log.info(
                    "stream-convert seg=%d frames=%d → %s",
                    take.segment_index, take.frame_count, mov_path.name,
                )
                self._update(
                    state="converting",
                    stage=f"encoding seg{take.segment_index:02d} ({take.frame_count} frames)",
                )
                self._runner.convert_one_take(
                    take=take,
                    output_path=mov_path,
                    cleanup_tgas=True,
                )
                converted_movs[take.segment_index] = mov_path
                with self._lock:
                    if self._session:
                        self._session.segments_done = len(converted_movs)
                        self._session.output_movs = tuple(
                            converted_movs[k] for k in sorted(converted_movs)
                        )
                # Resume capturing-state label for the next polling cycle.
                self._update(state="capturing", stage="capturing gameplay")

            try:
                result = self._runner.wait_for_capture(
                    plan,
                    pre_take_index=pre_take,
                    timeout_sec=CAPTURE_TIMEOUT_SEC,
                    on_progress=on_progress,
                    on_take_finalized=on_take_finalized,
                )
            except RuntimeError as e:
                # Streaming-convert error during capture (ffmpeg failure).
                # Surface it instead of swallowing — the partial output
                # in mov_dir lets us debug, but the reel is incomplete.
                log.error("stream-convert failed mid-capture: %s", e)
                raise

            # Backfill mov_paths into the result dataclass.
            result = result.with_movs(converted_movs)

            self._update(
                progress=PROGRESS_STAGING + PROGRESS_LAUNCHING + PROGRESS_CAPTURING + PROGRESS_CONVERTING,
                stage=f"captured + encoded {result.total_frames} frames across {len(result.takes)} segment(s)",
            )

            # Always close CS2 once capture is done — the user shouldn't have
            # to clean up, and leaving CS2 running prevents future renders.
            self._runner.terminate_cs2()

            if self._cancel_requested.is_set():
                return self._mark_cancelled()

            if self._cancel_requested.is_set():
                return self._mark_cancelled()

            # Stage 5: Remotion composition (final .mp4)
            if self.editor_dir is not None and self.editor_dir.is_dir():
                mp4_path = self.output_dir / f"{plan.demo_basename}.mp4"
                self._update(state="rendering", stage="composing final MP4")
                try:
                    self._runner.render_remotion(
                        result=result,
                        output_mp4=mp4_path,
                        editor_dir=self.editor_dir,
                        base_props=reel_props or {},
                    )
                    with self._lock:
                        if self._session:
                            self._session.output_mp4 = mp4_path
                except Exception as e:
                    # Remotion failures shouldn't lose the .mov files the
                    # user already has — log and degrade.
                    log.warning("remotion stage skipped: %s", e)
                    self._update(stage=f"MOVs ready (remotion skipped: {e})")
            else:
                self._update(
                    stage=f"{len(result.takes)} MOV(s) ready at {mov_dir.name}/ "
                    f"(no editor_dir for Remotion)"
                )

            self._mark_done()
        except Exception as e:
            tb = traceback.format_exc()
            log.error("render crashed: %s\n%s", e, tb)
            with self._lock:
                if self._session is not None:
                    self._session.state = "error"
                    self._session.stage = "failed"
                    self._session.error = f"{type(e).__name__}: {e}"
                    self._session.finished_at = time.time()
            # Make sure we don't leave CS2 running after a crash.
            try:
                kill_running_cs2()
            except Exception:
                pass

    # -- helpers ------------------------------------------------------------

    def _estimate_frames(self, plan: RenderPlan) -> int:
        total_ticks = sum(e - s for s, e in plan.segments)
        return int(total_ticks * FRAMES_PER_TICK)

    def _update(
        self,
        *,
        state: str | None = None,
        stage: str | None = None,
        progress: float | None = None,
    ) -> None:
        with self._lock:
            if self._session is None:
                return
            if state is not None:
                self._session.state = state
            if stage is not None:
                self._session.stage = stage
            if progress is not None:
                self._session.progress = progress

    def _mark_done(self) -> None:
        with self._lock:
            if self._session is None:
                return
            self._session.state = "done"
            self._session.stage = "complete"
            self._session.progress = 1.0
            self._session.finished_at = time.time()

    def _mark_done_partial(self, take_dir: Path | None) -> None:
        """End state when capture succeeded but ffmpeg/Remotion couldn't run."""
        with self._lock:
            if self._session is None:
                return
            self._session.state = "done"
            self._session.progress = 0.85
            self._session.finished_at = time.time()

    def _mark_cancelled(self) -> None:
        with self._lock:
            if self._session is None:
                return
            self._session.state = "cancelled"
            self._session.stage = "cancelled by user"
            self._session.finished_at = time.time()
        try:
            kill_running_cs2()
        except Exception:
            pass
