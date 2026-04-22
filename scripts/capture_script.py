"""Generate a CS2 console `.cfg` that drives HLAE capture via `mirv_cmd`.

Source 1 `.vdm` auto-loading is gone in CS2; HLAE ships `mirv_cmd` as the
canonical tick-based scheduler. The subcommand we use is:

    mirv_cmd addAtTick <iTick> <command-part-1> <command-part-2> ...

Each invocation queues ONE console command at the given demo tick. To run
several commands at the same tick, emit several `addAtTick` lines.

The generated file is loaded into CS2 with `exec <relative-cfg-path>`
right after `playdemo`. Paths are relative to `<CS2>/game/csgo/cfg/`.

Capture pipeline per segment:

- At `start_tick`: install stream, pin killfeed to the user, lock engine
  (`host_framerate` + `host_timescale 0`), start recording.
- At `end_tick`: stop recording, release engine.

The user's Steam Account ID (SteamID3) is what CS2 expects in
`mirv_deathmessage localPlayer <id>`. Convert from SteamID64 with
`account_id = steamid64 - 76561197960265728`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


DEFAULT_RECORD_NAME = "fragreel"
DEFAULT_STREAM_NAME = "default"
DEFAULT_HOST_FRAMERATE = 300
DEFAULT_KILLFEED_LIFETIME_SEC = 90
STEAM64_BASE = 76561197960265728


def steamid64_to_account_id(steamid64: str | int) -> int:
    """Convert a SteamID64 (community ID) to the 32-bit Account ID / SteamID3.

    HLAE's `mirv_deathmessage localPlayer` and many Source commands want the
    32-bit account ID, not the 64-bit form.
    """
    v = int(steamid64)
    acc = v - STEAM64_BASE
    if acc <= 0:
        raise ValueError(f"not a valid SteamID64: {steamid64}")
    return acc


@dataclass(frozen=True)
class CaptureSegment:
    """A single [start_tick, end_tick) range to record."""

    start_tick: int
    end_tick: int

    def __post_init__(self) -> None:
        if self.start_tick < 0 or self.end_tick <= self.start_tick:
            raise ValueError(
                f"invalid tick range: start={self.start_tick} end={self.end_tick}"
            )


@dataclass(frozen=True)
class CaptureScriptPlan:
    """Inputs for generating a capture .cfg."""

    segments: tuple[CaptureSegment, ...]
    user_account_id: int | None = None
    record_name: str = DEFAULT_RECORD_NAME
    stream_name: str = DEFAULT_STREAM_NAME
    host_framerate: int = DEFAULT_HOST_FRAMERATE
    killfeed_lifetime_sec: int = DEFAULT_KILLFEED_LIFETIME_SEC
    pre_seek_tick: int | None = None  # auto-seek to this tick right after demo loads
    extra_start_commands: tuple[str, ...] = field(default_factory=tuple)
    extra_end_commands: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.segments:
            raise ValueError("at least one segment required")
        if self.host_framerate <= 0:
            raise ValueError(f"host_framerate must be > 0, got {self.host_framerate}")
        if not self.record_name or "/" in self.record_name or "\\" in self.record_name:
            raise ValueError(
                f"record_name must be a plain token, got {self.record_name!r}"
            )
        if not self.stream_name:
            raise ValueError("stream_name is required")
        if self.user_account_id is not None and self.user_account_id <= 0:
            raise ValueError(f"user_account_id must be positive, got {self.user_account_id}")
        prev_end = -1
        for seg in self.segments:
            if seg.start_tick < prev_end:
                raise ValueError(
                    f"segments overlap: {seg.start_tick} starts before previous end {prev_end}"
                )
            prev_end = seg.end_tick
        if self.pre_seek_tick is not None:
            if self.pre_seek_tick < 0:
                raise ValueError(f"pre_seek_tick must be >= 0, got {self.pre_seek_tick}")
            first_start = self.segments[0].start_tick
            if self.pre_seek_tick >= first_start:
                raise ValueError(
                    f"pre_seek_tick {self.pre_seek_tick} must be < first segment start {first_start}"
                )


def _emit_addAtTick(tick: int, command: str) -> str:
    return f"mirv_cmd addAtTick {tick} {command}"


def _start_commands(plan: CaptureScriptPlan) -> list[str]:
    cmds = [
        f"mirv_streams add normal {plan.stream_name}",
        f"mirv_streams record name {plan.record_name}",
    ]
    if plan.user_account_id is not None:
        cmds += [
            f"mirv_deathmsg lifetime {plan.killfeed_lifetime_sec}",
            f"mirv_deathmsg localPlayer {plan.user_account_id}",
        ]
    cmds += [
        f"host_framerate {plan.host_framerate}",
        "host_timescale 0",
        "mirv_streams record start",
    ]
    cmds.extend(plan.extra_start_commands)
    return cmds


def _end_commands(plan: CaptureScriptPlan) -> list[str]:
    cmds = [
        "mirv_streams record end",
        "host_framerate 0",
        "host_timescale 1",
    ]
    cmds.extend(plan.extra_end_commands)
    return cmds


def _shutdown_commands() -> list[str]:
    """Commands run on the last segment's end_tick+1 — after capture is
    fully flushed to disk. A clean `quit` lets CS2 restore the display
    mode the user had before launch; killing it with TerminateProcess
    instead can leave the desktop stuck at a low resolution.
    """
    return ["quit"]


def build_cfg_content(plan: CaptureScriptPlan) -> str:
    lines: list[str] = [
        "// FragReel HLAE capture script — auto-generated",
        "// Load with: exec <this-file-relative-to-cfg/>",
        "//",
        f"// segments:      {len(plan.segments)}",
        f"// record_name:   {plan.record_name}",
        f"// stream_name:   {plan.stream_name}",
        f"// host_framerate:{plan.host_framerate}",
        f"// user_xuid:     {plan.user_account_id if plan.user_account_id else '(not pinned)'}",
        "",
        "// Cleanup (warns harmlessly on first run when stream does not exist yet).",
        f"mirv_streams remove {plan.stream_name}",
        "mirv_cmd clear",
        "",
    ]

    start_cmds = _start_commands(plan)
    end_cmds = _end_commands(plan)

    if plan.pre_seek_tick is not None:
        lines.append(
            f"// pre-seek: jump near first segment so user doesn't wait "
            f"minutes of demo playback"
        )
        lines.append(_emit_addAtTick(1, f"demo_gototick {plan.pre_seek_tick}"))
        lines.append("")

    for i, seg in enumerate(plan.segments):
        lines.append(f"// segment {i}: ticks {seg.start_tick} .. {seg.end_tick}")
        for c in start_cmds:
            lines.append(_emit_addAtTick(seg.start_tick, c))
        for c in end_cmds:
            lines.append(_emit_addAtTick(seg.end_tick, c))
        lines.append("")

    # Graceful shutdown: fire `quit` a few ticks after the final segment's
    # end so the recording is fully flushed and CS2 can restore display
    # state on exit.
    last_end = plan.segments[-1].end_tick
    shutdown_tick = last_end + 10
    lines.append(f"// graceful shutdown at tick {shutdown_tick}")
    for c in _shutdown_commands():
        lines.append(_emit_addAtTick(shutdown_tick, c))
    lines.append("")

    lines.append("echo [FragReel] capture script loaded")
    return "\n".join(lines) + "\n"


PRE_SEEK_LEAD_TICKS = 100  # how many ticks before first segment we seek to


def generate_capture_cfg(
    output_path: Path | str,
    segments: Iterable[tuple[int, int]] | Iterable[CaptureSegment],
    *,
    user_account_id: int | None = None,
    user_steamid64: str | int | None = None,
    record_name: str = DEFAULT_RECORD_NAME,
    stream_name: str = DEFAULT_STREAM_NAME,
    host_framerate: int = DEFAULT_HOST_FRAMERATE,
    killfeed_lifetime_sec: int = DEFAULT_KILLFEED_LIFETIME_SEC,
    pre_seek: bool = True,
    pre_seek_tick: int | None = None,
    extra_start_commands: Iterable[str] = (),
    extra_end_commands: Iterable[str] = (),
) -> Path:
    """Write a `.cfg` file at `output_path` that captures the given segments.

    Pass either `user_account_id` (already SteamID3) or `user_steamid64`
    (community ID, will be converted). Either one pins the killfeed to
    that player via `mirv_deathmsg localPlayer`.

    `pre_seek=True` (default) emits a `demo_gototick` at tick 1 that jumps
    near the first segment — so the user doesn't sit through minutes of
    demo playback waiting for their highlight. `pre_seek_tick` lets you
    override the exact seek target; otherwise it's `first_start - PRE_SEEK_LEAD_TICKS`.
    """
    if user_account_id is None and user_steamid64 is not None:
        user_account_id = steamid64_to_account_id(user_steamid64)

    norm_segments = tuple(
        s if isinstance(s, CaptureSegment) else CaptureSegment(start_tick=s[0], end_tick=s[1])
        for s in segments
    )

    resolved_pre_seek: int | None = None
    if pre_seek:
        first_start = norm_segments[0].start_tick if norm_segments else 0
        if pre_seek_tick is not None:
            resolved_pre_seek = pre_seek_tick
        elif first_start > PRE_SEEK_LEAD_TICKS:
            resolved_pre_seek = first_start - PRE_SEEK_LEAD_TICKS
        # else: first segment too close to demo start — no pre-seek needed

    plan = CaptureScriptPlan(
        segments=norm_segments,
        user_account_id=user_account_id,
        record_name=record_name,
        stream_name=stream_name,
        host_framerate=host_framerate,
        killfeed_lifetime_sec=killfeed_lifetime_sec,
        pre_seek_tick=resolved_pre_seek,
        extra_start_commands=tuple(extra_start_commands),
        extra_end_commands=tuple(extra_end_commands),
    )

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(build_cfg_content(plan), encoding="utf-8", newline="\n")
    return target


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Generate a CS2 HLAE capture .cfg")
    ap.add_argument("output", type=Path, help="Path to write the .cfg")
    ap.add_argument(
        "--segment",
        action="append",
        required=True,
        metavar="START:END",
        help="Segment ticks, e.g. 10000:10900. Repeatable.",
    )
    ap.add_argument("--steamid64", default=None, help="User SteamID64 (for killfeed pin)")
    ap.add_argument("--record-name", default=DEFAULT_RECORD_NAME)
    ap.add_argument("--stream-name", default=DEFAULT_STREAM_NAME)
    ap.add_argument("--host-framerate", type=int, default=DEFAULT_HOST_FRAMERATE)
    ap.add_argument("--print", action="store_true")
    args = ap.parse_args()

    segments = []
    for s in args.segment:
        a, b = s.split(":")
        segments.append((int(a), int(b)))

    written = generate_capture_cfg(
        args.output,
        segments,
        user_steamid64=args.steamid64,
        record_name=args.record_name,
        stream_name=args.stream_name,
        host_framerate=args.host_framerate,
    )
    print(f"wrote {written}")
    if args.print:
        print("---")
        print(written.read_text(encoding="utf-8"))
