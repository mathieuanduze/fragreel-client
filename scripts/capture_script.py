"""Generate a CS2 console `.cfg` that drives HLAE capture via `mirv_cmd`.

Source 1 `.vdm` auto-loading is gone in CS2; HLAE ships `mirv_cmd` as the
canonical tick-based scheduler. The subcommand we use is:

    mirv_cmd addAtTick <iTick> <command-part-1> <command-part-2> ...

Each invocation queues ONE console command at the given demo tick. To run
several commands at the same tick, emit several `addAtTick` lines.

The generated file is loaded into CS2 with `exec <relative-cfg-path>`
right after `playdemo`. Paths are relative to `<CS2>/game/csgo/cfg/`.

Capture pipeline per segment:

- At `start_tick`: lock spectator camera to the user (`spec_player "<name>"`
  + `spec_mode 1`), lock engine (`host_framerate` + `host_timescale 0`),
  start recording. The killfeed and stream are installed once before the
  first segment.
- At `end_tick`: stop recording, release engine.
- At `end_tick + 1` (when there's another segment): emit `demo_gototick`
  to skip the silent gap to the next segment instead of letting CS2 play
  it back at normal speed (1 round ≈ 115s of wasted wall-clock).

Spec target + mode (history of pain):
- v0.2.5: used `spec_player_by_accountid` — that command doesn't exist in
  CS2 (Source 2 dropped the `_by_accountid` variants). Camera ended up
  free-cammed at the spawn point because the spec call no-op'd.
- v0.2.6 .. v0.2.10: switched the target to `spec_player "<name>"` (correct
  for CS2) BUT kept `spec_mode 4` from the old code. `spec_mode 4` in
  Source/Source 2 is **roaming / static camera**, not in-eye — that's
  exactly why every release in this range still showed "câmera parada no
  spawn" even though the `spec_player` part was right.
- v0.2.11: corrected to `spec_mode 1` (POV / olhos do jogador). Reference:
    spec_mode 1 → POV (first-person, in-eye)
    spec_mode 3 → POV (third-person)
    spec_mode 4 → roaming / static
  The killfeed pin (`mirv_deathmsg localPlayer <id>`) still uses the
  SteamID3 / Account ID — that's HLAE-side and correct.

The user's Steam Account ID (SteamID3) is what HLAE expects in
`mirv_deathmessage localPlayer <id>`. Convert from SteamID64 with
`account_id = steamid64 - 76561197960265728`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


DEFAULT_RECORD_NAME = "fragreel"
DEFAULT_STREAM_NAME = "default"
# 60 fps capture: 0.94x speed vs CS2's 64 tps — basically real-time, no
# slow-motion. Chosen as default in v0.2.6 because 1080p TGA + 60 fps
# = ~3000 frames per 50s segment ≈ 18 GB peak with v0.2.6 streaming
# convert. 120 fps doubled that and pushed users with < 30 GB free out
# of the product entirely.
#
# History:
#   - v0.2.3: 300 fps (4.69x slow-mo) — ~60 min capture, hit timeout
#   - v0.2.4: 120 fps (1.875x slow-mo) — ~25 min capture, ~36 GB peak
#   - v0.2.6: 60 fps (real-time)       — ~12 min capture, ~18 GB peak
#
# Slow-motion as a premium feature is on the roadmap (let user choose
# 120/240 from the web). For now real-time 60 fps is the safest default.
DEFAULT_HOST_FRAMERATE = 60
DEFAULT_KILLFEED_LIFETIME_SEC = 90
STEAM64_BASE = 76561197960265728

# Camera re-lock cadence (in demo ticks) INSIDE each segment.
# CS2's auto-director can drift the spectator off the user's POV mid-segment
# (e.g. on a local event like a flash, a teammate death, round-end ambience),
# leaving the camera frozen or jumping to someone else. Emitting
# `spec_player "<name>"` + `spec_mode 1` every ~0.5s of demo time (32 ticks
# at 64 tps) re-asserts the lock without measurable overhead — `spec_player`
# is a no-op if we're already on that player.
#
# v0.2.9 regression: single emit at segment start wasn't enough; users
# reported camera sitting at player's spawn while the player walked off.
CAMERA_RELOCK_INTERVAL_TICKS = 32

# Stagger between spec_player and spec_mode 1 (in ticks). Source 2's camera
# system appears to race: if spec_mode runs in the same tick as the
# preceding spec_player, the mode-switch can land BEFORE the target is
# committed, leaving the camera attached to the auto-director's previous
# pick (or worse, an undefined entity). A 1-tick gap lets the spec target
# settle first.
CAMERA_MODE_DELAY_TICKS = 1

# Gap between camera lock and engine freeze (`host_timescale 0`). If we
# freeze the demo clock in the same tick as spec_player, the engine may
# process the freeze before the camera lock takes effect. Giving it ticks
# of headroom is cheap insurance and effectively invisible (4 ticks ≈ 62ms
# at 64 tps).
#
# v0.2.10 used 2 ticks; v0.2.11 bumps to 4 because we still saw "câmera
# parada no spawn" reports — Source 2's spec subsystem appears to need
# more frames to acquire the entity than CS:GO did. Going from 2→4 is
# imperceptible to the user but doubles the budget for spec to settle.
ENGINE_FREEZE_DELAY_TICKS = 4

# Gap AFTER `mirv_streams record start` where we re-emit the spec lock
# block one more time. v0.2.11 insurance: if spec_player/spec_mode missed
# the first attach (e.g. demo was still buffering the round entity table),
# this guarantees we hit it once more while the camera is in the recorded
# frame range. Source 2 commits the spec change next tick, so we still
# get a clean attach within ~30ms of record start.
POST_RECORD_REASSERT_DELAY_TICKS = 2


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


# ─────────────────────────────────────────────────────────────────────────
# v0.3.0-beta — Cluster round kills into capture windows (client-side)
#
# v0.3.0-alpha (server) switched scoring from cluster-based to round-based:
# 1 highlight = 1 round. Cada HighlightOut carrega `kill_ticks` +
# `kill_timestamps` do user naquele round. A decisão de CAPTURA (qual trecho
# do round gravar) vive aqui no client, não no server — o server não sabe
# do tickrate exato do demo, do budget de tempo de captura do user, nem das
# constraints do HLAE.
#
# Algoritmo:
#   1. Agrupa kills consecutivas com gap ≤ GAP_THRESHOLD_S num cluster único
#   2. Pra cada cluster, define janela: [first_kill - PAD_PRE, last_kill + PAD_POST]
#   3. Clampa no intervalo do round pra não vazar pro round anterior/próximo
#
# Garantia de não-echo (mesmo trecho capturado 2x):
#   GAP_THRESHOLD_S (10s) >= PAD_PRE_S (5s) + PAD_POST_S (3.5s) = 8.5s
#   Folga de 1.5s absorve imprecisão de tick→segundo e reconhece kills
#   9-10s apartadas como mesmo fluxo de combate narrativo.
#
# Inter-round: garantido pelo freezetime do CS2 (15s+ entre rounds),
# estruturalmente maior que pad sum.
#
# Spec completa em `Obsidian.../FragReel/v0.3 Plano Produto.md` §2.
# ─────────────────────────────────────────────────────────────────────────

CLUSTER_PAD_PRE_S = 5.0
"""Segundos antes da primeira kill do cluster (build-up cinematográfico)."""

CLUSTER_PAD_POST_S = 3.5
"""Segundos depois da última kill do cluster (reação + body drop + reload início)."""

CLUSTER_GAP_THRESHOLD_S = 10.0
"""Kills consecutivas com gap <= este valor agrupam no mesmo cluster.

Escolhido 1.5s acima do piso matemático (PAD_PRE+PAD_POST=8.5s) pra margem
de tick→segundo e reconhecer kills 9-10s apartadas como mesmo fluxo.
"""

DEFAULT_TICKRATE = 64
"""CS2 matchmaking padrão. Esports / tournament demos usam 128 tps —
quando for suportado, o tickrate virá no payload do server."""


def cluster_round_kills(
    kill_ticks: list[int] | tuple[int, ...],
    kill_timestamps: list[float] | tuple[float, ...],
    round_start_tick: int,
    round_end_tick: int,
    tickrate: int = DEFAULT_TICKRATE,
    pad_pre_s: float = CLUSTER_PAD_PRE_S,
    pad_post_s: float = CLUSTER_PAD_POST_S,
    gap_threshold_s: float = CLUSTER_GAP_THRESHOLD_S,
) -> list[tuple[int, int]]:
    """Expande 1 round window em N janelas de captura menores, 1 por cluster.

    Args:
        kill_ticks: ticks exatos de cada kill do user no round (sorted asc).
        kill_timestamps: mesmos kills em segundos do jogo (tick/tickrate).
        round_start_tick: limite inferior pra clamp (impede vazar pro round anterior).
        round_end_tick: limite superior pra clamp.
        tickrate: ticks por segundo do demo (64 matchmaking, 128 tournament).
        pad_pre_s / pad_post_s: padding em segundos antes/depois do cluster.
        gap_threshold_s: kills com gap <= este agrupam no mesmo cluster.

    Returns:
        Lista `[(start_tick, end_tick), ...]` ordenada. Clampada no round.
        Se `kill_ticks` vazio OU desalinhado com timestamps → retorna
        `[(round_start_tick, round_end_tick)]` (fallback pro comportamento
        pre-v0.3: captura o round inteiro).

    Exemplos:
        Round com triple kill rápido (≤10s entre cada) → 1 janela de ~10-15s
        Round com 3 kills espaçadas (>10s entre cada) → 3 janelas de ~8.5s cada
        Round com 1 kill solo                          → 1 janela de ~8.5s
    """
    if not kill_ticks or len(kill_ticks) != len(kill_timestamps):
        # Dados ausentes ou inconsistentes → fallback pro round inteiro.
        # Isso é o comportamento pre-v0.3.0-beta e garante que demos
        # parseadas por scorers pre-v0.3.0-alpha (sem `kill_ticks`) continuem
        # funcionando sem regressão.
        return [(round_start_tick, round_end_tick)]

    if round_end_tick <= round_start_tick:
        raise ValueError(
            f"invalid round window: start={round_start_tick} end={round_end_tick}"
        )

    # 1. Agrupa kills em sub-clusters por gap temporal
    cluster_indices: list[list[int]] = [[0]]
    for i in range(1, len(kill_timestamps)):
        last_in_cluster = cluster_indices[-1][-1]
        gap = kill_timestamps[i] - kill_timestamps[last_in_cluster]
        if gap <= gap_threshold_s:
            cluster_indices[-1].append(i)
        else:
            cluster_indices.append([i])

    # 2. Converte cada cluster numa janela (start_tick, end_tick) com padding
    pad_pre_ticks = int(pad_pre_s * tickrate)
    pad_post_ticks = int(pad_post_s * tickrate)
    windows: list[tuple[int, int]] = []
    for indices in cluster_indices:
        first = kill_ticks[indices[0]]
        last = kill_ticks[indices[-1]]
        # 3. Clampa no range do round pra não vazar pra rounds adjacentes
        start = max(round_start_tick, first - pad_pre_ticks)
        end = min(round_end_tick, last + pad_post_ticks)
        # Defensivo: se clamping colapsou a janela (kill exatamente em
        # round_start ou round_end), mantém 1 tick mínimo pra CaptureSegment
        # não rejeitar no __post_init__.
        if end <= start:
            end = min(round_end_tick, start + 1)
        windows.append((start, end))

    # Segurança: se algum pad deu merge entre clusters adjacentes (não
    # deveria pelas invariantes, mas paranoia), mescla greedy. Mesma lógica
    # do `/render` handler em local_api.
    windows.sort(key=lambda w: w[0])
    merged: list[tuple[int, int]] = []
    for start, end in windows:
        if merged and start <= merged[-1][1]:
            prev_s, prev_e = merged[-1]
            merged[-1] = (prev_s, max(prev_e, end))
        else:
            merged.append((start, end))
    return merged


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


def _quote_player_name(name: str) -> str:
    """Escape a player name for safe use in a CS2 console command.

    CS2 console parses double-quoted args as single tokens. Stripping `"`
    avoids breaking the surrounding `spec_player "…"` quoting; control
    chars are also stripped because the console cuts on \\n. Names in
    CS2 may contain spaces, unicode, and most punctuation — those are
    fine inside `"…"`.
    """
    cleaned = "".join(c for c in name if c.isprintable() and c != '"').strip()
    return cleaned


@dataclass(frozen=True)
class CaptureScriptPlan:
    """Inputs for generating a capture .cfg."""

    segments: tuple[CaptureSegment, ...]
    user_account_id: int | None = None
    user_player_name: str | None = None  # in-game name for `spec_player "<name>"`
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
        if self.user_player_name is not None:
            cleaned = _quote_player_name(self.user_player_name)
            if not cleaned:
                raise ValueError(
                    f"user_player_name is empty after sanitization: {self.user_player_name!r}"
                )
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


def _setup_commands(plan: CaptureScriptPlan) -> list[str]:
    """Run ONCE at the first segment's start tick (or earlier) — installs the
    stream + names the record dir + pins the killfeed. Re-running these per
    segment would noop-warn at best and confuse HLAE state at worst.

    v0.2.11: enable `developer 1` so spec_player/spec_mode error messages
    surface in the CS2 console.log file (default: %CSGO%/console.log when
    -condebug is set, which our launcher always sets). Without this we have
    no signal when spec_player silently fails to attach (typical cause: the
    in-game name has a unicode glyph CS2 console doesn't recognize).
    """
    cmds = [
        # `developer 1` is verbose but the only way to see WHY spec_player
        # didn't attach. We turn it back to 0 at shutdown.
        "developer 1",
        f'echo "[FragReel] setup tick — stream={plan.stream_name} record={plan.record_name}"',
        f"mirv_streams add normal {plan.stream_name}",
        f"mirv_streams record name {plan.record_name}",
    ]
    if plan.user_account_id is not None:
        cmds += [
            f"mirv_deathmsg lifetime {plan.killfeed_lifetime_sec}",
            f"mirv_deathmsg localPlayer {plan.user_account_id}",
        ]
    return cmds


def _camera_lock_commands(plan: CaptureScriptPlan) -> list[str]:
    """Commands that lock the spectator camera to the target player.

    Returned as an ordered list — the caller staggers them across ticks
    (see `CAMERA_MODE_DELAY_TICKS`). Returns an empty list if we have
    nothing to lock to (no player name + no account id).

    Spec target precedence:
      1. `spec_player "<name>"` — the canonical CS2 (Source 2) command.
         Takes the player's in-game name. CS2 has NO `*_by_accountid`
         variant; that died with Source 1.
      2. `spec_mode 1` — POV / olhos do jogador (in-eye / first-person).
         Issued AFTER spec_player (one tick later in the emitted cfg) so
         the camera attaches to the right player before switching mode.

    spec_mode reference (DON'T touch without re-checking):
        1 → POV first-person ("olhos do jogador")  ← what we want
        3 → POV third-person (chase cam)
        4 → roaming / static camera                ← the v0.2.5..v0.2.10
                                                     bug — produced "câmera
                                                     parada no spawn".
    """
    cmds: list[str] = []
    if plan.user_player_name:
        safe_name = _quote_player_name(plan.user_player_name)
        cmds.append(f'spec_player "{safe_name}"')
        cmds.append("spec_mode 1")
    elif plan.user_account_id is not None:
        # No player name — can't pin the target, but at least force first-person
        # POV on whatever the auto-director picks. Better than roaming cam.
        cmds.append("spec_mode 1")
    return cmds


def _engine_freeze_commands(plan: CaptureScriptPlan) -> list[str]:
    """Commands that freeze the demo clock and start recording.

    Emitted `ENGINE_FREEZE_DELAY_TICKS` after the camera lock so the lock
    has time to settle before host_timescale 0 pauses everything.

    v0.2.11: emit an `echo` first so the user can grep the console.log to
    confirm we entered the freeze block. If we DON'T see this line in the
    log but the capture ran, the .cfg load itself failed mid-way.
    """
    return [
        f'echo "[FragReel] engine freeze + record start (host_framerate={plan.host_framerate})"',
        f"host_framerate {plan.host_framerate}",
        "host_timescale 0",
        "mirv_streams record start",
    ]


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

    v0.2.11: dial `developer` back to 0 before quit. If CS2 ever crashes
    after our `quit` (it shouldn't but Source 2 is Source 2), at least
    the next user-launched session won't have spammy dev output.
    """
    return [
        'echo "[FragReel] capture done — shutting down CS2"',
        "developer 0",
        "quit",
    ]


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
        f"// user_name:     {plan.user_player_name if plan.user_player_name else '(camera not locked)'}",
        "",
        "// Cleanup (warns harmlessly on first run when stream does not exist yet).",
        f"mirv_streams remove {plan.stream_name}",
        "mirv_cmd clear",
        "",
    ]

    setup_cmds = _setup_commands(plan)
    camera_lock_cmds = _camera_lock_commands(plan)
    engine_freeze_cmds = _engine_freeze_commands(plan)
    extra_start_cmds = list(plan.extra_start_commands)
    end_cmds = _end_commands(plan)

    if plan.pre_seek_tick is not None:
        lines.append(
            f"// pre-seek: jump near first segment so user doesn't wait "
            f"minutes of demo playback"
        )
        lines.append(_emit_addAtTick(1, f"demo_gototick {plan.pre_seek_tick}"))
        lines.append("")

    # One-time setup: schedule a few ticks before the first segment so HLAE
    # has the stream + record name + killfeed pin ready before recording
    # starts. Falls back to tick 1 if there's no headroom.
    first_start = plan.segments[0].start_tick
    setup_tick = max(1, first_start - 2)
    lines.append(f"// one-time setup at tick {setup_tick}")
    for c in setup_cmds:
        lines.append(_emit_addAtTick(setup_tick, c))
    lines.append("")

    for i, seg in enumerate(plan.segments):
        lines.append(f"// segment {i}: ticks {seg.start_tick} .. {seg.end_tick}")

        # Diagnostic marker — first thing emitted in the segment. Lets the
        # user (or us, with their console.log) verify we entered the segment
        # logic and which player we're trying to lock to.
        target_label = (
            f'spec_player="{plan.user_player_name}"'
            if plan.user_player_name
            else (f"acct={plan.user_account_id}" if plan.user_account_id else "no-target")
        )
        lines.append(_emit_addAtTick(
            seg.start_tick,
            f'echo "[FragReel] seg {i} start tick={seg.start_tick} target={target_label}"',
        ))

        # Stagger camera-lock commands across ticks so spec_player settles
        # before spec_mode 1 fires (Source 2 race condition — see
        # CAMERA_MODE_DELAY_TICKS). When there's no player name, only one
        # command is in the list (spec_mode 1), so the stagger is a no-op.
        for offset, cmd in enumerate(camera_lock_cmds):
            emit_tick = seg.start_tick + offset * CAMERA_MODE_DELAY_TICKS
            lines.append(_emit_addAtTick(emit_tick, cmd))

        # Engine freeze + record start: delayed by ENGINE_FREEZE_DELAY_TICKS
        # so the camera lock has propagated before host_timescale 0 freezes
        # the world. Without this gap the camera can freeze BEFORE attaching
        # to the player, causing the static-at-spawn bug from v0.2.9 testing.
        freeze_tick = seg.start_tick + len(camera_lock_cmds) * CAMERA_MODE_DELAY_TICKS
        freeze_tick += ENGINE_FREEZE_DELAY_TICKS
        # Keep freeze strictly inside the segment window.
        freeze_tick = min(freeze_tick, seg.end_tick - 1)
        for c in engine_freeze_cmds:
            lines.append(_emit_addAtTick(freeze_tick, c))
        for c in extra_start_cmds:
            lines.append(_emit_addAtTick(freeze_tick, c))

        # v0.2.11 insurance: re-assert the spec lock right after record
        # start. The original lock was emitted before host_timescale 0;
        # if the entity wasn't yet known to the spec subsystem, the lock
        # silently fell back to free-cam. Re-emitting once more after the
        # demo has progressed a few engine frames catches that race.
        # (CS2 keeps running its own internal tick even with timescale=0;
        # mirv_cmd's addAtTick is keyed off the demo tick, not wall-clock,
        # so this still fires reliably.)
        if camera_lock_cmds:
            reassert_base = freeze_tick + POST_RECORD_REASSERT_DELAY_TICKS
            for offset, cmd in enumerate(camera_lock_cmds):
                emit_tick = min(
                    reassert_base + offset * CAMERA_MODE_DELAY_TICKS,
                    seg.end_tick - 1,
                )
                lines.append(_emit_addAtTick(emit_tick, cmd))
            lines.append(_emit_addAtTick(
                min(reassert_base, seg.end_tick - 1),
                f'echo "[FragReel] seg {i} re-asserted spec lock post-record"',
            ))

        # Periodic camera re-lock inside the segment. Guards against the
        # auto-director drifting the spec target mid-take (happens on
        # flashes, teammate deaths, round-end camera ambience). Every
        # CAMERA_RELOCK_INTERVAL_TICKS we re-issue the whole lock block;
        # spec_player is a no-op when we're already on that player, so
        # this is effectively free.
        if camera_lock_cmds:
            relock_tick = seg.start_tick + CAMERA_RELOCK_INTERVAL_TICKS
            while relock_tick < seg.end_tick:
                for offset, cmd in enumerate(camera_lock_cmds):
                    emit_tick = min(relock_tick + offset * CAMERA_MODE_DELAY_TICKS, seg.end_tick - 1)
                    lines.append(_emit_addAtTick(emit_tick, cmd))
                relock_tick += CAMERA_RELOCK_INTERVAL_TICKS

        for c in end_cmds:
            lines.append(_emit_addAtTick(seg.end_tick, c))

        # Inter-segment fast-forward: without this, the demo plays at normal
        # speed through the gap between `record end` here and `record start`
        # of the next segment — no frames captured but full wall-clock time
        # elapses (1 round ≈ 115s of real-time demo playback). With a seek
        # we skip straight to just before the next segment.
        if i + 1 < len(plan.segments):
            next_start = plan.segments[i + 1].start_tick
            gap_ticks = next_start - seg.end_tick
            # Only worth seeking if gap > ~1.5s of demo time. Tiny gaps stay
            # linear (seek has its own ~200-500ms overhead in CS2).
            if gap_ticks > INTER_SEGMENT_SEEK_MIN_GAP:
                seek_target = next_start - INTER_SEGMENT_SEEK_LEAD_TICKS
                lines.append(
                    f"// skip {gap_ticks} ticks of silent playback to next segment"
                )
                lines.append(
                    _emit_addAtTick(seg.end_tick + 1, f"demo_gototick {seek_target}")
                )
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
# Inter-segment seek: how many ticks before the next segment's start_tick to
# land after seeking. Gives spec_player + spec_mode 1 (queued at
# next_start) a small window to settle before record start fires.
INTER_SEGMENT_SEEK_LEAD_TICKS = 50
# Minimum gap (in ticks) between segments worth seeking through. Below this,
# we let the demo play linearly — the seek's own overhead (~200-500ms) eats
# the savings. 100 ticks ≈ 1.5s of demo time at 64 tps.
INTER_SEGMENT_SEEK_MIN_GAP = 100


def generate_capture_cfg(
    output_path: Path | str,
    segments: Iterable[tuple[int, int]] | Iterable[CaptureSegment],
    *,
    user_account_id: int | None = None,
    user_steamid64: str | int | None = None,
    user_player_name: str | None = None,
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

    `user_player_name` is the in-game CS2 name of the player to lock the
    spectator camera to via `spec_player "<name>"`. Required for the camera
    to actually follow the user — without it, only the killfeed gets pinned
    and the camera follows whoever the auto-director picks.

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
        user_player_name=user_player_name,
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
