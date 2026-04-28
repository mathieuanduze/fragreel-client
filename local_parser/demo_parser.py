"""
CS2 demo parser — wraps demoparser2 and returns a clean ParsedDemo object.

demoparser2 docs: https://github.com/LaihoE/demoparser
"""
from __future__ import annotations

import logging
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger("fragreel.parser")

try:
    from demoparser2 import DemoParser as _DP
    HAS_DEMOPARSER = True
except ImportError:
    HAS_DEMOPARSER = False
    log.warning("demoparser2 not installed — demo parsing unavailable")


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class Kill:
    tick: int
    timestamp: float        # seconds from demo start
    round_num: int          # 1-indexed
    weapon: str             # clean name, no "weapon_" prefix
    headshot: bool
    attacker_steamid: str
    victim_steamid: str
    attacker_team: Optional[int] = None   # 2=CT, 3=T (None if not in demo)
    victim_team: Optional[int] = None     # 2=CT, 3=T
    # v0.3.1 — cinema flags pra scoring boost (B4). Default false/None pra
    # backwards-compat com demos parseadas pré-v0.3.1.
    noscope: bool = False                 # AWP no-scope
    thrusmoke: bool = False               # kill através de smoke
    penetrated: int = 0                   # wallbang count (0 = sem penetration)
    attackerblind: bool = False           # attacker estava cego (flashed)
    attackerinair: bool = False           # jumping kill
    distance: Optional[float] = None      # distância attacker→victim em units CS2
    attacker_health: Optional[int] = None # HP do attacker no tick da kill (low-HP heroico)


@dataclass
class BombEvent:
    """A bomb_planted or bomb_defused event."""
    tick: int
    timestamp: float
    round_num: int
    player_steamid: str          # planter or defuser
    action: str                  # "planted" or "defused"


@dataclass
class RoundState:
    """Per-round context: who won, bomb actions, user team for that round."""
    round_num: int
    winner_team: Optional[int] = None         # 2=T, 3=CT (demoparser2 convention)
    bomb_planted_by: Optional[str] = None     # steamid
    bomb_defused_by: Optional[str] = None     # steamid
    user_team: Optional[int] = None           # 2=T, 3=CT (snapshot for this round)
    user_won: bool = False


@dataclass
class ParsedDemo:
    map_name: str
    tickrate: float
    duration: float         # total demo duration in seconds
    player_steamid: str
    player_kills: list[Kill]
    all_kills: list[Kill]
    ct_score: int
    t_score: int
    bomb_events: list[BombEvent] = field(default_factory=list)
    round_states: dict[int, RoundState] = field(default_factory=dict)
    # v0.3.0-beta-3 — in-game name pra `spec_player "<name>"` no client.
    # CS2 (Source 2) NÃO tem `spec_player_by_accountid` — só aceita name.
    # Bug #11 (catched 25/04 madrugada-4): se vazio, capture_script cai no
    # elif que emite só `spec_mode 1` SEM `spec_player`, câmera segue
    # auto-director's pick (random per segment) ao invés do user.
    # Extraído via parse_player_info() ou fallback parse_ticks(["name"]).
    player_name: Optional[str] = None
    # v0.3.1 (A3 do roadmap) — game mode detection robusto. Web mostrava
    # heurística por round count que falhava em casos edge (Premier 13-5
    # mostrava como Wingman). Agora extraído de server_cvar event +
    # server_name + player_count com confiança alta.
    # Valores possíveis: "premier" | "competitive" | "wingman" | "casual"
    #                    | "deathmatch" | "scrimmage" | "workshop" | None
    game_mode: Optional[str] = None


# ── DataFrame compat helpers (Polars 0.x and 1.x) ────────────────────────────

def _df_is_empty(df) -> bool:
    """Works for both Polars 0.x (is_empty) and 1.x (len == 0)."""
    if df is None:
        return True
    try:
        if hasattr(df, "is_empty"):
            return df.is_empty()
    except Exception:
        pass
    try:
        return len(df) == 0
    except Exception:
        return True


def _df_iter_rows(df) -> list[dict]:
    """Return rows as a list of dicts — handles Polars, pandas, and future APIs."""
    log.info(f"_df_iter_rows: df type={type(df).__module__}.{type(df).__name__}")

    # ── Pandas DataFrame ──────────────────────────────────────────────────────
    # pandas: df.to_dict(orient="records") → list[dict]
    to_dict = getattr(df, "to_dict", None)
    if to_dict is not None:
        try:
            result = to_dict(orient="records")
            if isinstance(result, list) and result and isinstance(result[0], dict):
                log.info(f"_df_iter_rows: pandas to_dict → {len(result)} dicts")
                return result
        except Exception as e:
            log.debug(f"_df_iter_rows: to_dict(orient='records') failed: {e}")

    # ── Polars DataFrame ──────────────────────────────────────────────────────
    attempts = [
        ("to_dicts",  {}),
        ("rows",      {"named": True}),
        ("iter_rows", {"named": True}),
    ]
    for method_name, kwargs in attempts:
        method = getattr(df, method_name, None)
        if method is None:
            log.debug(f"_df_iter_rows: {method_name} not found on {type(df).__name__}")
            continue
        try:
            result = list(method(**kwargs))
            if not result:
                log.debug(f"_df_iter_rows: {method_name}({kwargs}) returned empty list")
                continue
            if isinstance(result[0], dict):
                log.info(f"_df_iter_rows: {method_name}({kwargs}) → {len(result)} dicts")
                return result
            log.debug(f"_df_iter_rows: {method_name}({kwargs}) returned {type(result[0])}, not dict")
        except Exception as e:
            log.warning(f"_df_iter_rows: {method_name}({kwargs}) raised {type(e).__name__}: {e}")

    log.warning(f"_df_iter_rows: all methods exhausted — df type={type(df)}, attrs={[a for a in dir(df) if not a.startswith('_')][:20]}")
    return []


def _df_last_row(df) -> dict:
    """Return the last row as a dict (Polars and pandas)."""
    try:
        tail = df.tail(1)
        rows = _df_iter_rows(tail)
        return rows[0] if rows else {}
    except Exception as e:
        log.debug(f"_df_last_row fallback: {e}")
    return {}


# ── Public API ────────────────────────────────────────────────────────────────

def parse(demo_path: Path, player_steamid: Optional[str] = None) -> ParsedDemo:
    """
    Parse a CS2 .dem file and return structured kill data.

    Raises:
        RuntimeError: if demoparser2 is not installed
        FileNotFoundError: if the demo file doesn't exist
    """
    if not HAS_DEMOPARSER:
        raise RuntimeError("demoparser2 not installed")

    demo_path = Path(demo_path)
    if not demo_path.exists():
        raise FileNotFoundError(f"Demo not found: {demo_path}")

    log.info(f"Parsing demo: {demo_path.name}")
    dp = _DP(str(demo_path))

    # ── Header ────────────────────────────────────────────────────────────────
    header = dp.parse_header()
    map_name   = str(header.get("map_name", "unknown"))
    pb_ticks   = int(header.get("playback_ticks", 0))
    pb_time    = float(header.get("playback_time", 0.0))
    tickrate   = round(pb_ticks / pb_time, 1) if pb_time > 0 else 64.0
    duration   = pb_time

    log.info(f"Map: {map_name} | Tickrate: {tickrate} | Duration: {duration:.1f}s")

    # ── Kill events ───────────────────────────────────────────────────────────
    kills = _parse_kills(dp, tickrate)
    log.info(f"Total kills parsed: {len(kills)}")

    # ── Enrich kills with team data (v0.3.0-alpha — non-fatal) ────────────────
    # player_death events don't always carry team_num columns. We cross-reference
    # parse_ticks(["team_num"]) at each round's anchor tick to derive
    # (steamid, round_num) → team. Side-swap at half is naturally handled because
    # we look up per round_num.
    try:
        n_enriched = _enrich_kills_with_teams(dp, kills)
        log.info(f"Kills enriched with team_num: {n_enriched}/{len(kills) * 2} slots filled")
    except Exception as e:
        log.warning(f"Team enrichment failed (non-fatal): {e}")

    # ── Scores ────────────────────────────────────────────────────────────────
    ct_score, t_score = _parse_score(dp)

    # ── Filter by player ──────────────────────────────────────────────────────
    steamid_str = str(player_steamid).strip() if player_steamid else ""
    if steamid_str:
        player_kills = [k for k in kills if k.attacker_steamid == steamid_str]
        log.info(f"Player {steamid_str} kills: {len(player_kills)}")
    else:
        player_kills = kills

    # ── Bomb events + round states (v0.3.0-alpha — scoring v2 context) ────────
    try:
        bomb_events = _parse_bomb_events(dp, tickrate)
    except Exception as e:
        log.warning(f"Bomb event parsing failed (non-fatal): {e}")
        bomb_events = []

    try:
        round_winners = _parse_round_winners(dp)
        log.info(f"Round winners parsed: {len(round_winners)} rounds")
    except Exception as e:
        log.warning(f"Round winner parsing failed (non-fatal): {e}")
        round_winners = {}

    try:
        round_states = _build_round_states(kills, bomb_events, round_winners, steamid_str)
        log.info(f"Round states built: {len(round_states)} rounds")
    except Exception as e:
        log.warning(f"Round state building failed (non-fatal): {e}")
        round_states = {}

    # ── v0.3.1 (A3): game mode detection ─────────────────────────────────────
    # Combina 3 sinais de alta confiança:
    #   1. server_name (header) — "Premier" string explícita pra Premier
    #   2. mp_maxrounds (server_cvar) — 24=MR12 (Premier/Comp), 16=MR8 (Wingman)
    #   3. player count (parse_player_info) — 10=5v5, 4=Wingman 2v2, <4=outro
    # Web hoje usa heurística por round_count que erra (13-5 vira "Wingman"
    # em vez de "Premier"). Extração no server elimina a heurística do front.
    game_mode: Optional[str] = None
    try:
        # Sinal 1: server_name
        server_name_lower = str(header.get("server_name", "")).lower()
        is_premier_server = "premier" in server_name_lower

        # Sinal 2: mp_maxrounds via server_cvar event
        maxrounds: Optional[int] = None
        try:
            cvars_df = dp.parse_event("server_cvar")
            if not _df_is_empty(cvars_df):
                # Pega último valor de mp_maxrounds (rounds finais valem mais
                # que defaults iniciais)
                mr_rows = [r for r in _df_iter_rows(cvars_df) if r.get("name") == "mp_maxrounds"]
                if mr_rows:
                    maxrounds = int(mr_rows[-1].get("value") or 0)
        except Exception as e:
            log.debug(f"Game mode: server_cvar lookup failed: {e}")

        # Sinal 3: player count
        player_count = 0
        try:
            info_df = dp.parse_player_info()
            if not _df_is_empty(info_df):
                player_count = len(_df_iter_rows(info_df))
        except Exception:
            pass
        # Fallback: count unique steamids em parse_ticks(name, steamid)
        if player_count == 0:
            try:
                tick_df = dp.parse_ticks(["name", "steamid"])
                if not _df_is_empty(tick_df):
                    seen = {str(r.get("steamid", "")) for r in _df_iter_rows(tick_df)}
                    seen.discard("")
                    player_count = len(seen)
            except Exception:
                pass

        # Decisão combinada
        if is_premier_server and maxrounds == 24 and player_count >= 9:
            game_mode = "premier"
        elif maxrounds == 24 and player_count >= 9:
            game_mode = "competitive"  # Premier ou Comp 5v5 (sem signal explícito)
        elif maxrounds == 16 and player_count == 4:
            game_mode = "wingman"
        elif player_count == 10:
            game_mode = "casual"  # 5v5 mas sem MR12 (casual round limits variam)
        elif player_count >= 12:
            game_mode = "deathmatch"  # DM tem 16 slots tipicamente
        elif player_count > 0:
            game_mode = "scrimmage"  # outros formatos
        # else: None (não conseguiu detectar)

        log.info(
            f"Game mode: {game_mode!r} (server_name='{server_name_lower[:40]}...', "
            f"maxrounds={maxrounds}, player_count={player_count})"
        )
    except Exception as e:
        log.warning(f"Game mode detection failed (non-fatal): {e}")

    # ── In-game name lookup pra `spec_player "<name>"` no client ─────────────
    # v0.3.0-beta-3 (Bug #11 fix): client precisa do nome in-game pra travar
    # câmera no user durante render. CS2 Source 2 não tem `spec_player_by_*`
    # — só aceita name string. Sem isso, capture_script cai num elif que
    # emite só `spec_mode 1`, câmera vira free-cam autodirector.
    player_name: Optional[str] = None
    if steamid_str:
        try:
            info_df = dp.parse_player_info()
            if not _df_is_empty(info_df):
                rows = _df_iter_rows(info_df)
                for r in rows:
                    if str(r.get("steamid", "")) == steamid_str:
                        player_name = str(r.get("name", "")).strip() or None
                        break
            # Fallback: parse_player_info pode vir vazio (broadcast captures
            # raros). Cai pra parse_ticks ([name]) tirando primeira ocorrência.
            if not player_name:
                tick_df = dp.parse_ticks(["name", "steamid"])
                if not _df_is_empty(tick_df):
                    for r in _df_iter_rows(tick_df):
                        if str(r.get("steamid", "")) == steamid_str:
                            n = str(r.get("name", "")).strip()
                            if n:
                                player_name = n
                                break
            log.info(f"Player name resolved: {player_name!r} for steamid {steamid_str}")
        except Exception as e:
            log.warning(f"Player name lookup failed (non-fatal): {e}")

    return ParsedDemo(
        map_name=map_name,
        tickrate=tickrate,
        duration=duration,
        player_steamid=steamid_str,
        player_kills=player_kills,
        all_kills=kills,
        ct_score=ct_score,
        t_score=t_score,
        bomb_events=bomb_events,
        round_states=round_states,
        player_name=player_name,
        game_mode=game_mode,
    )


# ── Internal helpers ──────────────────────────────────────────────────────────

def _parse_kills(dp, tickrate: float) -> list[Kill]:
    """Extract kill events from demo."""
    df = None
    last_err = None
    for kwargs in [
        {"other": ["total_rounds_played"]},
        {},
    ]:
        try:
            df = dp.parse_event("player_death", **kwargs)
            if df is not None:
                break
        except Exception as e:
            last_err = e
            log.warning(f"parse_event attempt failed ({kwargs}): {e}")

    if _df_is_empty(df):
        log.warning(f"No player_death events found in demo (last_err={last_err})")
        return []

    cols = set(df.columns)
    log.info(f"player_death columns ({len(df)} rows): {sorted(cols)}")

    # Try to detect attacker steamid column name automatically
    # (varies across demoparser2 versions)
    _ATTACKER_COLS = [
        "attacker_steamid", "attacker_steamID",
        "attacker_steam_id", "attacker_xuid",
    ]
    _VICTIM_COLS = [
        "user_steamid", "victim_steamid",
        "victim_steam_id", "user_xuid",
    ]
    attacker_col = next((c for c in _ATTACKER_COLS if c in cols), None)
    victim_col   = next((c for c in _VICTIM_COLS   if c in cols), None)

    if attacker_col is None:
        # Last resort: look for any column containing "attacker" and "id"
        attacker_col = next(
            (c for c in cols if "attacker" in c.lower() and ("id" in c.lower() or "xuid" in c.lower())),
            None,
        )
    if victim_col is None:
        victim_col = next(
            (c for c in cols if ("victim" in c.lower() or "user" in c.lower()) and ("id" in c.lower() or "xuid" in c.lower())),
            None,
        )

    # Probe team columns (demoparser2 versions vary)
    _ATTACKER_TEAM_COLS = ["attacker_team_num", "attacker_team", "attacker_team_number"]
    _VICTIM_TEAM_COLS   = ["user_team_num", "user_team", "victim_team_num", "victim_team"]
    attacker_team_col = next((c for c in _ATTACKER_TEAM_COLS if c in cols), None)
    victim_team_col   = next((c for c in _VICTIM_TEAM_COLS   if c in cols), None)

    log.info(
        f"Using attacker_col={attacker_col!r}, victim_col={victim_col!r}, "
        f"attacker_team_col={attacker_team_col!r}, victim_team_col={victim_team_col!r}"
    )

    kills: list[Kill] = []
    skipped = 0
    for row in _df_iter_rows(df):
        try:
            raw_attacker = row.get(attacker_col) if attacker_col else None
            attacker = str(raw_attacker or "").strip()
            if not attacker or attacker in ("0", "None", "nan", ""):
                skipped += 1
                continue

            tick     = int(row.get("tick") or 0)
            weapon   = _clean_weapon(str(row.get("weapon") or "unknown"))
            headshot = bool(row.get("headshot", False))

            round_raw = row.get("total_rounds_played")
            round_num = (int(round_raw) + 1) if round_raw is not None else 1

            raw_victim = row.get(victim_col) if victim_col else None
            victim = str(raw_victim or "").strip()

            # Team numbers — 2=CT, 3=T (None if column absent or invalid)
            attacker_team = _coerce_team(row.get(attacker_team_col)) if attacker_team_col else None
            victim_team   = _coerce_team(row.get(victim_team_col))   if victim_team_col   else None

            # v0.3.1 — Cinema flags do player_death event. Defaults seguros
            # se a coluna não existir (demoparser2 antigo ou demo formato
            # legacy). Não-fatal: kill ainda registra mesmo sem esses dados.
            noscope        = bool(row.get("noscope", False))
            thrusmoke      = bool(row.get("thrusmoke", False))
            penetrated_raw = row.get("penetrated")
            penetrated     = int(penetrated_raw) if penetrated_raw is not None else 0
            attackerblind  = bool(row.get("attackerblind", False))
            attackerinair  = bool(row.get("attackerinair", False))
            distance_raw   = row.get("distance")
            distance       = float(distance_raw) if distance_raw is not None else None
            # attacker_health: demoparser2 expõe via parse_ticks(["health"]) per tick
            # OU as vezes vem direto no player_death (depending da versão).
            # Tentamos ambos com defaults graceful.
            attacker_health_raw = row.get("attacker_health") or row.get("attacker_hp")
            attacker_health = int(attacker_health_raw) if attacker_health_raw is not None else None

            kills.append(Kill(
                tick=tick,
                timestamp=round(tick / tickrate, 3),
                round_num=round_num,
                weapon=weapon,
                headshot=headshot,
                attacker_steamid=attacker,
                victim_steamid=victim,
                attacker_team=attacker_team,
                victim_team=victim_team,
                noscope=noscope,
                thrusmoke=thrusmoke,
                penetrated=penetrated,
                attackerblind=attackerblind,
                attackerinair=attackerinair,
                distance=distance,
                attacker_health=attacker_health,
            ))
        except Exception as e:
            log.debug(f"Skipping kill row: {e}")
            skipped += 1
            continue

    log.info(f"Kills extracted: {len(kills)} (skipped {skipped})")
    return kills


def _coerce_team(raw) -> Optional[int]:
    """Coerce raw team value to demoparser2 convention: 2=T, 3=CT.

    Returns None for anything else. This matches the actual `team_num` enum
    used by demoparser2 across CS2 demos (verified empirically against
    bomb_planted/defused on de_nuke matchmaking demo Apr 2026).

    Accepts:
      - int 2 / 3 → returned as-is
      - numeric strings "2" / "3"
      - team name strings "CT" / "T" / "TERRORIST" / "COUNTER-TERRORIST" (case-insensitive)
      - some demos use "Ct" or "Terrorist" → all normalized
    """
    if raw is None:
        return None
    # Try int first (covers ints + numeric strings)
    try:
        n = int(raw)
        if n in (2, 3):
            return n
    except (TypeError, ValueError):
        pass
    # Try string team name — note: 3=CT, 2=T (demoparser2 convention)
    if isinstance(raw, str):
        s = raw.strip().upper()
        if s in ("CT", "COUNTER-TERRORIST", "COUNTERTERRORIST"):
            return 3
        if s in ("T", "TERRORIST", "TERRORISTS"):
            return 2
    return None


def _enrich_kills_with_teams(dp, kills: list[Kill]) -> int:
    """Mutate kills in-place, filling attacker_team and victim_team via parse_ticks.

    player_death events on most CS2 demos don't carry team columns. We cross-
    reference parse_ticks(['team_num']) at each round's anchor tick (= first
    kill tick of that round) and snapshot every player's team at that moment.
    Side-swap at half is naturally handled because we re-anchor per round.

    Returns the number of (kill.attacker_team, kill.victim_team) slots that
    got filled (max = len(kills) * 2).
    """
    if not kills:
        return 0

    # Pull team_num for every (tick, steamid) — this returns ~1M+ rows on a
    # full match demo but pandas/polars handles the indexing fine.
    try:
        ticks_df = dp.parse_ticks(["team_num"])
    except Exception as e:
        log.warning(f"_enrich_kills_with_teams: parse_ticks failed: {e}")
        return 0

    if _df_is_empty(ticks_df):
        return 0

    # Anchor tick per round = the earliest kill tick in that round.
    anchor_per_round: dict[int, int] = {}
    for k in kills:
        prev = anchor_per_round.get(k.round_num)
        if prev is None or k.tick < prev:
            anchor_per_round[k.round_num] = k.tick

    # Build {(steamid, round_num): team} by snapshotting team_num at each
    # round's anchor tick. Use pandas .query when df is pandas, else fall
    # back to dict iteration on _df_iter_rows output.
    team_lookup: dict[tuple[str, int], int] = {}

    is_pandas = type(ticks_df).__module__.startswith("pandas")

    for round_num, anchor_tick in anchor_per_round.items():
        # Fast path: pandas filter (vectorized)
        if is_pandas:
            try:
                snap = ticks_df[ticks_df["tick"] == anchor_tick]
                if len(snap) == 0:
                    # Try a slightly later tick (within 64 ticks = 1s) as fallback
                    snap = ticks_df[(ticks_df["tick"] >= anchor_tick) &
                                   (ticks_df["tick"] <= anchor_tick + 64)].drop_duplicates("steamid")
                for _, row in snap.iterrows():
                    sid = str(row.get("steamid") or "").strip()
                    team = _coerce_team(row.get("team_num"))
                    if sid and team is not None:
                        team_lookup[(sid, round_num)] = team
                continue
            except Exception as e:
                log.debug(f"_enrich pandas path failed at round {round_num}: {e}")

        # Slow path: iterate (works for polars or anything else)
        for row in _df_iter_rows(ticks_df):
            try:
                if int(row.get("tick") or -1) != anchor_tick:
                    continue
                sid = str(row.get("steamid") or "").strip()
                team = _coerce_team(row.get("team_num"))
                if sid and team is not None:
                    team_lookup[(sid, round_num)] = team
            except Exception:
                continue

    # Apply lookup to each kill (only fills if currently None)
    filled = 0
    for k in kills:
        if k.attacker_team is None:
            t = team_lookup.get((k.attacker_steamid, k.round_num))
            if t is not None:
                k.attacker_team = t
                filled += 1
        if k.victim_team is None:
            t = team_lookup.get((k.victim_steamid, k.round_num))
            if t is not None:
                k.victim_team = t
                filled += 1

    return filled


def _parse_bomb_events(dp, tickrate: float) -> list[BombEvent]:
    """Parse bomb_planted and bomb_defused events from the demo."""
    events: list[BombEvent] = []

    for event_name, action in [("bomb_planted", "planted"), ("bomb_defused", "defused")]:
        df = None
        for kwargs in [{"other": ["total_rounds_played"]}, {}]:
            try:
                df = dp.parse_event(event_name, **kwargs)
                if df is not None:
                    break
            except Exception as e:
                log.debug(f"parse_event({event_name}, {kwargs}) failed: {e}")

        if _df_is_empty(df):
            log.info(f"No {event_name} events found")
            continue

        cols = set(df.columns) if hasattr(df, "columns") else set()
        log.info(f"{event_name} columns ({len(df)} rows): {sorted(cols)}")

        # Find the steamid column (usually "user_steamid" — bomb planter/defuser)
        _USER_COLS = ["user_steamid", "user_steamID", "user_steam_id", "user_xuid", "userid_steamid"]
        user_col = next((c for c in _USER_COLS if c in cols), None)
        if user_col is None:
            user_col = next((c for c in cols if "steam" in c.lower() or "xuid" in c.lower()), None)

        for row in _df_iter_rows(df):
            try:
                raw_user = row.get(user_col) if user_col else None
                player_steamid = str(raw_user or "").strip()
                if not player_steamid or player_steamid in ("0", "None", "nan", ""):
                    continue

                tick = int(row.get("tick") or 0)
                round_raw = row.get("total_rounds_played")
                round_num = (int(round_raw) + 1) if round_raw is not None else 1

                events.append(BombEvent(
                    tick=tick,
                    timestamp=round(tick / tickrate, 3),
                    round_num=round_num,
                    player_steamid=player_steamid,
                    action=action,
                ))
            except Exception as e:
                log.debug(f"Skipping {event_name} row: {e}")

    log.info(f"Bomb events parsed: {len(events)}")
    return events


def _parse_round_winners(dp) -> dict[int, int]:
    """Return {round_num: winner_team} where winner_team is 2 (T) or 3 (CT)."""
    winners: dict[int, int] = {}
    for kwargs in [{"other": ["total_rounds_played"]}, {}]:
        try:
            df = dp.parse_event("round_end", **kwargs)
            if _df_is_empty(df):
                continue
            cols = set(df.columns) if hasattr(df, "columns") else set()
            log.info(f"round_end winner-probe columns: {sorted(cols)}")

            winner_col = next((c for c in ("winner", "winning_team", "win_team") if c in cols), None)
            if not winner_col:
                continue

            rows = _df_iter_rows(df)
            for i, row in enumerate(rows, start=1):
                round_raw = row.get("total_rounds_played")
                # round_end fires AFTER the round, so total_rounds_played is the round that just ended
                round_num = (int(round_raw) + 1) if round_raw is not None else i
                winner_raw = row.get(winner_col)
                if winner_raw is None:
                    continue
                # Use _coerce_team to handle both int (2/3) and string ("CT"/"T") winners
                w = _coerce_team(winner_raw)
                if w is not None:
                    winners[round_num] = w
            if winners:
                return winners
        except Exception as e:
            log.warning(f"_parse_round_winners attempt {kwargs} failed: {e}")
    return winners


def _build_round_states(
    all_kills: list[Kill],
    bomb_events: list[BombEvent],
    round_winners: dict[int, int],
    user_steamid: str,
) -> dict[int, RoundState]:
    """Compose per-round state: winner, bomb actions, user team, user_won."""
    states: dict[int, RoundState] = {}

    # Collect every round_num we've seen anywhere
    round_nums = set()
    round_nums.update(k.round_num for k in all_kills)
    round_nums.update(b.round_num for b in bomb_events)
    round_nums.update(round_winners.keys())

    for rn in sorted(round_nums):
        state = RoundState(round_num=rn)
        state.winner_team = round_winners.get(rn)

        # Derive user_team from any kill in this round where user is attacker or victim
        if user_steamid:
            for k in all_kills:
                if k.round_num != rn:
                    continue
                if k.attacker_steamid == user_steamid and k.attacker_team is not None:
                    state.user_team = k.attacker_team
                    break
                if k.victim_steamid == user_steamid and k.victim_team is not None:
                    state.user_team = k.victim_team
                    break

        if state.user_team and state.winner_team:
            state.user_won = (state.user_team == state.winner_team)

        # Bomb actions in this round
        for b in bomb_events:
            if b.round_num != rn:
                continue
            if b.action == "planted":
                state.bomb_planted_by = b.player_steamid
            elif b.action == "defused":
                state.bomb_defused_by = b.player_steamid

        states[rn] = state

    return states


def _parse_score(dp) -> tuple[int, int]:
    """Extract final CT and T round wins from round_end events."""
    # Try with cumulative score columns first, then without
    for kwargs in [{"other": ["ct_win_rounds", "t_win_rounds"]}, {}]:
        try:
            df = dp.parse_event("round_end", **kwargs)
            if _df_is_empty(df):
                continue
            cols = set(df.columns) if hasattr(df, "columns") else set()
            log.info(f"round_end columns: {sorted(cols)}")
            last = _df_last_row(df)
            if not last:
                continue
            # Try cumulative columns
            ct = last.get("ct_win_rounds")
            t  = last.get("t_win_rounds")
            if ct is not None and t is not None:
                return int(ct or 0), int(t or 0)
            # Try winner column: 2=T, 3=CT (or "CT"/"T" string in newer demos)
            winner_col = next((c for c in ("winner", "win_reason", "round_win_reason") if c in cols), None)
            if winner_col:
                # Count CT/T wins across all rows. Skip rows where winner is None
                # (e.g. round_end fired by warmup/intermission with no winner)
                rows = _df_iter_rows(df)
                ct_w = sum(1 for r in rows if _coerce_team(r.get(winner_col)) == 3)
                t_w  = sum(1 for r in rows if _coerce_team(r.get(winner_col)) == 2)
                if ct_w + t_w > 0:
                    return ct_w, t_w
        except Exception as e:
            log.warning(f"_parse_score attempt {kwargs} failed: {e}")
    log.warning("_parse_score: could not determine score, returning 0-0")
    return 0, 0


def _clean_weapon(weapon: str) -> str:
    return weapon.removeprefix("weapon_").strip()
