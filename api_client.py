"""
api_client.py — Cliente HTTP pra chamar a API de scoring do FragReel.

Arquitetura (Round 5 — API migration, planeja 2026-05+):
  Antes: client upload .dem (50-200MB) → backend parseia + scoreia → highlights
  Depois: client parseia .dem LOCAL → POST events JSON (~MB) → API scoreia → highlights

Vantagens da nova arquitetura:
  • Algoritmo de scoring + cluster fica privado (binário deployado, código nunca
    exposto no client OSS público que SignPath assina)
  • Latência menor (envia só JSON em vez de demo inteiro)
  • Server cost menor (processamento leve em vez de parsing pesado)
  • Iteração instantânea: tweakar scoring no servidor → todos users recebem
    na próxima call (sem precisar release de novo .exe)
  • Possibilidade de A/B testing real do scoring
  • Anti-abuse via rate limit por IP

Trade-offs honestos:
  • Requer internet pra usar (mitigado pelo fallback offline LITE abaixo)
  • API outage = scoring quebra (mitigado pelo fallback)
  • Privacy: events sobem pro servidor (DOCUMENTADO em PRIVACY.md)

Status atual: STUB. O endpoint api.fragreel.gg/api/score retorna mock highlights
que validam o contrato. Migração real do scorer Python pra TypeScript no Vercel
ou via subprocess Python rola na Fase B do Round 5.

Uso típico:
    from api_client import score_via_api, ApiUnavailable

    try:
        highlights = score_via_api(parsed_demo, player_steamid, timeout=5.0)
    except ApiUnavailable:
        # Fallback offline LITE — top kills por contagem, sem bonuses
        highlights = score_offline_lite(parsed_demo, player_steamid)
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

try:
    import requests  # type: ignore
except ImportError:  # pragma: no cover — requests é peer dep do client
    requests = None  # type: ignore

from version import __version__ as CLIENT_VERSION

log = logging.getLogger("fragreel.api_client")

# ── Config ────────────────────────────────────────────────────────────────────

# Default prod endpoint. Override com FRAGREEL_API_URL pra dev/staging.
DEFAULT_API_BASE = "https://fragreel.gg"
API_BASE = os.environ.get("FRAGREEL_API_URL", DEFAULT_API_BASE).rstrip("/")
SCORE_ENDPOINT = f"{API_BASE}/api/score"

# Schema version do contrato request/response. BUMP CONJUNTO com a API
# (web/app/api/score/route.ts). Major bump = client incompatível, vai pro
# fallback offline.
SCHEMA_VERSION = "1"

# Timeout default. Stub atual responde <100ms; futuro scoring real deve
# ficar <500ms (matemática pura sobre eventos). 5s é margem segura.
DEFAULT_TIMEOUT = 5.0

# Max retries com backoff exponencial. 3 retries cobre transient (DNS hiccup,
# Vercel cold start). Acima disso, vai pro fallback.
MAX_RETRIES = 3
BACKOFF_BASE = 0.5  # 0.5s, 1.0s, 2.0s


# ── Exceptions ────────────────────────────────────────────────────────────────


class ApiUnavailable(Exception):
    """API inacessível depois de N retries. Caller deve cair pro fallback."""


class ApiSchemaError(Exception):
    """API respondeu com schema_version incompatível. Cliente desatualizado."""


# ── Public API ────────────────────────────────────────────────────────────────


@dataclass
class HighlightFromApi:
    """Highlight retornado pela API. Mirror da estrutura de scorer.py.Highlight."""

    rank: int
    round_num: int
    label: str
    narrative: str
    score: float
    start: float
    end: float
    clutch_situation: Optional[str] = None
    won_round: bool = False
    bomb_action: Optional[str] = None
    is_round_winning_kill: bool = False
    kill_ticks: list[int] = field(default_factory=list)
    kill_timestamps: list[float] = field(default_factory=list)
    kills: list[dict[str, Any]] = field(default_factory=list)
    alive_timeline: list[dict[str, Any]] = field(default_factory=list)


def score_via_api(
    parsed_demo: Any,
    player_steamid: str,
    *,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[HighlightFromApi]:
    """
    Chama POST /api/score com os eventos parseados do demo.

    Args:
        parsed_demo: ParsedDemo do parser local (TEM kills, rounds, bomb_events,
                     map, tickrate). Estrutura igual à do api/parser/demo_parser.py.
        player_steamid: SteamID do user (atacante alvo do scoring).
        timeout: Timeout por request HTTP (segundos).

    Returns:
        Lista de HighlightFromApi ranqueada (best first), até 10 highlights.

    Raises:
        ApiUnavailable: API down ou network error após MAX_RETRIES tentativas.
        ApiSchemaError: API respondeu com schema_version diferente — client
                        precisa de update.
    """
    if requests is None:
        raise ApiUnavailable("requests lib not installed")

    body = _build_request_body(parsed_demo, player_steamid)
    headers = {
        "Content-Type": "application/json",
        "User-Agent": f"FragReel-Client/{CLIENT_VERSION}",
    }

    last_err: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(
                SCORE_ENDPOINT,
                data=json.dumps(body),
                headers=headers,
                timeout=timeout,
            )
        except requests.RequestException as e:
            last_err = e
            log.warning(
                "POST /api/score attempt %d/%d failed: %s",
                attempt + 1, MAX_RETRIES, e,
            )
            _backoff(attempt)
            continue

        if resp.status_code == 200:
            payload = resp.json()
            if payload.get("schema_version") != SCHEMA_VERSION:
                raise ApiSchemaError(
                    f"server schema {payload.get('schema_version')} != "
                    f"client schema {SCHEMA_VERSION} — please update FragReel"
                )
            return _parse_highlights(payload.get("highlights", []))

        if resp.status_code == 400:
            # Bad request — payload inválido. Bug no cliente, não retry.
            raise ApiSchemaError(f"server rejected payload: {resp.text[:200]}")

        if resp.status_code == 413:
            # Payload too large — não retry, fallback necessário.
            raise ApiUnavailable(f"payload too large for API: {resp.text[:200]}")

        # 500/502/503/504 — server error, retry.
        last_err = Exception(f"HTTP {resp.status_code}: {resp.text[:200]}")
        log.warning(
            "POST /api/score HTTP %d (attempt %d/%d): %s",
            resp.status_code, attempt + 1, MAX_RETRIES, resp.text[:100],
        )
        _backoff(attempt)

    raise ApiUnavailable(f"all {MAX_RETRIES} attempts failed; last error: {last_err}")


def score_offline_lite(
    parsed_demo: Any,
    player_steamid: str,
) -> list[HighlightFromApi]:
    """
    Fallback offline LITE quando a API está fora.

    NÃO replica o scorer completo (esse fica privado no servidor). Faz só
    o mínimo pra usuário não ficar travado:
      • Agrupa kills do user por round
      • Score = N kills * 100 + headshots * 20
      • Rank desc, top 10
      • Sem clutch detection, sem bomb bonuses, sem cinema events

    Resultado é INFERIOR ao scoring real — UX mostra warning "API offline,
    scoring básico aplicado".

    Args:
        parsed_demo: mesma assinatura de score_via_api
        player_steamid: SteamID do user

    Returns:
        Lista de HighlightFromApi (com narrative warning).
    """
    log.warning("API unavailable — falling back to offline LITE scoring")

    user_kills = [
        k for k in getattr(parsed_demo, "all_kills", [])
        if getattr(k, "attacker_steamid", None) == player_steamid
    ]
    if not user_kills:
        return []

    by_round: dict[int, list[Any]] = {}
    for k in sorted(user_kills, key=lambda k: k.tick):
        by_round.setdefault(k.round_num, []).append(k)

    rounds: list[tuple[int, list[Any], int]] = []
    for round_num, kills in by_round.items():
        n = len(kills)
        hs = sum(1 for k in kills if getattr(k, "headshot", False))
        score = n * 100 + hs * 20
        rounds.append((round_num, kills, score))

    rounds.sort(key=lambda r: r[2], reverse=True)

    out: list[HighlightFromApi] = []
    for rank, (round_num, kills, score) in enumerate(rounds[:10], start=1):
        first, last = kills[0], kills[-1]
        n = len(kills)
        tag = "ACE" if n >= 5 else f"{n}K" if n >= 2 else "Solo"
        out.append(HighlightFromApi(
            rank=rank,
            round_num=round_num,
            label=f"{tag} · Round {round_num}",
            narrative=f"{tag} no round {round_num}. (scoring básico — API offline)",
            score=float(score),
            start=max(0.0, first.timestamp - 15.0),
            end=last.timestamp + 5.0,
            kill_ticks=[k.tick for k in kills],
            kill_timestamps=[k.timestamp for k in kills],
            kills=[{
                "label": f"{k.weapon}{' · HS' if getattr(k, 'headshot', False) else ''}",
                "weapon": k.weapon,
                "headshot": getattr(k, "headshot", False),
            } for k in kills],
        ))

    return out


def score_with_fallback(
    parsed_demo: Any,
    player_steamid: str,
    *,
    timeout: float = DEFAULT_TIMEOUT,
) -> tuple[list[HighlightFromApi], str]:
    """
    Helper conveniente: tenta API primeiro, cai pra LITE se falhar.

    Returns:
        (highlights, source) onde source é "api" ou "offline_lite".
        Caller usa source pra mostrar UI warning quando "offline_lite".
    """
    try:
        return score_via_api(parsed_demo, player_steamid, timeout=timeout), "api"
    except ApiSchemaError:
        # Schema mismatch é UPDATE-REQUIRED — propaga, não fallback.
        raise
    except ApiUnavailable as e:
        log.info("score_via_api unavailable (%s) — using offline_lite", e)
        return score_offline_lite(parsed_demo, player_steamid), "offline_lite"


# ── Sprint I.5 — Full migration end-to-end (parse local + score API) ─────────


def parse_and_score_locally(
    demo_path: "PathLike",
    player_steamid: str,
    *,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Sprint I.5 (28/04 noite): pipeline completo CLIENT-SIDE — parse local
    da .dem + score via API + build match doc no schema do Railway.

    Substitui o flow legacy:
        client → upload .dem (50-200MB) → Railway → parse + score → match_id

    Pelo flow novo:
        client → parse_and_score_locally(demo_path) → match_doc

    Output: dict no MESMO schema que Railway retornava em /matches/{id},
    pra cliente expor via /matches/{id} no local_api.py (Fase 3 de Sprint I.5)
    e pra web consumir transparente.

    Args:
        demo_path: Path do .dem local
        player_steamid: SteamID do user (pra filtrar player_kills)
        timeout: timeout HTTP pra /api/score (default 5s)

    Returns:
        match_doc: dict com chaves esperadas por web/lib/api.ts:
            - id, map, date, score, status, highlights_count, top_play
            - rating, kd, stats {kd, hs, adr, rating}
            - player_name, game_mode
            - highlights[] (cada um com rank, label, narrative, score, start,
              end, kills, kill_ticks/timestamps, alive_timeline,
              clutch_situation, won_round, bomb_action, etc)

    Raises:
        FileNotFoundError: demo path inválido
        RuntimeError: demoparser2 não disponível (build incompleto)
        ApiUnavailable: /api/score down + sem fallback offline_lite ainda
                        (Sprint I.5 minimal version dispara fallback se
                        FRAGREEL_USE_API_FALLBACK=1, senão raise)
        ApiSchemaError: client/server schema mismatch — propagar pra UI
                        mostrar "atualize o FragReel"
    """
    from pathlib import Path
    from local_parser.demo_parser import parse as _parse  # type: ignore[import-not-found]

    demo_path = Path(demo_path)
    if not demo_path.exists():
        raise FileNotFoundError(f"demo not found: {demo_path}")

    log.info("Sprint I.5: parsing local %s (steamid=%s)", demo_path.name, player_steamid)
    t0 = time.time()
    parsed = _parse(demo_path, player_steamid=player_steamid)
    parse_ms = int((time.time() - t0) * 1000)
    log.info(
        "Sprint I.5: parse OK em %dms — %d player_kills, %d all_kills, "
        "%d bomb_events, %d round_states, map=%s",
        parse_ms, len(parsed.player_kills), len(parsed.all_kills),
        len(parsed.bomb_events), len(parsed.round_states), parsed.map_name,
    )

    # Score via /api/score (TS scorer, validado bit-exact com Python no
    # Sprint I.4). Fallback LITE se API down e flag opt-in setada.
    fallback_enabled = os.environ.get("FRAGREEL_USE_API_FALLBACK", "1").lower() in (
        "1", "true", "yes"
    )

    t1 = time.time()
    try:
        highlights = score_via_api(parsed, player_steamid, timeout=timeout)
        scoring_source = "api"
    except ApiUnavailable as e:
        if not fallback_enabled:
            log.error("Sprint I.5: API down + FRAGREEL_USE_API_FALLBACK=0 → propagando")
            raise
        log.warning(
            "Sprint I.5: /api/score down (%s), fallback LITE — qualidade reduzida", e
        )
        highlights = score_offline_lite(parsed, player_steamid)
        scoring_source = "offline_lite"
    score_ms = int((time.time() - t1) * 1000)
    log.info(
        "Sprint I.5: scoring OK em %dms — %d highlights (source=%s)",
        score_ms, len(highlights), scoring_source,
    )

    # Build match_doc — schema espelha Railway pro web ler transparente.
    match_id = demo_path.stem  # ex: "match730_003abc" from "match730_003abc.dem"
    match_doc = _build_match_doc(parsed, highlights, match_id, scoring_source)

    return match_doc


def _build_match_doc(
    parsed: Any,
    highlights: list[HighlightFromApi],
    match_id: str,
    scoring_source: str,
) -> dict[str, Any]:
    """Constrói match_doc no schema esperado por web (espelha Railway).

    Schema source: api/routes/demo.py em fragreel repo, build do match_doc
    pós-parse + score (linhas 88-172). Mantemos compatibilidade pra web não
    precisar mudar consumo.
    """
    from datetime import datetime, timezone

    total_kills = len(parsed.player_kills)
    hs_kills = sum(1 for k in parsed.player_kills if k.headshot)

    player_deaths = (
        sum(
            1 for k in parsed.all_kills
            if k.victim_steamid == parsed.player_steamid
        )
        if parsed.player_steamid
        else (len(parsed.all_kills) - total_kills)
    )

    rounds_from_score = parsed.ct_score + parsed.t_score
    rounds_from_kills = max((k.round_num for k in parsed.all_kills), default=1)
    total_rounds = max(rounds_from_score, rounds_from_kills, 1)

    adr_approx = round((total_kills * 100) / total_rounds, 1)
    kd_approx = f"{total_kills}/{player_deaths}"
    rating = _estimate_rating(total_kills, total_rounds)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return {
        # Identity
        "steamid": parsed.player_steamid or "",
        "player_name": parsed.player_name,
        "game_mode": parsed.game_mode,
        # Summary fields (for list view)
        "id": match_id,
        "map": parsed.map_name,
        "date": today,
        "score": f"{parsed.ct_score}–{parsed.t_score}",
        "side": "ct",  # Static — Railway tinha mesma constante (compat)
        "status": "parsed",
        "highlights_count": len(highlights),
        "top_play": highlights[0].label if highlights else "—",
        "rating": rating,
        "kd": kd_approx,
        # Sprint I.5 specific — telemetria de qual scorer foi usado
        "scoring_source": scoring_source,  # "api" | "offline_lite"
        # Detail fields (for match view)
        "stats": {
            "kd": kd_approx,
            "hs": f"{round(hs_kills / total_kills * 100)}%" if total_kills else "0%",
            "adr": str(adr_approx),
            "rating": rating,
        },
        "highlights": [
            {
                "rank": h.rank,
                "round_num": h.round_num,
                "label": h.label,
                "narrative": h.narrative,
                "score": h.score,
                "start": h.start,
                "end": h.end,
                "kills": h.kills,
                "clutch_situation": h.clutch_situation,
                "won_round": h.won_round,
                "bomb_action": h.bomb_action,
                "is_round_winning_kill": h.is_round_winning_kill,
                "kill_ticks": h.kill_ticks,
                "kill_timestamps": h.kill_timestamps,
                "alive_timeline": h.alive_timeline,
                "bomb_action_tick": getattr(h, "bomb_action_tick", None),
                "bomb_action_timestamp": getattr(h, "bomb_action_timestamp", None),
            }
            for h in highlights
        ],
    }


def _estimate_rating(kills: int, rounds: int) -> str:
    """Approx rating 2.0 — mesma fórmula do Railway."""
    if rounds <= 0:
        return "1.00"
    kpr = kills / rounds  # kills per round
    # Magic numbers from Railway api/routes/demo.py
    rating = 0.0073 * 100.0 + 0.3591 * kpr + 0.5329
    return f"{rating:.2f}"


# Type alias for clarity
PathLike = Any  # accepts str | os.PathLike


# ── Internals ─────────────────────────────────────────────────────────────────


def _build_request_body(parsed_demo: Any, player_steamid: str) -> dict[str, Any]:
    """Serializa ParsedDemo no contrato schema v1 esperado pela API."""
    kills = []
    for k in getattr(parsed_demo, "all_kills", []):
        kills.append({
            "tick": k.tick,
            "timestamp": k.timestamp,
            "attacker_steamid": getattr(k, "attacker_steamid", "") or "",
            "victim_steamid": getattr(k, "victim_steamid", "") or "",
            "victim_team": getattr(k, "victim_team", None),
            "weapon": k.weapon,
            "headshot": k.headshot,
            "round_num": k.round_num,
            "attacker_health": getattr(k, "attacker_health", None),
            # Cinema flags (v0.3.1+) — opcionais
            "thrusmoke": getattr(k, "thrusmoke", False),
            "noscope": getattr(k, "noscope", False),
            "penetrated": getattr(k, "penetrated", 0),
            "attackerblind": getattr(k, "attackerblind", False),
        })

    rounds = []
    for round_num, state in getattr(parsed_demo, "round_states", {}).items():
        rounds.append({
            "round_num": round_num,
            "user_won": bool(getattr(state, "user_won", False)),
            "user_team": getattr(state, "user_team", None),
            "bomb_planted_by": getattr(state, "bomb_planted_by", None),
            "bomb_defused_by": getattr(state, "bomb_defused_by", None),
        })

    bomb_events = []
    for be in getattr(parsed_demo, "bomb_events", []):
        bomb_events.append({
            "round_num": be.round_num,
            "action": be.action,
            "player_steamid": be.player_steamid,
            "tick": be.tick,
            "timestamp": be.timestamp,
        })

    return {
        "schema_version": SCHEMA_VERSION,
        "client_version": CLIENT_VERSION,
        "demo_meta": {
            "map": getattr(parsed_demo, "map_name", "unknown"),
            "tickrate": getattr(parsed_demo, "tickrate", 64.0),
            "match_id": getattr(parsed_demo, "match_id", None),
        },
        "player_steamid": player_steamid,
        "events": {
            "kills": kills,
            "rounds": rounds,
            "bomb_events": bomb_events,
        },
    }


def _parse_highlights(raw: list[dict[str, Any]]) -> list[HighlightFromApi]:
    return [
        HighlightFromApi(
            rank=h["rank"],
            round_num=h["round_num"],
            label=h["label"],
            narrative=h.get("narrative", ""),
            score=float(h["score"]),
            start=float(h["start"]),
            end=float(h["end"]),
            clutch_situation=h.get("clutch_situation"),
            won_round=bool(h.get("won_round", False)),
            bomb_action=h.get("bomb_action"),
            is_round_winning_kill=bool(h.get("is_round_winning_kill", False)),
            kill_ticks=h.get("kill_ticks", []),
            kill_timestamps=h.get("kill_timestamps", []),
            kills=h.get("kills", []),
            alive_timeline=h.get("alive_timeline", []),
        )
        for h in raw
    ]


def _backoff(attempt: int) -> None:
    import time
    delay = BACKOFF_BASE * (2 ** attempt)
    time.sleep(delay)
