"""
local_matches_store.py — Sprint I.5 (28/04 noite).

Persiste match_doc gerados pelo `api_client.parse_and_score_locally()` em
disco local pra que `local_api.py /matches/{id}` possa servir transparente
pra web (fragreel.gg) — sem Railway no caminho de scoring.

Storage: `%APPDATA%/FragReel/matches/` em Windows, `~/.fragreel/matches/`
em outros. Um arquivo JSON por match: `<match_id>.json`.

Lifecycle:
  - Sprint I.5: Cliente parseia demo → API scoreia → store.save(match_doc)
  - Web pede /matches/{id} → local_api lê store + retorna
  - Cleanup automático: matches > 30 dias old são removidos no boot do cliente
    (mantém disco limpo, retém histórico curto-prazo pra UX)

Schema versioning (Sprint v5.7.12, 08/05/2026 Mathieu spec):
  Match docs salvos antes de adicionarmos campos novos (ex: victim_name na
  v0.6.52, bomb_action_timestamp fallback na v0.7.0) viram STALE — o reel
  gerado a partir deles vai ter dados faltando ("INIMIGO", sem bomb timer).

  Solução: campo `_schema_version` no match_doc. Quando bumpamos a
  constante MATCH_DOC_SCHEMA_VERSION abaixo, todos os match_docs antigos
  no disco automaticamente viram cache miss → load_match retorna None
  → fluxo re-score automaticamente (web detecta 404 no /matches/{id} →
  cai pro AutoReanalyze → /api/score com client v0.6.53+ → match_doc
  novo com campos populados).

  Bump version checklist:
    1. Mudou shape do match_doc (campo novo, rename, struct change) → bump
    2. Mudou semântica de campo existente (ex: timestamp em sec → ms) → bump
    3. NÃO precisa bumpar pra: bug fix em scoring que mantém shape
       (ex: bomb_action_timestamp fix retorna número onde antes null —
       aceitável usar dado antigo, só vai ter mais nulls)
       Mas em DÚVIDA bumpa — re-score local é barato (~5-15s).

Sync com Railway: cliente continua mandando match_doc pro Railway via
`/demo/analyze` POST quando flag `FRAGREEL_RAILWAY_BACKUP=1` está setada
(Sprint I.6 futuro). Por enquanto, local-only.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger("fragreel.local_matches_store")

# Sprint v5.7.12 (08/05/2026 Mathieu spec): "Schema versioning em
# match_doc pra auto-invalidate cache cross-version (PC diag suggestion)".
#
# History de bumps:
#   v1 (initial): match_doc Sprint I.5 — id, map, date, score, highlights[]
#   v2 (07/05): + Sprint #6.5 (POV cuts, victim_steamid/name, kill_tick,
#               aesthetic_score/style, bomb_planted_timestamp)
#   v3 (08/05): + v0.6.52 client victim_name event-row capture (kill.victim_name
#               agora populated diretamente, não só via roster lookup);
#               + v0.7.0 scorer bomb_action_timestamp 2-tier fallback
#               (campo agora populated em rounds com bomb_event sem steamid)
#   v4 (09/05): + v0.7.1 scorer orphan bomb attribution heuristic
#               (defuse/plant_won detection quando state.bomb_*_by vazio,
#               via fallback "user side + won + orphan event"). Match docs
#               v3 podem ter bomb_action=null em rounds onde user defusou
#               mas parser falhou attribution — re-score com v0.7.1 vai
#               populate corretamente.
#   v5 (09/05): + v0.7.2 scorer score_ct_at_round / score_t_at_round per
#               highlight (computed acumulado pré-round). Editor HUD
#               agora mostra placar correto AT THAT round em vez de
#               match.score final repetido em todos.
#   v6 (09/05): + v5.7.18 — winner_team agora chega na wire format
#               (api_client.rounds[].winner_team). Match docs v5 escoraram
#               com winner_team=undefined → score_ct_at_round caía no
#               fallback "user_won false ⇒ outro time venceu" que falha
#               pra rounds sem user kill (Pro Demo Picker). Re-score com
#               v6 popula score_ct_at_round CORRETO em todos rounds.
#   v7 (10/05): + v5.7.18 round 4 (Mathieu 6ª iteração defuse + 3ª 7×0):
#               v6 ainda deixava 7×0 stuck pra HLTV pro demos onde
#               _parse_round_winners falhava em metade dos round_end
#               events (winner=null pra rounds T-side). Scorer agora
#               deriva winner_team_inferred de last-kill por round +
#               bomb_events fallback (cobre ~95% rounds mesmo se parser
#               não conseguir). Force re-score via schema bump.
MATCH_DOC_SCHEMA_VERSION = "v7"


# ── Storage path ──────────────────────────────────────────────────────────────


def _matches_dir() -> Path:
    """Pasta persistente pra match docs JSON.

    Windows: %APPDATA%/FragReel/matches/
    Outros: ~/.fragreel/matches/
    """
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", Path.home())) / "FragReel"
    else:
        base = Path.home() / ".fragreel"
    matches = base / "matches"
    matches.mkdir(parents=True, exist_ok=True)
    return matches


# ── Public API ────────────────────────────────────────────────────────────────


def save_match(match_id: str, match_doc: dict) -> Path:
    """Salva match_doc em disco pra ser servido por /matches/{id}.

    Idempotente — sobrescreve se já existe (caso user re-mapeie).
    Atomic write: escreve em <id>.json.tmp + rename pra evitar corrupção
    em concurrent reads.

    Returns:
        Path do arquivo salvo.
    """
    if not match_id:
        raise ValueError("match_id vazio")
    if not isinstance(match_doc, dict):
        raise TypeError(f"match_doc deve ser dict, got {type(match_doc).__name__}")

    target = _matches_dir() / f"{match_id}.json"
    tmp = target.with_suffix(".json.tmp")

    # Add metadata pra debugging + cleanup + schema versioning
    # (Sprint v5.7.12 — auto-invalidate caches cross-version)
    match_doc_with_meta = {
        **match_doc,
        "_schema_version": MATCH_DOC_SCHEMA_VERSION,
        "_local_saved_at": time.time(),
        "_local_saved_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    payload = json.dumps(match_doc_with_meta, ensure_ascii=False, indent=2)
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(target)  # atomic rename
    log.info(
        "save_match: %s.json (%d bytes, %d highlights)",
        match_id, len(payload), len(match_doc.get("highlights", [])),
    )
    return target


def load_match(match_id: str) -> Optional[dict]:
    """Carrega match_doc pelo id. Returns None se não existe/corrompido/STALE.

    Sprint v5.7.12 (Mathieu spec): valida `_schema_version` contra
    MATCH_DOC_SCHEMA_VERSION. Se mismatch (cache foi gravado com schema
    antigo, antes de adicionarmos campos novos), retorna None pra
    forçar re-score com scorer atual. Web então cai no fluxo
    AutoReanalyze que dispara /api/score fresh.

    NÃO inclui as chaves `_schema_version` / `_local_saved_at*` no output
    (são metadata interna).
    """
    if not match_id:
        return None
    target = _matches_dir() / f"{match_id}.json"
    if not target.exists():
        return None
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
        # Schema validation — invalida automaticamente caches stale
        cached_version = data.get("_schema_version")
        if cached_version != MATCH_DOC_SCHEMA_VERSION:
            log.info(
                "load_match %s: schema mismatch (cached=%s, expected=%s) — "
                "invalidating, will trigger re-score",
                match_id, cached_version, MATCH_DOC_SCHEMA_VERSION,
            )
            # Não deleta automaticamente — Mathieu pode preferir investigar
            # docs antigos. Cleanup viria por cleanup_old_matches() em 30d.
            return None
        # Strip metadata interna
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except (json.JSONDecodeError, OSError) as e:
        log.warning("load_match %s: %s", match_id, e)
        return None


def list_matches() -> list[dict]:
    """Lista summary de todos os matches locais (sorted por data desc).

    Cada entry: chaves de "list view" (id, map, date, score, highlights_count,
    top_play, rating, kd, status, _local_saved_at, is_stale). NÃO inclui
    highlights[] pra response ser leve.

    Sprint v5.7.12: `is_stale` indica que o match_doc foi gravado com
    schema antigo. UI pode mostrar badge "Re-score required" se quiser —
    quando user clica, load_match retorna None automaticamente e flow
    AutoReanalyze re-scora.
    """
    summaries: list[dict] = []
    for path in sorted(
        _matches_dir().glob("*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    ):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            cached_version = data.get("_schema_version")
            is_stale = cached_version != MATCH_DOC_SCHEMA_VERSION
            summaries.append({
                "id": data.get("id"),
                "map": data.get("map", "unknown"),
                "date": data.get("date"),
                "score": data.get("score", "—"),
                "side": data.get("side", "ct"),
                "status": data.get("status", "parsed"),
                "highlights_count": data.get("highlights_count", 0),
                "top_play": data.get("top_play", "—"),
                "rating": data.get("rating", "1.00"),
                "kd": data.get("kd", "—"),
                "scoring_source": data.get("scoring_source"),
                "_local_saved_at": data.get("_local_saved_at"),
                "is_stale": is_stale,
                "schema_version": cached_version or "v0",
            })
        except (json.JSONDecodeError, OSError) as e:
            log.warning("list_matches: skipping corrupted %s: %s", path.name, e)
    return summaries


def delete_match(match_id: str) -> bool:
    """Remove match_doc do disco. Returns True se deletou."""
    if not match_id:
        return False
    target = _matches_dir() / f"{match_id}.json"
    if not target.exists():
        return False
    try:
        target.unlink()
        log.info("delete_match: %s", match_id)
        return True
    except OSError as e:
        log.warning("delete_match %s: %s", match_id, e)
        return False


def cleanup_old_matches(*, max_age_days: int = 30) -> int:
    """Remove matches > max_age_days old. Roda no boot do cliente.

    Mantém disco limpo. User raramente revisita match > 1 mês old. Se
    precisar, podem re-mapear a demo (que ainda existe em disco).

    Returns: número de matches removidos.
    """
    cutoff = time.time() - (max_age_days * 86400)
    removed = 0
    for path in _matches_dir().glob("*.json"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        except OSError as e:
            log.warning("cleanup_old_matches: %s: %s", path.name, e)
    if removed > 0:
        log.info("cleanup_old_matches: removed %d matches > %d days old", removed, max_age_days)
    return removed
