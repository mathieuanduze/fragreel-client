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

Schema do match_doc: ver `api_client.parse_and_score_locally()` docstring.
Espelha o que Railway retornava em `/matches/{id}`.

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

    # Add metadata pra debugging + cleanup
    match_doc_with_meta = {
        **match_doc,
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
    """Carrega match_doc pelo id. Returns None se não existe/corrompido.

    NÃO inclui as chaves `_local_saved_at*` no output (são metadata interna).
    """
    if not match_id:
        return None
    target = _matches_dir() / f"{match_id}.json"
    if not target.exists():
        return None
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
        # Strip metadata interna
        return {k: v for k, v in data.items() if not k.startswith("_local_")}
    except (json.JSONDecodeError, OSError) as e:
        log.warning("load_match %s: %s", match_id, e)
        return None


def list_matches() -> list[dict]:
    """Lista summary de todos os matches locais (sorted por data desc).

    Cada entry: chaves de "list view" (id, map, date, score, highlights_count,
    top_play, rating, kd, status, _local_saved_at). NÃO inclui highlights[]
    pra response ser leve.
    """
    summaries: list[dict] = []
    for path in sorted(
        _matches_dir().glob("*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    ):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
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
