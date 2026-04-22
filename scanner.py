"""
Retroactive scanner — encontra todas as demos nas pastas candidatas,
filtra só as que têm o SteamID do usuário, e retorna metadata básica
pra construir a tela de "partidas antigas encontradas".

Estratégia:
  1. Para cada pasta candidata, lista .dem com mtime descendente
  2. Pula arquivos muito pequenos (<50KB) ou já processados (via hash cache)
  3. Parseia cada .dem com demoparser2 só o suficiente pra:
     - Confirmar que o SteamID do usuário está na partida
     - Extrair mapa, placar, data, kills do jogador
  4. Retorna lista ordenada por mtime

Cache: `~/.fragreel/scanned.json` — mapa de {sha1: {match_id, skipped_reason}}
para não re-escanear demos já vistas.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

log = logging.getLogger("fragreel.scanner")

CACHE_DIR = Path.home() / ".fragreel"
CACHE_FILE = CACHE_DIR / "scanned.json"
CACHE_VERSION = 2           # bump pra invalidar entries antigas (ex: skipped sem demoparser2)
MIN_SIZE = 50 * 1024        # 50KB — abaixo disso é arquivo temp ou corrompido
MAX_SCAN_PER_RUN = 50       # limite pra primeira execução não travar


@dataclass
class ScannedMatch:
    """Metadata de uma demo escaneada que pertence ao usuário."""
    demo_path: str
    sha1: str
    mtime: float           # epoch segundos
    map_name: str
    score_ct: int
    score_t: int
    player_kills: int
    player_deaths: int
    size_mb: float

    def to_dict(self) -> dict:
        return asdict(self)


def _sha1_quick(path: Path) -> str:
    """Hash do primeiro + último MB do arquivo — suficiente pra deduplicar demos
    (colisão entre demos reais é praticamente impossível)."""
    h = hashlib.sha1()
    h.update(str(path.stat().st_size).encode())
    try:
        with path.open("rb") as f:
            h.update(f.read(1024 * 1024))
            size = path.stat().st_size
            if size > 2 * 1024 * 1024:
                f.seek(-1024 * 1024, 2)
                h.update(f.read(1024 * 1024))
    except Exception:
        pass
    return h.hexdigest()


def _load_cache() -> dict[str, dict]:
    if not CACHE_FILE.exists():
        return {}
    try:
        raw = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        # Filtra entries de versão antiga (ex: skipped por demoparser2 ausente em v0.1.1)
        return {k: v for k, v in raw.items() if v.get("v") == CACHE_VERSION}
    except Exception:
        return {}


def _save_cache(cache: dict[str, dict]) -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    # Garante que toda entry tem o version stamp
    for v in cache.values():
        v.setdefault("v", CACHE_VERSION)
    CACHE_FILE.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def _parse_demo_summary(path: Path, steamid: str) -> Optional[ScannedMatch]:
    """Tenta extrair metadata da demo. Retorna None se:
      - Parse falhar
      - SteamID não estiver na demo
    """
    try:
        from demoparser2 import DemoParser  # type: ignore
    except ImportError:
        log.error("demoparser2 NÃO INSTALADO — nenhuma demo será detectada. "
                  "Build do .exe está incompleto (faltou demoparser2 nos requirements/spec).")
        return None

    try:
        parser = DemoParser(str(path))

        # player_death é barato e já traz tudo que precisamos
        events = parser.parse_event(
            "player_death",
            player=["name", "steamid"],
            other=["total_rounds_played"],
        )

        # Compat pandas / polars / list
        if hasattr(events, "to_dict"):
            rows = events.to_dict(orient="records") if hasattr(events, "to_dict") else list(events)
            if isinstance(rows, dict):
                # polars retorna dict de colunas
                cols = rows
                n = len(next(iter(cols.values()))) if cols else 0
                rows = [{k: cols[k][i] for k in cols} for i in range(n)]
        else:
            rows = list(events)

        if not rows:
            return None

        # SteamID do usuário está nessa demo?
        player_in_match = any(
            str(r.get("attacker_steamid")) == steamid or str(r.get("user_steamid")) == steamid
            for r in rows
        )
        if not player_in_match:
            return None

        # Stats do jogador
        kills = sum(1 for r in rows if str(r.get("attacker_steamid")) == steamid)
        deaths = sum(1 for r in rows if str(r.get("user_steamid")) == steamid)

        # Mapa + rounds
        header = parser.parse_header()
        map_name = header.get("map_name", "unknown") if isinstance(header, dict) else "unknown"

        max_round = 0
        for r in rows:
            rv = r.get("total_rounds_played") or 0
            if isinstance(rv, (int, float)) and rv > max_round:
                max_round = int(rv)

        # Placar estimado: round_end seria ideal mas pode ser caro; usa 0-0 como fallback
        # (a tela de scan mostra o placar só se tiver; caso contrário, mostra só rounds)
        score_ct, score_t = 0, 0
        try:
            round_end = parser.parse_event("round_end")
            if hasattr(round_end, "to_dict"):
                re_rows = round_end.to_dict(orient="records")
                if re_rows:
                    score_ct = sum(1 for r in re_rows if r.get("winner") in (3, "CT"))
                    score_t = sum(1 for r in re_rows if r.get("winner") in (2, "T"))
        except Exception:
            pass

        return ScannedMatch(
            demo_path=str(path),
            sha1=_sha1_quick(path),
            mtime=path.stat().st_mtime,
            map_name=map_name,
            score_ct=score_ct,
            score_t=score_t,
            player_kills=kills,
            player_deaths=deaths,
            size_mb=round(path.stat().st_size / (1024 * 1024), 1),
        )
    except Exception as e:
        log.warning(f"Parse falhou para {path.name}: {e}")
        return None


def scan_all(
    demo_dirs: list[Path],
    steamid: str,
    max_results: int = MAX_SCAN_PER_RUN,
) -> list[ScannedMatch]:
    """
    Scan retroativo completo.

    Retorna lista de ScannedMatch ordenada por mtime descendente (mais recentes primeiro),
    filtrada para só incluir demos que:
      - Têm pelo menos 50KB
      - Contêm o SteamID do usuário
      - Não estão no cache como já processadas

    Max `max_results` demos por execução (evita travar se o usuário tem 500 demos antigas).
    """
    cache = _load_cache()
    cache_hits = 0
    candidates: list[Path] = []

    # Coletar .dem de todas as pastas
    for d in demo_dirs:
        try:
            for p in d.glob("*.dem"):
                if p.stat().st_size >= MIN_SIZE:
                    candidates.append(p)
        except Exception as e:
            log.warning(f"Falha ao listar {d}: {e}")

    # Ordenar por mtime descendente
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    results: list[ScannedMatch] = []
    t0 = time.time()

    for p in candidates:
        if len(results) >= max_results:
            log.info(f"Limite de {max_results} atingido — parando scan")
            break

        sha = _sha1_quick(p)
        if sha in cache:
            cache_hits += 1
            cached = cache[sha]
            if cached.get("match_id"):
                # Já processada
                continue
            if cached.get("skipped_reason"):
                # Já sabemos que é de outra pessoa
                continue

        match = _parse_demo_summary(p, steamid)
        if match:
            results.append(match)
            # Não marca como processada ainda — só quando usuário confirmar upload
        else:
            # Marca no cache pra não re-parsear
            cache[sha] = {"skipped_reason": "not_user_demo_or_parse_failed"}

    _save_cache(cache)
    log.info(
        f"Scan completo: {len(candidates)} demos encontradas, "
        f"{len(results)} do usuário, {cache_hits} cache hits, "
        f"{round(time.time() - t0, 1)}s"
    )
    return results


def mark_processed(sha1: str, match_id: str) -> None:
    """Depois do upload bem-sucedido, marca essa demo como processada no cache."""
    cache = _load_cache()
    cache[sha1] = {"match_id": match_id, "processed_at": time.time()}
    _save_cache(cache)


def is_already_processed(path: Path) -> bool:
    """Checa rapidamente se uma demo já foi processada antes (usa cache por hash)."""
    cache = _load_cache()
    sha = _sha1_quick(path)
    entry = cache.get(sha)
    return bool(entry and entry.get("match_id"))
