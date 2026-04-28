"""
Upload queue — serializa uploads de .dem para a API do FragReel.

Por que uma fila?
  - Se o scan retroativo encontra 12 partidas antigas, não dá pra subir
    todas em paralelo — satura a banda do usuário e o servidor.
  - O regime normal também: o jogador pode terminar 2 partidas seguidas
    (rare mas possível em FFA / DM); melhor processar em ordem.

Uma única thread de worker consome a fila. Cada item é uploaded com retry
e, ao terminar, marca a demo no cache do scanner para nunca mais ser
re-processada.

Sprint I.4 (28/04, validation cross-check): se env FRAGREEL_API_CROSS_CHECK=1,
após receber resposta do Railway com highlights (Python scorer), passa
events parseados pelo Railway pro fragreel.gg/api/score (TS scorer) e
compara highlights field-a-field. Loga divergências em fragreel.log pra
detectar drift entre os dois scorers em produção real, ANTES de migrar
pro TS scorer no Sprint I.5.
"""
from __future__ import annotations

import hashlib
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import requests

from config import API_URL, MIN_DEMO_BYTES
from scanner import (
    _sha1_quick,
    get_cached_processing,
    is_already_processed,
    mark_processed,
)

log = logging.getLogger("fragreel.uploader")

# Sprint I.4 — cross-check opt-in flag. Default OFF: zero impact em
# produção. Ative com FRAGREEL_API_CROSS_CHECK=1 pra validação.
_CROSS_CHECK_ENABLED = os.environ.get("FRAGREEL_API_CROSS_CHECK", "").lower() in ("1", "true", "yes")

# Sprint I.5 — full migration flag. Quando ON, cliente NÃO uploads .dem
# pro Railway — parseia local + chama Vercel /api/score + salva match doc
# em ~/.fragreel/matches/ (servido por local_api /matches/{id}).
# Default OFF inicialmente (rollout gradual). User opt-in via env var,
# ou default ON em v0.5.x quando estável.
_USE_API_ENABLED = os.environ.get("FRAGREEL_USE_API", "").lower() in ("1", "true", "yes")

MAX_RETRIES = 3
RETRY_BACKOFF = 5  # segundos entre tentativas


@dataclass
class UploadJob:
    demo_path: Path
    steamid: str
    source: str = "watcher"   # "watcher" | "scan_retroativo"
    attempts: int = 0
    enqueued_at: float = field(default_factory=time.time)


# Callback assinaturas — main.py / tray.py registram pra reagir a mudanças
OnEvent = Callable[[str, dict], None]


class UploadQueue:
    """
    Fila thread-safe com um único worker.

    Uso:
        q = UploadQueue(steamid="765...", on_event=cb)
        q.start()
        q.enqueue(Path("partida.dem"))
        ...
        q.stop()

    Eventos emitidos via on_event(event_name, payload):
      - "queued"     {path, position}
      - "uploading"  {path, attempt}
      - "done"       {path, match_id, highlights, duration_s}
      - "skipped"    {path, reason}    # já processada, não-pertence-ao-user, etc
      - "failed"     {path, error}
      - "idle"       {}                # fila esvaziou
    """

    def __init__(self, steamid: str, on_event: Optional[OnEvent] = None):
        self.steamid = steamid
        self._user_on_event = on_event or (lambda _e, _p: None)
        self._queue: queue.Queue[UploadJob] = queue.Queue()
        self._worker: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._enqueued_paths: set[str] = set()
        self._lock = threading.Lock()
        # Job status por sha1 — consultado pelo /jobs/{sha} do local_api
        self._jobs: dict[str, dict] = {}

    def on_event(self, event: str, payload: dict) -> None:
        """Wrapper que atualiza self._jobs antes de chamar o callback do user."""
        sha = payload.get("sha")
        if sha:
            with self._lock:
                self._jobs[sha] = {**self._jobs.get(sha, {}), "event": event, **payload}
        self._user_on_event(event, payload)

    def get_job(self, sha: str) -> Optional[dict]:
        with self._lock:
            return dict(self._jobs[sha]) if sha in self._jobs else None

    def force_store_job(self, sha: str, payload: dict) -> None:
        """Store a job entry keyed by the CALLER's sha, bypassing the sha
        computation that `on_event()` does from `payload["sha"]`.

        Exists because of the v0.2.16 cache-HIT robustness fix (Bug #6v3).
        The web calls POST /demos/<sha>/upload using the sha produced by
        scanner (the one it saw in the /demos list). In most cases that
        matches what `_sha1_quick(path)` returns inside enqueue(), but if
        the file changed between scan and enqueue (e.g. CS2 appended to a
        live demo, AV scan touched mtime, filesystem metadata drift), the
        internal recomputation would emit `done` keyed under a DIFFERENT
        sha than the one the frontend polls via GET /jobs/<URL_sha>. End
        result: frontend polls forever on "Iniciando análise…".

        Solution: the /demos/<sha>/upload handler force-stores the done
        payload under the URL sha directly, so the subsequent poll always
        hits a live entry. The `sha` inside the payload itself is kept as
        whatever the caller passed (for downstream consumers that read it)
        — but the DICT KEY is the URL sha.
        """
        with self._lock:
            self._jobs[sha] = dict(payload)

    # ── API pública ──────────────────────────────────────────────────────

    def start(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stop.clear()
        self._worker = threading.Thread(target=self._run, daemon=True, name="fragreel-uploader")
        self._worker.start()
        log.info("Upload worker iniciado.")

    def stop(self) -> None:
        self._stop.set()
        # Sentinel pra desbloquear o get()
        self._queue.put(None)  # type: ignore[arg-type]
        if self._worker:
            self._worker.join(timeout=5)
        log.info("Upload worker parado.")

    def enqueue(self, demo_path: Path, source: str = "watcher") -> bool:
        """Adiciona uma demo à fila. Retorna False se duplicada/inválida."""
        key = str(demo_path.resolve())
        with self._lock:
            if key in self._enqueued_paths:
                log.debug(f"Já na fila: {demo_path.name}")
                return False
            try:
                size = demo_path.stat().st_size
            except FileNotFoundError:
                return False
            if size < MIN_DEMO_BYTES:
                return False
            sha = _sha1_quick(demo_path)
            cached = get_cached_processing(demo_path)
            if cached:
                # v0.2.15 Bug #6v2: cache HIT branch.
                #
                # The scanner cache knows this demo was uploaded before and
                # has a server-side match_id. Before this fix, we emitted
                # `skipped` and bailed — which was correct for the watcher /
                # scan_retroativo paths (nothing was waiting), but broke the
                # web path: when the user clicks "Mapear plays" from the
                # browser, the AnalyzeModal polls /jobs/{sha} for `done` and
                # would hang forever on "Iniciando análise…" because the
                # terminal event was `skipped`, not `done`.
                #
                # Fix: when the caller is the web (user-initiated
                # re-analyze), emit `done` immediately with the cached
                # match_id so the modal can unlock + redirect to /match/{id}
                # as soon as the ad timer finishes. For the watcher /
                # scan_retroativo paths, keep the old `skipped` semantic
                # since nothing is polling.
                if source == "web":
                    match_id = cached.get("match_id")
                    highlights = int(cached.get("highlights") or 0)
                    log.info(
                        "cache HIT [web]: %s → match_id=%s (highlights=%d, skipping re-upload)",
                        demo_path.name, match_id, highlights,
                    )
                    self.on_event("done", {
                        "path": str(demo_path),
                        "sha": sha,
                        "match_id": match_id,
                        "highlights": highlights,
                        "duration_s": 0.0,
                        "cache_hit": True,
                    })
                else:
                    log.info(f"Pulando (já processada antes): {demo_path.name}")
                    self.on_event("skipped", {
                        "path": str(demo_path),
                        "sha": sha,
                        "reason": "already_processed",
                    })
                return False
            self._enqueued_paths.add(key)

        job = UploadJob(demo_path=demo_path, steamid=self.steamid, source=source)
        self._queue.put(job)
        position = self._queue.qsize()
        log.info(f"Enfileirado [{source}]: {demo_path.name} (posição {position})")
        self.on_event("queued", {"path": str(demo_path), "sha": sha, "position": position, "source": source})
        return True

    def pending(self) -> int:
        return self._queue.qsize()

    # ── Worker ───────────────────────────────────────────────────────────

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                job = self._queue.get(timeout=1)
            except queue.Empty:
                continue
            if job is None:
                break

            try:
                self._process(job)
            except Exception as e:
                log.exception(f"Worker crash em {job.demo_path.name}: {e}")
            finally:
                with self._lock:
                    self._enqueued_paths.discard(str(job.demo_path.resolve()))
                if self._queue.empty():
                    self.on_event("idle", {})

    def _process_local_api(
        self, job: UploadJob, path: Path, sha: str,
    ) -> None:
        """Sprint I.5 (28/04 noite): pipeline LOCAL — parse + score API + save local.

        Substitui o flow Railway upload quando FRAGREEL_USE_API=1.
        Failures aqui são propagadas pro caller (que faz fallback Railway).

        Steps:
        1. api_client.parse_and_score_locally(path, steamid) → match_doc
           (parseia .dem local + chama /api/score TS + builds match doc)
        2. local_matches_store.save_match(match_id, match_doc) → disco
        3. mark_processed(sha, match_id, highlights) → cache scanner
        4. self.on_event("done", ...) → tray + UI feedback

        Sem upload de bytes pro Railway. Toda a inteligência vem da API
        (scorer.ts) ou fallback offline_lite. Match doc fica em
        ~/.fragreel/matches/<match_id>.json, servido por
        local_api.py /matches/{id}.
        """
        from api_client import parse_and_score_locally
        from local_matches_store import save_match

        self.on_event("uploading", {"path": str(path), "sha": sha, "attempt": 1})
        t0 = time.time()

        match_doc = parse_and_score_locally(path, self.steamid)

        match_id = match_doc.get("id") or path.stem
        highlights_count = match_doc.get("highlights_count", 0)
        scoring_source = match_doc.get("scoring_source", "unknown")

        # Salva match_doc em disco pra local_api /matches/{id} servir
        save_match(match_id, match_doc)

        # Cache scanner (compat com Bug #6v2 cache-hit fast path)
        mark_processed(sha, match_id, highlights=highlights_count)

        duration = round(time.time() - t0, 1)
        log.info(
            "✓ Sprint I.5: %s → match_id=%s (%d highlights, source=%s, %ds total)",
            path.name, match_id, highlights_count, scoring_source, duration,
        )
        self.on_event("done", {
            "path": str(path),
            "sha": sha,
            "match_id": match_id,
            "highlights": highlights_count,
            "duration_s": duration,
            "scoring_source": scoring_source,
        })

    def _process(self, job: UploadJob) -> None:
        path = job.demo_path
        sha = _sha1_quick(path) if path.exists() else ""
        if not path.exists():
            log.warning(f"Sumiu antes de uploadar: {path}")
            return

        if job.source == "watcher" and not _is_stable(path):
            log.warning(f"Não estabilizou: {path.name}")
            self.on_event("failed", {"path": str(path), "sha": sha, "error": "file_not_stable"})
            return

        # Sprint I.5 — branch arquitetural por flag opt-in.
        # Default (legacy): upload .dem inteiro pro Railway, scoring server-side.
        # FRAGREEL_USE_API=1 (novo): parse local + Vercel /api/score + save local.
        if _USE_API_ENABLED:
            try:
                self._process_local_api(job, path, sha)
            except Exception as e:
                log.error(
                    "Sprint I.5: parse_and_score_locally falhou pra %s: %s — "
                    "FALLBACK pro Railway upload",
                    path.name, e,
                )
                # Fallback gracioso: se Sprint I.5 path falha, tenta Railway
                # (preserva UX enquanto migração estabiliza)
            else:
                return  # sucesso Sprint I.5, skip Railway upload

        for attempt in range(1, MAX_RETRIES + 1):
            job.attempts = attempt
            self.on_event("uploading", {"path": str(path), "sha": sha, "attempt": attempt})
            t0 = time.time()
            try:
                with path.open("rb") as f:
                    # Sprint I.4: include_events=true quando cross-check opt-in
                    # ativo. Railway responde com events parseados pra cliente
                    # comparar com /api/score (TS scorer) em paralelo.
                    params = {"steamid": self.steamid}
                    if _CROSS_CHECK_ENABLED:
                        params["include_events"] = "true"
                    resp = requests.post(
                        f"{API_URL}/demo/analyze",
                        files={"file": (path.name, f, "application/octet-stream")},
                        params=params,
                        timeout=180,
                    )
                resp.raise_for_status()
                data = resp.json()
                match_id = data.get("match_id") or path.stem
                highlights = data.get("highlights", 0)

                # v0.2.15 Bug #6v2: cache highlights count so the cache-HIT
                # fast path in enqueue() can emit a `done` with the real
                # number instead of 0.
                mark_processed(sha, match_id, highlights=highlights)

                duration = round(time.time() - t0, 1)
                log.info(f"✓ {path.name} → match_id={match_id} ({highlights} highlights, {duration}s)")

                # Sprint I.4 cross-check (silencioso, não-disruptivo):
                # compara Railway Python scorer vs Vercel TS scorer pra
                # detectar drift em produção. Failures aqui são logged mas
                # não afetam o fluxo principal — flag opt-in via env var.
                if _CROSS_CHECK_ENABLED and "events" in data:
                    try:
                        _run_api_cross_check(
                            data, match_id=match_id, demo_basename=path.name
                        )
                    except Exception as e:
                        log.warning(
                            "Sprint I.4 cross-check raised (non-fatal): %s: %s",
                            type(e).__name__, e,
                        )

                self.on_event("done", {
                    "path": str(path),
                    "sha": sha,
                    "match_id": match_id,
                    "highlights": highlights,
                    "duration_s": duration,
                })
                return

            except requests.exceptions.ConnectionError:
                log.warning(f"API offline (tentativa {attempt}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)
            except requests.exceptions.HTTPError as e:
                if 400 <= e.response.status_code < 500:
                    msg = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
                    log.error(f"Falha definitiva em {path.name}: {msg}")
                    self.on_event("failed", {"path": str(path), "sha": sha, "error": msg})
                    return
                log.warning(f"HTTP {e.response.status_code} (tentativa {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)
            except Exception as e:
                log.error(f"Erro em {path.name}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)

        self.on_event("failed", {"path": str(path), "sha": sha, "error": "max_retries_exceeded"})


def _is_stable(path: Path, wait: float = 2.0) -> bool:
    """True quando o tamanho do arquivo não muda durante `wait` segundos."""
    try:
        a = path.stat().st_size
        time.sleep(wait)
        b = path.stat().st_size
        return a == b and b >= MIN_DEMO_BYTES
    except FileNotFoundError:
        return False


# ── Sprint I.4 — Cross-check Railway vs Vercel TS scorer ──────────────────────


def _run_api_cross_check(
    railway_data: dict,
    *,
    match_id: str,
    demo_basename: str,
) -> None:
    """Sprint I.4 (28/04): compara Railway Python scorer vs Vercel TS scorer.

    Recebe `railway_data` (resposta do /demo/analyze com `?include_events=true`),
    extrai os events parseados pelo Railway, manda pro fragreel.gg/api/score
    (TS scorer), e compara highlights field-a-field. Loga divergências.

    Não bloqueia o fluxo principal — silent failure se API down ou response
    incompatível. Objetivo é COLETAR DATA em produção pra validar que
    portar pro TS scorer (Sprint I.5) é seguro.

    Args:
        railway_data: dict da resposta do POST /demo/analyze (inclui events,
            demo_meta, player_steamid quando ?include_events=true)
        match_id: pra logging/correlação
        demo_basename: pra logging
    """
    events = railway_data.get("events")
    demo_meta = railway_data.get("demo_meta")
    player_steamid = railway_data.get("player_steamid")
    if not events or not demo_meta or not player_steamid:
        log.debug(
            "Sprint I.4 cross-check skip (match=%s): payload incompleto. "
            "events=%s demo_meta=%s player_steamid=%s",
            match_id, bool(events), bool(demo_meta), bool(player_steamid),
        )
        return

    # Build payload pro Vercel /api/score
    api_payload = {
        "schema_version": "1",
        "client_version": _client_version(),
        "demo_meta": demo_meta,
        "player_steamid": player_steamid,
        "events": events,
    }

    api_url = os.environ.get("FRAGREEL_API_URL", "https://fragreel.gg").rstrip("/")
    score_endpoint = f"{api_url}/api/score"

    t0 = time.time()
    try:
        resp = requests.post(
            score_endpoint,
            json=api_payload,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
    except requests.RequestException as e:
        log.warning(
            "Sprint I.4 cross-check (match=%s): API call failed: %s",
            match_id, e,
        )
        return

    api_call_ms = int((time.time() - t0) * 1000)

    if resp.status_code != 200:
        log.warning(
            "Sprint I.4 cross-check (match=%s): /api/score HTTP %d: %s",
            match_id, resp.status_code, resp.text[:300],
        )
        return

    api_response = resp.json()
    api_highlights = api_response.get("highlights", [])
    scorer_version = api_response.get("scorer_version", "unknown")

    # Get Railway highlights from a separate call to /matches/{id} —
    # mais limpo do que tentar parsear o resumo do /demo/analyze
    # que só tem o count, não os highlights inteiros.
    try:
        match_resp = requests.get(
            f"{API_URL}/matches/{match_id}",
            timeout=10,
        )
        match_resp.raise_for_status()
        match_data = match_resp.json()
        railway_highlights = match_data.get("highlights", [])
    except requests.RequestException as e:
        log.warning(
            "Sprint I.4 cross-check (match=%s): falhou buscar highlights "
            "do Railway pra comparar: %s",
            match_id, e,
        )
        return

    # Comparação minimalista — campos críticos
    diffs = _compare_highlights(railway_highlights, api_highlights)

    if not diffs:
        log.info(
            "Sprint I.4 cross-check ✅ MATCH (match=%s, demo=%s, "
            "api_ms=%d, scorer=%s, n_highlights=%d): Railway Python == Vercel TS",
            match_id, demo_basename, api_call_ms, scorer_version,
            len(railway_highlights),
        )
    else:
        log.warning(
            "Sprint I.4 cross-check ❌ DIVERGENCE (match=%s, demo=%s, "
            "api_ms=%d, scorer=%s, n_diffs=%d): Railway Python ≠ Vercel TS",
            match_id, demo_basename, api_call_ms, scorer_version, len(diffs),
        )
        for diff in diffs[:10]:  # Cap pra não inflacionar log
            log.warning("  diff: %s", diff)
        if len(diffs) > 10:
            log.warning("  ... and %d more diffs (truncated)", len(diffs) - 10)


def _compare_highlights(
    railway_highlights: list[dict],
    api_highlights: list[dict],
) -> list[str]:
    """Compara highlights field-a-field. Retorna lista de strings descrevendo
    divergências. Vazia = bit-exact match.

    Compara campos críticos: rank, round_num, score, label, narrative,
    clutch_situation, won_round, bomb_action, is_round_winning_kill.
    Tolerância 0.05 pra floats (start/end timestamps).
    """
    diffs: list[str] = []

    if len(railway_highlights) != len(api_highlights):
        diffs.append(
            f"count mismatch: railway={len(railway_highlights)} "
            f"vs api={len(api_highlights)}"
        )
        return diffs  # Sem comparação detalhada se contagem difere

    fields_exact = (
        "rank", "round_num", "label", "narrative",
        "clutch_situation", "won_round", "bomb_action",
        "is_round_winning_kill",
    )
    fields_float = ("score", "start", "end")

    for i, (rwy, api) in enumerate(zip(railway_highlights, api_highlights)):
        for f in fields_exact:
            rv = rwy.get(f)
            av = api.get(f)
            if rv != av:
                diffs.append(
                    f"#{i+1} R{rwy.get('round_num')} {f}: "
                    f"railway={rv!r} vs api={av!r}"
                )
        for f in fields_float:
            rv = rwy.get(f)
            av = api.get(f)
            if rv is None or av is None:
                if rv != av:
                    diffs.append(f"#{i+1} {f}: railway={rv} vs api={av}")
                continue
            try:
                if abs(float(rv) - float(av)) > 0.05:
                    diffs.append(
                        f"#{i+1} R{rwy.get('round_num')} {f}: "
                        f"railway={rv} vs api={av} (Δ={abs(float(rv) - float(av)):.3f})"
                    )
            except (TypeError, ValueError):
                pass

    return diffs


def _client_version() -> str:
    """Lê __version__ do version.py pra incluir no payload do /api/score."""
    try:
        from version import __version__
        return __version__
    except Exception:
        return "unknown"
