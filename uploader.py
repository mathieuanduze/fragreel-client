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
"""
from __future__ import annotations

import hashlib
import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import requests

from config import API_URL, MIN_DEMO_BYTES
from scanner import _sha1_quick, mark_processed, is_already_processed

log = logging.getLogger("fragreel.uploader")

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
        self.on_event = on_event or (lambda _e, _p: None)
        self._queue: queue.Queue[UploadJob] = queue.Queue()
        self._worker: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._enqueued_paths: set[str] = set()
        self._lock = threading.Lock()

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
            if is_already_processed(demo_path):
                log.info(f"Pulando (já processada antes): {demo_path.name}")
                self.on_event("skipped", {"path": str(demo_path), "reason": "already_processed"})
                return False
            self._enqueued_paths.add(key)

        job = UploadJob(demo_path=demo_path, steamid=self.steamid, source=source)
        self._queue.put(job)
        position = self._queue.qsize()
        log.info(f"Enfileirado [{source}]: {demo_path.name} (posição {position})")
        self.on_event("queued", {"path": str(demo_path), "position": position, "source": source})
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

    def _process(self, job: UploadJob) -> None:
        path = job.demo_path
        if not path.exists():
            log.warning(f"Sumiu antes de uploadar: {path}")
            return

        # Aguarda o arquivo estabilizar (CS2 pode estar terminando de escrever)
        if job.source == "watcher" and not _is_stable(path):
            log.warning(f"Não estabilizou: {path.name}")
            self.on_event("failed", {"path": str(path), "error": "file_not_stable"})
            return

        for attempt in range(1, MAX_RETRIES + 1):
            job.attempts = attempt
            self.on_event("uploading", {"path": str(path), "attempt": attempt})
            t0 = time.time()
            try:
                with path.open("rb") as f:
                    resp = requests.post(
                        f"{API_URL}/demo/upload",
                        files={"file": (path.name, f, "application/octet-stream")},
                        params={"steamid": self.steamid},
                        timeout=180,
                    )
                resp.raise_for_status()
                data = resp.json()
                match_id = data.get("match_id") or path.stem
                highlights = data.get("highlights", 0)

                # Marca no cache pra nunca mais re-uploadar
                mark_processed(_sha1_quick(path), match_id)

                duration = round(time.time() - t0, 1)
                log.info(f"✓ {path.name} → match_id={match_id} ({highlights} highlights, {duration}s)")
                self.on_event("done", {
                    "path": str(path),
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
                # 4xx é definitivo — não retenta
                if 400 <= e.response.status_code < 500:
                    msg = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
                    log.error(f"Falha definitiva em {path.name}: {msg}")
                    self.on_event("failed", {"path": str(path), "error": msg})
                    return
                log.warning(f"HTTP {e.response.status_code} (tentativa {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)
            except Exception as e:
                log.error(f"Erro em {path.name}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)

        # Esgotou retries
        self.on_event("failed", {"path": str(path), "error": "max_retries_exceeded"})


def _is_stable(path: Path, wait: float = 2.0) -> bool:
    """True quando o tamanho do arquivo não muda durante `wait` segundos."""
    try:
        a = path.stat().st_size
        time.sleep(wait)
        b = path.stat().st_size
        return a == b and b >= MIN_DEMO_BYTES
    except FileNotFoundError:
        return False
