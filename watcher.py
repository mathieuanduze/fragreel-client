"""
File watcher — monitors the CS2 demo folder and triggers processing
whenever a new .dem file is fully written.

Após o upload:
  1. API parseia a demo e retorna highlights com timestamps
  2. Se o Recorder estiver ativo, extrai os clipes de vídeo automaticamente
  3. Clipes salvos em ~/Videos/FragReel/{match_id}/
"""
import time
import threading
import logging
from pathlib import Path
from typing import Optional

import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

from config import API_URL, MIN_DEMO_BYTES, POLL_INTERVAL, CLIPS_DIR
from notifier import notify, open_match

log = logging.getLogger("fragreel.watcher")


def _is_ready(path: Path) -> bool:
    """Return True when the file is fully written (size stable for 2s)."""
    try:
        size_a = path.stat().st_size
        time.sleep(2)
        size_b = path.stat().st_size
        return size_a == size_b and size_b >= MIN_DEMO_BYTES
    except FileNotFoundError:
        return False


def _upload_demo(
    demo_path: Path,
    steamid: str,
    recorder: "Optional[object]" = None,
) -> None:
    log.info(f"Uploading demo: {demo_path.name}")
    notify("FragReel", f"Nova partida detectada! Analisando highlights...")

    try:
        with demo_path.open("rb") as f:
            resp = requests.post(
                f"{API_URL}/demo/upload",
                files={"file": (demo_path.name, f, "application/octet-stream")},
                params={"steamid": steamid},
                timeout=120,
            )
        resp.raise_for_status()
        data = resp.json()
        log.info(f"API response: {data}")

        match_id   = data.get("match_id") or demo_path.stem
        n_hlights  = data.get("highlights", 0)
        status     = data.get("status")

        if status not in ("parsed", "queued"):
            return

        # ── Extrair e fazer upload dos clipes ──────────────────────────────
        clips: list[Path] = []
        if recorder is not None and getattr(recorder, "is_recording", False) and n_hlights > 0:
            try:
                highlights = _fetch_highlights(match_id)
                if highlights:
                    match_dur = _estimate_match_duration(highlights)
                    out_dir   = CLIPS_DIR / match_id
                    clips     = recorder.extract_clips(highlights, match_dur, out_dir)  # type: ignore[attr-defined]
                    if clips:
                        _upload_clips(match_id, clips)
            except Exception as e:
                log.error(f"Erro ao extrair/enviar clipes: {e}")

        # ── Notificação ─────────────────────────────────────────────────────
        if clips:
            notify(
                "FragReel · Clipes prontos! 🎬",
                f"{len(clips)} clipes salvos. Abrindo no site...",
                match_id=match_id,
            )
        elif n_hlights > 0:
            notify(
                "FragReel · Highlights prontos!",
                f"{n_hlights} highlights encontrados em {data.get('map','?')}.",
                match_id=match_id,
            )
        else:
            notify("FragReel", "Demo processada. Nenhum highlight encontrado.")

        open_match(match_id)

    except requests.exceptions.ConnectionError:
        log.error("FragReel API offline.")
        notify("FragReel", "API offline. Verifique sua conexão.")
    except Exception as e:
        log.error(f"Upload failed: {e}")
        notify("FragReel", f"Erro ao processar demo: {e}")


def _fetch_highlights(match_id: str) -> list[dict]:
    """Busca os highlights da API com retry (parse pode demorar alguns segundos)."""
    for attempt in range(6):
        try:
            r = requests.get(f"{API_URL}/matches/{match_id}", timeout=15)
            r.raise_for_status()
            data = r.json()
            if data.get("highlights"):
                return data["highlights"]
        except Exception:
            pass
        time.sleep(5)
    return []


def _upload_clips(match_id: str, clips: list[Path]) -> None:
    """Faz upload dos clipes extraídos para a API."""
    log.info(f"Enviando {len(clips)} clipes para a API...")
    uploaded = 0
    for clip in clips:
        try:
            with clip.open("rb") as f:
                resp = requests.post(
                    f"{API_URL}/clips/{match_id}",
                    files={"file": (clip.name, f, "video/mp4")},
                    timeout=300,
                )
            resp.raise_for_status()
            uploaded += 1
            log.info(f"Clipe enviado: {clip.name}")
        except Exception as e:
            log.error(f"Falha ao enviar {clip.name}: {e}")
    log.info(f"{uploaded}/{len(clips)} clipes enviados com sucesso.")


def _estimate_match_duration(highlights: list[dict]) -> float:
    """Usa o timestamp mais alto dos highlights como estimativa de duração da partida."""
    if not highlights:
        return 1800.0   # 30 min fallback
    max_end = max(h.get("end", 0) for h in highlights)
    return max_end + 60.0   # +60s de margem após o último highlight


class DemoHandler(FileSystemEventHandler):
    def __init__(self, steamid: str, recorder: "Optional[object]" = None):
        self.steamid   = steamid
        self.recorder  = recorder
        self._processing: set[str] = set()
        self._lock = threading.Lock()

    def _handle(self, path: Path) -> None:
        key = str(path)
        with self._lock:
            if key in self._processing:
                return
            self._processing.add(key)

        def run():
            try:
                if _is_ready(path):
                    _upload_demo(path, self.steamid, recorder=self.recorder)
            finally:
                with self._lock:
                    self._processing.discard(key)

        threading.Thread(target=run, daemon=True).start()

    def on_created(self, event: FileCreatedEvent) -> None:
        if not event.is_directory and str(event.src_path).endswith(".dem"):
            self._handle(Path(event.src_path))

    def on_moved(self, event: FileMovedEvent) -> None:
        # CS2 sometimes writes to a temp file then renames to .dem
        if not event.is_directory and str(event.dest_path).endswith(".dem"):
            self._handle(Path(event.dest_path))


def watch(
    demo_dir: Path,
    steamid: str,
    stop_event=None,
    recorder: "Optional[object]" = None,
) -> None:
    log.info(f"Watching: {demo_dir} | SteamID: {steamid}")
    rec_status = "Gravação ativa 🔴" if recorder else "Sem gravação"
    notify("FragReel", f"Monitorando CS2. {rec_status}. Pode jogar!")

    handler = DemoHandler(steamid=steamid, recorder=recorder)
    observer = Observer()
    observer.schedule(handler, str(demo_dir), recursive=False)
    observer.start()

    try:
        while observer.is_alive():
            if stop_event and stop_event.is_set():
                break
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        observer.stop()
        observer.join()
        log.info("Watcher stopped.")
