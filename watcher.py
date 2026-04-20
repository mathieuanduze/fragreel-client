"""
File watcher — monitors the CS2 demo folder and triggers processing
whenever a new .dem file is fully written.
"""
import time
import threading
import logging
from pathlib import Path

import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

from config import API_URL, MIN_DEMO_BYTES, POLL_INTERVAL
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


def _upload_demo(demo_path: Path, steamid: str) -> None:
    log.info(f"Uploading demo: {demo_path.name}")
    notify("FragReel", f"Nova partida detectada: {demo_path.stem}. Processando highlights...")

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

        match_id = data.get("match_id") or demo_path.stem
        if data.get("status") in ("parsed", "queued"):
            notify(
                "FragReel · Highlights prontos!",
                f"{data.get('highlights', '?')} highlights encontrados. Clique para ver.",
                match_id=match_id,
            )
            open_match(match_id)

    except requests.exceptions.ConnectionError:
        log.error("FragReel API offline. Demo será reprocessada quando o servidor voltar.")
        notify("FragReel", "API offline. Verifique sua conexão.")
    except Exception as e:
        log.error(f"Upload failed: {e}")
        notify("FragReel", f"Erro ao processar demo: {e}")


class DemoHandler(FileSystemEventHandler):
    def __init__(self, steamid: str):
        self.steamid = steamid
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
                    _upload_demo(path, self.steamid)
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


def watch(demo_dir: Path, steamid: str) -> None:
    log.info(f"Watching: {demo_dir} | SteamID: {steamid}")
    notify("FragReel", f"Monitorando demos em {demo_dir.name}. Pode jogar!")

    handler = DemoHandler(steamid=steamid)
    observer = Observer()
    observer.schedule(handler, str(demo_dir), recursive=False)
    observer.start()

    try:
        while observer.is_alive():
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        observer.stop()
        observer.join()
        log.info("Watcher stopped.")
