"""
File watcher — monitora TODAS as pastas onde o CS2 pode salvar demos:
  - ...\\Counter-Strike Global Offensive\\game\\csgo\\          (auto-record + match730_*.dem baixadas via "Watch")
  - ...\\Counter-Strike Global Offensive\\game\\csgo\\replays\\  (replays salvos pelo UI)
  - ~/Downloads                                                 (demos baixadas do HLTV/FACEIT)

Sempre que um novo .dem completa escrita, enfileira na UploadQueue.
O filtro por SteamID e a dedup por hash ficam no uploader/scanner — o
watcher só detecta e enfileira.
"""
from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Optional

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

from config import POLL_INTERVAL
from uploader import UploadQueue

log = logging.getLogger("fragreel.watcher")


class DemoHandler(FileSystemEventHandler):
    """Handler único compartilhado entre todos os Observers — a fila é 1 só."""

    def __init__(self, queue: UploadQueue):
        self.queue = queue

    def _enqueue(self, path: Path) -> None:
        if path.suffix.lower() != ".dem":
            return
        log.info(f"Novo .dem detectado: {path}")
        # O uploader espera o arquivo estabilizar antes de subir — aqui só enfileira.
        self.queue.enqueue(path, source="watcher")

    def on_created(self, event: FileCreatedEvent) -> None:
        if not event.is_directory:
            self._enqueue(Path(event.src_path))

    def on_moved(self, event: FileMovedEvent) -> None:
        # CS2 às vezes escreve num arquivo temp e renomeia para .dem
        if not event.is_directory:
            self._enqueue(Path(event.dest_path))


def watch(
    demo_dirs: list[Path],
    queue: UploadQueue,
    stop_event: Optional[threading.Event] = None,
) -> None:
    """
    Inicia um watchdog.Observer por pasta. Todos compartilham o mesmo
    handler e a mesma UploadQueue, então a ordem de upload é global
    (não por pasta).
    """
    if not demo_dirs:
        log.error("Nenhuma pasta para monitorar.")
        return

    handler = DemoHandler(queue=queue)
    observer = Observer()
    watched: list[Path] = []

    for d in demo_dirs:
        try:
            if not d.exists():
                log.warning(f"Pasta inexistente, pulando: {d}")
                continue
            observer.schedule(handler, str(d), recursive=False)
            watched.append(d)
            log.info(f"Monitorando: {d}")
        except Exception as e:
            log.error(f"Falha ao monitorar {d}: {e}")

    if not watched:
        log.error("Nenhuma pasta monitorada com sucesso.")
        return

    observer.start()
    log.info(f"Watcher ativo em {len(watched)} pastas.")

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
        log.info("Watcher parado.")
