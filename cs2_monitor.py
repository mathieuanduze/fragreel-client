"""
CS2 Monitor — detecta quando cs2.exe inicia e para.

Chama on_start() quando o processo aparece e on_stop() quando some.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

log = logging.getLogger("fragreel.cs2monitor")

CS2_EXE      = "cs2.exe"
POLL_INTERVAL = 5.0   # segundos entre verificações


def _is_cs2_running() -> bool:
    try:
        import psutil
        for proc in psutil.process_iter(["name"]):
            name = (proc.info.get("name") or "").lower()
            if name == CS2_EXE:
                return True
    except Exception:
        pass
    return False


def monitor_cs2(
    on_start: Callable,
    on_stop: Callable,
    stop_event: Optional[threading.Event] = None,
) -> None:
    """
    Loop de monitoramento. Bloqueia até stop_event ser setado (ou KeyboardInterrupt).
    Deve ser chamado de uma thread daemon.
    """
    was_running = False
    log.info("Monitor CS2 iniciado.")

    while not (stop_event and stop_event.is_set()):
        running = _is_cs2_running()

        if running and not was_running:
            log.info("CS2 detectado! Iniciando gravação...")
            try:
                on_start()
            except Exception as e:
                log.error(f"on_start() falhou: {e}")

        elif not running and was_running:
            log.info("CS2 fechado. Parando gravação...")
            try:
                on_stop()
            except Exception as e:
                log.error(f"on_stop() falhou: {e}")

        was_running = running
        time.sleep(POLL_INTERVAL)


def start_monitor_thread(
    on_start: Callable,
    on_stop: Callable,
    stop_event: Optional[threading.Event] = None,
) -> threading.Thread:
    t = threading.Thread(
        target=monitor_cs2,
        args=(on_start, on_stop, stop_event),
        daemon=True,
        name="cs2-monitor",
    )
    t.start()
    return t
