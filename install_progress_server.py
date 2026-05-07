"""
install_progress_server.py — Sprint Install Indicator B (06/05).

Minimal HTTP server pra responder /install-status e /version DURANTE
first-run setup, antes do Flask full subir.

Por que não usar Flask aqui? First-run setup pode demorar 30-90s,
queremos que web detecte client rodando IMEDIATAMENTE (ms após click
do .exe). Flask + dependências pesadas adicionam ~1-3s ao boot. Stdlib
http.server é instant.

Pós-setup: este server é PARADO em main.py e o Flask full toma o port
5775. Flask também expõe /install-status retornando ready=True.

Web pollar:
  GET http://127.0.0.1:5775/install-status
  → 200 { phase, phase_label, components, elapsed_sec, ready }
  → 503 só se servidor caiu (raro)

CORS pra fragreel.gg consumir.
"""
from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from install_state import get_state

log = logging.getLogger("fragreel.install_progress_server")


class _InstallProgressHandler(BaseHTTPRequestHandler):
    """Handler minimal — 3 endpoints, JSON responses, CORS open."""

    def log_message(self, format: str, *args: object) -> None:
        # Suprime stderr noise — Werkzeug-like (via fragreel logger só)
        try:
            log.debug("install_progress: " + format, *args)
        except Exception:
            pass

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler API)
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?")[0]
        if path == "/health":
            self._send_json(200, {"ok": True, "phase": "setup"})
        elif path == "/install-status":
            self._send_json(200, get_state())
        elif path == "/version":
            # Tenta ler version.py — fallback pra "v0.0.0-setup" pra
            # web detectar client running mesmo sem version conhecida.
            version = "v0.0.0-setup"
            try:
                from version import __version__
                version = __version__
            except Exception:
                pass
            self._send_json(200, {"version": version, "setup_in_progress": True})
        else:
            # Outros endpoints retornam 503 — Flask full vai responder
            # quando setup terminar.
            self._send_json(503, {
                "error": "setup_in_progress",
                "message": "Client ainda está inicializando. Aguarde o setup terminar.",
            })


def start_progress_server(port: int = 5775) -> tuple[HTTPServer, threading.Thread]:
    """Inicia servidor HTTP em background thread.

    Retorna (server, thread) — caller deve chamar server.shutdown() pra
    parar antes de subir o Flask full.
    """
    server = HTTPServer(("127.0.0.1", port), _InstallProgressHandler)
    server.allow_reuse_address = True
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name="fragreel-install-progress-server",
    )
    thread.start()
    log.info(
        "install_progress_server: ouvindo em http://127.0.0.1:%d "
        "(pre-setup HTTP)",
        port,
    )
    return server, thread


def stop_progress_server(server: HTTPServer) -> None:
    """Para o servidor + libera port pra Flask full assumir.

    Thread eventually exits via daemon flag mesmo se shutdown demorar.
    """
    try:
        server.shutdown()
        server.server_close()
        log.info("install_progress_server: parado, port liberado")
    except Exception as e:
        log.warning("install_progress_server: shutdown falhou (não-fatal): %s", e)
