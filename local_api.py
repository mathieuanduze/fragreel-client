"""
Local HTTP API — exposta em 127.0.0.1:5775 só pra que a web (fragreel.vercel.app)
consiga ver as demos no PC do usuário e disparar upload on-demand.

Endpoints:
  GET  /health                      → ping
  GET  /demos                       → lista (cache em memória; ?refresh=1 força re-scan)
  POST /demos/{sha}/upload          → enfileira upload da demo
  GET  /jobs/{sha}                  → status do job (queued/uploading/done/failed)

CORS: liberado só pra fragreel.vercel.app + http://localhost:3000 (dev).
"""
from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request
from flask_cors import CORS

from scanner import scan_all
from uploader import UploadQueue
from version import __version__ as CLIENT_VERSION

log = logging.getLogger("fragreel.local_api")

import re

ALLOWED_ORIGINS = [
    "https://fragreel.vercel.app",
    re.compile(r"^https://.*\.vercel\.app$"),
    re.compile(r"^http://(localhost|127\.0\.0\.1):\d+$"),
]


def create_app(steamid: str, demo_dirs: list[Path], queue: UploadQueue) -> Flask:
    app = Flask(__name__)
    CORS(app, origins=ALLOWED_ORIGINS)

    # Estado do scan — atualizado pelo background thread, lido pelo /demos.
    # `scan_done` vira True depois do PRIMEIRO scan completo (sucesso ou erro).
    state: dict = {
        "matches": [],
        "scanning": False,
        "scan_done": False,
        "scan_error": None,
    }
    state_lock = threading.Lock()

    def _bg_scan():
        with state_lock:
            if state["scanning"]:
                log.info("[bg-scan] ja rodando, pulando")
                return
            state["scanning"] = True
            state["scan_error"] = None
        log.info("[bg-scan] iniciando…")
        try:
            matches = scan_all(demo_dirs, steamid)
            data = [m.to_dict() for m in matches]
            with state_lock:
                state["matches"] = data
            log.info(f"[bg-scan] OK — {len(data)} demos do usuario")
        except BaseException as e:
            import traceback
            log.error(f"[bg-scan] CRASH: {type(e).__name__}: {e}")
            log.error(traceback.format_exc())
            with state_lock:
                state["scan_error"] = f"{type(e).__name__}: {e}"
        finally:
            with state_lock:
                state["scanning"] = False
                state["scan_done"] = True
            log.info("[bg-scan] terminado")

    @app.get("/health")
    def health():
        return {
            "ok": True,
            "steamid": steamid,
            "dirs": [str(d) for d in demo_dirs],
            "version": CLIENT_VERSION,
        }

    @app.get("/version")
    def version():
        return {"version": CLIENT_VERSION}

    @app.get("/demos")
    def demos():
        refresh = request.args.get("refresh") == "1"
        with state_lock:
            need_scan = refresh or (not state["scan_done"] and not state["scanning"])
            snapshot = {
                "matches": list(state["matches"]),
                "scanning": state["scanning"] or need_scan,
                "scan_done": state["scan_done"],
                "error": state["scan_error"],
            }
        if need_scan:
            log.info(f"/demos — disparando bg-scan (refresh={refresh}, scan_done={snapshot['scan_done']})")
            threading.Thread(target=_bg_scan, daemon=True, name="bg-scan").start()
        return jsonify(snapshot)

    @app.post("/demos/<sha>/upload")
    def trigger_upload(sha: str):
        # Lê do mesmo state do /demos — refatoramos pro modelo async em
        # v0.1.6 mas esquecemos de atualizar essa função (estava usando
        # _cache que não existe mais → NameError).
        with state_lock:
            matches = list(state["matches"])
        match = next((m for m in matches if m["sha1"] == sha), None)
        if not match:
            return {"error": "demo_not_found"}, 404
        path = Path(match["demo_path"])
        if not path.exists():
            return {"error": "file_missing"}, 410
        ok = queue.enqueue(path, source="web")
        if not ok:
            existing = queue.get_job(sha)
            if existing:
                return jsonify(existing), 200
            return {"error": "could_not_enqueue"}, 409
        return jsonify(queue.get_job(sha) or {"event": "queued", "sha": sha}), 202

    @app.get("/jobs/<sha>")
    def job_status(sha: str):
        job = queue.get_job(sha)
        if not job:
            return {"error": "no_such_job"}, 404
        return jsonify(job)

    return app


def serve(
    steamid: str,
    demo_dirs: list[Path],
    queue: UploadQueue,
    host: str = "127.0.0.1",
    port: int = 5775,
    stop_event: Optional[threading.Event] = None,
) -> threading.Thread:
    """Inicia o servidor numa thread daemon e retorna a thread."""
    app = create_app(steamid, demo_dirs, queue)

    def _run():
        from werkzeug.serving import make_server
        server = make_server(host, port, app, threaded=True)
        log.info(f"Local API rodando em http://{host}:{port}")
        if stop_event:
            t = threading.Thread(target=lambda: (stop_event.wait(), server.shutdown()), daemon=True)
            t.start()
        server.serve_forever()

    thread = threading.Thread(target=_run, daemon=True, name="fragreel-local-api")
    thread.start()
    return thread
