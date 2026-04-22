"""
Local HTTP API — exposta em 127.0.0.1:5775 só pra que a web (fragreel.vercel.app)
consiga ver as demos no PC do usuário e disparar upload + render on-demand.

Endpoints:
  GET  /health                      → ping
  GET  /demos                       → lista (cache em memória; ?refresh=1 força re-scan)
  POST /demos/{sha}/upload          → enfileira upload da demo
  GET  /jobs/{sha}                  → status do job (queued/uploading/done/failed)
  POST /render                      → kicks off a HLAE capture + encode pipeline
                                       body: {demo_path, segments:[{start_tick,end_tick}...]}
  GET  /render/status               → current render progress (polled by AdModal)
  POST /render/cancel               → abort the active render, kill CS2

CORS: liberado só pra fragreel.vercel.app + http://localhost:3000 (dev).
"""
from __future__ import annotations

import logging
import threading
import uuid
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request
from flask_cors import CORS

from hlae_runner import HlaeRunnerConfig, RenderPlan
from render_coordinator import RenderCoordinator
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


def create_app(
    steamid: str,
    demo_dirs: list[Path],
    queue: UploadQueue,
    render_coordinator: Optional[RenderCoordinator] = None,
) -> Flask:
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

    # ── Render endpoints (HLAE capture pipeline) ───────────────────────

    @app.get("/render/preflight")
    def render_preflight():
        """Quick readiness check the web calls BEFORE showing the ad.
        Returns {ready: true} if the user can render now, or
        {ready: false, reason: "cs2_running"|"render_in_progress"} so
        the web can show a friendly prompt instead of wasting an ad-watch."""
        if render_coordinator is None:
            return {"ready": False, "reason": "render_not_configured"}, 503
        return jsonify(render_coordinator.preflight())

    @app.post("/render")
    def start_render():
        if render_coordinator is None:
            return {"error": "render_not_configured",
                    "detail": "CS2 install or HLAE dir not detected on this PC"}, 503

        body = request.get_json(silent=True) or {}
        try:
            segments = [
                (int(s["start_tick"]), int(s["end_tick"]))
                for s in body.get("segments", [])
            ]
        except (KeyError, TypeError, ValueError) as e:
            return {"error": "bad_segments", "detail": str(e)}, 400
        if not segments:
            return {"error": "no_segments"}, 400

        demo_path = body.get("demo_path")
        if not demo_path:
            return {"error": "missing_demo_path"}, 400
        demo = Path(demo_path)
        if not demo.exists():
            return {"error": "demo_not_found", "path": str(demo)}, 404

        plan = RenderPlan(
            demo_path=demo,
            segments=tuple(segments),
            user_steamid64=body.get("user_steamid64") or steamid,
            record_name=body.get("record_name", "fragreel"),
            stream_name=body.get("stream_name", "default"),
        )

        render_id = body.get("render_id") or uuid.uuid4().hex[:12]
        force = bool(body.get("force", False))
        # `reel_props` is the full ReelProps payload from the server's
        # /matches/{id}/render-plan endpoint (match, selectedRanks, mood,
        # playerName, orientation). Runner injects per-segment .mov paths
        # into match.highlights[*].gameplayVideoSrc before calling Remotion.
        reel_props = body.get("reel_props")
        try:
            session = render_coordinator.start(
                plan,
                render_id,
                force_kill_cs2=force,
                reel_props=reel_props,
            )
        except RenderCoordinator.CS2BusyError as e:
            return {
                "error": "cs2_running",
                "detail": "Close CS2 before rendering, or POST again with {\"force\": true} to terminate it.",
                "cs2_pids": e.pids,
            }, 409
        return jsonify(session.to_dict()), 202

    @app.get("/render/status")
    def render_status():
        if render_coordinator is None:
            return {"state": "unavailable"}, 503
        current = render_coordinator.current()
        if current is None:
            return {"state": "idle"}
        return jsonify(current.to_dict())

    @app.post("/render/cancel")
    def render_cancel():
        if render_coordinator is None:
            return {"error": "render_not_configured"}, 503
        render_coordinator.cancel()
        current = render_coordinator.current()
        return jsonify(current.to_dict() if current else {"state": "idle"})

    return app


def _build_render_coordinator() -> Optional[RenderCoordinator]:
    """Auto-detect CS2 + HLAE + output dir and build a coordinator.

    On first run, downloads HLAE + ffmpeg into vendor/ via setup_vendor.
    Returns None if the PC isn't set up for rendering (e.g., dev machine
    without CS2 installed) — the endpoints then return 503 and the web UI
    degrades gracefully.
    """
    from steam_detect import _cs2_roots
    roots = _cs2_roots()
    if not roots:
        log.warning("no CS2 installation detected; render endpoints disabled")
        return None
    cs2_install = roots[0]

    # First-run vendor bootstrap. Only attempts download on Windows since
    # HLAE is a Win32 binary; on other OSes we just check for a pre-staged
    # vendor (e.g. CI builds on linux for testing the code paths).
    try:
        from setup_vendor import default_layout, ensure_vendor
        layout = default_layout()
        if not layout.is_complete():
            log.info("vendor incomplete at %s — downloading HLAE + ffmpeg", layout.vendor_root)
            ensure_vendor(layout=layout)
        hlae_dir = layout.hlae_dir
    except Exception as e:
        log.warning("setup_vendor failed (%s); render endpoints disabled", e)
        return None

    if not hlae_dir.exists():
        log.warning("vendor/hlae missing at %s; render endpoints disabled", hlae_dir)
        return None
    output_dir = Path.home() / "Desktop" / "FragReel"
    editor_dir = Path(__file__).parent.parent / "main" / "editor"
    config = HlaeRunnerConfig(cs2_install=cs2_install, hlae_dir=hlae_dir)
    return RenderCoordinator(
        config,
        output_dir=output_dir,
        editor_dir=editor_dir if editor_dir.is_dir() else None,
    )


def serve(
    steamid: str,
    demo_dirs: list[Path],
    queue: UploadQueue,
    host: str = "127.0.0.1",
    port: int = 5775,
    stop_event: Optional[threading.Event] = None,
    render_coordinator: Optional[RenderCoordinator] = None,
) -> threading.Thread:
    """Inicia o servidor numa thread daemon e retorna a thread."""
    if render_coordinator is None:
        render_coordinator = _build_render_coordinator()
    app = create_app(steamid, demo_dirs, queue, render_coordinator=render_coordinator)

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
