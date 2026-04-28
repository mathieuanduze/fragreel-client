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
  POST /render/open                 → open the rendered video in the OS default player

CORS: liberado só pra fragreel.vercel.app + http://localhost:3000 (dev).
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request
from flask_cors import CORS

from client_config import (
    clear_output_dir,
    resolve_output_dir,
    set_output_dir,
)
from hlae_runner import HlaeRunnerConfig, RenderPlan
from render_coordinator import InsufficientDiskError, RenderCoordinator
from scanner import scan_all, _load_cache as _load_scan_cache, get_cached_processing, clear_cached_processing
from uploader import UploadQueue
from version import __version__ as CLIENT_VERSION


def _open_in_os(path: Path) -> None:
    """Open a file or folder in the OS default app, cross-platform.

    Windows is the only target we ship today (FragReel.exe), but the macOS
    branch makes development on Mac actually exercise the same code path.
    Linux uses xdg-open which is best-effort.

    Raises whatever the underlying call raises so the caller can degrade
    to "open the parent folder" / surface the error to the user.
    """
    if sys.platform.startswith("win"):
        # os.startfile is the right primitive on Windows — it picks the
        # default app for files (Reprodutor de Mídia, VLC, etc) and opens
        # folders in Explorer. Doesn't block.
        os.startfile(str(path))  # type: ignore[attr-defined]
    elif sys.platform == "darwin":
        subprocess.Popen(["open", str(path)])
    else:
        subprocess.Popen(["xdg-open", str(path)])

log = logging.getLogger("fragreel.local_api")

import re

# Bug #17 (28/04): adicionado fragreel.gg (domínio próprio comprado).
# Vercel mantido pra: (a) preview deploys de PR, (b) fallback se DNS quebrar.
ALLOWED_ORIGINS = [
    "https://fragreel.gg",
    "https://www.fragreel.gg",
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

    # Chrome 120+ Private Network Access fix — v0.2.13 regression story:
    #
    # v0.2.12 used @app.after_request to inject `Access-Control-Allow-Private-
    # Network: true`. But flask-cors ALSO emits this header on preflight OPTIONS
    # (defaulting to "false" because it considers PNA an explicit opt-in), and
    # after_request handlers run in REVERSE registration order — our hook fired
    # first, flask-cors fired last, result was two headers on the same response:
    #     Access-Control-Allow-Private-Network: true    ← us
    #     Access-Control-Allow-Private-Network: false   ← flask-cors
    # Chrome reads the `false`, blocks, and the client still looks offline.
    #
    # WSGI middleware is the only layer that's guaranteed to run AFTER
    # everything Flask does, so we strip duplicates and inject a single
    # canonical `true` for origins we already trust via CORS.
    _inner_wsgi = app.wsgi_app

    def _pna_wsgi(environ, start_response):
        origin = environ.get("HTTP_ORIGIN", "")
        allow = bool(origin) and any(
            (origin == o if isinstance(o, str) else o.match(origin))
            for o in ALLOWED_ORIGINS
        )

        def _patched_start(status, headers, exc_info=None):
            # Drop any pre-existing PNA headers (flask-cors "false" is the one
            # we care about, but also any accidental duplicates from our
            # previous after_request hook if code is reloaded).
            headers = [
                (k, v) for (k, v) in headers
                if k.lower() != "access-control-allow-private-network"
            ]
            if allow:
                headers.append(("Access-Control-Allow-Private-Network", "true"))
            return start_response(status, headers, exc_info)

        return _inner_wsgi(environ, _patched_start)

    app.wsgi_app = _pna_wsgi

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
            base_matches = list(state["matches"])
            snapshot = {
                "matches": base_matches,
                "scanning": state["scanning"] or need_scan,
                "scan_done": state["scan_done"],
                "error": state["scan_error"],
            }
        if need_scan:
            log.info(f"/demos — disparando bg-scan (refresh={refresh}, scan_done={snapshot['scan_done']})")
            threading.Thread(target=_bg_scan, daemon=True, name="bg-scan").start()

        # Merge fresh upload status from disk cache. The bg-scan only runs once;
        # after it completes, mark_processed() writes match_id/processed_at to
        # scanned.json on disk but state["matches"] is never updated, so the web
        # would keep seeing match_id=null and fall through to the server fallback
        # instead of triggering local /render. Cheap re-read fixes that.
        try:
            disk_cache = _load_scan_cache()
            patched = []
            for m in snapshot["matches"]:
                entry = disk_cache.get(m.get("sha1"))
                if entry:
                    mid = entry.get("match_id")
                    pat = entry.get("processed_at")
                    if mid and not m.get("match_id"):
                        m = {**m, "match_id": mid, "processed_at": pat}
                patched.append(m)
            snapshot["matches"] = patched
        except Exception as e:
            log.warning(f"/demos — falha ao mesclar match_id do cache em disco: {e}")

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

        # Bug #10 fix V2 (28/04 — Mathieu reportou ciclo infinito):
        # ?force=true bypassa cache E invalida o match_id local stale.
        # Frontend usa quando detecta 404 no /match/{id} (server perdeu
        # dados após Railway redeploy). Sem isso o ciclo era:
        #   1. user clica "Mapear" → cache hit retorna match_id antigo
        #   2. /match/{antigo} → 404 (server perdeu)
        #   3. user volta → demo ainda mostra "processada" → loop.
        # Com ?force=true a invalidação local quebra o ciclo + dispara
        # upload real → novo match_id válido.
        force = request.args.get("force", "").lower() in ("1", "true", "yes")
        if force:
            log.info("/demos/%s/upload — force=true: invalidando cache local + re-upload", sha)
            try:
                clear_cached_processing(path)
            except Exception as e:
                log.warning("/demos/%s/upload — clear cache falhou: %s (não-fatal)", sha, e)

        # v0.2.16 Bug #6v3 (cache-hit cold-start):
        # A versão anterior confiava na rama "cache HIT [web]" dentro de
        # uploader.enqueue(), que recomputa a sha via _sha1_quick(path). Na
        # maioria dos casos isso bate com a sha da URL, MAS se o arquivo
        # mudou entre o scan e o enqueue (CS2 anexando dados em uma demo
        # ainda live, AV tocou mtime, drift de metadata), o `done` era
        # emitido sob uma sha DIFERENTE da que o frontend usa no
        # GET /jobs/<URL_sha>, e a AnalyzeModal ficava presa em
        # "Iniciando análise..." até o timeout.
        #
        # Aqui a gente resolve no handler: a URL sha é a verdade.
        # Se a cache em disco tem match_id pra ESSA sha, força o job
        # direto no dicionário sob essa sha e responde 200 imediatamente,
        # sem passar pelo enqueue/recomputação.
        # NOTA: skipar esse bloco quando force=true (já invalidamos acima).
        cached = None if force else get_cached_processing(path)
        if cached and cached.get("match_id"):
            match_id = cached["match_id"]
            highlights = int(cached.get("highlights") or 0)
            done_payload = {
                "event": "done",
                "path": str(path),
                "sha": sha,
                "match_id": match_id,
                "highlights": highlights,
                "duration_s": 0.0,
                "cache_hit": True,
            }
            log.info(
                "/demos/%s/upload — cache HIT (match_id=%s, highlights=%d) → force-store + done",
                sha, match_id, highlights,
            )
            queue.force_store_job(sha, done_payload)
            # Dispara os listeners externos (tray, etc) sem re-escrever
            # _jobs (force_store_job já fez) — on_event vai re-gravar sob
            # a sha do payload, que aqui é a MESMA URL sha, então é no-op.
            queue.on_event("done", {k: v for k, v in done_payload.items() if k != "event"})
            return jsonify(done_payload), 200

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

        # Cada raw segment carrega contexto v0.3.0-beta-2 enriquecido:
        # base = (round_start_tick, round_end_tick) (campos sempre presentes)
        # opcionais (None se ausente):
        #   kill_ticks, kill_timestamps      → cluster + spread
        #   clutch_situation                 → pad_pre += 3s flat
        #   is_round_winning_kill            → pad_post += 3s
        #   bomb_action + bomb_action_tick   → garante captura inteira da
        #                                      animação (defuse 10s no-kit /
        #                                      plant 3.2s)
        # Clients antigos ou demos pré-v0.3.0-alpha não têm campos novos e
        # caem em fallback gracioso (1 janela = round inteiro).
        try:
            raw_segments: list[dict] = []
            for s in body.get("segments", []):
                raw_segments.append({
                    "start_tick": int(s["start_tick"]),
                    "end_tick": int(s["end_tick"]),
                    "kill_ticks": [int(t) for t in (s.get("kill_ticks") or [])],
                    "kill_timestamps": [float(t) for t in (s.get("kill_timestamps") or [])],
                    "clutch_situation": s.get("clutch_situation"),
                    "is_round_winning_kill": bool(s.get("is_round_winning_kill") or False),
                    "bomb_action": s.get("bomb_action"),
                    "bomb_action_tick": int(s["bomb_action_tick"]) if s.get("bomb_action_tick") is not None else None,
                })
        except (KeyError, TypeError, ValueError) as e:
            return {"error": "bad_segments", "detail": str(e)}, 400
        if not raw_segments:
            return {"error": "no_segments"}, 400

        # v0.3.0-beta-2 — clustering scenario-aware.
        # Algoritmo final calibrado em [[v0.3 Cluster Tuning Research]] após
        # análise de 14 demos pro (173 highlight rounds). Resolve 100% dos
        # bugs de defuse/plant truncados (regra dura: animação INTEIRA
        # capturada). Constantes: PAD 7/5, GAP 22, MIN 15, clutch +3 flat,
        # RWK +3 post, bomb-merge se gap ≤ 5s.
        #
        # Continua gated por FRAGREEL_ENABLE_CLUSTERING enquanto o quirk do
        # HLAE warmup (Bug #9 no Status do Projeto) não for resolvido.
        # MIN_WINDOW=15s deveria mitigar o issue (vs 8.5s do v1), mas só
        # smoke test no PC vai confirmar. Default OFF preserva v0.2.16
        # behavior (1 segment por round, HLAE-safe).
        clustering_enabled = os.environ.get(
            "FRAGREEL_ENABLE_CLUSTERING", ""
        ).strip().lower() in ("1", "true", "yes", "on")

        from scripts.capture_script import cluster_round_kills_v2  # lazy import

        expanded: list[tuple[int, int]] = []
        clusters_count = 0
        for seg in raw_segments:
            start, end = seg["start_tick"], seg["end_tick"]
            if end <= start:
                continue
            if clustering_enabled:
                windows = cluster_round_kills_v2(
                    kill_ticks=seg["kill_ticks"],
                    kill_timestamps=seg["kill_timestamps"],
                    round_start_tick=start,
                    round_end_tick=end,
                    clutch_situation=seg["clutch_situation"],
                    is_round_winning_kill=seg["is_round_winning_kill"],
                    bomb_action=seg["bomb_action"],
                    bomb_action_tick=seg["bomb_action_tick"],
                )
                if seg["kill_ticks"]:
                    clusters_count += len(windows)
                expanded.extend(windows)
            else:
                # v0.2.16-compat: 1 segment por round (HLAE-safe).
                expanded.append((start, end))

        # The web sends segments in highlight-score order (not tick order),
        # and two highlights for kills close together can overlap by a few
        # hundred ticks. capture_script.py validates strict ascending +
        # non-overlapping and would error out as "segments overlap". Sort
        # by start_tick and greedily merge any overlap so the user gets a
        # single contiguous capture instead of an error. This ALSO catches
        # any residual overlap entre clusters de rounds adjacentes (o
        # invariante do freezetime do CS2 >= pad sum cobre o caso normal;
        # este merge é belt-and-braces).
        expanded = [(s, e) for s, e in expanded if e > s]
        expanded.sort(key=lambda se: se[0])
        segments: list[tuple[int, int]] = []
        for start, end in expanded:
            if segments and start <= segments[-1][1]:
                prev_start, prev_end = segments[-1]
                segments[-1] = (prev_start, max(prev_end, end))
            else:
                segments.append((start, end))
        if len(segments) != len(expanded):
            log.info(
                "/render — merged %d overlapping segment(s) → %d final segment(s)",
                len(expanded) - len(segments), len(segments),
            )
        if clustering_enabled and clusters_count:
            log.info(
                "/render — v0.3.0-beta-2 clustering ENABLED: %d rounds → %d cluster windows → %d final (after merge)",
                len(raw_segments), clusters_count, len(segments),
            )
        elif not clustering_enabled:
            log.info(
                "/render — v0.3.0-beta-2 clustering DISABLED (default): %d round windows → HLAE captura rounds inteiros (v0.2.16 behavior). Set FRAGREEL_ENABLE_CLUSTERING=1 pra ativar.",
                len(segments),
            )
        if not segments:
            return {"error": "no_segments"}, 400

        demo_path = body.get("demo_path")
        if not demo_path:
            return {"error": "missing_demo_path"}, 400
        demo = Path(demo_path)
        if not demo.exists():
            return {"error": "demo_not_found", "path": str(demo)}, 404

        # `reel_props` is the full ReelProps payload from the server's
        # /matches/{id}/render-plan endpoint (match, selectedRanks, mood,
        # playerName, orientation). Runner injects per-segment .mov paths
        # into match.highlights[*].gameplayVideoSrc before calling Remotion.
        reel_props = body.get("reel_props")

        # Player name precedence for spec_player camera lock:
        #   1. Explicit `user_player_name` in the request body
        #   2. `reel_props.playerName` from the server's render-plan
        # Without a name, the .cfg falls back to `spec_mode 1` only — camera
        # follows the auto-director's pick in first-person POV. Better than
        # the v0.2.5..v0.2.10 bug, where the wrong spec_mode (4 = roaming/
        # static) made the camera sit at the spawn point even when the
        # spec_player target was correct.
        user_player_name = body.get("user_player_name")
        if not user_player_name and isinstance(reel_props, dict):
            user_player_name = reel_props.get("playerName")

        plan = RenderPlan(
            demo_path=demo,
            segments=tuple(segments),
            user_steamid64=body.get("user_steamid64") or steamid,
            user_player_name=user_player_name,
            record_name=body.get("record_name", "fragreel"),
            stream_name=body.get("stream_name", "default"),
            # Round 4c Fase 1.21 — x-ray opt-in. Web envia bool no payload.
            show_xray=bool(body.get("show_xray", False)),
        )

        render_id = body.get("render_id") or uuid.uuid4().hex[:12]
        force = bool(body.get("force", False))
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
        except InsufficientDiskError as e:
            # 507 = "Insufficient Storage" (WebDAV but used widely for this).
            # Surface the per-drive breakdown so the web can show "free up
            # X GB on C:" instead of a generic error.
            return {
                "error": "insufficient_disk",
                "detail": str(e),
                "issues": e.issues,
            }, 507
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

    @app.post("/render/open")
    def render_open():
        """Open the most recently rendered output in the OS default player.

        The web "Abrir FragReel" CTA hits this so the user doesn't have to
        copy a path and paste it in Explorer. Browsers can't invoke
        `os.startfile` on local paths (file:// to a binary triggers download
        UX, not "open in app"), so we proxy through the local client.

        Output preference (most polished → fallback):
          1. session.output_mp4    — Remotion's final h264 MP4
          2. session.output_movs[0] — first ProRes segment (won't play in
                                      Windows Media Player but might in VLC)
          3. session.output_mov    — legacy single-take field
          4. <fallback> open the parent folder so the user can pick by hand

        Returns {opened, path, kind, reason?} so the web can show the
        path-copy chip when we couldn't open the file directly (e.g. on
        non-Windows hosts or when the file vanished between status and open).
        """
        if render_coordinator is None:
            return {"opened": False, "path": None, "kind": None,
                    "reason": "render_not_configured"}, 503
        current = render_coordinator.current()
        if current is None:
            return {"opened": False, "path": None, "kind": None,
                    "reason": "no render has run yet"}, 404

        # Pick the best available output. Falling back through the list keeps
        # this endpoint useful even when Remotion is skipped (which is the
        # case today inside the .exe — see editor_dir bug in render_coordinator).
        candidates: list[Path] = []
        if getattr(current, "output_mp4", None):
            candidates.append(Path(current.output_mp4))
        for mov in getattr(current, "output_movs", None) or []:
            candidates.append(Path(mov))
        if getattr(current, "output_mov", None):
            candidates.append(Path(current.output_mov))

        target_file = next((p for p in candidates if p.exists()), None)
        if target_file is not None:
            try:
                _open_in_os(target_file)
                return jsonify({
                    "opened": True,
                    "path": str(target_file),
                    "kind": "file",
                })
            except Exception as e:
                log.warning("could not open %s: %s — falling back to folder", target_file, e)

        # Fallback: open the parent dir so the user can at least find the file.
        parent_dir: Optional[Path] = None
        for p in candidates:
            if p.parent.exists():
                parent_dir = p.parent
                break
        if parent_dir is None:
            return {"opened": False, "path": None, "kind": None,
                    "reason": "no output file or folder exists yet"}, 404
        try:
            _open_in_os(parent_dir)
            return jsonify({
                "opened": True,
                "path": str(parent_dir),
                "kind": "folder",
                "reason": "opened parent folder (no playable file)",
            })
        except Exception as e:
            return {"opened": False, "path": str(parent_dir), "kind": None,
                    "reason": f"could not open folder: {e}"}, 500

    # ── Auto-update (v0.2.11+) ─────────────────────────────────────────
    #
    # User pediu no v0.2.10 testing: "Não daria pra fazer isto
    # automaticamente ao baixar a nova versão do client?". Implementação:
    #   1. /update baixa o novo .exe pra %TEMP%
    #   2. Cria um helper .bat que: espera o PID atual morrer → move o
    #      .exe novo pro lugar do antigo → relança
    #   3. Spawn o .bat detachado, agenda os.exit() em 2s
    #   4. Frontend faz polling em /version e detecta a versão nova
    #      voltando online (~5-15s no total)
    #
    # Limitações:
    #   - Só roda no .exe (PyInstaller frozen). Em dev (python main.py)
    #     retorna 501 — atualizar Python source é responsabilidade do dev.
    #   - Só Windows. macOS/Linux dev volta 501.
    #   - Não verifica assinatura / checksum. Se um atacante MITM o
    #     tráfego HTTPS do GitHub, dá pra injetar binário arbitrário.
    #     Mitigação aceitável hoje porque o atacante já precisaria do
    #     mesmo MITM pra trojanar o download manual via /download. Quando
    #     SignPath signing entrar de verdade, dá pra adicionar verificação
    #     de Authenticode aqui antes do swap.

    UPDATE_URL = (
        "https://github.com/mathieuanduze/fragreel-client/releases/latest/download/FragReel.exe"
    )

    @app.post("/update")
    def trigger_update():
        """Download the latest .exe and spawn a helper that swaps + relaunches.

        Returns 202 with `{started: true, ...}` on success. The Python
        process exits ~2s later — the frontend should poll `/version`
        until the new version answers (typically 5-15s end-to-end).
        """
        # Hard guards: only frozen Windows builds can self-update.
        if not getattr(sys, "frozen", False):
            return {
                "error": "not_frozen",
                "detail": "auto-update only works in the packaged .exe",
            }, 501
        if not sys.platform.startswith("win"):
            return {
                "error": "unsupported_platform",
                "detail": f"auto-update is Windows-only (got {sys.platform})",
            }, 501

        current_exe = Path(sys.executable)
        current_pid = os.getpid()

        # Download into %TEMP%. We use a deterministic name (with PID) so a
        # half-finished file from a previous attempt gets overwritten cleanly.
        new_exe_path = Path(tempfile.gettempdir()) / f"FragReel-update-{current_pid}.exe"

        log.info("auto-update: downloading %s -> %s", UPDATE_URL, new_exe_path)
        try:
            # urllib.request handles redirects (GitHub redirects to a CDN
            # URL with the actual binary). 5-min timeout — slow connections
            # need it; the .exe is ~30-50 MB.
            req = urllib.request.Request(
                UPDATE_URL,
                headers={"User-Agent": f"FragReel-client/{CLIENT_VERSION}"},
            )
            with urllib.request.urlopen(req, timeout=300) as resp, \
                 open(new_exe_path, "wb") as out:
                while True:
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
        except Exception as e:
            log.exception("auto-update: download failed")
            try:
                if new_exe_path.exists():
                    new_exe_path.unlink()
            except OSError:
                pass
            return {"error": "download_failed", "detail": str(e)}, 502

        # Sanity check — anything < 5 MB is almost certainly an error page
        # or partial download. Real .exe is ~30-50 MB.
        size = new_exe_path.stat().st_size
        if size < 5 * 1024 * 1024:
            log.error("auto-update: downloaded file too small (%d bytes)", size)
            try:
                new_exe_path.unlink()
            except OSError:
                pass
            return {
                "error": "download_too_small",
                "detail": f"downloaded only {size} bytes — likely an error page, not the .exe",
            }, 502

        # Build the swap+relaunch helper. Has to be a .bat (or PowerShell)
        # because we need to outlive the Python process — once Python exits,
        # the .exe lock releases and the bat can move the new file in.
        bat_path = Path(tempfile.gettempdir()) / f"FragReel-update-{current_pid}.bat"
        bat_log = Path(tempfile.gettempdir()) / f"FragReel-update-{current_pid}.log"

        # v0.2.15 Bug #5 fix — belt & suspenders swap.
        #
        # v0.2.14 field test: the swap `move /Y` occasionally failed silently
        # and the user ended up on the OLD version, thinking the update worked.
        # Root causes identified:
        #   (a) A stray FragReel.exe process (orphan from a previous crash,
        #       duplicate tray instance, or child spawned by hlae/ffmpeg that
        #       re-exec'd fragreel somehow) was still holding a lock on the
        #       .exe even after OUR PID exited.
        #   (b) Windows Defender's real-time scan opens a read lock on the
        #       freshly-written .exe for a few seconds after it lands in
        #       %TEMP%. The old 2-second timeout wasn't enough on slower
        #       machines.
        #   (c) Single `move` attempt with no retry meant any transient lock
        #       skipped straight to the fallback-launch-staging branch.
        #   (d) No log of what happened inside the bat, so debugging was blind.
        #
        # This version:
        #   1. Waits for our PID (unchanged).
        #   2. Runs `taskkill /F /IM FragReel.exe /T` (recursive tree kill) to
        #      sweep any remaining FragReel processes regardless of origin.
        #   3. Sleeps 5s (was 2s) to let Windows Defender release its read lock.
        #   4. Attempts `move` up to 3 times with 2s between attempts.
        #   5. Logs every step to %TEMP%\FragReel-update-<PID>.log — left in
        #      place on exit so post-mortems are possible.
        bat_content = (
            f"@echo off\r\n"
            f"REM FragReel auto-update helper (PID {current_pid})\r\n"
            f"REM v0.2.15 Bug #5 fix — taskkill /T + 5s grace + 3x retry + log\r\n"
            f'set "LOGFILE={bat_log}"\r\n'
            f'echo [%date% %time%] update helper started for PID {current_pid} > "%LOGFILE%"\r\n'
            f'echo [%date% %time%] new_exe={new_exe_path} >> "%LOGFILE%"\r\n'
            f'echo [%date% %time%] current_exe={current_exe} >> "%LOGFILE%"\r\n'
            f":wait_loop\r\n"
            f'tasklist /FI "PID eq {current_pid}" 2>NUL | find /I "{current_pid}" >NUL\r\n'
            f"if not errorlevel 1 (\r\n"
            f"  timeout /t 1 /nobreak >NUL\r\n"
            f"  goto wait_loop\r\n"
            f")\r\n"
            f'echo [%date% %time%] PID {current_pid} gone — running taskkill /F /T on any remaining FragReel.exe >> "%LOGFILE%"\r\n'
            f'taskkill /F /IM FragReel.exe /T >> "%LOGFILE%" 2>&1\r\n'
            f'echo [%date% %time%] sleeping 5s for AV scan / file lock release >> "%LOGFILE%"\r\n'
            f"timeout /t 5 /nobreak >NUL\r\n"
            f'set "ATTEMPT=1"\r\n'
            f":swap_retry\r\n"
            f'echo [%date% %time%] swap attempt %ATTEMPT%/3 >> "%LOGFILE%"\r\n'
            f'move /Y "{new_exe_path}" "{current_exe}" >> "%LOGFILE%" 2>&1\r\n'
            f"if not errorlevel 1 goto swap_ok\r\n"
            f'if "%ATTEMPT%"=="3" (\r\n'
            f'  echo [%date% %time%] swap FAILED after 3 attempts — launching staging copy as fallback >> "%LOGFILE%"\r\n'
            f'  start "" "{new_exe_path}"\r\n'
            f"  exit /b 1\r\n"
            f")\r\n"
            f'echo [%date% %time%] attempt %ATTEMPT% failed — retrying in 2s >> "%LOGFILE%"\r\n'
            f"timeout /t 2 /nobreak >NUL\r\n"
            f"set /a ATTEMPT+=1\r\n"
            f"goto swap_retry\r\n"
            f":swap_ok\r\n"
            f'echo [%date% %time%] swap OK — waiting 3s for AV to finish scanning the new .exe >> "%LOGFILE%"\r\n'
            f"REM v0.2.16 Bug #7 fix — auto-update launch quirk.\r\n"
            f"REM v0.2.15 field report: depois do swap, o novo .exe demorava 2-3min\r\n"
            f"REM para começar a escutar a porta 5775. Causas identificadas:\r\n"
            f"REM   (a) Windows Defender re-escaneia o binário recém-movido e mantém\r\n"
            f"REM       um lock de leitura por alguns segundos DEPOIS do move. Se o\r\n"
            f"REM       `start` dispara durante esse lock, o PyInstaller boot (MEIPASS\r\n"
            f"REM       extract) trava aguardando I/O.\r\n"
            f"REM   (b) `start \"\" \"exe\"` via cmd herda as flags de console do cmd\r\n"
            f"REM       detached — em algumas máquinas isso mantém o processo acoplado\r\n"
            f"REM       ao cmd que está prestes a sair, tornando o spawn instável.\r\n"
            f"REM Fix: (1) grace de 3s pós-swap pra AV soltar; (2) usar PowerShell\r\n"
            f"REM Start-Process, que cria o processo com handle próprio, desacoplado\r\n"
            f"REM do cmd pai. O `-WindowStyle Hidden` evita o flash de console.\r\n"
            f"timeout /t 3 /nobreak >NUL\r\n"
            f'echo [%date% %time%] launching new FragReel via PowerShell Start-Process >> "%LOGFILE%"\r\n'
            f'powershell -NoProfile -WindowStyle Hidden -Command "Start-Process -FilePath \'{current_exe}\'" >> "%LOGFILE%" 2>&1\r\n'
            f"REM Leave the .log in %TEMP% for post-mortem; delete only the .bat.\r\n"
            f'del "%~f0"\r\n'
        )
        bat_path.write_text(bat_content, encoding="utf-8")
        log.info("auto-update: wrote helper bat=%s log=%s", bat_path, bat_log)

        # Spawn detached so the bat survives our exit. CREATE_NO_WINDOW
        # keeps the cmd console invisible to the user.
        DETACHED_PROCESS = 0x00000008
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        CREATE_NO_WINDOW = 0x08000000
        try:
            subprocess.Popen(
                ["cmd.exe", "/c", str(bat_path)],
                creationflags=(
                    DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
                ),
                close_fds=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as e:
            log.exception("auto-update: failed to spawn helper")
            return {"error": "helper_spawn_failed", "detail": str(e)}, 500

        # Schedule our own exit on a daemon thread so the response can flush
        # before we die. 2s is enough for Flask to send back the JSON +
        # close the socket cleanly. os._exit (not sys.exit) because we want
        # to skip atexit handlers — they can hang if something has open
        # file handles in vendored DLLs.
        def _exit_after():
            time.sleep(2.0)
            log.info("auto-update: exiting now to let helper swap the binary")
            os._exit(0)

        threading.Thread(
            target=_exit_after, daemon=True, name="fragreel-update-exit"
        ).start()

        return jsonify({
            "started": True,
            "new_exe": str(new_exe_path),
            "current_exe": str(current_exe),
            "bat_log": str(bat_log),  # v0.2.15: surface log path for post-mortem
            "pid": current_pid,
            "size_mb": round(size / 1024 / 1024, 1),
            "message": "downloaded — swap helper spawned, client exits in ~2s",
        }), 202

    @app.post("/shutdown")
    def shutdown():
        """Graceful exit — used by a NEWER client at startup to evict an
        OLDER instance that's still bound to port 5775.

        v0.2.11 PC testing reveal: when a user installed v0.2.11 manually
        without auto-update, the old v0.2.10 tray process kept running and
        held the port. The web kept seeing the stale version and "Mapear
        Plays" did nothing. Killing it via Task Manager worked but no end
        user is going to do that.

        Flow:
          1. New `.exe` boots, calls POST http://127.0.0.1:5775/shutdown
          2. We log + ack 202 and schedule os._exit(0) on a daemon thread
             (1.5s — enough to flush the response and let TCP TIME_WAIT
             reset, but short enough that the new client doesn't timeout)
          3. New `.exe` waits ~3s, then binds the port itself

        Security: this is only exposed on 127.0.0.1 + CORS-restricted to
        fragreel.vercel.app, so the only callers in practice are (a) the
        new self-evicting client or (b) the web (which has no UI to call
        it — it's not in lib/local.ts). Still, we don't take a body or
        do anything irreversible beyond exiting our own process.
        """
        log.info("/shutdown received — newer client probably evicting us; exiting in 1.5s")

        def _exit_after():
            time.sleep(1.5)
            log.info("/shutdown: exiting now (os._exit(0))")
            os._exit(0)

        threading.Thread(
            target=_exit_after, daemon=True, name="fragreel-shutdown-exit"
        ).start()
        return jsonify({
            "ok": True,
            "version": CLIENT_VERSION,
            "message": "exiting in 1.5s",
        }), 202

    # ── Config endpoints (v0.2.7+ — Settings UI in web) ─────────────────

    def _serialize_resolved(resolved) -> dict:
        return {
            "output_dir": str(resolved.path),
            "source": resolved.source,
            "default": str(resolved.default),
            "env_override": str(resolved.env_override) if resolved.env_override else None,
        }

    @app.get("/config")
    def get_config():
        """Returns current effective output_dir + provenance.
        Settings UI shows `source: "env"` as a read-only banner ("override
        ativo via FRAGREEL_OUTPUT_DIR — remova a env var para usar a UI")."""
        resolved = resolve_output_dir()
        return jsonify(_serialize_resolved(resolved))

    @app.post("/config")
    def post_config():
        """Update output_dir. Body: {"output_dir": "D:\\\\FragReel"}.
        Validates: non-empty string, can be created/exists, is writable.
        On success, hot-reloads RenderCoordinator's output_dir so the
        next render uses it without restarting the .exe."""
        body = request.get_json(silent=True) or {}
        new_dir_raw = body.get("output_dir")
        if not isinstance(new_dir_raw, str) or not new_dir_raw.strip():
            return {"error": "missing_output_dir",
                    "detail": "body must be {output_dir: <non-empty string>}"}, 400

        new_path = Path(new_dir_raw.strip()).expanduser()
        # Validate: try to create + write a probe file. We don't trust
        # path.is_dir() alone — Windows can show writable-looking paths
        # under restricted system folders that fail at runtime.
        try:
            new_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            return {"error": "cannot_create",
                    "detail": f"could not create {new_path}: {e}"}, 400
        probe = new_path / ".fragreel-write-probe"
        try:
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
        except OSError as e:
            return {"error": "not_writable",
                    "detail": f"cannot write to {new_path}: {e}"}, 400

        resolved = set_output_dir(new_path)
        # Hot-reload coordinator so next render picks up the new path.
        # When env var is overriding, resolved.path != new_path — log a
        # warning so the user sees why their save "didn't take".
        if render_coordinator is not None:
            render_coordinator.update_output_dir(resolved.path)
            if resolved.source == "env":
                log.warning(
                    "saved output_dir=%s but FRAGREEL_OUTPUT_DIR=%s overrides it; "
                    "next render will still use the env value",
                    new_path, resolved.env_override,
                )
        return jsonify(_serialize_resolved(resolved))

    @app.post("/config/reset")
    def reset_config():
        """Clear output_dir override → falls back to env or default."""
        resolved = clear_output_dir()
        if render_coordinator is not None:
            render_coordinator.update_output_dir(resolved.path)
        return jsonify(_serialize_resolved(resolved))

    @app.post("/config/pick-folder")
    def pick_folder():
        """Open the OS-native folder picker dialog and return the chosen
        path. Does NOT save it — the web shows the result in the input,
        user clicks Save, then POST /config persists.

        Why a separate endpoint: HTML5 has no folder picker. <input
        webkitdirectory> only gives File objects, not the absolute path.
        Asking the user to type "D:\\Users\\...\\FragReel" by hand is
        terrible UX. The local client has tkinter for free, so we open
        the native dialog from here.

        Implementation note: tkinter wants to be on the main thread.
        Flask runs us on a worker thread. On Windows this still works
        for transient (Tk + filedialog + destroy) usage because no real
        event loop runs. If it ever breaks, the web falls back to the
        text input the user can type into manually.
        """
        try:
            import tkinter as tk
            from tkinter import filedialog
        except ImportError:
            return {"error": "tkinter_unavailable",
                    "detail": "native picker not bundled; type the path manually"}, 501

        try:
            root = tk.Tk()
            root.withdraw()
            # On Windows, dialogs from background threads can render
            # behind other windows. -topmost forces it to the front.
            root.attributes("-topmost", True)
            initial = str(resolve_output_dir().path)
            try:
                Path(initial).mkdir(parents=True, exist_ok=True)
            except OSError:
                initial = str(Path.home())
            chosen = filedialog.askdirectory(
                parent=root,
                title="Escolha a pasta onde os FragReels serão salvos",
                initialdir=initial,
                mustexist=False,
            )
            root.destroy()
        except Exception as e:
            log.exception("native folder picker failed")
            return {"error": "picker_failed",
                    "detail": f"{type(e).__name__}: {e}"}, 500

        if not chosen:
            # User cancelled — distinguish from error so the web can just
            # close the picker silently instead of showing a toast.
            return jsonify({"cancelled": True})
        return jsonify({"cancelled": False, "path": chosen})

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

    # Output directory precedence (v0.2.7+, see client_config.py):
    #   1. FRAGREEL_OUTPUT_DIR env var (CI/power-user escape hatch)
    #   2. config.json `output_dir` (Settings UI in the web)
    #   3. Default: ~/Desktop/FragReel
    # Note: this only redirects the FINAL .mov / .mp4 output. The TGA
    # capture itself goes under <CS2_install>/game/bin/win64/fragreel/
    # because HLAE writes there directly (mirv_streams record name is
    # joined to the engine bin dir). Redirecting TGA capture to another
    # drive needs a Steam library transfer or a junction point at the
    # CS2 capture path — see the project Status doc for that workaround.
    resolved = resolve_output_dir()
    output_dir = resolved.path
    log.info("output_dir resolved: %s (source=%s)", output_dir, resolved.source)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        log.warning(
            "could not create output_dir %s (%s); falling back to %s",
            output_dir, e, resolved.default,
        )
        output_dir = resolved.default
        output_dir.mkdir(parents=True, exist_ok=True)

    editor_dir = _resolve_editor_dir()
    config = HlaeRunnerConfig(cs2_install=cs2_install, hlae_dir=hlae_dir)
    return RenderCoordinator(
        config,
        output_dir=output_dir,
        editor_dir=editor_dir,
    )


def _resolve_editor_dir() -> Path | None:
    """Round 4c (Fase 1): resolve editor_dir corretamente em dev/frozen.

    Bug histórico: local_api.py:1001 chumbava
    `Path(__file__).parent.parent / "main" / "editor"` que NUNCA existia:
      - Dev: __file__.parent.parent = <fragreel-client repo root>; o
        editor vive em outro repo (<fragreel/editor/>), normalmente como
        sibling do client repo no mesmo workspace
      - Frozen .exe: __file__.parent.parent = _MEIPASS/.. (não existe);
        editor não estava bundlado em FragReel.spec

    Resultado: `editor_dir.is_dir()` sempre False → render_coordinator
    Stage 5 sempre caía no fallback ffmpeg concat (cru, sem música/overlays
    do Remotion). Mathieu viu MP4 concat há semanas — esperava reel editado.

    Resolução nova com 3 fontes ranqueadas (escape hatches → defaults):
      1. ENV var FRAGREEL_EDITOR_DIR (dev escape hatch)
      2. Frozen .exe: _MEIPASS / 'editor' (Round 4c Fase 2 vai bundlar)
      3. Dev sibling repo convention: <client>/../fragreel/editor

    Retorna Path se válido, None se nenhum bate (Stage 5 cai no fallback
    gracioso).
    """
    # 1. ENV var override
    env_path = os.environ.get("FRAGREEL_EDITOR_DIR")
    if env_path:
        p = Path(env_path)
        if p.is_dir():
            log.info("editor_dir from FRAGREEL_EDITOR_DIR env: %s", p)
            return p
        log.warning(
            "FRAGREEL_EDITOR_DIR set to %s but dir doesn't exist — fallback continua",
            p,
        )

    # 2. Frozen .exe (Round 4c Fase 2 bundla editor/ em _MEIPASS via spec)
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            p = Path(meipass) / "editor"
            if p.is_dir():
                log.info("editor_dir from _MEIPASS (frozen): %s", p)
                return p
            log.debug("_MEIPASS/editor not found yet (Round 4c Fase 2 pending)")

    # 3. Dev mode: sibling repo conventions — testa nomes comuns. PC test
    # (26/04) revelou layouts variados (e.g. `C:\FragReel\client` +
    # `C:\FragReel\main` ao invés do `<root>/fragreel-client` + `<root>/fragreel`
    # convencional). Testamos vários nomes pra evitar exigir rename/symlink.
    client_parent = Path(__file__).parent.parent  # one level up from fragreel-client root
    sibling_candidates_names = ["fragreel", "main", "fragreel-server", "server"]
    for name in sibling_candidates_names:
        candidate = client_parent / name / "editor"
        if candidate.is_dir():
            log.info("editor_dir from sibling repo '%s/' (dev mode): %s", name, candidate)
            return candidate

    # 4. Last resort: parent dir convention (workspace 1-level acima)
    workspace_parent = client_parent.parent
    for name in sibling_candidates_names:
        candidate = workspace_parent / name / "editor"
        if candidate.is_dir():
            log.info("editor_dir from workspace parent '%s/' (dev mode alt): %s", name, candidate)
            return candidate

    # 5. Auto-discovery: glob qualquer sibling/editor com package.json+remotion
    # (último resort caso layout seja exótico). Limitado a 1-level pra evitar
    # full filesystem scan.
    try:
        for sibling_dir in client_parent.iterdir():
            if not sibling_dir.is_dir() or sibling_dir == Path(__file__).parent:
                continue
            candidate = sibling_dir / "editor"
            pkg_json = candidate / "package.json"
            if candidate.is_dir() and pkg_json.is_file():
                # Confirma que é repo do Remotion lendo package.json
                try:
                    import json
                    pkg = json.loads(pkg_json.read_text(encoding="utf-8"))
                    deps = (pkg.get("dependencies") or {})
                    if any("remotion" in k for k in deps):
                        log.info(
                            "editor_dir auto-discovered (sibling '%s/editor' has remotion): %s",
                            sibling_dir.name, candidate,
                        )
                        return candidate
                except Exception:
                    continue
    except (OSError, PermissionError):
        pass

    log.info(
        "editor_dir not resolved — Stage 5 vai usar fallback ffmpeg concat. "
        "Pra forçar Remotion: set FRAGREEL_EDITOR_DIR=<path/to/fragreel/editor>"
    )
    return None


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
