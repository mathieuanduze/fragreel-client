"""
steam_gc.py — Python wrapper pro Node sidecar Steam GC.

Sprint DEMO-3 (08/05/2026) — auto match history sem abrir CS2.

Arquitetura: Python parent spawn child Node process (steam_gc_sidecar/steam_gc.js)
com IPC stdin/stdout JSON-line protocol. Esta classe encapsula spawn lifecycle +
async request/response correlation por uuid.

Por que Node sidecar em vez de Python ValvePython libs:
  ValvePython csgo lib abandonada (último commit fev/2021), GitHub issues #62,
  #63 confirmam GC connection broken pós CS2 update. Node DoctorMcKay/steam-user
  (dez/2025) + globaloffensive (mar/2026) ativamente mantidos. Reference impl:
  cs-demo-manager (akiver, abr/2026).

Uso típico:
    from steam_gc import SteamGCSidecar

    gc = SteamGCSidecar()
    gc.start()
    response = gc.request("ping", timeout=2.0)
    print(response)  # {"pong": True, "version": "0.1.0", ...}
    gc.shutdown()

Lifecycle:
  - .start() spawna node child process, espera "ready" event
  - .request(action, params) envia JSON via stdin, espera response com mesmo id
  - .shutdown() envia action shutdown + termina process gracefully
  - Auto-cleanup via __del__ + atexit handler

Pra Sprint 1 MVP: ping + status + shutdown funcionam end-to-end.
Login/match_history/resolve_sharecode são stubs no node side, implementação
real Sprint 2.
"""
from __future__ import annotations

import atexit
import json
import logging
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("fragreel.steam_gc")


class SteamGCError(Exception):
    """Erro do sidecar (login fail, GC disconnect, etc)."""
    pass


class SteamGCNotRunning(Exception):
    """Sidecar não foi iniciado OU já terminou."""
    pass


class SteamGCTimeout(Exception):
    """Request timed out aguardando response do sidecar."""
    pass


def _resolve_sidecar_dir() -> Path:
    """Resolve path pro steam_gc_sidecar/ — funciona em dev (source) E em
    PyInstaller bundle (.exe).

    Em PyInstaller bundle, sidecar fica em sys._MEIPASS/steam_gc_sidecar/.
    Em dev source, ao lado de steam_gc.py.
    """
    if hasattr(sys, "_MEIPASS"):
        # PyInstaller frozen
        base = Path(sys._MEIPASS)
        candidate = base / "steam_gc_sidecar"
        if candidate.exists():
            return candidate
    # Dev: ao lado deste file
    return Path(__file__).resolve().parent / "steam_gc_sidecar"


def _resolve_node_executable() -> Optional[str]:
    """Resolve qual node binary usar.

    Prioridade:
      1. NODE_BINARY env var (override pra testes)
      2. Bundled node em steam_gc_sidecar/node.exe (PyInstaller, ship com .exe)
      3. node global do sistema (dev / fallback)

    Returns None se nada encontrado (caller faz fallback gracioso).
    """
    override = os.environ.get("NODE_BINARY")
    if override and Path(override).exists():
        return override

    sidecar_dir = _resolve_sidecar_dir()
    if sys.platform == "win32":
        bundled = sidecar_dir / "node.exe"
    else:
        bundled = sidecar_dir / "node"
    if bundled.exists():
        return str(bundled)

    system_node = shutil.which("node")
    if system_node:
        return system_node

    return None


class SteamGCSidecar:
    """Gerencia lifecycle do node steam_gc.js sidecar process.

    Thread-safe: múltiplas threads podem chamar .request() concorrentemente,
    cada uma recebe sua resposta correlacionada por uuid.

    Auto-cleanup: __del__ + atexit garantem subprocess termina mesmo em
    crash do parent.
    """

    REQUEST_TIMEOUT_DEFAULT = 10.0  # segundos

    def __init__(self) -> None:
        self._proc: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._pending: dict[str, queue.Queue] = {}
        self._reader_thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None
        self._ready_event = threading.Event()
        self._stopped = False
        atexit.register(self._atexit_cleanup)

    # ── Lifecycle ───────────────────────────────────────────────────────────

    def start(self, *, ready_timeout: float = 10.0) -> None:
        """Spawn node sidecar. Bloqueia até receber "ready" event ou timeout.

        Idempotente: chamadas repetidas em sidecar já running são no-op.
        """
        with self._lock:
            if self._proc is not None and self._proc.poll() is None:
                log.debug("sidecar já rodando, skip start")
                return

            node_exe = _resolve_node_executable()
            if not node_exe:
                raise SteamGCError(
                    "node executable not found — install node.js OR ship bundled "
                    "node binary em steam_gc_sidecar/"
                )

            sidecar_dir = _resolve_sidecar_dir()
            entry = sidecar_dir / "steam_gc.js"
            if not entry.exists():
                raise SteamGCError(f"steam_gc.js não encontrado em {entry}")

            log.info("spawning steam_gc sidecar: node=%s entry=%s", node_exe, entry)

            self._proc = subprocess.Popen(
                [node_exe, str(entry)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(sidecar_dir),
                text=True,
                bufsize=1,  # line-buffered
                # No Windows, evitar console window pra child
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )

            self._stopped = False
            self._ready_event.clear()

            # Spawn reader threads pra stdout (IPC) e stderr (debug log)
            self._reader_thread = threading.Thread(
                target=self._reader_loop, name="steam_gc-reader", daemon=True
            )
            self._reader_thread.start()

            self._stderr_thread = threading.Thread(
                target=self._stderr_loop, name="steam_gc-stderr", daemon=True
            )
            self._stderr_thread.start()

        if not self._ready_event.wait(timeout=ready_timeout):
            self.shutdown(timeout=2.0)
            raise SteamGCError(
                f"sidecar não respondeu 'ready' em {ready_timeout}s — pode ser "
                "que node binary tá quebrado ou steam_gc.js tá com erro"
            )

    def shutdown(self, *, timeout: float = 5.0) -> None:
        """Envia action shutdown ao sidecar + aguarda exit graceful.

        Se não responder em `timeout`, force-kill via SIGTERM. Idempotente.
        """
        with self._lock:
            if self._proc is None or self._proc.poll() is not None:
                self._proc = None
                self._stopped = True
                return

            self._stopped = True

        # Tenta graceful via IPC (fora do lock pra não deadlock)
        try:
            self.request("shutdown", timeout=2.0)
        except Exception as e:
            log.debug("shutdown IPC falhou (esperado se já terminou): %s", e)

        # Aguarda process exit
        try:
            self._proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            log.warning("sidecar não exited gracefully em %.1fs, force-killing", timeout)
            try:
                self._proc.terminate()
                self._proc.wait(timeout=2.0)
            except Exception:
                self._proc.kill()
        finally:
            self._proc = None

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    # ── Request/response ────────────────────────────────────────────────────

    def request(
        self,
        action: str,
        params: Optional[dict] = None,
        *,
        timeout: float = REQUEST_TIMEOUT_DEFAULT,
    ) -> dict:
        """Envia request ao sidecar, aguarda response correlacionada por id.

        Returns: response.data dict (sucesso) — caller já recebe payload limpo.
        Raises:
          SteamGCNotRunning: sidecar não foi iniciado ou morreu
          SteamGCTimeout: response não chegou em `timeout`
          SteamGCError: sidecar respondeu com error string
        """
        if not self.is_running():
            raise SteamGCNotRunning("sidecar not running — call .start() first")

        request_id = uuid.uuid4().hex[:12]
        request_obj = {"id": request_id, "action": action, "params": params or {}}

        # Registra queue pra response correlacionada por id
        response_q: queue.Queue = queue.Queue(maxsize=1)
        with self._lock:
            self._pending[request_id] = response_q

        try:
            payload = json.dumps(request_obj) + "\n"
            assert self._proc and self._proc.stdin
            self._proc.stdin.write(payload)
            self._proc.stdin.flush()
        except Exception as e:
            with self._lock:
                self._pending.pop(request_id, None)
            raise SteamGCNotRunning(f"falhou ao escrever stdin: {e}")

        # Bloqueia até response OU timeout
        try:
            response = response_q.get(timeout=timeout)
        except queue.Empty:
            with self._lock:
                self._pending.pop(request_id, None)
            raise SteamGCTimeout(
                f"sidecar não respondeu em {timeout}s pra action={action}"
            )

        if not response.get("ok"):
            err = response.get("error", "unknown error")
            raise SteamGCError(f"{action} failed: {err}")

        return response.get("data", {})

    # ── Internals ───────────────────────────────────────────────────────────

    def _reader_loop(self) -> None:
        """Background thread lendo stdout linha por linha + dispatching."""
        assert self._proc and self._proc.stdout
        try:
            for line in self._proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    log.warning("steam_gc stdout non-JSON line: %r", line[:200])
                    continue

                # Event (não-correlacionado): ready / steam_guard_required / etc
                if "event" in msg:
                    self._handle_event(msg)
                    continue

                # Response correlacionado por id
                req_id = msg.get("id")
                if not req_id:
                    log.warning("steam_gc response sem id: %r", msg)
                    continue

                with self._lock:
                    q = self._pending.pop(req_id, None)
                if q is not None:
                    try:
                        q.put_nowait(msg)
                    except queue.Full:
                        log.warning("response queue cheia pra id=%s", req_id)
                else:
                    log.debug("response sem pending pra id=%s (timeout?)", req_id)
        except Exception as e:
            if not self._stopped:
                log.warning("reader_loop terminou com erro: %s", e)

    def _stderr_loop(self) -> None:
        """Background thread lendo stderr (debug logs do node)."""
        assert self._proc and self._proc.stderr
        try:
            for line in self._proc.stderr:
                line = line.rstrip()
                if line:
                    log.debug("steam_gc stderr: %s", line)
        except Exception:
            pass

    def _handle_event(self, msg: dict) -> None:
        event = msg.get("event")
        if event == "ready":
            log.info(
                "steam_gc sidecar ready — version=%s node=%s",
                msg.get("version"), msg.get("node"),
            )
            self._ready_event.set()
        elif event == "steam_guard_required":
            log.info("steam_gc: Steam Guard code requerido (domain=%s)", msg.get("domain"))
            # TODO Sprint 2: propagar pra parent via callback OR signal Flask endpoint
        else:
            log.debug("steam_gc event %s: %s", event, msg)

    def _atexit_cleanup(self) -> None:
        """Garante sidecar termina mesmo em crash do parent."""
        if self.is_running():
            try:
                self.shutdown(timeout=2.0)
            except Exception:
                pass

    def __del__(self) -> None:
        self._atexit_cleanup()


# ── Singleton helper (Flask endpoints reusa) ─────────────────────────────────
_singleton: Optional[SteamGCSidecar] = None
_singleton_lock = threading.Lock()


def get_sidecar() -> SteamGCSidecar:
    """Returns singleton sidecar, started lazy on first call."""
    global _singleton
    with _singleton_lock:
        if _singleton is None or not _singleton.is_running():
            _singleton = SteamGCSidecar()
            _singleton.start()
        return _singleton


def shutdown_sidecar() -> None:
    """Atômico shutdown do singleton (chamado no FragReel exit)."""
    global _singleton
    with _singleton_lock:
        if _singleton is not None:
            try:
                _singleton.shutdown()
            except Exception as e:
                log.debug("shutdown_sidecar: %s", e)
            _singleton = None
