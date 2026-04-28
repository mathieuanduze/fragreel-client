"""
FragReel Client — entry point.

Modelo on-demand:
  1. Detecta SteamID + todas as pastas onde o CS2 salva .dem
  2. Inicia a UploadQueue (worker único, on-demand via API local)
  3. Sobe API HTTP local em 127.0.0.1:5775 — fragreel.gg conversa com ela
  4. Aguarda comandos vindos da web (lista demos, dispara upload de uma específica)

Usage:
  python main.py                                    # auto-detecta tudo
  python main.py --demo-dir ./demos --steamid 765…  # dev: pasta local
  python main.py --no-tray                          # sem ícone na bandeja
"""
import argparse
import atexit
import logging
import os
import signal
import socket
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path


# ─── PyInstaller windowed-mode safety ─────────────────────────────────────────
# Quando o build é feito com console=False, sys.stdout / sys.stderr são None.
# Werkzeug (servidor Flask), watchdog e várias libs escrevem em stderr a cada
# request — sem essa guarda, a primeira chamada HTTP estoura AttributeError e
# mata a thread da API silenciosamente (sintoma: "Client conectado" → cai).
class _NullStream:
    def write(self, *_a, **_k): return 0
    def flush(self): pass
    def isatty(self): return False
    def fileno(self): raise OSError("no fileno on null stream")

if sys.stdout is None:
    sys.stdout = _NullStream()
if sys.stderr is None:
    sys.stderr = _NullStream()
# ───────────────────────────────────────────────────────────────────────────────


def _log_dir() -> Path:
    """Pasta persistente para logs (e futuro cache).
    Windows: %APPDATA%/FragReel/  ·  outros: ~/.fragreel/"""
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", Path.home())) / "FragReel"
    else:
        base = Path.home() / ".fragreel"
    base.mkdir(parents=True, exist_ok=True)
    return base


LOG_FILE = _log_dir() / "fragreel.log"

_handlers: list[logging.Handler] = [
    logging.FileHandler(LOG_FILE, encoding="utf-8"),
    # stdout pode ser _NullStream em modo windowed — não tem efeito visual,
    # mas mantém handler chain consistente e pega tty quando há um.
    logging.StreamHandler(sys.stdout),
]
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=_handlers,
)
# Werkzeug loga cada request — em modo windowed o NullStream patch acima
# garante que isso é seguro. Mantemos em INFO pra ter visibilidade durante
# debug (qual endpoint foi chamado, quanto tempo, status).

log = logging.getLogger("fragreel")
log.info(f"=== FragReel iniciando · log em {LOG_FILE} ===")

_stop_event = threading.Event()
_BOOT_TIME = time.time()


# ── Bug 5 instrumentation (v0.2.12) ───────────────────────────────────────
# User reported on PC testing (2026-04-23): "client morre silenciosamente
# depois de ~10min idle". Logs terminam num GET /demos 200 e simplesmente
# param — sem traceback, sem ERROR, nada. Possíveis causas em investigação:
# antivírus, Windows kill policy, atexit hang, leak de os._exit() em algum
# helper. Adicionamos abaixo um conjunto mínimo de instrumentação pra que
# a próxima ocorrência deixe pegada no log:
#   • atexit handler que loga o exit "normal" (Python encerrando via main loop)
#   • signal handlers SIGTERM / SIGINT / SIGBREAK que logam ANTES de morrer
#   • heartbeat thread que loga a cada 60s "alive (uptime Xs)" — quando a
#     gente vê o último heartbeat e nada depois, sabemos minute-precision
#     quando o processo foi morto

def _log_exit() -> None:
    uptime = time.time() - _BOOT_TIME
    log.info(f"=== FragReel saindo (atexit) · uptime {uptime:.0f}s ===")


def _signal_handler(sig: int, _frame) -> None:
    name = {
        signal.SIGINT: "SIGINT (Ctrl+C ou kill -2)",
        signal.SIGTERM: "SIGTERM (TerminateProcess / kill -15)",
    }.get(sig, f"signal {sig}")
    # SIGBREAK só existe no Windows.
    if hasattr(signal, "SIGBREAK") and sig == signal.SIGBREAK:
        name = "SIGBREAK (Ctrl+Break ou closing console)"
    uptime = time.time() - _BOOT_TIME
    log.warning(f"=== FragReel recebeu {name} · uptime {uptime:.0f}s · saindo ===")
    # Não chamamos sys.exit aqui — apenas sinalizamos pro main loop sair
    # limpo (queue.stop, etc). O atexit handler vai logar o resto.
    _stop_event.set()


def _install_signal_handlers() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _signal_handler)
        except (ValueError, OSError):
            # Threads sem suporte a signal (improvável aqui — main thread)
            # ou plataformas que não expõem o sinal.
            pass
    # Windows-only: console close / Ctrl+Break
    if hasattr(signal, "SIGBREAK"):
        try:
            signal.signal(signal.SIGBREAK, _signal_handler)
        except (ValueError, OSError):
            pass


def _start_heartbeat() -> None:
    """Loga a cada 60s pra confirmar que o processo ainda está vivo.
    Quando investigarmos uma silent death, basta ver no log qual foi o
    último heartbeat — diferença com o timestamp atual ≈ minutos parado."""
    def _beat():
        while not _stop_event.wait(60.0):
            uptime = time.time() - _BOOT_TIME
            log.info(f"♥ heartbeat · uptime {uptime:.0f}s")
    threading.Thread(target=_beat, daemon=True, name="fragreel-heartbeat").start()


# ── Bug 1 (v0.2.12): evict an older client still bound to port 5775 ──────
LOCAL_PORT = 5775

def _evict_stale_instance(timeout_total: float = 6.0) -> None:
    """If port 5775 is already bound by a previous FragReel client, ask it
    to shut down via POST /shutdown (added in v0.2.12 local_api). Wait for
    the port to become free before we try to bind ourselves.

    No-op if:
      • Port is already free (nothing to evict)
      • Port is bound but /shutdown endpoint missing (pre-v0.2.12 client →
        falls through; user needs to kill it manually). We still wait the
        full timeout so the new bind doesn't 100% fail — gives the user a
        chance to close it via tray, and surfaces a clearer error if not.
    """
    # Quick port probe — if we can connect, something is listening.
    try:
        with socket.create_connection(("127.0.0.1", LOCAL_PORT), timeout=0.5):
            pass
    except OSError:
        # Nothing on port; clean boot.
        return

    log.warning(
        "port %d already bound — outra instância do FragReel rodando? "
        "tentando evict via POST /shutdown", LOCAL_PORT,
    )
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{LOCAL_PORT}/shutdown",
            method="POST",
            data=b"",  # /shutdown ignora body, mas precisa do POST
            headers={"User-Agent": "FragReel-self-evict"},
        )
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            log.info("evict: /shutdown respondeu %d — old client deve sair em ~1.5s", resp.status)
    except urllib.error.HTTPError as e:
        log.warning("evict: /shutdown HTTP %d — client antigo (<v0.2.12) sem o endpoint?", e.code)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        log.warning("evict: falha em chamar /shutdown (%s) — vou só esperar a porta liberar", e)

    # Polling: porta liberou?
    deadline = time.time() + timeout_total
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", LOCAL_PORT), timeout=0.3):
                pass
            time.sleep(0.4)
        except OSError:
            log.info("port %d liberada — booting normalmente", LOCAL_PORT)
            return
    log.error(
        "port %d ainda ocupada após %.1fs — bind vai provavelmente falhar. "
        "Feche o FragReel.exe antigo na bandeja do sistema.", LOCAL_PORT, timeout_total,
    )


def _on_upload_event(event: str, payload: dict) -> None:
    """Callback global da fila — log + (futuramente) notificações desktop."""
    if event == "queued":
        log.info(f"⏳ na fila ({payload['source']}): {Path(payload['path']).name} — pos {payload['position']}")
    elif event == "uploading":
        log.info(f"⬆ enviando (tentativa {payload['attempt']}): {Path(payload['path']).name}")
    elif event == "done":
        name = Path(payload["path"]).name
        log.info(f"✅ {name} → {payload['highlights']} highlights")
        try:
            from notifier import notify
            notify("FragReel", f"{name}: {payload['highlights']} highlights prontos!")
        except Exception:
            pass
    elif event == "skipped":
        log.info(f"⤼ pulada ({payload.get('reason')}): {Path(payload['path']).name}")
    elif event == "failed":
        log.error(f"❌ falhou: {Path(payload['path']).name} — {payload.get('error')}")


def main() -> None:
    parser = argparse.ArgumentParser(description="FragReel Client")
    parser.add_argument("--demo-dir", help="Pasta única para watch (sobrescreve auto-detect)")
    parser.add_argument("--steamid",  help="SteamID64 (auto-detect se omitido)")
    parser.add_argument("--no-tray",  action="store_true", help="Desabilita ícone do tray")
    args = parser.parse_args()

    # Bug 1 + Bug 5 instrumentation (v0.2.12). Evict-stale roda ANTES de
    # qualquer bind pra garantir que se o user instalou um .exe novo sem
    # passar pelo auto-update, o tray velho não fica segurando a porta.
    _install_signal_handlers()
    atexit.register(_log_exit)
    _evict_stale_instance()
    _start_heartbeat()

    # ── Steam ID ────────────────────────────────────────────────────
    steamid = args.steamid
    if not steamid:
        from steam_detect import find_active_steamid
        steamid = find_active_steamid() or ""
        if steamid:
            log.info(f"SteamID detectado: {steamid}")
        else:
            msg = ("SteamID não detectado.\n\n"
                   "Verifique se o Steam está instalado e se você logou pelo menos 1 vez.\n"
                   "Se o problema persistir, rode pelo terminal com:\n"
                   "  FragReel.exe --steamid SEU_STEAMID64")
            log.error(msg.replace("\n", " "))
            _show_fatal(msg)
            sys.exit(1)

    # ── Pastas a monitorar ──────────────────────────────────────────
    if args.demo_dir:
        demo_dirs = [Path(args.demo_dir)]
    else:
        from steam_detect import find_all_demo_dirs
        demo_dirs = find_all_demo_dirs()
        if not demo_dirs:
            fallback = Path(__file__).parent.parent / "demos"
            fallback.mkdir(exist_ok=True)
            demo_dirs = [fallback]
            log.warning(f"CS2 não encontrado. Usando pasta dev: {fallback}")

    log.info(f"Monitorando {len(demo_dirs)} pasta(s):")
    for d in demo_dirs:
        log.info(f"  • {d}")

    # ── Upload queue ────────────────────────────────────────────────
    from uploader import UploadQueue
    queue = UploadQueue(steamid=steamid, on_event=_on_upload_event)
    queue.start()

    # ── Tray ────────────────────────────────────────────────────────
    if not args.no_tray:
        try:
            from notifier import open_dashboard
            from tray import start_tray_thread

            def on_quit():
                log.info("Quit solicitado pelo tray.")
                _stop_event.set()

            start_tray_thread(
                on_quit=on_quit,
                on_open=open_dashboard,
                demo_dir=str(demo_dirs[0]),
            )
        except Exception as e:
            log.warning(f"Tray indisponível ({e}) — seguindo sem ícone")

    # ── Local API (web descobre demos + dispara uploads via 127.0.0.1:5775) ──
    from local_api import serve as serve_local_api
    serve_local_api(steamid=steamid, demo_dirs=demo_dirs, queue=queue, stop_event=_stop_event)
    log.info("Pronto. Aguardando ações via fragreel.gg …")

    # Bug #16 (28/04): fechar PyInstaller splash screen agora que boot
    # completou. pyi_splash só existe quando rodando como .exe frozen com
    # Splash() configurado no spec — em dev mode o módulo não existe, então
    # try/except é gracioso.
    try:
        import pyi_splash  # type: ignore[import-not-found]
        pyi_splash.close()
        log.info("Splash screen closed")
    except (ImportError, ModuleNotFoundError):
        # Dev mode (não-frozen) — sem splash, sem problema
        pass
    except Exception as e:
        log.warning(f"Splash close failed (non-fatal): {e}")

    # Notificação desktop confirmando que o client está vivo — sem isso, em modo
    # windowed (sem terminal preto) o user pode achar que não aconteceu nada.
    # Bug #17 (28/04): URL atualizada de fragreel.vercel.app → fragreel.gg.
    try:
        from notifier import notify
        notify("FragReel está rodando",
               "Pronto! Abra fragreel.gg/library pra ver suas demos.")
    except Exception:
        pass

    # Bloqueia até receber stop (do tray ou Ctrl+C)
    try:
        _stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        queue.stop()


def _show_fatal(message: str) -> None:
    """Mostra um messagebox com o erro antes de sair (Windows)."""
    try:
        if sys.platform == "win32":
            import ctypes
            ctypes.windll.user32.MessageBoxW(
                0,
                f"FragReel não conseguiu iniciar.\n\n{message}\n\nLog completo em:\n{LOG_FILE}",
                "FragReel — Erro",
                0x10,  # MB_ICONERROR
            )
    except Exception:
        pass


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        pass
    except Exception as e:
        tb = traceback.format_exc()
        log.error(f"FATAL: {e}\n{tb}")
        _show_fatal(f"{type(e).__name__}: {e}")
        sys.exit(1)
