"""
FragReel Client — entry point.

Modelo on-demand:
  1. Detecta SteamID + todas as pastas onde o CS2 salva .dem
  2. Inicia a UploadQueue (worker único, on-demand via API local)
  3. Sobe API HTTP local em 127.0.0.1:5775 — fragreel.vercel.app conversa com ela
  4. Aguarda comandos vindos da web (lista demos, dispara upload de uma específica)

Usage:
  python main.py                                    # auto-detecta tudo
  python main.py --demo-dir ./demos --steamid 765…  # dev: pasta local
  python main.py --no-tray                          # sem ícone na bandeja
"""
import argparse
import logging
import os
import sys
import threading
import traceback
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
    log.info("Pronto. Aguardando ações via fragreel.vercel.app …")

    # Notificação desktop confirmando que o client está vivo — sem isso, em modo
    # windowed (sem terminal preto) o user pode achar que não aconteceu nada.
    try:
        from notifier import notify
        notify("FragReel está rodando",
               "Pronto! Abra fragreel.vercel.app/library pra ver suas demos.")
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
