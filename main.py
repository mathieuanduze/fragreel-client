"""
FragReel Client — entry point.

Fluxo (Escopo A+):
  1. Detecta SteamID + todas as pastas onde o CS2 salva .dem
  2. Inicia a UploadQueue (worker único)
  3. Na primeira execução (cache vazio): scan retroativo das pastas, enfileira
     demos que pertencem ao usuário
  4. Inicia o watcher monitorando todas as pastas em paralelo — qualquer .dem
     novo (auto-salvo, replay UI, download HLTV/FACEIT) entra na mesma fila

Usage:
  python main.py                                    # auto-detecta tudo
  python main.py --demo-dir ./demos --steamid 765…  # dev: pasta local
  python main.py --no-tray                          # sem ícone na bandeja
  python main.py --no-scan                          # pula scan retroativo
"""
import argparse
import logging
import sys
import threading
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fragreel")

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
    parser.add_argument("--no-scan",  action="store_true", help="Pula o scan retroativo")
    args = parser.parse_args()

    # ── Steam ID ────────────────────────────────────────────────────
    steamid = args.steamid
    if not steamid:
        from steam_detect import find_active_steamid
        steamid = find_active_steamid() or ""
        if steamid:
            log.info(f"SteamID detectado: {steamid}")
        else:
            log.error("SteamID não detectado. Passe --steamid manualmente.")
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

    # ── Scan retroativo (primeira execução) ─────────────────────────
    if not args.no_scan:
        from scanner import scan_all, CACHE_FILE
        first_run = not CACHE_FILE.exists()
        if first_run:
            log.info("Primeira execução — escaneando demos antigas…")
        else:
            log.info("Verificando se há demos antigas ainda não processadas…")

        try:
            matches = scan_all(demo_dirs, steamid)
            for m in matches:
                queue.enqueue(Path(m.demo_path), source="scan_retroativo")
            if matches:
                log.info(f"📼 {len(matches)} partidas antigas enfileiradas")
        except Exception as e:
            log.error(f"Scan retroativo falhou: {e}")

    # ── Watcher (bloqueia até stop_event) ───────────────────────────
    from watcher import watch
    try:
        watch(demo_dirs=demo_dirs, queue=queue, stop_event=_stop_event)
    finally:
        queue.stop()


if __name__ == "__main__":
    main()
