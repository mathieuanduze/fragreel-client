"""
FragReel Client — entry point.

Usage:
  python main.py                        # auto-detect Steam + CS2
  python main.py --demo-dir /path/demos --steamid 76561198XXXXXXXXX
  python main.py --demo-dir ./demos     # dev: watch local folder
  python main.py --no-tray              # disable system tray
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


def main() -> None:
    parser = argparse.ArgumentParser(description="FragReel Client")
    parser.add_argument("--demo-dir", help="Path to folder to watch for .dem files")
    parser.add_argument("--steamid",  help="Your SteamID64 (auto-detected if omitted)")
    parser.add_argument("--no-tray",  action="store_true", help="Disable system tray icon")
    args = parser.parse_args()

    # ── Resolve Steam ID ────────────────────────────────────────────
    steamid = args.steamid
    if not steamid:
        from steam_detect import find_active_steamid
        steamid = find_active_steamid() or ""
        if steamid:
            log.info(f"Detected SteamID: {steamid}")
        else:
            log.warning("Could not detect SteamID. Pass --steamid manually.")

    # ── Resolve demo directory ──────────────────────────────────────
    demo_dir_str = args.demo_dir
    if not demo_dir_str:
        from steam_detect import find_cs2_demo_dir
        detected = find_cs2_demo_dir()
        if detected:
            demo_dir_str = str(detected)
            log.info(f"Detected CS2 demo folder: {demo_dir_str}")
        else:
            fallback = Path(__file__).parent.parent / "demos"
            fallback.mkdir(exist_ok=True)
            demo_dir_str = str(fallback)
            log.warning(f"CS2 not found. Watching dev folder: {demo_dir_str}")

    demo_dir = Path(demo_dir_str)
    if not demo_dir.exists():
        log.error(f"Demo directory does not exist: {demo_dir}")
        sys.exit(1)

    # ── System tray ─────────────────────────────────────────────────
    if not args.no_tray:
        from notifier import open_dashboard
        from tray import start_tray_thread

        def on_quit():
            log.info("Quit requested from tray.")
            _stop_event.set()

        start_tray_thread(on_quit=on_quit, on_open=open_dashboard, demo_dir=demo_dir_str)

    # ── Start watcher ───────────────────────────────────────────────
    from watcher import watch
    watch(demo_dir=demo_dir, steamid=steamid, stop_event=_stop_event)


if __name__ == "__main__":
    main()
