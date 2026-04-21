"""
System tray icon for FragReel client (Windows).
Requires: pystray, pillow
"""
import threading
import logging
from pathlib import Path

log = logging.getLogger("fragreel.tray")


def _make_icon():
    """Generate a simple orange square icon with 'FR' text."""
    try:
        from PIL import Image, ImageDraw, ImageFont
        img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        # Orange background circle
        draw.ellipse([4, 4, 60, 60], fill=(255, 100, 0, 255))
        # White text
        try:
            font = ImageFont.truetype("arial.ttf", 20)
        except Exception:
            font = ImageFont.load_default()
        draw.text((32, 32), "FR", fill="white", font=font, anchor="mm")
        return img
    except ImportError:
        return None


def run_tray(on_quit: callable, on_open: callable, demo_dir: str = "") -> None:
    """
    Start the system tray icon. Runs in current thread (blocking).
    Call from a daemon thread so it doesn't block the watcher.

    on_quit  — called when user clicks Quit
    on_open  — called when user clicks Open Dashboard
    """
    try:
        import pystray
        from pystray import MenuItem as item
    except ImportError:
        log.warning("pystray not installed — running without tray icon.")
        return

    icon_image = _make_icon()
    if icon_image is None:
        log.warning("Pillow not installed — running without tray icon.")
        return

    def _open(_icon, _item):
        on_open()

    def _quit(_icon, _item):
        _icon.stop()
        on_quit()

    menu = pystray.Menu(
        item("Abrir FragReel", _open, default=True),
        item(f"Watching: {Path(demo_dir).name}" if demo_dir else "FragReel", lambda *_: None, enabled=False),
        pystray.Menu.SEPARATOR,
        item("Sair", _quit),
    )

    icon = pystray.Icon("FragReel", icon_image, "FragReel — Ativo", menu)
    log.info("Tray icon started.")
    icon.run()


def start_tray_thread(on_quit: callable, on_open: callable, demo_dir: str = "") -> threading.Thread:
    """Starts the tray in a daemon thread. Returns the thread."""
    t = threading.Thread(
        target=run_tray,
        args=(on_quit, on_open, demo_dir),
        daemon=True,
        name="tray",
    )
    t.start()
    return t
