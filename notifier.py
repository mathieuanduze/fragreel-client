"""
Desktop notifications and browser launcher.
Uses plyer for cross-platform notifications (works on Windows natively).
"""
import webbrowser
from config import DASHBOARD_URL


def notify(title: str, message: str, match_id: "str | None" = None) -> None:
    """Show a desktop notification."""
    try:
        from plyer import notification
        notification.notify(
            title=title,
            message=message,
            app_name="FragReel",
            timeout=10,
        )
    except Exception:
        print(f"[notify] {title}: {message}")


def open_match(match_id: str) -> None:
    url = f"{DASHBOARD_URL.rstrip('/')}/match/{match_id}"
    webbrowser.open(url)


def open_dashboard() -> None:
    webbrowser.open(DASHBOARD_URL)
