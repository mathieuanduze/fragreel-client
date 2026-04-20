"""
Desktop notifications and browser launcher.
Uses plyer for cross-platform notifications (works on Windows natively).
"""
import webbrowser
from config import DASHBOARD_URL


def notify(title: str, message: str, match_id: "str | None" = None) -> None:
    """Show a desktop notification. Clicking it opens the dashboard."""
    try:
        from plyer import notification
        notification.notify(
            title=title,
            message=message,
            app_name="FragReel",
            timeout=10,
        )
    except Exception:
        # plyer not available or notification failed — silent fallback
        print(f"[notify] {title}: {message}")


def open_match(match_id: str) -> None:
    url = f"{DASHBOARD_URL.rstrip('/dashboard')}/match/{match_id}"
    webbrowser.open(url)
