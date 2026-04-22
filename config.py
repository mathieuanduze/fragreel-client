"""
FragReel Client — configuration.
Values are read from env vars or fall back to defaults.
"""
import os

API_URL = os.getenv("FRAGREEL_API_URL", "https://fragreel-production.up.railway.app")

# CS2 demo folder (auto-detectado via steam_detect.py; sobrescreva aqui se necessário)
CS2_DEMO_DIR = os.getenv("FRAGREEL_DEMO_DIR", "")

# Polling fallback interval (seconds) if watchdog inotify fails
POLL_INTERVAL = int(os.getenv("FRAGREEL_POLL_INTERVAL", "5"))

# Dashboard URL opened after highlights are ready
DASHBOARD_URL = os.getenv("FRAGREEL_DASHBOARD_URL", "https://fragreel.vercel.app")

# Minimum file size (bytes) to consider a .dem valid (avoids temp files)
MIN_DEMO_BYTES = 1024 * 50  # 50 KB
