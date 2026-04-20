"""
FragReel Client — configuration.
Values are read from env vars or fall back to defaults.
"""
import os
from pathlib import Path

API_URL = os.getenv("FRAGREEL_API_URL", "http://localhost:8001")

# On Windows: usually C:\Program Files (x86)\Steam\steamapps\common\Counter-Strike Global Offensive\game\csgo
# Detected automatically by steam_detect.py; override here if needed.
CS2_DEMO_DIR = os.getenv("FRAGREEL_DEMO_DIR", "")

# Polling fallback interval (seconds) if watchdog inotify fails
POLL_INTERVAL = int(os.getenv("FRAGREEL_POLL_INTERVAL", "5"))

# Dashboard URL opened after highlights are ready
DASHBOARD_URL = os.getenv("FRAGREEL_DASHBOARD_URL", "http://localhost:3033/dashboard")

# Minimum file size (bytes) to consider a .dem valid (avoids temp files)
MIN_DEMO_BYTES = 1024 * 50  # 50 KB
