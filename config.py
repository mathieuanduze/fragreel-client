"""
FragReel Client — configuration.
Values are read from env vars or fall back to defaults.
"""
import os
from pathlib import Path

API_URL = os.getenv("FRAGREEL_API_URL", "https://fragreel-production.up.railway.app")

# CS2 demo folder (auto-detectado via steam_detect.py; sobrescreva aqui se necessário)
CS2_DEMO_DIR = os.getenv("FRAGREEL_DEMO_DIR", "")

# Polling fallback interval (seconds) if watchdog inotify fails
POLL_INTERVAL = int(os.getenv("FRAGREEL_POLL_INTERVAL", "5"))

# Dashboard URL opened after highlights are ready
DASHBOARD_URL = os.getenv("FRAGREEL_DASHBOARD_URL", "https://fragreel.vercel.app")

# Minimum file size (bytes) to consider a .dem valid (avoids temp files)
MIN_DEMO_BYTES = 1024 * 50  # 50 KB

# Onde os clipes extraídos são salvos (~/Videos/FragReel/{match_id}/)
CLIPS_DIR = Path(os.getenv("FRAGREEL_CLIPS_DIR", "")) or Path.home() / "Videos" / "FragReel"

# Desabilitar gravação de vídeo (apenas faz upload do .dem)
RECORDING_ENABLED = os.getenv("FRAGREEL_RECORDING", "1") != "0"
