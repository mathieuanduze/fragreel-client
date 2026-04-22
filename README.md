# FragReel Client

Windows companion app for [FragReel](https://fragreel.vercel.app) — a free service that turns your Counter-Strike 2 demos into shareable highlight videos automatically.

The client runs silently in your system tray, watches the folders where CS2 stores match demos, and uploads new `.dem` files to the FragReel API. The server parses the demo, scores the best plays, and renders a vertical highlight reel you can share on WhatsApp, Instagram, or TikTok.

**No screen recording. No OBS. No manual upload.** You just play CS2 — your highlights are waiting on the website when the match ends.

---

## Features

- 🎯 **Auto-detection** of new CS2 demos in real time (matchmaking, replays, downloaded match demos)
- 🔍 **Retroactive scan** on first run — finds your existing demos and offers to process them
- 🛡 **SteamID filtering** — only processes demos where you actually played (ignores demos you watched)
- 🔁 **Hash-based deduplication** — never uploads the same demo twice
- 📋 **Queue uploader** — processes one match at a time to keep your bandwidth free
- 🔔 **Desktop notification** when your video is ready
- 🎛 **System tray icon** — pause, resume, view status, quit

---

## How it works

```
┌─────────────────┐         ┌──────────────────┐         ┌──────────────────┐
│   CS2 finishes  │  .dem   │  FragReel Client │  HTTPS  │  FragReel API    │
│   a match       │ ──────> │  (this repo)     │ ──────> │  fragreel.app    │
└─────────────────┘         └──────────────────┘         └──────────────────┘
                                                                 │
                                                                 ▼
                                                         Parses demo → scores
                                                         highlights → renders
                                                         video → notifies user
```

The client is a thin uploader — all video generation, scoring, and rendering happens server-side. Source code is fully open so you can verify exactly what it sends.

---

## Install (end users)

Download the latest signed `.exe` from [Releases](https://github.com/mathieuanduze/fragreel-client/releases/latest) and run the installer. The build is signed via [SignPath Foundation](https://signpath.org), so Windows SmartScreen will not block it.

After installing, log in once with your Steam account at [fragreel.vercel.app](https://fragreel.vercel.app), and the client takes care of the rest.

---

## Build from source (developers)

### Requirements
- Windows 10/11 (64-bit)
- Python 3.11+
- ffmpeg (bundled at build time via the GitHub Actions workflow)

### Setup
```bash
git clone https://github.com/mathieuanduze/fragreel-client.git
cd fragreel-client
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Run in development mode
```bash
python main.py
```

### Build the `.exe` locally
```bash
build.bat
```
The signed release `.exe` is produced by GitHub Actions on every `v*.*.*` tag — see [`.github/workflows/release.yml`](.github/workflows/release.yml).

---

## Project structure

| File | Purpose |
|------|---------|
| `main.py` | Entry point — wires the tray icon, watcher, and API client |
| `watcher.py` | Monitors all CS2 demo folders and enqueues new `.dem` files |
| `uploader.py` | Single-worker upload queue with retry, dedup, and event callbacks |
| `scanner.py` | Retroactive scan + hash-based dedup cache (`~/.fragreel/scanned.json`) |
| `steam_detect.py` | Auto-discovers Steam install path, all demo folders, and the logged-in SteamID |
| `tray.py` | System tray icon (pystray) |
| `notifier.py` | Desktop notifications (plyer) |
| `config.py` | Environment-overridable configuration |
| `FragReel.spec` | PyInstaller build specification |

---

## Privacy

The client only sends the following to the FragReel API:
- The `.dem` file itself (Valve's binary demo format — contains match events, no personal data beyond your in-game name and SteamID)
- Your Steam authentication token (so the server can associate the demo with your account)

It does **not**:
- Record your screen
- Read keystrokes or mouse input
- Access any file outside the configured CS2 demo folders
- Upload telemetry or analytics

---

## Code signing

Released builds are signed by [SignPath Foundation](https://signpath.org), a non-profit that provides free code signing for open source projects. The certificate is issued in the name of the SignPath Foundation, who verifies that the binary was built reproducibly from this repository.

---

## License

MIT — see [LICENSE](LICENSE).

---

## Links

- 🌐 Product: https://fragreel.vercel.app
- 🐛 Issues: https://github.com/mathieuanduze/fragreel-client/issues
- 📝 API source (private — backend): hosted on Railway
