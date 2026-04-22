"""
Detects Steam installation path and the active player's SteamID.
Works on Windows (winreg) and falls back to common paths on macOS/Linux for dev.
"""
import os
import re
import sys
from pathlib import Path


def _steam_root_windows() -> "Path | None":
    try:
        import winreg
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Valve\Steam")
        path, _ = winreg.QueryValueEx(key, "InstallPath")
        return Path(path)
    except Exception:
        return None


def _steam_root_mac() -> "Path | None":
    candidate = Path.home() / "Library/Application Support/Steam"
    return candidate if candidate.exists() else None


def _steam_root_linux() -> "Path | None":
    candidates = [
        Path.home() / ".steam/steam",
        Path.home() / ".local/share/Steam",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def find_steam_root() -> "Path | None":
    if sys.platform == "win32":
        return _steam_root_windows()
    elif sys.platform == "darwin":
        return _steam_root_mac()
    return _steam_root_linux()


def _cs2_roots() -> list[Path]:
    """Todas as instalações CS2 detectadas (main library + libraryfolders.vdf)."""
    root = find_steam_root()
    if not root:
        return []

    roots: list[Path] = []
    main = root / "steamapps/common/Counter-Strike Global Offensive"
    if main.exists():
        roots.append(main)

    vdf_path = root / "steamapps/libraryfolders.vdf"
    if vdf_path.exists():
        text = vdf_path.read_text(encoding="utf-8", errors="ignore")
        for m in re.finditer(r'"path"\s+"([^"]+)"', text):
            lib = Path(m.group(1).replace("\\\\", "\\"))
            cand = lib / "steamapps/common/Counter-Strike Global Offensive"
            if cand.exists() and cand not in roots:
                roots.append(cand)
    return roots


def find_cs2_demo_dir() -> "Path | None":
    """Retorna a primeira pasta de demos detectada (compat antigo)."""
    dirs = find_all_demo_dirs()
    return dirs[0] if dirs else None


def find_all_demo_dirs() -> list[Path]:
    """
    Retorna TODAS as pastas candidatas onde demos do CS2 podem aparecer:
      - .../csgo/                       (partidas auto-salvas + match730_*.dem baixadas)
      - .../csgo/replays/               (replays salvos via UI do CS2)
      - .../Downloads/                  (usuário baixou .dem do HLTV/FACEIT manualmente)

    Todas as pastas existentes são retornadas — o watcher monitora todas em paralelo.
    """
    dirs: list[Path] = []
    for cs2_root in _cs2_roots():
        for sub in ("game/csgo", "game/csgo/replays"):
            p = cs2_root / sub
            if p.exists() and p not in dirs:
                dirs.append(p)

    # Downloads (onde HLTV/FACEIT demos normalmente caem)
    if sys.platform == "win32":
        downloads = Path.home() / "Downloads"
    else:
        downloads = Path.home() / "Downloads"
    if downloads.exists() and downloads not in dirs:
        dirs.append(downloads)

    return dirs


def find_active_steamid() -> "str | None":
    """
    Reads loginusers.vdf to find the most recently active Steam account.
    Returns SteamID64 as string, or None if not found.
    """
    root = find_steam_root()
    if not root:
        return None

    loginusers = root / "config/loginusers.vdf"
    if not loginusers.exists():
        return None

    text = loginusers.read_text(encoding="utf-8", errors="ignore")

    # Find the steamid associated with MostRecent "1"
    # VDF blocks look like: "76561198XXXXXXXXX" { ... "MostRecent" "1" ... }
    blocks = re.split(r'(?="\d{17}")', text)
    for block in blocks:
        sid_match = re.match(r'"(\d{17})"', block)
        if sid_match and '"MostRecent"\t\t"1"' in block:
            return sid_match.group(1)

    # Fallback: return first steamid found
    first = re.search(r'"(\d{17})"', text)
    return first.group(1) if first else None


if __name__ == "__main__":
    print("Steam root:", find_steam_root())
    print("CS2 roots:", _cs2_roots())
    print("Demo dirs:")
    for d in find_all_demo_dirs():
        print("  -", d)
    print("SteamID:  ", find_active_steamid())
