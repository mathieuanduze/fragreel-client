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


def find_cs2_demo_dir() -> "Path | None":
    """
    Returns the CS2 folder where demos are saved.
    CS2 demo path: <steam>/steamapps/common/Counter-Strike Global Offensive/game/csgo
    """
    root = find_steam_root()
    if not root:
        return None

    cs2_path = root / "steamapps/common/Counter-Strike Global Offensive/game/csgo"
    if cs2_path.exists():
        return cs2_path

    # Check additional Steam library folders from libraryfolders.vdf
    vdf_path = root / "steamapps/libraryfolders.vdf"
    if vdf_path.exists():
        text = vdf_path.read_text(encoding="utf-8", errors="ignore")
        for path_match in re.finditer(r'"path"\s+"([^"]+)"', text):
            lib = Path(path_match.group(1))
            candidate = lib / "steamapps/common/Counter-Strike Global Offensive/game/csgo"
            if candidate.exists():
                return candidate

    return None


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
    print("CS2 demos:", find_cs2_demo_dir())
    print("SteamID:  ", find_active_steamid())
