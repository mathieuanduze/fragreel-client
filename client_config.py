"""Persistent user-configurable client settings.

Currently exposes a single key — `output_dir` — but designed to grow:
host_framerate, vendor path overrides, etc. land here too.

Storage layout (matches main.py's _log_dir):
  Windows: %APPDATA%/FragReel/config.json
  other:   ~/.fragreel/config.json

Precedence for `output_dir`:
  1. FRAGREEL_OUTPUT_DIR env var (v0.2.6 escape hatch — still wins so
     CI / power users can pin a specific path without touching the file)
  2. config.json `output_dir` (v0.2.7+ — what the Settings UI writes)
  3. Default: ~/Desktop/FragReel

The Settings UI in the web (v0.2.7) calls GET /config to inspect current
state and POST /config to update it. The render coordinator hot-reloads
its `output_dir` attribute whenever the file is updated, so the next
render uses the new path without restarting the .exe.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_CONFIG_LOCK = threading.Lock()


def _config_dir() -> Path:
    """Same logic as main._log_dir — kept duplicated to avoid an import
    cycle (main imports local_api which imports this)."""
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", Path.home())) / "FragReel"
    else:
        base = Path.home() / ".fragreel"
    base.mkdir(parents=True, exist_ok=True)
    return base


CONFIG_FILE = _config_dir() / "config.json"


def _default_output_dir() -> Path:
    return Path.home() / "Desktop" / "FragReel"


@dataclass(frozen=True)
class ResolvedOutputDir:
    """What the API actually returns to the web — includes provenance so the
    Settings UI can show "currently overridden by FRAGREEL_OUTPUT_DIR env var"
    instead of pretending the value came from config.json."""
    path: Path
    source: str  # "env" | "config" | "default"
    default: Path
    env_override: Optional[Path]  # set when FRAGREEL_OUTPUT_DIR is present


def load_raw() -> dict:
    """Read config.json, returning {} if missing or malformed.
    Never raises — a corrupt file shouldn't brick the client."""
    try:
        with CONFIG_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            log.warning("config.json is not an object (got %s); ignoring", type(data).__name__)
            return {}
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError) as e:
        log.warning("could not read %s (%s); using defaults", CONFIG_FILE, e)
        return {}


def _save_raw(data: dict) -> None:
    """Atomic write — write to .tmp then rename, so a crash mid-write
    doesn't leave a half-written config that can't be parsed."""
    tmp = CONFIG_FILE.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(CONFIG_FILE)


def resolve_output_dir() -> ResolvedOutputDir:
    """Apply the precedence chain. Pure function — does not mkdir.
    Caller (RenderCoordinator) handles mkdir + fallback on OSError."""
    default = _default_output_dir()
    env_raw = os.environ.get("FRAGREEL_OUTPUT_DIR", "").strip()
    env_override = Path(env_raw).expanduser() if env_raw else None

    if env_override is not None:
        return ResolvedOutputDir(
            path=env_override, source="env",
            default=default, env_override=env_override,
        )

    with _CONFIG_LOCK:
        data = load_raw()
    cfg_raw = data.get("output_dir")
    if isinstance(cfg_raw, str) and cfg_raw.strip():
        return ResolvedOutputDir(
            path=Path(cfg_raw).expanduser(), source="config",
            default=default, env_override=None,
        )

    return ResolvedOutputDir(
        path=default, source="default",
        default=default, env_override=None,
    )


def set_output_dir(new_path: Path) -> ResolvedOutputDir:
    """Persist a user-chosen output_dir to config.json.

    Caller is responsible for validating the path is writable; we just
    write the string. Returns the freshly-resolved value (which may
    differ from `new_path` if the env var still overrides).
    """
    with _CONFIG_LOCK:
        data = load_raw()
        data["output_dir"] = str(new_path)
        _save_raw(data)
        log.info("config.json updated: output_dir → %s", new_path)
    return resolve_output_dir()


def clear_output_dir() -> ResolvedOutputDir:
    """Remove the override; falls back to env or default. Used by the
    "Reset to default" button in the Settings UI."""
    with _CONFIG_LOCK:
        data = load_raw()
        data.pop("output_dir", None)
        _save_raw(data)
        log.info("config.json: output_dir cleared")
    return resolve_output_dir()
