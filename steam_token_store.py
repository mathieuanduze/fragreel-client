"""
steam_token_store.py — encrypted storage pra Steam refresh_token + auth_code.

Sprint DEMO-3 Sprint 2 (08/05/2026).

Stores em %APPDATA%\\FragReel\\steam_auth.enc (Windows) ou
~/.fragreel/steam_auth.enc (Mac/Linux).

Encryption strategy:
  - Windows: DPAPI (Data Protection API) — encryption key derivada da
    Windows user account, ninguém além do mesmo user pode decrypt.
    Standard pra Windows credential storage.
  - Mac/Linux fallback (dev): cryptography lib com Fernet, key armazenada
    em arquivo separado com perms 0600. Não é production-grade pra non-Windows
    mas funciona pra dev no Mac.

Schema (after decrypt):
  {
    "steamid64": "76561198XXXXXXXXX",
    "account_name": "username",
    "refresh_token": "long_jwt_like_string",
    "match_sharing_auth_code": "ABC1",  // 4-char Steam page code
    "last_known_sharecode": "CSGO-XXXXX-...",  // cursor pra match history
    "saved_at": 1778211455.0,
  }
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional

log = logging.getLogger("fragreel.steam_token_store")


def _store_path() -> Path:
    """Path do arquivo encrypted."""
    if sys.platform == "win32":
        appdata = os.environ.get("APPDATA")
        if appdata:
            base = Path(appdata) / "FragReel"
        else:
            base = Path.home() / "AppData" / "Roaming" / "FragReel"
    else:
        base = Path.home() / ".fragreel"
    base.mkdir(parents=True, exist_ok=True)
    return base / "steam_auth.enc"


def _key_path() -> Path:
    """Path da master key (Mac/Linux fallback only)."""
    if sys.platform == "win32":
        # Windows usa DPAPI, sem key file
        raise RuntimeError("DPAPI does not use external key file")
    return _store_path().parent / ".steam_auth.key"


# ── Windows DPAPI (production) ───────────────────────────────────────────────


def _win_encrypt(plaintext: bytes) -> bytes:
    """DPAPI encrypt — só user atual pode decrypt."""
    try:
        import win32crypt  # pywin32
    except ImportError:
        raise RuntimeError(
            "pywin32 não instalado — necessário pra DPAPI no Windows. "
            "Adicionar `pywin32` aos requirements do client."
        )
    # CryptProtectData retorna bytes encrypted
    encrypted = win32crypt.CryptProtectData(
        plaintext,
        "FragReel-SteamAuth",  # description (visível em CryptUnprotectData logs)
        None, None, None, 0,
    )
    return encrypted


def _win_decrypt(ciphertext: bytes) -> bytes:
    try:
        import win32crypt
    except ImportError:
        raise RuntimeError("pywin32 não instalado")
    # CryptUnprotectData retorna (description, plaintext_bytes)
    _, plaintext = win32crypt.CryptUnprotectData(ciphertext, None, None, None, 0)
    return plaintext


# ── Mac/Linux fallback (dev) ─────────────────────────────────────────────────


def _ensure_master_key() -> bytes:
    """Gera ou carrega master key Fernet (32 bytes base64)."""
    key_path = _key_path()
    if key_path.exists():
        return key_path.read_bytes()
    try:
        from cryptography.fernet import Fernet
    except ImportError:
        raise RuntimeError(
            "cryptography lib não instalada — `pip install cryptography`"
        )
    key = Fernet.generate_key()
    key_path.write_bytes(key)
    try:
        os.chmod(key_path, 0o600)
    except Exception:
        pass
    log.info("master key gerada em %s", key_path)
    return key


def _fernet_encrypt(plaintext: bytes) -> bytes:
    from cryptography.fernet import Fernet
    f = Fernet(_ensure_master_key())
    return f.encrypt(plaintext)


def _fernet_decrypt(ciphertext: bytes) -> bytes:
    from cryptography.fernet import Fernet
    f = Fernet(_ensure_master_key())
    return f.decrypt(ciphertext)


# ── Public API ───────────────────────────────────────────────────────────────


def save(data: dict) -> None:
    """Encrypt + grava `data` (dict serializable) no disco."""
    import time
    payload = dict(data)
    payload["saved_at"] = time.time()
    plaintext = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    if sys.platform == "win32":
        ciphertext = _win_encrypt(plaintext)
    else:
        ciphertext = _fernet_encrypt(plaintext)

    path = _store_path()
    # Atomic write: write to temp + rename
    tmp = path.with_suffix(".enc.tmp")
    tmp.write_bytes(ciphertext)
    try:
        os.chmod(tmp, 0o600)
    except Exception:
        pass
    tmp.replace(path)
    log.info("steam_auth saved (encrypted, %d bytes)", len(ciphertext))


def load() -> Optional[dict]:
    """Returns dict desencriptado, ou None se file não existe.

    Raises RuntimeError se encryption deps missing.
    Returns None silenciosamente se decrypt falhar (corrupção / key mismatch).
    """
    path = _store_path()
    if not path.exists():
        return None
    try:
        ciphertext = path.read_bytes()
        if sys.platform == "win32":
            plaintext = _win_decrypt(ciphertext)
        else:
            plaintext = _fernet_decrypt(ciphertext)
        return json.loads(plaintext.decode("utf-8"))
    except Exception as e:
        log.warning("steam_auth decrypt failed: %s — file pode estar corrupto", e)
        return None


def delete() -> bool:
    """Remove arquivo encrypted (logout / clear). Returns True se existia."""
    path = _store_path()
    if path.exists():
        path.unlink()
        log.info("steam_auth deleted")
        return True
    return False


def update(**kwargs) -> dict:
    """Lê store atual, atualiza fields, salva. Returns dict atualizado."""
    current = load() or {}
    current.update(kwargs)
    save(current)
    return current
