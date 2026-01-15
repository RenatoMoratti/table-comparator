from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

try:
    import keyring
except Exception:  # pragma: no cover
    keyring = None


_SERVICE_NAME = "table-comparator"
_SETTINGS_PATH = Path(__file__).resolve().parent / "data" / "local_connection_settings.json"


def _ensure_data_dir() -> None:
    _SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)


def load_connection_settings() -> Dict[str, Any]:
    """Load persisted connection settings.

    Non-sensitive values come from a local JSON file.
    Tokens come from the OS keyring (Windows Credential Manager on Windows).
    """
    settings: Dict[str, Any] = {}

    if _SETTINGS_PATH.exists():
        try:
            settings = json.loads(_SETTINGS_PATH.read_text(encoding="utf-8"))
        except Exception:
            settings = {}

    if keyring is not None:
        dev_token = keyring.get_password(_SERVICE_NAME, "dev_token")
        prod_token = keyring.get_password(_SERVICE_NAME, "prod_token")
        if dev_token:
            settings["dev_token"] = dev_token
        if prod_token:
            settings["prod_token"] = prod_token

    return settings


def save_connection_settings(settings: Dict[str, Any]) -> None:
    """Persist connection settings safely.

    - Writes non-sensitive fields to JSON (git-ignored).
    - Writes tokens to OS keyring.

    If a token is missing/empty, the previously saved token (if any) is preserved.
    """
    _ensure_data_dir()

    existing = load_connection_settings()

    def pick(name: str) -> str:
        val = settings.get(name)
        if val is None:
            return str(existing.get(name, ""))
        val_str = str(val).strip()
        return val_str if val_str else str(existing.get(name, ""))

    # Persist non-sensitive values to JSON
    file_payload = {
        "dev_host": pick("dev_host"),
        "dev_port": pick("dev_port"),
        "dev_database": pick("dev_database"),
        "prod_host": pick("prod_host"),
        "prod_port": pick("prod_port"),
        "prod_database": pick("prod_database"),
    }
    _SETTINGS_PATH.write_text(json.dumps(file_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    # Persist tokens to keyring
    if keyring is None:
        # No secure store available; do not write tokens to disk.
        return

    dev_token = pick("dev_token")
    prod_token = pick("prod_token")

    if dev_token:
        keyring.set_password(_SERVICE_NAME, "dev_token", dev_token)
    if prod_token:
        keyring.set_password(_SERVICE_NAME, "prod_token", prod_token)


def clear_connection_settings() -> None:
    """Remove persisted settings (file + keyring tokens)."""
    try:
        if _SETTINGS_PATH.exists():
            _SETTINGS_PATH.unlink()
    except Exception:
        pass

    if keyring is None:
        return

    for name in ("dev_token", "prod_token"):
        try:
            keyring.delete_password(_SERVICE_NAME, name)
        except Exception:
            pass
