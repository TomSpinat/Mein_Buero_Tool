from __future__ import annotations

import json
import os
import sys
from pathlib import Path


_STORAGE_SETTING_KEY = "external_storage_dir"


def app_root_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def settings_file_path() -> Path:
    return app_root_dir() / "settings.json"


def default_external_storage_dir() -> Path:
    desktop_dir = Path.home() / "Desktop"
    if desktop_dir.is_dir():
        return desktop_dir / "Mein_Buero_Tool_Ablage"
    return app_root_dir() / "Mein_Buero_Tool_Ablage"


def _read_plain_settings() -> dict:
    settings_file = settings_file_path()
    if not settings_file.exists():
        return {}

    try:
        with settings_file.open("r", encoding="utf-8-sig") as handle:
            data = json.load(handle)
    except Exception:
        return {}

    return data if isinstance(data, dict) else {}


def configured_storage_dir() -> Path | None:
    env_override = os.environ.get("MEIN_BUERO_STORAGE_DIR", "").strip()
    if env_override:
        return Path(env_override).expanduser()

    settings = _read_plain_settings()
    setting_value = str(settings.get(_STORAGE_SETTING_KEY, "") or "").strip()
    if not setting_value:
        return None
    return Path(setting_value).expanduser()


def storage_root_dir() -> Path:
    configured = configured_storage_dir()
    if configured is not None:
        return configured
    return app_root_dir()


def storage_path(*parts: str) -> Path:
    base_path = storage_root_dir()
    for part in parts:
        base_path = base_path / part
    return base_path

