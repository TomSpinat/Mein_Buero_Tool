"""
test_reset.py
Hilfsfunktionen fuer reproduzierbare Teststarts:
- Datenbankdaten leeren
- mapping.json zuruecksetzen
"""

import json
from datetime import datetime

from config import resource_path
from module.database_manager import DatabaseManager
from module.crash_logger import log_exception


EMPTY_MAPPING = {
    "shops": {},
    "zahlungsarten": {}
}


def _to_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    txt = str(value).strip().lower()
    return txt in ("1", "true", "yes", "ja", "on")


def reset_mapping_file():
    mapping_path = resource_path("mapping.json")
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(EMPTY_MAPPING, f, indent=4, ensure_ascii=False)
    return mapping_path


def wipe_database_and_mapping(settings_manager, db_manager=None):
    manager = db_manager or DatabaseManager(settings_manager)
    db_result = manager.wipe_all_data_for_testing()
    mapping_path = reset_mapping_file()

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        settings_manager.save_setting("test_last_wipe_at", ts)
    except Exception as e:
        log_exception(__name__, e)

    return {
        "performed": True,
        "wiped_tables": db_result.get("wiped_tables", []),
        "mapping_path": mapping_path,
        "timestamp": ts,
    }


def maybe_wipe_on_start(settings_manager, db_manager=None):
    enabled = _to_bool(settings_manager.get("test_wipe_on_start", False))
    if not enabled:
        return {"performed": False, "reason": "disabled"}

    try:
        return wipe_database_and_mapping(settings_manager, db_manager=db_manager)
    except Exception as e:
        log_exception(__name__, e)
        return {
            "performed": False,
            "reason": "error",
            "error": str(e),
        }
