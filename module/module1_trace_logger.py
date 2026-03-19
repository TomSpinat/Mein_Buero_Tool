from __future__ import annotations

import json
import logging
from datetime import datetime

from storage_paths import storage_path


_TRACE_DIR = storage_path("data", "debug_logs")
_TRACE_FILE = _TRACE_DIR / "module1_scan_trace.jsonl"
_MAX_TRACE_FILE_BYTES = 5 * 1024 * 1024
_KEEP_ROTATED_TRACE_FILES = 3


def _cleanup_old_trace_files() -> None:
    try:
        rotated_files = sorted(
            _TRACE_DIR.glob("module1_scan_trace_*.jsonl"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return

    for stale_file in rotated_files[_KEEP_ROTATED_TRACE_FILES:]:
        try:
            stale_file.unlink()
        except OSError:
            pass


def _rotate_trace_file_if_needed() -> None:
    try:
        if not _TRACE_FILE.exists():
            return
        if _TRACE_FILE.stat().st_size < _MAX_TRACE_FILE_BYTES:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archived_file = _TRACE_DIR / f"module1_scan_trace_{timestamp}.jsonl"
        _TRACE_FILE.replace(archived_file)
        _cleanup_old_trace_files()
    except OSError:
        pass


def write_module1_trace(event: str, **payload) -> None:
    try:
        _TRACE_DIR.mkdir(parents=True, exist_ok=True)
        _rotate_trace_file_if_needed()
        row = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "event": str(event or "").strip() or "unknown",
            "payload": payload or {},
        }
        with _TRACE_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=True, default=str) + "\n")
    except Exception as exc:
        logging.debug("Modul-1-Trace konnte nicht geschrieben werden: %s", exc)
