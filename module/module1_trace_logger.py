from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path


_TRACE_FILE = Path(__file__).resolve().parents[1] / "data" / "debug_logs" / "module1_scan_trace.jsonl"


def write_module1_trace(event: str, **payload) -> None:
    try:
        _TRACE_FILE.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "event": str(event or "").strip() or "unknown",
            "payload": payload or {},
        }
        with _TRACE_FILE.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=True, default=str) + "\n")
    except Exception as exc:
        logging.debug("Modul-1-Trace konnte nicht geschrieben werden: %s", exc)
