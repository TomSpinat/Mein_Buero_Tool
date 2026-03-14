"""
Kleine zentrale Hilfsklasse fuer Hintergrundarbeiten ausserhalb des GUI-Threads.

Geeignet fuer:
- Netzwerk-Checks
- Dateizugriffe
- langsame API-Aufrufe
- DB-Tests
"""

from __future__ import annotations

import inspect

from PyQt6.QtCore import QThread, pyqtSignal

from module.crash_logger import log_exception
from module.secret_store import sanitize_text


class BackgroundTask(QThread):
    result_signal = pyqtSignal(object, object)
    error_signal = pyqtSignal(object, str)
    finished_signal = pyqtSignal(object)
    progress_signal = pyqtSignal(object, object)

    def __init__(self, fn, *args, task_id=None, parent=None, **kwargs):
        super().__init__(parent)
        self.fn = fn
        self.args = args
        self.kwargs = dict(kwargs)
        self.task_id = task_id
        self._cancelled = False

    def cancel(self):
        self._cancelled = True
        self.requestInterruption()

    def is_cancelled(self):
        return self._cancelled or self.isInterruptionRequested()

    def _emit_progress(self, payload):
        if not self.is_cancelled():
            self.progress_signal.emit(self.task_id, payload)

    def run(self):
        try:
            kwargs = dict(self.kwargs)
            try:
                params = inspect.signature(self.fn).parameters
            except (TypeError, ValueError):
                params = {}

            if "progress_callback" in params and "progress_callback" not in kwargs:
                kwargs["progress_callback"] = self._emit_progress
            if "is_cancelled" in params and "is_cancelled" not in kwargs:
                kwargs["is_cancelled"] = self.is_cancelled

            result = self.fn(*self.args, **kwargs)
            if not self.is_cancelled():
                self.result_signal.emit(self.task_id, result)
        except Exception as exc:
            log_exception(
                __name__,
                exc,
                extra={
                    "task_id": self.task_id,
                    "callable": getattr(self.fn, "__name__", repr(self.fn)),
                },
            )
            if not self.is_cancelled():
                self.error_signal.emit(self.task_id, sanitize_text(exc))
        finally:
            self.finished_signal.emit(self.task_id)
