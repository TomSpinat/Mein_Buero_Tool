"""
ean_search_worker.py
Fuehrt die EAN-Suche im Hintergrund aus, damit die PyQt-Oberflaeche nicht einfriert.
"""

from PyQt6.QtCore import QThread, pyqtSignal

from module.crash_logger import (
    AppError,
    classify_upcitemdb_error,
    log_classified_error,
    user_message_from_error,
)
from module.ean_service import EanService


class EanLookupWorker(QThread):
    result_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, settings_manager, produkt_name, varianten_info="", limit=25, allow_api_fallback=True):
        super().__init__()
        self.settings_manager = settings_manager
        self.produkt_name = str(produkt_name or "").strip()
        self.varianten_info = str(varianten_info or "").strip()
        self.limit = int(limit or 25)
        self.allow_api_fallback = bool(allow_api_fallback)

    def run(self):
        try:
            service = EanService(self.settings_manager)
            result = service.lookup_candidates_by_name(
                self.produkt_name,
                varianten_info=self.varianten_info,
                limit=self.limit,
                allow_api_fallback=self.allow_api_fallback,
            )
            self.result_signal.emit(result or {})
        except Exception as e:
            app_error = e if isinstance(e, AppError) else classify_upcitemdb_error(e, query_text=self.produkt_name)
            log_classified_error(
                f"{__name__}.EanLookupWorker.run",
                app_error.category if isinstance(app_error, AppError) else "unknown",
                app_error.user_message if isinstance(app_error, AppError) else str(e),
                status_code=app_error.status_code if isinstance(app_error, AppError) else None,
                service=app_error.service if isinstance(app_error, AppError) else "upcitemdb",
                exc=e,
                extra={"produkt_name": self.produkt_name, "varianten_info": self.varianten_info},
            )
            self.error_signal.emit(user_message_from_error(app_error, "EAN-Suche fehlgeschlagen."))
