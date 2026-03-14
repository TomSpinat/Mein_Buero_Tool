from PyQt6.QtCore import QThread, pyqtSignal

from module.product_image_search_service import ProductImageSearchError, ProductImageSearchService


class ProductImageSearchWorker(QThread):
    result_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, settings_manager, produkt_name, varianten_info='', ean='', limit=3, context=None):
        super().__init__()
        self.settings_manager = settings_manager
        self.produkt_name = str(produkt_name or '').strip()
        self.varianten_info = str(varianten_info or '').strip()
        self.ean = str(ean or '').strip()
        self.limit = int(limit or 3)
        self.context = dict(context or {}) if isinstance(context, dict) else {}

    def run(self):
        try:
            service = ProductImageSearchService(self.settings_manager)
            result = service.search_candidates(
                self.produkt_name,
                varianten_info=self.varianten_info,
                ean=self.ean,
                limit=self.limit,
                context=self.context,
            )
            self.result_signal.emit(result or {})
        except Exception as exc:
            if isinstance(exc, ProductImageSearchError):
                self.error_signal.emit(str(exc))
                return
            self.error_signal.emit(f'Bildsuche fehlgeschlagen: {exc}')
