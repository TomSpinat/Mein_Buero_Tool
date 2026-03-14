from PyQt6.QtCore import QThread, pyqtSignal

from module.shop_logo_search_service import ShopLogoSearchError, ShopLogoSearchService


class ShopLogoSearchWorker(QThread):
    result_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, settings_manager, canonical_shop_name='', sender_domain='', shop_key='', limit=3, context=None):
        super().__init__()
        self.settings_manager = settings_manager
        self.canonical_shop_name = str(canonical_shop_name or '').strip()
        self.sender_domain = str(sender_domain or '').strip()
        self.shop_key = str(shop_key or '').strip()
        self.limit = int(limit or 3)
        self.context = dict(context or {}) if isinstance(context, dict) else {}

    def run(self):
        try:
            service = ShopLogoSearchService(self.settings_manager)
            result = service.search_candidates(
                canonical_shop_name=self.canonical_shop_name,
                sender_domain=self.sender_domain,
                shop_key=self.shop_key,
                limit=self.limit,
                context=self.context,
            )
            self.result_signal.emit(result or {})
        except Exception as exc:
            if isinstance(exc, ShopLogoSearchError):
                self.error_signal.emit(str(exc))
                return
            self.error_signal.emit(f'Logo-Suche fehlgeschlagen: {exc}')
