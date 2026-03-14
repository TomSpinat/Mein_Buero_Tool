import logging

from PyQt6.QtCore import QObject, pyqtSignal


class _OrderVisualInvalidationBus(QObject):
    visualsInvalidated = pyqtSignal(object)


class OrderVisualState:
    _revision = 0
    _bus = None

    @classmethod
    def current_revision(cls):
        return int(cls._revision)

    @classmethod
    def bus(cls):
        if cls._bus is None:
            cls._bus = _OrderVisualInvalidationBus()
        return cls._bus

    @classmethod
    def invalidate(cls, reason="", einkauf_id=None, ausgangs_paket_id=None, scope="global"):
        cls._revision += 1
        payload = {
            "revision": int(cls._revision),
            "reason": str(reason or "unknown").strip() or "unknown",
            "einkauf_id": int(einkauf_id) if einkauf_id not in (None, "") else None,
            "ausgangs_paket_id": int(ausgangs_paket_id) if ausgangs_paket_id not in (None, "") else None,
            "scope": str(scope or "global").strip() or "global",
        }
        logging.debug(
            "Order-Visual-Invalidierung: revision=%s, reason=%s, einkauf_id=%s, paket_id=%s, scope=%s",
            payload["revision"],
            payload["reason"],
            payload["einkauf_id"],
            payload["ausgangs_paket_id"],
            payload["scope"],
        )
        cls.bus().visualsInvalidated.emit(payload)
        return payload
