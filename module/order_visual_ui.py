from __future__ import annotations

import logging

from PyQt6.QtCore import QPointF, QRectF, Qt, QSize
from PyQt6.QtGui import QColor, QFont, QIcon, QPainter, QPainterPath, QPixmap
from PyQt6.QtWidgets import QLabel

from module.database_manager import DatabaseManager
from module.media.media_service import MediaService
from module.order_visual_state import OrderVisualState
from module.ui_media_pixmap import create_placeholder_pixmap, render_preview_pixmap


class OrderVisualResolver:
    def __init__(self, settings_manager):
        self._settings_manager = settings_manager
        self._media_service = None
        self._service_failed = False
        self._thumbnail_cache = {}
        self._placeholder_cache = {}
        self._visual_cache = {}
        self._seen_revision = OrderVisualState.current_revision()

    def _ensure_revision_current(self):
        current_revision = OrderVisualState.current_revision()
        if self._seen_revision == current_revision:
            return
        self._thumbnail_cache = {}
        self._visual_cache = {}
        self._seen_revision = current_revision
        logging.debug("Order-Visual-Cache lokal invalidiert: revision=%s", current_revision)
    def get_media_service(self):
        if self._media_service is not None:
            return self._media_service
        if self._service_failed or self._settings_manager is None:
            return None
        try:
            self._media_service = MediaService(DatabaseManager(self._settings_manager))
            return self._media_service
        except Exception as exc:
            self._service_failed = True
            logging.warning("Bestell-Visuals konnten nicht initialisiert werden: %s", exc)
            return None

    def build_order_preview(self, einkauf_id, shop_name="", sender_domain="", payload=None, max_item_images=2):
        self._ensure_revision_current()
        service = self.get_media_service()
        if service is None:
            return self._empty_preview(order_id=einkauf_id, shop_name=shop_name)
        try:
            return service.build_order_visual_preview(
                einkauf_id=einkauf_id,
                shop_name=shop_name,
                sender_domain=sender_domain,
                payload=payload,
                max_item_images=max_item_images,
            )
        except Exception as exc:
            logging.warning("Bestell-Visual konnte nicht geladen werden: einkauf_id=%s, error=%s", einkauf_id, exc)
            return self._empty_preview(order_id=einkauf_id, shop_name=shop_name)

    def build_package_preview(self, ausgangs_paket_id, max_item_images=2):
        self._ensure_revision_current()
        service = self.get_media_service()
        if service is None:
            return self._empty_preview(order_id=0, shop_name="")
        try:
            return service.build_package_visual_preview(
                ausgangs_paket_id=ausgangs_paket_id,
                max_item_images=max_item_images,
            )
        except Exception as exc:
            logging.warning("Paket-Visual konnte nicht geladen werden: paket_id=%s, error=%s", ausgangs_paket_id, exc)
            return self._empty_preview(order_id=0, shop_name="")

    def render_visual_pixmap(self, preview, width=86, height=48):
        self._ensure_revision_current()
        preview = preview if isinstance(preview, dict) else {}
        shop = preview.get("shop", {}) if isinstance(preview.get("shop"), dict) else {}
        item_previews = list(preview.get("item_previews", []) or [])
        cache_key = (
            int(preview.get("einkauf_id", preview.get("ausgangs_paket_id", 0)) or 0),
            str(shop.get("path", "") or ""),
            tuple(str(item.get("path", "") or item.get("source_url", "") or "") for item in item_previews[:2]),
            int(preview.get("remaining_item_count", 0) or 0),
            int(width),
            int(height),
        )
        if cache_key in self._visual_cache:
            return self._visual_cache[cache_key]

        canvas = QPixmap(int(width), int(height))
        canvas.fill(Qt.GlobalColor.transparent)
        painter = QPainter(canvas)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)

        logo_size = max(34, int(height) - 8)
        logo_rect = QRectF(2, (height - logo_size) / 2.0, logo_size, logo_size)
        shop_context = shop.get("context", {}) if isinstance(shop.get("context"), dict) else {}
        logo_pixmap = self._square_pixmap(
            str(shop.get("path", "") or ""),
            logo_size,
            "?",
            background="#f3f4f6",
            foreground="#4b5563",
        )
        painter.drawPixmap(int(logo_rect.x()), int(logo_rect.y()), logo_pixmap)

        thumb_size = 18
        thumb_positions = [QPointF(width - 34, 6), QPointF(width - 22, 24)]
        visible_items = item_previews[:2]
        if not visible_items:
            visible_items = [{"produkt_name": "Produkt", "path": "", "source_url": ""}]
        for index, item in enumerate(visible_items[:2]):
            if index >= len(thumb_positions):
                break
            item_label = str(item.get("produkt_name", "") or "PR").strip()[:2] or "PR"
            item_pixmap = self._square_pixmap(
                str(item.get("path", "") or ""),
                thumb_size,
                item_label,
                background="#202233",
                foreground="#a9b1d6",
            )
            position = thumb_positions[index]
            painter.drawPixmap(int(position.x()), int(position.y()), item_pixmap)

        remaining = max(0, int(preview.get("remaining_item_count", 0) or 0))
        if remaining > 0:
            badge_rect = QRectF(width - 28, 0, 26, 18)
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor("#7aa2f7"))
            painter.drawRoundedRect(badge_rect, 9, 9)
            painter.setPen(QColor("#111827"))
            font = QFont()
            font.setBold(True)
            font.setPointSize(8)
            painter.setFont(font)
            painter.drawText(badge_rect, int(Qt.AlignmentFlag.AlignCenter), f"+{remaining}")

        painter.end()
        self._visual_cache[cache_key] = canvas
        return canvas

    def render_visual_icon(self, preview, width=86, height=48):
        return QIcon(self.render_visual_pixmap(preview, width=width, height=height))

    def build_tooltip(self, preview):
        self._ensure_revision_current()
        preview = preview if isinstance(preview, dict) else {}
        parts = []
        shop = preview.get("shop", {}) if isinstance(preview.get("shop"), dict) else {}
        shop_context = shop.get("context", {}) if isinstance(shop.get("context"), dict) else {}
        shop_name = str(shop_context.get("shop_name", "") or shop_context.get("sender_domain", "") or "").strip()
        if shop_name:
            parts.append(shop_name)
        names = [
            str(item.get("produkt_name", "") or "").strip()
            for item in list(preview.get("item_previews", []) or [])
            if str(item.get("produkt_name", "") or "").strip()
        ]
        if names:
            parts.append(", ".join(names[:2]))
        remaining = max(0, int(preview.get("remaining_item_count", 0) or 0))
        if remaining > 0:
            parts.append(f"+{remaining} weitere Produkte")
        if not parts:
            parts.append("Keine Bestell-Visuals verfuegbar")
        return "\n".join(parts)

    def _empty_preview(self, order_id=0, shop_name=""):
        return {
            "einkauf_id": int(order_id or 0),
            "shop": {
                "path": "",
                "context": {"shop_name": str(shop_name or "").strip()},
            },
            "item_previews": [],
            "remaining_item_count": 0,
        }

    def _placeholder_pixmap(self, label, size, background, foreground):
        label = str(label or "").strip().upper()[:2] or "?"
        cache_key = (label, int(size), str(background), str(foreground))
        if cache_key in self._placeholder_cache:
            return self._placeholder_cache[cache_key]

        pixmap = create_placeholder_pixmap(label, size, background=background, foreground=foreground, radius=8)
        self._placeholder_cache[cache_key] = pixmap
        return pixmap

    def _square_pixmap(self, path_value, size, fallback_label, background, foreground):
        cache_key = (str(path_value or ""), int(size), str(fallback_label or ""), str(background), str(foreground))
        if cache_key in self._thumbnail_cache:
            return self._thumbnail_cache[cache_key]

        source = QPixmap(str(path_value or ""))
        if source.isNull():
            result = self._placeholder_pixmap(fallback_label, size, background, foreground)
            self._thumbnail_cache[cache_key] = result
            return result

        canvas = render_preview_pixmap(source, size, background="#ffffff", radius=8, inset=2)
        self._thumbnail_cache[cache_key] = canvas
        return canvas


class CompactOrderVisualWidget(QLabel):
    def __init__(self, resolver, parent=None):
        super().__init__(parent)
        self._resolver = resolver
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setStyleSheet("QLabel { background-color: transparent; border: none; }")
        self.setMinimumSize(QSize(90, 52))

    def set_visual_preview(self, preview, tooltip=""):
        pixmap = self._resolver.render_visual_pixmap(preview)
        self.setPixmap(pixmap)
        self.setFixedSize(pixmap.size())
        self.setToolTip(str(tooltip or self._resolver.build_tooltip(preview) or "").strip())

