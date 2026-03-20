"""
modul_tracker.py
Kartenbasiertes Tracking-Board fuer Inbound und Outbound mit Status-Historie.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

from PyQt6.QtCore import Qt, QTimer, QUrl, pyqtSignal
from PyQt6.QtGui import QDesktopServices, QPixmap
from PyQt6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from config import resource_path
from module.crash_logger import log_exception
from module.database_manager import DatabaseManager
from module.order_visual_state import OrderVisualState
from module.order_visual_ui import OrderVisualResolver
from module.status_model import ShipmentStatus, shipment_db_value
from module.tracking_link_utils import build_tracking_target
from module.ui_media_pixmap import create_placeholder_pixmap, render_card_visual_pixmap, render_preview_pixmap


STATUS_SECTION_ORDER = (
    ShipmentStatus.ISSUE_DELAYED.value,
    ShipmentStatus.OUT_FOR_DELIVERY.value,
    ShipmentStatus.IN_TRANSIT.value,
    ShipmentStatus.NOT_DISPATCHED.value,
    ShipmentStatus.DELIVERED.value,
)

STATUS_SECTION_TITLES = {
    ShipmentStatus.ISSUE_DELAYED.value: "Problem / Verzoegert",
    ShipmentStatus.OUT_FOR_DELIVERY.value: "In Auslieferung",
    ShipmentStatus.IN_TRANSIT.value: "Unterwegs",
    ShipmentStatus.NOT_DISPATCHED.value: "Noch nicht los",
    ShipmentStatus.DELIVERED.value: "Geliefert (7 Tage)",
}

STATUS_STYLE_MAP = {
    ShipmentStatus.ISSUE_DELAYED.value: {"bg": "#4c1d1d", "fg": "#fca5a5", "border": "#dc2626"},
    ShipmentStatus.OUT_FOR_DELIVERY.value: {"bg": "#3b2f14", "fg": "#fcd34d", "border": "#f59e0b"},
    ShipmentStatus.IN_TRANSIT.value: {"bg": "#1f3a5f", "fg": "#93c5fd", "border": "#3b82f6"},
    ShipmentStatus.NOT_DISPATCHED.value: {"bg": "#2a2f45", "fg": "#c0caf5", "border": "#565f89"},
    ShipmentStatus.DELIVERED.value: {"bg": "#17321f", "fg": "#86efac", "border": "#22c55e"},
}

SOURCE_LABELS = {
    "tracker_manual": "Tracking Radar",
    "wareneingang_finish": "Wareneingang",
    "order_processing_upsert": "Bestellimport",
}

CARRIER_ICON_PATHS = {
    "dhl": "assets/icons/tracker_carrier_dhl.svg",
    "dpd": "assets/icons/tracker_carrier_dpd.svg",
    "gls": "assets/icons/tracker_carrier_gls.svg",
    "hermes": "assets/icons/tracker_carrier_hermes.svg",
    "ups": "assets/icons/tracker_carrier_ups.svg",
    "amazon": "assets/icons/tracker_carrier_amazon.svg",
}


def _format_date(value) -> str:
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    text = str(value or "").strip()
    if not text:
        return "-"
    return text[:10]


def _format_datetime(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%Y %H:%M")
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    text = str(value or "").strip()
    return text or "-"


def _short_tracking(value: str) -> str:
    text = str(value or "").strip()
    if len(text) <= 16:
        return text or "-"
    return f"{text[:7]}...{text[-6:]}"


def _carrier_key(paketdienst: str) -> str:
    text = str(paketdienst or "").strip().lower()
    if "dhl" in text or "deutsche post" in text:
        return "dhl"
    if "dpd" in text:
        return "dpd"
    if "gls" in text:
        return "gls"
    if "hermes" in text:
        return "hermes"
    if "ups" in text:
        return "ups"
    if "amazon" in text or "swiship" in text:
        return "amazon"
    return ""


def _clear_layout(layout):
    while layout.count():
        item = layout.takeAt(0)
        widget = item.widget()
        child_layout = item.layout()
        if widget is not None:
            widget.deleteLater()
        elif child_layout is not None:
            _clear_layout(child_layout)


def _load_tracker_visual_pixmaps(preview: dict):
    """Baut die grosse Kartenoptik wie im Wareneingang: Shop-Logo plus Produktbild-Overlay."""
    logo_px = None
    item_px = None
    total_menge = 1
    try:
        preview = preview if isinstance(preview, dict) else {}

        shop_info = preview.get("shop") or {}
        logo_path = str(shop_info.get("path", "") or "").strip()
        if logo_path:
            src = QPixmap(logo_path)
            if not src.isNull():
                logo_px = render_preview_pixmap(src, 96, background="#ffffff", radius=12, inset=4)

        items = list(preview.get("item_previews") or [])
        remaining = int(preview.get("remaining_item_count", 0) or 0)
        total_menge = len(items) + remaining
        if total_menge < 1:
            total_menge = 1
        if items:
            item = items[0] or {}
            item_path = str(item.get("path", "") or "").strip()
            if item_path:
                src_item = QPixmap(item_path)
                if not src_item.isNull():
                    item_px = render_preview_pixmap(src_item, 52, background="#202233", radius=8, inset=2)
        return logo_px, item_px, total_menge
    except Exception as exc:
        logging.debug("Tracker-Visual konnte nicht vorbereitet werden: %s", exc)
        return logo_px, item_px, total_menge


class TrackerOrderVisualWidget(QLabel):
    def __init__(self, preview: dict, shop_name: str, parent=None):
        super().__init__(parent)
        self._preview = preview if isinstance(preview, dict) else {}
        self._shop_name = str(shop_name or "").strip()
        self.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.setStyleSheet("QLabel { background: transparent; border: none; }")
        self.setFixedSize(122, 122)
        QTimer.singleShot(0, self._load_visuals)

    def _load_visuals(self):
        shop_context = self._preview.get("shop", {}) if isinstance(self._preview.get("shop"), dict) else {}
        fallback_shop_name = str(
            (shop_context.get("context") or {}).get("shop_name", "")
            or (shop_context.get("context") or {}).get("sender_domain", "")
            or self._shop_name
            or ""
        ).strip()
        logo_px, item_px, total_menge = _load_tracker_visual_pixmaps(
            self._preview,
        )
        if logo_px is None or logo_px.isNull():
            logo_px = create_placeholder_pixmap(
                fallback_shop_name[:2] or "?",
                96,
                background="#f3f4f6",
                foreground="#4b5563",
                radius=12,
            )
        if item_px is None or item_px.isNull():
            item_px = create_placeholder_pixmap(
                "...",
                52,
                background="#2a2f45",
                foreground="#7aa2f7",
                radius=8,
            )
        composite = render_card_visual_pixmap(logo_px, item_px, total_menge, logo_size=96, item_size=52)
        self.setPixmap(composite)
        self.setFixedSize(composite.size())


class CarrierBadgeWidget(QFrame):
    def __init__(self, paketdienst: str, parent=None):
        super().__init__(parent)
        self.setObjectName("CarrierBadgeWidget")
        self.setStyleSheet("QFrame#CarrierBadgeWidget { background-color: #202233; border: 1px solid #414868; border-radius: 12px; }")

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(0)

        text = str(paketdienst or "").strip() or "Unbekannt"
        key = _carrier_key(text)
        icon_path = CARRIER_ICON_PATHS.get(key)
        pixmap = QPixmap(resource_path(icon_path)) if icon_path else QPixmap()

        if icon_path and not pixmap.isNull():
            icon_label = QLabel()
            icon_label.setPixmap(pixmap)
            icon_label.setFixedSize(108, 48)
            icon_label.setScaledContents(True)
            icon_label.setStyleSheet("QLabel { border: none; background: transparent; }")
            layout.addWidget(icon_label, 0, Qt.AlignmentFlag.AlignCenter)
            self.setFixedSize(128, 72)
            return

        label = QLabel(text[:3].upper() or "?")
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setStyleSheet("QLabel { color: #c0caf5; font-size: 18px; font-weight: bold; border: none; }")
        layout.addWidget(label, 0, Qt.AlignmentFlag.AlignCenter)
        self.setFixedSize(128, 72)


class HistoryEntryWidget(QFrame):
    def __init__(self, row: dict, parent=None):
        super().__init__(parent)
        self.setObjectName("HistoryEntryWidget")
        self.setStyleSheet("QFrame#HistoryEntryWidget { background-color: #1f2335; border: 1px solid #414868; border-radius: 8px; }")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)
        layout.setSpacing(4)

        title = QLabel(_format_datetime(row.get("created_at")))
        title.setStyleSheet("QLabel { color: #7aa2f7; font-size: 12px; font-weight: bold; border: none; }")
        layout.addWidget(title)

        old_status = str(row.get("old_status", "") or "Unbekannt").strip()
        new_status = str(row.get("new_status", "") or "Unbekannt").strip()
        lbl_change = QLabel(f"{old_status} -> {new_status}")
        lbl_change.setStyleSheet("QLabel { color: #c0caf5; font-size: 13px; border: none; }")
        layout.addWidget(lbl_change)

        source = SOURCE_LABELS.get(str(row.get("source", "") or "").strip(), str(row.get("source", "") or "").strip() or "System")
        note = str(row.get("note", "") or "").strip()
        meta_text = f"Quelle: {source}"
        if note:
            meta_text += f" | {note}"
        lbl_meta = QLabel(meta_text)
        lbl_meta.setWordWrap(True)
        lbl_meta.setStyleSheet("QLabel { color: #a9b1d6; font-size: 11px; border: none; }")
        layout.addWidget(lbl_meta)


class StatusHistoryDialog(QDialog):
    def __init__(self, title: str, rows: list[dict], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Status-Historie")
        self.resize(560, 420)
        self.setStyleSheet("background-color: #1a1b26; color: #c0caf5;")

        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(12)

        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("font-size: 18px; font-weight: bold; color: #7aa2f7;")
        root.addWidget(lbl_title)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setStyleSheet("QScrollArea { background: transparent; border: none; }")
        root.addWidget(scroll, 1)

        content = QWidget()
        content.setStyleSheet("background: transparent;")
        layout = QVBoxLayout(content)
        layout.setContentsMargins(0, 0, 4, 0)
        layout.setSpacing(8)

        if rows:
            for row in rows:
                layout.addWidget(HistoryEntryWidget(row, content))
        else:
            empty = QLabel("Noch keine aufgezeichneten Statusaenderungen vorhanden.")
            empty.setWordWrap(True)
            empty.setStyleSheet("QLabel { color: #a9b1d6; font-size: 13px; border: none; }")
            layout.addWidget(empty)

        layout.addStretch(1)
        scroll.setWidget(content)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)


class TrackerShipmentCard(QFrame):
    refreshRequested = pyqtSignal()
    expandRequested = pyqtSignal(object)

    def __init__(self, db: DatabaseManager, order_visuals: OrderVisualResolver, shipment: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.order_visuals = order_visuals
        self.shipment = dict(shipment or {})
        self._expanded = False
        self.setObjectName("TrackerShipmentCard")

        self.setStyleSheet(
            "QFrame#TrackerShipmentCard { background-color: #171824; border: 1px solid #414868; border-radius: 12px; }"
            "QFrame#TrackerShipmentCard:hover { background-color: #1c2030; }"
        )

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        self.summary_frame = QFrame(self)
        self.summary_frame.setObjectName("TrackerSummaryFrame")
        self.summary_frame.setStyleSheet("QFrame#TrackerSummaryFrame { background: transparent; border: none; }")
        summary = QHBoxLayout(self.summary_frame)
        summary.setContentsMargins(0, 0, 0, 0)
        summary.setSpacing(14)
        root.addWidget(self.summary_frame)

        preview = self._build_preview()
        self.visual_widget = TrackerOrderVisualWidget(
            preview,
            self.shipment.get("shop_name", ""),
            self,
        )
        summary.addWidget(self.visual_widget, 0, Qt.AlignmentFlag.AlignTop)

        info_col = QVBoxLayout()
        info_col.setSpacing(4)
        info_col.setContentsMargins(0, 0, 0, 0)
        summary.addLayout(info_col, 1)

        self.carrier_widget = CarrierBadgeWidget(self.shipment.get("paketdienst", ""), self)
        summary.addWidget(self.carrier_widget, 0, Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignRight)

        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(8)
        lbl_title = QLabel(self._title_text())
        lbl_title.setStyleSheet("QLabel { color: #c0caf5; font-size: 16px; font-weight: bold; border: none; }")
        top_row.addWidget(lbl_title, 1)
        top_row.addWidget(self._build_status_badge(self.shipment.get("status_code")))
        info_col.addLayout(top_row)

        lbl_subtitle = QLabel(self._subtitle_text())
        lbl_subtitle.setStyleSheet("QLabel { color: #7aa2f7; font-size: 12px; border: none; }")
        info_col.addWidget(lbl_subtitle)

        meta_parts = []
        item_count = int(self.shipment.get("item_count", 0) or 0)
        if item_count > 0:
            meta_parts.append(f"{item_count} Artikel")
        meta_parts.append(f"Tracking: {_short_tracking(self.shipment.get('tracking_number', ''))}")
        lbl_meta = QLabel(" | ".join(meta_parts))
        lbl_meta.setWordWrap(True)
        lbl_meta.setStyleSheet("QLabel { color: #a9b1d6; font-size: 12px; border: none; }")
        info_col.addWidget(lbl_meta)

        lbl_date = QLabel(self._date_line_text())
        lbl_date.setStyleSheet("QLabel { color: #565f89; font-size: 11px; border: none; }")
        info_col.addWidget(lbl_date)

        info_col.addStretch(1)

        self.detail_frame = QFrame(self)
        self.detail_frame.setVisible(False)
        self.detail_frame.setStyleSheet("QFrame { background-color: #1f2335; border: 1px solid #30374d; border-radius: 10px; }")
        root.addWidget(self.detail_frame)

        detail = QVBoxLayout(self.detail_frame)
        detail.setContentsMargins(12, 12, 12, 12)
        detail.setSpacing(10)

        for text in self._detail_lines():
            label = QLabel(text)
            label.setWordWrap(True)
            label.setStyleSheet("QLabel { color: #c0caf5; font-size: 12px; border: none; }")
            detail.addWidget(label)

        action_row = QHBoxLayout()
        action_row.setContentsMargins(0, 0, 0, 0)
        action_row.setSpacing(8)
        detail.addLayout(action_row)

        self.status_combo = QComboBox()
        self.status_combo.setStyleSheet(
            "QComboBox { background-color: #24283b; color: #c0caf5; border: 1px solid #414868; border-radius: 6px; padding: 6px 10px; }"
        )
        for status in (
            ShipmentStatus.NOT_DISPATCHED,
            ShipmentStatus.IN_TRANSIT,
            ShipmentStatus.OUT_FOR_DELIVERY,
            ShipmentStatus.ISSUE_DELAYED,
            ShipmentStatus.DELIVERED,
        ):
            self.status_combo.addItem(shipment_db_value(status), status.value)
        current_status = str(self.shipment.get("status_code", ShipmentStatus.NOT_DISPATCHED.value) or ShipmentStatus.NOT_DISPATCHED.value)
        for index in range(self.status_combo.count()):
            if self.status_combo.itemData(index) == current_status:
                self.status_combo.setCurrentIndex(index)
                break
        action_row.addWidget(self.status_combo, 1)

        btn_save = QPushButton("Status speichern")
        btn_save.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_save.setStyleSheet(
            "QPushButton { background-color: #7aa2f7; color: #111827; border: none; border-radius: 6px; padding: 6px 12px; font-weight: bold; }"
            "QPushButton:hover { background-color: #a9c4fb; }"
        )
        btn_save.clicked.connect(self._save_status_change)
        action_row.addWidget(btn_save)

        footer_row = QHBoxLayout()
        footer_row.setContentsMargins(0, 0, 0, 0)
        footer_row.setSpacing(8)
        detail.addLayout(footer_row)

        btn_track = QPushButton("Tracken")
        btn_track.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_track.setStyleSheet(
            "QPushButton { background-color: #3b4261; color: white; border: none; border-radius: 6px; padding: 6px 12px; }"
        )
        btn_track.clicked.connect(self._open_tracking)
        footer_row.addWidget(btn_track)

        btn_history = QPushButton("H")
        btn_history.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_history.setFixedWidth(36)
        btn_history.setToolTip("Status-Historie anzeigen")
        btn_history.setStyleSheet(
            "QPushButton { background-color: #202233; color: #e0af68; border: 1px solid #414868; border-radius: 18px; font-weight: bold; padding: 6px 0px; }"
            "QPushButton:hover { background-color: #2a3050; }"
        )
        btn_history.clicked.connect(self._show_history)
        footer_row.addWidget(btn_history)
        footer_row.addStretch(1)

    def _build_preview(self):
        if self.shipment.get("direction") == "inbound":
            return self.order_visuals.build_order_preview(
                self.shipment.get("id"),
                shop_name=self.shipment.get("shop_name", ""),
            )
        return self.order_visuals.build_package_preview(self.shipment.get("id"))

    def _title_text(self) -> str:
        if self.shipment.get("direction") == "inbound":
            return str(self.shipment.get("shop_name", "") or "Unbekannter Shop").strip()
        return str(self.shipment.get("title", "") or f"Paket #{self.shipment.get('id', '-')}")

    def _subtitle_text(self) -> str:
        if self.shipment.get("direction") == "inbound":
            return f"Bestellung {str(self.shipment.get('bestellnummer', '') or '-').strip()}"
        return f"Tracking {_short_tracking(self.shipment.get('tracking_number', ''))}"

    def _date_line_text(self) -> str:
        if self.shipment.get("direction") == "inbound":
            return f"Bestellt: {_format_date(self.shipment.get('kaufdatum'))} | Letzte Aenderung: {_format_datetime(self.shipment.get('last_status_change_at'))}"
        return f"Versanddatum: {_format_date(self.shipment.get('versanddatum'))} | Letzte Aenderung: {_format_datetime(self.shipment.get('last_status_change_at'))}"

    def _detail_lines(self) -> list[str]:
        base = [
            f"Volle Tracking-Nummer: {str(self.shipment.get('tracking_number', '') or '-').strip()}",
            f"Paketdienst: {str(self.shipment.get('paketdienst', '') or 'Unbekannt').strip()}",
            f"Aktueller Status: {str(self.shipment.get('status_label', '') or '-').strip()}",
        ]
        direct_link = str(self.shipment.get("tracking_url", "") or "").strip()
        if direct_link:
            base.append(f"Direkter Tracking-Link: {direct_link}")
        if self.shipment.get("direction") == "inbound":
            base.insert(0, f"Bestellnummer: {str(self.shipment.get('bestellnummer', '') or '-').strip()}")
            base.append(f"Kaufdatum: {_format_date(self.shipment.get('kaufdatum'))}")
            base.append(f"Lieferdatum: {_format_date(self.shipment.get('lieferdatum'))}")
            base.append(f"Wareneingang: {_format_date(self.shipment.get('wareneingang_datum'))}")
        else:
            base.insert(0, f"Paket-ID: {self.shipment.get('id', '-')}")
            base.append(f"Versanddatum: {_format_date(self.shipment.get('versanddatum'))}")
        return base

    def _build_status_badge(self, status_code):
        code = str(status_code or ShipmentStatus.NOT_DISPATCHED.value)
        style = STATUS_STYLE_MAP.get(code, STATUS_STYLE_MAP[ShipmentStatus.NOT_DISPATCHED.value])
        label = QLabel(shipment_db_value(code))
        label.setStyleSheet(
            "QLabel {"
            f"background-color: {style['bg']}; color: {style['fg']}; border: 1px solid {style['border']};"
            "border-radius: 10px; padding: 4px 10px; font-size: 11px; font-weight: bold;"
            "}"
        )
        return label

    def set_expanded(self, expanded: bool):
        self._expanded = bool(expanded)
        self.detail_frame.setVisible(self._expanded)
        border_color = "#7aa2f7" if self._expanded else "#414868"
        border_width = "2px" if self._expanded else "1px"
        self.setStyleSheet(
            "QFrame#TrackerShipmentCard { background-color: #171824;"
            f" border: {border_width} solid {border_color}; border-radius: 12px; }}"
            "QFrame#TrackerShipmentCard:hover { background-color: #1c2030; }"
        )

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.expandRequested.emit(self)
        super().mousePressEvent(event)

    def _save_status_change(self):
        selected_status = self.status_combo.currentData()
        try:
            if self.shipment.get("direction") == "inbound":
                self.db.set_inbound_shipment_status(
                    self.shipment.get("id"),
                    selected_status,
                    source="tracker_manual",
                    note="Status im Tracking Radar geaendert",
                )
            else:
                self.db.set_outbound_shipment_status(
                    self.shipment.get("id"),
                    selected_status,
                    source="tracker_manual",
                    note="Status im Tracking Radar geaendert",
                )
            self.refreshRequested.emit()
        except Exception as exc:
            log_exception(__name__, exc)
            QMessageBox.critical(self, "DB Fehler", f"Konnte Status nicht aktualisieren:\n{exc}")

    def _show_history(self):
        try:
            rows = self.db.list_shipment_status_history(self.shipment.get("direction"), self.shipment.get("id"))
        except Exception as exc:
            log_exception(__name__, exc)
            QMessageBox.critical(self, "Fehler", f"Konnte Historie nicht laden:\n{exc}")
            return
        dialog = StatusHistoryDialog(self._title_text(), rows, self)
        dialog.exec()

    def _open_tracking(self):
        target = build_tracking_target(self.shipment)
        url = str(target.get("url", "") or "").strip()
        if not url:
            QMessageBox.information(
                self,
                "Tracking-Link fehlt",
                str(target.get("reason", "") or "Fuer diese Sendung ist kein passender Tracking-Link gespeichert."),
            )
            return
        QDesktopServices.openUrl(QUrl(url))


class TrackerStatusSection(QFrame):
    def __init__(self, title: str, collapsible=False, collapsed=False, parent=None):
        super().__init__(parent)
        self._collapsible = bool(collapsible)
        self._collapsed = bool(collapsible and collapsed)
        self.setObjectName("TrackerStatusSection")
        self.setStyleSheet("QFrame#TrackerStatusSection { background-color: transparent; border: none; }")

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(8)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)
        root.addLayout(header_row)

        self.btn_toggle = QPushButton()
        self.btn_toggle.setFlat(True)
        self.btn_toggle.setCursor(Qt.CursorShape.PointingHandCursor if self._collapsible else Qt.CursorShape.ArrowCursor)
        self.btn_toggle.setStyleSheet(
            "QPushButton { color: #7aa2f7; font-size: 15px; font-weight: bold; text-align: left; border: none; padding: 0px; }"
        )
        if self._collapsible:
            self.btn_toggle.clicked.connect(self._toggle)
        else:
            self.btn_toggle.setEnabled(False)
        header_row.addWidget(self.btn_toggle, 1)

        self.lbl_count = QLabel("0")
        self.lbl_count.setStyleSheet("QLabel { color: #565f89; font-size: 12px; border: none; }")
        header_row.addWidget(self.lbl_count)

        self.content = QWidget(self)
        self.content.setStyleSheet("background: transparent;")
        self.content_layout = QVBoxLayout(self.content)
        self.content_layout.setContentsMargins(0, 0, 0, 0)
        self.content_layout.setSpacing(10)
        root.addWidget(self.content)

        self._base_title = title
        self._update_header()

    def _toggle(self):
        self._collapsed = not self._collapsed
        self._update_header()

    def _update_header(self):
        prefix = ""
        if self._collapsible:
            prefix = "> " if self._collapsed else "v "
        self.btn_toggle.setText(f"{prefix}{self._base_title}")
        self.content.setVisible(not self._collapsed)

    def set_cards(self, cards: list[TrackerShipmentCard]):
        self.lbl_count.setText(str(len(cards)))
        _clear_layout(self.content_layout)
        if cards:
            for card in cards:
                self.content_layout.addWidget(card)
        else:
            empty = QLabel("Keine Sendungen in diesem Status.")
            empty.setStyleSheet("QLabel { color: #565f89; font-size: 12px; border: none; }")
            self.content_layout.addWidget(empty)
        self.content_layout.addStretch(1)


class TrackerBoardColumn(QWidget):
    refreshRequested = pyqtSignal()

    def __init__(self, title: str, direction: str, db: DatabaseManager, order_visuals: OrderVisualResolver, parent=None):
        super().__init__(parent)
        self.title = title
        self.direction = direction
        self.db = db
        self.order_visuals = order_visuals
        self._expanded_card = None

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        lbl_title = QLabel(title)
        lbl_title.setStyleSheet("font-size: 16px; font-weight: bold; color: #7aa2f7;")
        root.addWidget(lbl_title)

        self.empty_label = QLabel("Keine relevanten Sendungen vorhanden.")
        self.empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.empty_label.setStyleSheet("color: #565f89; font-size: 13px;")
        self.empty_label.hide()
        root.addWidget(self.empty_label)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setStyleSheet("QScrollArea { background: transparent; border: none; }")
        root.addWidget(scroll, 1)

        self.content = QWidget()
        self.content.setStyleSheet("background: transparent;")
        self.content_layout = QVBoxLayout(self.content)
        self.content_layout.setContentsMargins(0, 0, 4, 0)
        self.content_layout.setSpacing(16)
        scroll.setWidget(self.content)

        self.sections = {}
        for status_code in STATUS_SECTION_ORDER:
            section = TrackerStatusSection(
                STATUS_SECTION_TITLES[status_code],
                collapsible=(status_code == ShipmentStatus.DELIVERED.value),
                collapsed=(status_code == ShipmentStatus.DELIVERED.value),
                parent=self.content,
            )
            self.sections[status_code] = section
            self.content_layout.addWidget(section)
        self.content_layout.addStretch(1)

    def set_shipments(self, shipments: list[dict]):
        grouped = {status_code: [] for status_code in STATUS_SECTION_ORDER}
        for shipment in shipments:
            card = TrackerShipmentCard(self.db, self.order_visuals, shipment, self.content)
            card.expandRequested.connect(self._handle_expand_request)
            card.refreshRequested.connect(self.refreshRequested.emit)
            grouped.setdefault(shipment.get("status_code"), []).append(card)

        total = 0
        for status_code in STATUS_SECTION_ORDER:
            cards = grouped.get(status_code, [])
            total += len(cards)
            self.sections[status_code].set_cards(cards)
        self.empty_label.setVisible(total == 0)
        self._expanded_card = None

    def _handle_expand_request(self, card):
        if self._expanded_card is card:
            card.set_expanded(False)
            self._expanded_card = None
            return

        if self._expanded_card is not None:
            self._expanded_card.set_expanded(False)
        self._expanded_card = card
        self._expanded_card.set_expanded(True)


class TrackerApp(QWidget):
    def __init__(self, settings_manager):
        super().__init__()
        self.settings = settings_manager
        self.db = DatabaseManager(self.settings)
        self.order_visuals = OrderVisualResolver(self.settings)
        self._pending_visual_refresh = False
        self._visual_refresh_timer = QTimer(self)
        self._visual_refresh_timer.setSingleShot(True)
        self._visual_refresh_timer.timeout.connect(self._reload_visual_board)
        OrderVisualState.bus().visualsInvalidated.connect(self._on_visuals_invalidated)

        self._build_ui()
        QTimer.singleShot(100, self.refresh_data)

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        top_bar = QHBoxLayout()
        lbl_title = QLabel("Tracking Radar")
        lbl_title.setStyleSheet("font-size: 20px; font-weight: bold; color: #7aa2f7;")

        btn_refresh = QPushButton("Aktualisieren")
        btn_refresh.setProperty("class", "retro-btn")
        btn_refresh.clicked.connect(self.refresh_data)

        top_bar.addWidget(lbl_title)
        top_bar.addStretch()
        top_bar.addWidget(btn_refresh)
        layout.addLayout(top_bar)

        content_row = QHBoxLayout()
        content_row.setSpacing(16)
        layout.addLayout(content_row, 1)

        self.panel_inbound = QFrame()
        self.panel_inbound.setObjectName("TrackerPanel")
        self.panel_inbound.setFrameShape(QFrame.Shape.NoFrame)
        self.panel_inbound.setStyleSheet("QFrame#TrackerPanel { background-color: #1a1b26; border: 1px solid #414868; border-radius: 6px; }")
        inbound_layout = QVBoxLayout(self.panel_inbound)
        inbound_layout.setContentsMargins(0, 0, 0, 0)
        self.inbound_column = TrackerBoardColumn("Inbound Pakete", "inbound", self.db, self.order_visuals, self.panel_inbound)
        self.inbound_column.refreshRequested.connect(self.refresh_data)
        inbound_layout.addWidget(self.inbound_column)

        self.panel_outbound = QFrame()
        self.panel_outbound.setObjectName("TrackerPanel")
        self.panel_outbound.setFrameShape(QFrame.Shape.NoFrame)
        self.panel_outbound.setStyleSheet("QFrame#TrackerPanel { background-color: #1a1b26; border: 1px solid #414868; border-radius: 6px; }")
        outbound_layout = QVBoxLayout(self.panel_outbound)
        outbound_layout.setContentsMargins(0, 0, 0, 0)
        self.outbound_column = TrackerBoardColumn("Outbound Pakete", "outbound", self.db, self.order_visuals, self.panel_outbound)
        self.outbound_column.refreshRequested.connect(self.refresh_data)
        outbound_layout.addWidget(self.outbound_column)

        content_row.addWidget(self.panel_inbound, 1)
        content_row.addWidget(self.panel_outbound, 1)

    def _on_visuals_invalidated(self, payload):
        self._pending_visual_refresh = True
        if not self.isVisible():
            return
        logging.debug("Tracker-Visual-Refresh eingeplant: %s", (payload or {}).get("reason", "unknown"))
        self._visual_refresh_timer.start(150)

    def _reload_visual_board(self):
        self._pending_visual_refresh = False
        self.refresh_data()

    def showEvent(self, event):
        super().showEvent(event)
        if self._pending_visual_refresh:
            self._visual_refresh_timer.start(0)

    def refresh_data(self):
        try:
            inbound_rows = self.db.list_tracker_inbound_shipments(delivered_days=7)
            outbound_rows = self.db.list_tracker_outbound_shipments(delivered_days=7)
            self.inbound_column.set_shipments(inbound_rows)
            self.outbound_column.set_shipments(outbound_rows)
        except Exception as exc:
            log_exception(__name__, exc)
            QMessageBox.critical(self, "Fehler", f"Fehler beim Laden des Tracking Radar:\n{exc}")
