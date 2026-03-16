"""
modul_wareneingang.py
Wareneingang in zwei Seiten:
  Seite 1 – Kartenliste aller offenen eingehenden Sendungen (visuelles Pitch-Layout)
  Seite 2 – Detailprüfung der gewählten Bestellung per Auge oder Scanner
"""

from __future__ import annotations

import logging

from PyQt6.QtCore import Qt, QSize, QTimer, pyqtSignal
from PyQt6.QtGui import QColor, QFont, QIcon, QPixmap
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QButtonGroup,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QScrollArea,
    QSizePolicy,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from module.crash_logger import log_exception
from module.database_manager import DatabaseManager
from module.lookup_service import LookupService
from module.lookup_results import FieldState, FieldType
from module.order_visual_state import OrderVisualState
from module.order_visual_ui import CompactOrderVisualWidget, OrderVisualResolver
from module.status_model import InventoryStatus, ShipmentStatus, shipment_db_value
from module.ui_media_pixmap import (
    create_placeholder_pixmap,
    render_card_visual_pixmap,
    render_preview_pixmap,
)

# ---------------------------------------------------------------------------
# Hilfsfunktion – Mediapfade laden (Logo + erstes Produktbild)
# ---------------------------------------------------------------------------

def _load_card_pixmaps(order_visuals: OrderVisualResolver, order_id: int, shop_name: str):
    """Gibt (logo_px, item_px, total_menge) zurück – alle Fehlerfälle abgefangen."""
    logo_px = None
    item_px = None
    total_menge = 1
    try:
        preview = order_visuals.build_order_preview(order_id, shop_name=shop_name)
        if not isinstance(preview, dict):
            return logo_px, item_px, total_menge

        # --- Shop-Logo ---
        shop_info = preview.get("shop") or {}
        logo_path = str(shop_info.get("path", "") or "").strip()
        if logo_path:
            src = QPixmap(logo_path)
            if not src.isNull():
                logo_px = render_preview_pixmap(src, 72, background="#ffffff", radius=10, inset=3)

        # --- Erstes Produktbild ---
        items = preview.get("item_previews") or []
        remaining = int(preview.get("remaining_item_count", 0) or 0)
        total_menge = len(items) + remaining
        if total_menge < 1:
            total_menge = 1
        if items:
            first_path = str(items[0].get("path", "") or "").strip()
            if first_path:
                src_i = QPixmap(first_path)
                if not src_i.isNull():
                    item_px = render_preview_pixmap(src_i, 36, background="#202233", radius=6, inset=1)

    except Exception:
        pass
    return logo_px, item_px, total_menge


# ---------------------------------------------------------------------------
# OrderCardWidget – eine einzelne Bestellkarte (großer Radiobutton)
# ---------------------------------------------------------------------------

class OrderCardWidget(QFrame):
    """Zeigt eine Bestellung als große, klickbare Karte (exklusiv auswählbar).

    Signale:
        selected(order_id)  – Karte wurde angeklickt
        activated(order_id) – Karte wurde doppelgeklickt (→ Seite 2 öffnen)
    """

    selected = pyqtSignal(int)
    activated = pyqtSignal(int)

    _STYLE_NORMAL = (
        "OrderCardWidget { background-color: #171824; border: 1px solid #414868;"
        " border-radius: 10px; }"
        "OrderCardWidget:hover { background-color: #1c2030; }"
    )
    _STYLE_SELECTED = (
        "OrderCardWidget { background-color: #1a1f35; border: 2px solid #7aa2f7;"
        " border-radius: 10px; }"
    )

    def __init__(self, order_id: int, order_data: dict, order_visuals: OrderVisualResolver, parent=None):
        super().__init__(parent)
        self.order_id = order_id
        self._selected = False
        self.setStyleSheet(self._STYLE_NORMAL)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setMinimumHeight(96)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(12, 10, 16, 10)
        layout.setSpacing(14)

        # Radiobutton-Indikator (ohne Text, nur der Kreis)
        self._radio = QRadioButton()
        self._radio.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self._radio.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        self._radio.setStyleSheet("QRadioButton { border: none; }")
        layout.addWidget(self._radio, 0, Qt.AlignmentFlag.AlignVCenter)

        # Composite-Bild: Logo + Thumbnail-Overlay
        self._lbl_visual = QLabel()
        self._lbl_visual.setFixedSize(72 + 18, 72 + 18)  # Platz für Überlappung
        self._lbl_visual.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        layout.addWidget(self._lbl_visual, 0, Qt.AlignmentFlag.AlignVCenter)

        # Textblock
        text_layout = QVBoxLayout()
        text_layout.setSpacing(3)
        text_layout.setContentsMargins(0, 0, 0, 0)

        shop = str(order_data.get("shop_name") or "Unbekannter Shop").strip()
        bestellnr = str(order_data.get("bestellnummer") or "–").strip()
        lieferdienst = str(order_data.get("paketdienst") or "").strip()
        preis_raw = order_data.get("gesamt_ekp_brutto")
        preis = f"{float(preis_raw):.2f} €" if preis_raw is not None else "–"
        datum_raw = order_data.get("kaufdatum")
        datum = datum_raw.strftime("%d.%m.%Y") if hasattr(datum_raw, "strftime") else str(datum_raw or "–")
        anzahl = int(order_data.get("anzahl_positionen") or 0)
        tracking = str(order_data.get("tracking_nummer_einkauf") or "").strip()

        lbl_shop = QLabel(shop)
        lbl_shop.setStyleSheet("color: #c0caf5; font-size: 15px; font-weight: bold; border: none;")

        bestellnr_text = f"#{order_id}  ·  {bestellnr}"
        lbl_nr = QLabel(bestellnr_text)
        lbl_nr.setStyleSheet("color: #7aa2f7; font-size: 12px; border: none;")

        meta_parts = []
        if lieferdienst:
            meta_parts.append(lieferdienst)
        if anzahl:
            meta_parts.append(f"{anzahl} Artikel")
        meta_parts.append(preis)
        lbl_meta = QLabel("  ·  ".join(meta_parts))
        lbl_meta.setStyleSheet("color: #a9b1d6; font-size: 12px; border: none;")

        detail_parts = [f"Bestellt: {datum}"]
        if tracking:
            detail_parts.append(f"Trk: {tracking}")
        lbl_detail = QLabel("  ·  ".join(detail_parts))
        lbl_detail.setStyleSheet("color: #565f89; font-size: 11px; border: none;")

        text_layout.addWidget(lbl_shop)
        text_layout.addWidget(lbl_nr)
        text_layout.addWidget(lbl_meta)
        text_layout.addWidget(lbl_detail)
        text_layout.addStretch()
        layout.addLayout(text_layout, stretch=1)

        # Bilder asynchron per Timer laden (kein Blockieren des UI-Aufbaus)
        self._order_visuals = order_visuals
        self._shop_name = shop
        QTimer.singleShot(0, self._load_visuals)

    def _load_visuals(self):
        logo_px, item_px, total_menge = _load_card_pixmaps(
            self._order_visuals, self.order_id, self._shop_name
        )
        # Fallback-Logo falls nichts gefunden
        if logo_px is None or logo_px.isNull():
            logo_px = create_placeholder_pixmap(self._shop_name[:2] or "?", 72, background="#f3f4f6", foreground="#4b5563")
        # Fallback-Thumbnail
        if item_px is None or item_px.isNull():
            item_px = create_placeholder_pixmap("…", 36, background="#2a2f45", foreground="#7aa2f7")

        composite = render_card_visual_pixmap(logo_px, item_px, total_menge, logo_size=72, item_size=36)
        self._lbl_visual.setPixmap(composite)

    # --- Selektion ---

    def set_selected(self, value: bool):
        self._selected = value
        self._radio.setChecked(value)
        self.setStyleSheet(self._STYLE_SELECTED if value else self._STYLE_NORMAL)

    def is_selected(self) -> bool:
        return self._selected

    # --- Mausereignisse ---

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.selected.emit(self.order_id)
        super().mousePressEvent(event)

    def mouseDoubleClickEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.selected.emit(self.order_id)
            self.activated.emit(self.order_id)
        super().mouseDoubleClickEvent(event)


# ---------------------------------------------------------------------------
# OrderCardListPage – Seite 1: Übersicht der offenen Sendungen
# ---------------------------------------------------------------------------

class OrderCardListPage(QWidget):
    """Scrollbare Liste von OrderCardWidgets. Emittiert open_order(order_id)."""

    open_order = pyqtSignal(int)

    def __init__(self, db: DatabaseManager, order_visuals: OrderVisualResolver, parent=None):
        super().__init__(parent)
        self._db = db
        self._order_visuals = order_visuals
        self._cards: list[OrderCardWidget] = []
        self._selected_order_id: int | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(20, 20, 20, 16)
        root.setSpacing(10)

        # Header
        header_row = QHBoxLayout()
        lbl_title = QLabel("Wareneingang — Eingehende Sendungen")
        lbl_title.setStyleSheet("font-size: 22px; font-weight: bold; color: #7aa2f7;")
        btn_refresh = QPushButton("↺  Aktualisieren")
        btn_refresh.setProperty("class", "retro-btn")
        btn_refresh.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_refresh.clicked.connect(self.reload)
        header_row.addWidget(lbl_title)
        header_row.addStretch()
        header_row.addWidget(btn_refresh)
        root.addLayout(header_row)

        # Scrollbereich
        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._scroll.setStyleSheet("QScrollArea { background: transparent; border: none; }")

        self._cards_container = QWidget()
        self._cards_container.setStyleSheet("background: transparent;")
        self._cards_layout = QVBoxLayout(self._cards_container)
        self._cards_layout.setContentsMargins(0, 0, 4, 0)
        self._cards_layout.setSpacing(8)
        self._cards_layout.addStretch()

        self._scroll.setWidget(self._cards_container)
        root.addWidget(self._scroll, stretch=1)

        # Leer-Hinweis
        self._lbl_empty = QLabel("Keine offenen Sendungen vorhanden.")
        self._lbl_empty.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._lbl_empty.setStyleSheet("color: #565f89; font-size: 14px;")
        self._lbl_empty.hide()
        root.addWidget(self._lbl_empty)

        # Footer
        footer = QHBoxLayout()
        footer.addStretch()
        self._btn_weiter = QPushButton("Weiter  →")
        self._btn_weiter.setEnabled(False)
        self._btn_weiter.setMinimumHeight(40)
        self._btn_weiter.setMinimumWidth(160)
        self._btn_weiter.setStyleSheet(
            "QPushButton { background-color: #1a1b26; color: #565f89; border: 2px solid #414868;"
            " border-radius: 6px; padding: 8px 24px; font-size: 14px; font-weight: bold; }"
            "QPushButton:enabled { background-color: #7aa2f7; color: #1a1b26; border: none; }"
            "QPushButton:enabled:hover { background-color: #a9c4fb; }"
        )
        self._btn_weiter.setCursor(Qt.CursorShape.PointingHandCursor)
        self._btn_weiter.clicked.connect(self._on_weiter)
        footer.addWidget(self._btn_weiter)
        root.addLayout(footer)

    # --- Laden ---

    def reload(self):
        """Karten neu laden (z.B. nach Abschluss oder manuell)."""
        # Alte Karten entfernen
        for card in self._cards:
            self._cards_layout.removeWidget(card)
            card.deleteLater()
        self._cards.clear()
        self._selected_order_id = None
        self._btn_weiter.setEnabled(False)

        orders = self._fetch_orders()
        if not orders:
            self._lbl_empty.show()
            return

        self._lbl_empty.hide()
        # Karten vor dem Stretch einfügen
        insert_pos = self._cards_layout.count() - 1  # vor dem abschließenden Stretch
        for row in orders:
            card = OrderCardWidget(row["id"], row, self._order_visuals, self)
            card.selected.connect(self._on_card_selected)
            card.activated.connect(self._on_card_activated)
            self._cards_layout.insertWidget(insert_pos, card)
            self._cards.append(card)
            insert_pos += 1

    def _fetch_orders(self) -> list[dict]:
        try:
            conn = self._db._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT
                    b.id, b.bestellnummer, b.shop_name, b.paketdienst,
                    b.kaufdatum, b.gesamt_ekp_brutto, b.tracking_nummer_einkauf,
                    COUNT(p.id) AS anzahl_positionen
                FROM einkauf_bestellungen b
                LEFT JOIN waren_positionen p ON p.einkauf_id = b.id
                WHERE b.sendungsstatus != %s
                GROUP BY b.id
                ORDER BY b.id DESC
                """,
                (shipment_db_value(ShipmentStatus.DELIVERED),),
            )
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            return rows
        except Exception as exc:
            log_exception(__name__, exc)
            QMessageBox.critical(self, "Fehler", f"Konnte Bestellungen nicht laden:\n{exc}")
            return []

    # --- Selektion ---

    def _on_card_selected(self, order_id: int):
        self._selected_order_id = order_id
        for card in self._cards:
            card.set_selected(card.order_id == order_id)
        self._btn_weiter.setEnabled(True)

    def _on_card_activated(self, order_id: int):
        self.open_order.emit(order_id)

    def _on_weiter(self):
        if self._selected_order_id is not None:
            self.open_order.emit(self._selected_order_id)

    def selected_order_id(self) -> int | None:
        return self._selected_order_id


# ---------------------------------------------------------------------------
# OrderDetailPage – Seite 2: Detailprüfung (Auge / Scanner)
# ---------------------------------------------------------------------------

class OrderDetailPage(QWidget):
    """Zeigt die Positionen einer gewählten Bestellung zur Prüfung."""

    back_requested = pyqtSignal()
    order_finished = pyqtSignal(int)  # emittiert order_id nach Abschluss

    def __init__(self, db: DatabaseManager, order_visuals: OrderVisualResolver, parent=None):
        super().__init__(parent)
        self._db = db
        self.current_order_id: int | None = None
        self.positionen: list[dict] = []

        root = QVBoxLayout(self)
        root.setContentsMargins(20, 20, 20, 20)
        root.setSpacing(12)

        # --- Header mit Zurück-Button ---
        header_row = QHBoxLayout()
        btn_back = QPushButton("Zurueck")
        btn_back.setIcon(QIcon())          # kein natives Back-Icon
        btn_back.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_back.clicked.connect(self.back_requested.emit)

        self._lbl_title = QLabel("Bestellung prüfen")
        self._lbl_title.setStyleSheet("font-size: 18px; font-weight: bold; color: #7aa2f7;")

        header_row.addWidget(btn_back)
        header_row.addSpacing(12)
        header_row.addWidget(self._lbl_title)
        header_row.addStretch()
        root.addLayout(header_row)

        # --- Visueller Bestellvorschau-Strip ---
        preview_frame = QFrame()
        preview_frame.setStyleSheet(
            "QFrame { background-color: #1a1b26; border: 1px solid #30374d; border-radius: 8px; }"
        )
        preview_layout = QHBoxLayout(preview_frame)
        preview_layout.setContentsMargins(10, 8, 10, 8)
        preview_layout.setSpacing(10)

        self._order_visual_preview = CompactOrderVisualWidget(order_visuals, self)
        self._order_visual_preview.hide()
        self._lbl_visual_info = QLabel("Noch keine Bestellung geladen.")
        self._lbl_visual_info.setStyleSheet("color: #a9b1d6; font-size: 13px;")
        self._lbl_visual_info.setWordWrap(True)

        preview_layout.addWidget(self._order_visual_preview, 0, Qt.AlignmentFlag.AlignVCenter)
        preview_layout.addWidget(self._lbl_visual_info, 1)
        root.addWidget(preview_frame)

        # --- Modus-Auswahl ---
        mode_frame = QFrame()
        mode_frame.setStyleSheet(
            "QFrame { background-color: #1a1b26; border: 1px solid #414868; border-radius: 6px; }"
        )
        mode_layout = QHBoxLayout(mode_frame)

        lbl_mode = QLabel("Prüf-Modus:")
        lbl_mode.setStyleSheet("color: #7aa2f7; font-weight: bold; font-size: 14px; border: none;")

        self.radio_auge = QRadioButton("Prüfung per Auge")
        self.radio_auge.setStyleSheet("color: #a9b1d6; font-size: 14px; border: none;")
        self.radio_auge.setChecked(True)

        self.radio_scanner = QRadioButton("Prüfung per Scanner")
        self.radio_scanner.setStyleSheet("color: #a9b1d6; font-size: 14px; border: none;")

        self._mode_group = QButtonGroup(self)
        self._mode_group.addButton(self.radio_auge)
        self._mode_group.addButton(self.radio_scanner)
        self._mode_group.buttonClicked.connect(self._on_mode_changed)

        mode_layout.addWidget(lbl_mode)
        mode_layout.addWidget(self.radio_auge)
        mode_layout.addWidget(self.radio_scanner)
        mode_layout.addStretch()
        root.addWidget(mode_frame)

        # --- Scanner-Eingabe ---
        self._scanner_container = QWidget()
        scanner_layout = QHBoxLayout(self._scanner_container)
        scanner_layout.setContentsMargins(0, 0, 0, 0)
        self._txt_scanner = QLineEdit()
        self._txt_scanner.setPlaceholderText("EAN oder Barcode hier scannen... (Auto-Enter)")
        self._txt_scanner.setStyleSheet(
            "QLineEdit { background-color: #24283b; color: #9ece6a; font-size: 18px;"
            " padding: 10px; border: 2px solid #7aa2f7; border-radius: 4px; font-weight: bold; }"
        )
        self._txt_scanner.returnPressed.connect(self._handle_scan)
        scanner_layout.addWidget(self._txt_scanner)
        self._scanner_container.hide()
        root.addWidget(self._scanner_container)

        # --- Positions-Tabelle ---
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(
            ["ID", "Produkt", "EAN", "Erwartet", "Geliefert", "Aktionen (Auge)"]
        )
        hdr = self.table.horizontalHeader()
        hdr.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        hdr.setStretchLastSection(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setStyleSheet(
            "QTableWidget { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868;"
            " border-radius: 4px; gridline-color: #414868; font-size: 13px; }"
            "QHeaderView::section { background-color: #1f2335; color: #7aa2f7; font-weight: bold;"
            " padding: 5px; border: 1px solid #414868; }"
        )
        root.addWidget(self.table)

        # --- Footer ---
        footer = QHBoxLayout()
        self._lbl_status = QLabel("")
        self._lbl_status.setStyleSheet("font-size: 14px; font-weight: bold;")

        self._btn_finish = QPushButton("✅ Wareneingang abschließen")
        self._btn_finish.setStyleSheet(
            "QPushButton { background-color: #1a1b26; color: #565f89; border: 2px solid #414868;"
            " border-radius: 6px; padding: 15px; font-size: 16px; font-weight: bold; }"
            "QPushButton:enabled { background-color: #9ece6a; color: #1a1b26; border: none; }"
            "QPushButton:enabled:hover { background-color: #b9f27c; }"
        )
        self._btn_finish.setCursor(Qt.CursorShape.PointingHandCursor)
        self._btn_finish.setEnabled(False)
        self._btn_finish.clicked.connect(self._finish_inbound)

        footer.addWidget(self._lbl_status)
        footer.addStretch()
        footer.addWidget(self._btn_finish)
        root.addLayout(footer)

        self._order_visuals = order_visuals

    # --- Bestellung laden ---

    def load_order(self, order_id: int):
        self.current_order_id = order_id
        self._lbl_status.setText("")
        try:
            self._db.ensure_order_positions_are_unitized(order_id)
            conn = self._db._get_connection()
            cursor = conn.cursor(dictionary=True)

            # Kopfdaten für Titel
            cursor.execute(
                "SELECT bestellnummer, shop_name FROM einkauf_bestellungen WHERE id = %s",
                (order_id,),
            )
            head = cursor.fetchone() or {}
            shop = str(head.get("shop_name") or "").strip() or "Unbekannter Shop"
            nr = str(head.get("bestellnummer") or "").strip()
            self._lbl_title.setText(f"{shop}  –  #{order_id}  {nr}")

            # Visual-Preview
            preview = self._order_visuals.build_order_preview(order_id, shop_name=shop)
            if isinstance(preview, dict) and preview:
                tooltip = self._order_visuals.build_tooltip(preview)
                self._order_visual_preview.show()
                self._order_visual_preview.set_visual_preview(preview, tooltip=tooltip)
                self._lbl_visual_info.setText(tooltip or f"{shop} · #{order_id}")
            else:
                self._order_visual_preview.hide()
                self._lbl_visual_info.setText(f"{shop} · #{order_id}")

            # Positionen
            cursor.execute(
                """
                SELECT id, produkt_name, varianten_info, ean, menge, menge_geliefert
                FROM waren_positionen
                WHERE einkauf_id = %s
                """,
                (order_id,),
            )
            self.positionen = cursor.fetchall()
            cursor.close()
            conn.close()

            self._render_table()
        except Exception as exc:
            log_exception(__name__, exc)
            QMessageBox.critical(self, "Fehler", f"Konnte Positionen nicht laden:\n{exc}")

    # --- Modus ---

    def _on_mode_changed(self):
        if self.radio_scanner.isChecked():
            self._scanner_container.show()
            self._txt_scanner.setFocus()
            self.table.hideColumn(5)
        else:
            self._scanner_container.hide()
            self.table.showColumn(5)

    # --- Tabelle ---

    def _render_table(self):
        self.table.setRowCount(len(self.positionen))
        for idx, pos in enumerate(self.positionen):
            self.table.setItem(idx, 0, QTableWidgetItem(str(pos["id"])))

            name = pos["produkt_name"]
            if pos["varianten_info"]:
                name += f" [{pos['varianten_info']}]"
            self.table.setItem(idx, 1, QTableWidgetItem(name))
            self.table.setItem(idx, 2, QTableWidgetItem(str(pos["ean"] or "")))

            menge_erwartet = pos["menge"]
            menge_geliefert = pos["menge_geliefert"]

            item_e = QTableWidgetItem(str(menge_erwartet))
            item_e.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.table.setItem(idx, 3, item_e)

            item_g = QTableWidgetItem(str(menge_geliefert))
            item_g.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            if menge_geliefert == menge_erwartet:
                item_g.setForeground(QColor("#9ece6a"))
                item_g.setFont(QFont("Arial", 10, QFont.Weight.Bold))
            elif menge_geliefert > 0:
                item_g.setForeground(QColor("#e0af68"))
            else:
                item_g.setForeground(QColor("#f7768e"))
            self.table.setItem(idx, 4, item_g)

            action_widget = QWidget()
            h = QHBoxLayout(action_widget)
            h.setContentsMargins(2, 2, 2, 2)

            btn_minus = QPushButton(" - ")
            btn_minus.setCursor(Qt.CursorShape.PointingHandCursor)
            btn_minus.setStyleSheet(
                "background-color: #f7768e; color: #1a1b26; font-weight: bold; border-radius: 3px;"
            )
            btn_minus.clicked.connect(lambda _checked, i=idx: self._change_amount(i, -1))

            btn_plus = QPushButton(" + ")
            btn_plus.setCursor(Qt.CursorShape.PointingHandCursor)
            btn_plus.setStyleSheet(
                "background-color: #7aa2f7; color: #1a1b26; font-weight: bold; border-radius: 3px;"
            )
            btn_plus.clicked.connect(lambda _checked, i=idx: self._change_amount(i, 1))

            btn_all = QPushButton(" ✓ Alles ")
            btn_all.setCursor(Qt.CursorShape.PointingHandCursor)
            btn_all.setStyleSheet(
                "background-color: #9ece6a; color: #1a1b26; font-weight: bold; border-radius: 3px;"
            )
            btn_all.clicked.connect(lambda _checked, i=idx: self._set_amount_to_max(i))

            h.addWidget(btn_minus)
            h.addWidget(btn_plus)
            h.addWidget(btn_all)
            self.table.setCellWidget(idx, 5, action_widget)

        self.table.resizeColumnsToContents()
        self._update_finish_button()

    def _change_amount(self, row_idx: int, delta: int):
        pos = self.positionen[row_idx]
        new_amount = max(0, min(pos["menge"], pos["menge_geliefert"] + delta))
        pos["menge_geliefert"] = new_amount
        self._save_position_state(pos)
        self._render_table()

    def _set_amount_to_max(self, row_idx: int):
        pos = self.positionen[row_idx]
        pos["menge_geliefert"] = pos["menge"]
        self._save_position_state(pos)
        self._render_table()

    def _save_position_state(self, pos: dict):
        try:
            conn = self._db._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE waren_positionen SET menge_geliefert = %s WHERE id = %s",
                (pos["menge_geliefert"], pos["id"]),
            )
            conn.commit()
            cursor.close()
            conn.close()
        except Exception:
            pass

    def _handle_scan(self):
        ean = self._txt_scanner.text().strip()
        self._txt_scanner.clear()
        if not ean or not self.current_order_id:
            return
        found_idx = -1
        for idx, pos in enumerate(self.positionen):
            db_ean = str(pos["ean"] or "").strip().lower()
            if ean.lower() == db_ean or ean in db_ean:
                if pos["menge_geliefert"] < pos["menge"]:
                    found_idx = idx
                    break
        if found_idx == -1:
            # --- Reverse-Lookup: EAN in lokaler DB nachschlagen ---
            lookup_result = self._parent_app_lookup_service().reverse_lookup_ean_to_name(ean)
            if lookup_result.found:
                name = lookup_result.data.get("produkt_name", ean)
                self._lbl_status.setText(
                    f"\u26a0 EAN nicht in dieser Bestellung. DB-Treffer: {name}"
                )
                self._lbl_status.setStyleSheet(
                    "color: #e0af68; font-size: 14px; font-weight: bold;"
                )
            else:
                self._lbl_status.setText("\u274c EAN nicht gefunden oder bereits vollzaehlig!")
                self._lbl_status.setStyleSheet(
                    "color: #f7768e; font-size: 14px; font-weight: bold;"
                )
        else:
            self._lbl_status.setText(f"\u2705 EAN erkannt! {self.positionen[found_idx]['produkt_name']}")
            self._lbl_status.setStyleSheet("color: #9ece6a; font-size: 14px; font-weight: bold;")
            self._change_amount(found_idx, 1)

    def _parent_app_lookup_service(self) -> LookupService:
        """Holt den LookupService vom Parent (WareneingangApp)."""
        parent = self.parent()
        while parent is not None:
            if isinstance(parent, WareneingangApp):
                return parent.lookup_service
            parent = parent.parent()
        # Fallback: eigene DB-Instanz
        return LookupService(self.db)

    def _update_finish_button(self):
        if not self.positionen:
            self._btn_finish.setEnabled(False)
            return
        all_done = all(p["menge_geliefert"] >= p["menge"] for p in self.positionen)
        self._btn_finish.setEnabled(all_done)

    def _finish_inbound(self):
        if not self.current_order_id:
            return
        try:
            conn = self._db._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE einkauf_bestellungen SET sendungsstatus = %s WHERE id = %s",
                (shipment_db_value(ShipmentStatus.DELIVERED), self.current_order_id),
            )
            cursor.execute(
                "UPDATE waren_positionen SET status = %s WHERE einkauf_id = %s",
                (InventoryStatus.IN_STOCK.value, self.current_order_id),
            )
            conn.commit()
            cursor.close()
            conn.close()

            finished_id = self.current_order_id
            QMessageBox.information(
                self, "Erfolg", "Das Paket wurde erfolgreich vereinnahmt und eingelagert!"
            )
            self.order_finished.emit(finished_id)
        except Exception as exc:
            log_exception(__name__, exc)
            QMessageBox.critical(self, "Fehler", f"Fehler beim Abschluss:\n{exc}")


# ---------------------------------------------------------------------------
# WareneingangApp – Hauptwidget mit QStackedWidget (Seite 1 ↔ Seite 2)
# ---------------------------------------------------------------------------

class WareneingangApp(QWidget):
    def __init__(self, settings_manager):
        super().__init__()
        self.settings = settings_manager
        self.db = DatabaseManager(self.settings)
        self.lookup_service = LookupService(self.db)
        self.order_visuals = OrderVisualResolver(self.settings)

        self._pending_visual_refresh = False
        self._visual_refresh_timer = QTimer(self)
        self._visual_refresh_timer.setSingleShot(True)
        self._visual_refresh_timer.timeout.connect(self._do_visual_refresh)
        OrderVisualState.bus().visualsInvalidated.connect(self._on_visuals_invalidated)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.stack = QStackedWidget()
        layout.addWidget(self.stack)

        # Seite 1
        self._list_page = OrderCardListPage(self.db, self.order_visuals, self)
        self._list_page.open_order.connect(self._open_order)
        self.stack.addWidget(self._list_page)

        # Seite 2
        self._detail_page = OrderDetailPage(self.db, self.order_visuals, self)
        self._detail_page.back_requested.connect(self._go_back)
        self._detail_page.order_finished.connect(self._on_order_finished)
        self.stack.addWidget(self._detail_page)

        self.stack.setCurrentIndex(0)
        self._list_page.reload()

    def _open_order(self, order_id: int):
        self._detail_page.load_order(order_id)
        self.stack.setCurrentIndex(1)

    def _go_back(self):
        self.stack.setCurrentIndex(0)

    def _on_order_finished(self, _order_id: int):
        self.stack.setCurrentIndex(0)
        self._list_page.reload()

    def _on_visuals_invalidated(self, payload):
        self._pending_visual_refresh = True
        if self.isVisible():
            logging.debug("Wareneingang-Visual-Refresh geplant: %s", (payload or {}).get("reason", ""))
            self._visual_refresh_timer.start(120)

    def _do_visual_refresh(self):
        self._pending_visual_refresh = False
        if self.stack.currentIndex() == 0:
            self._list_page.reload()

    def showEvent(self, event):
        super().showEvent(event)
        if self._pending_visual_refresh:
            self._visual_refresh_timer.start(0)
