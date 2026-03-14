"""
modul_wareneingang.py
Ein UI-Modul zur ÃœberprÃ¼fung von Inbound-Paketen.
Es bietet zwei Modi: "PrÃ¼fung per Auge" (Klick) und "PrÃ¼fung per Scanner" (EAN).
"""

import logging

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, 
    QComboBox, QTableWidget, QTableWidgetItem, QHeaderView, 
    QLineEdit, QMessageBox, QFrame, QRadioButton, QButtonGroup,
    QAbstractItemView
)
from PyQt6.QtCore import Qt, QSize, QTimer
from PyQt6.QtGui import QColor, QFont

from module.database_manager import DatabaseManager
from module.status_model import InventoryStatus, ShipmentStatus, shipment_db_value

from module.crash_logger import log_exception
from module.order_visual_ui import CompactOrderVisualWidget, OrderVisualResolver
from module.order_visual_state import OrderVisualState
class WareneingangApp(QWidget):
    def __init__(self, settings_manager):
        super().__init__()
        self.settings = settings_manager
        self.db = DatabaseManager(self.settings)
        self.order_visuals = OrderVisualResolver(self.settings)
        self._pending_visual_refresh = False
        self._visual_refresh_timer = QTimer(self)
        self._visual_refresh_timer.setSingleShot(True)
        self._visual_refresh_timer.timeout.connect(self._reload_order_visuals)
        OrderVisualState.bus().visualsInvalidated.connect(self._on_visuals_invalidated)
        
        # Lokaler Status fÃ¼r die aktuell geladene Bestellung
        self.current_order_id = None
        self.positionen = [] # Liste aus dicts: id, produkt_name, ean, menge, menge_geliefert
        
        self._build_ui()
        self._load_pending_orders()

    def _build_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)
        
        # --- TOP HEADER ---
        lbl_title = QLabel("Wareneingang / Inbound")
        lbl_title.setStyleSheet("font-size: 24px; font-weight: bold; color: #7aa2f7;")
        main_layout.addWidget(lbl_title)
        
        # --- AUSWAHL BESTELLUNG ---
        order_layout = QHBoxLayout()
        lbl_order = QLabel("Offene Bestellung:")
        lbl_order.setStyleSheet("color: #a9b1d6; font-size: 14px;")
        
        self.combo_orders = QComboBox()
        self.combo_orders.setStyleSheet("""
            QComboBox { background-color: #24283b; color: #a9b1d6; padding: 8px; border: 1px solid #414868; border-radius: 4px; font-size: 14px; }
            QComboBox::drop-down { border: none; }
        """)
        self.combo_orders.setIconSize(QSize(86, 48))
        self.combo_orders.view().setIconSize(QSize(86, 48))
        self.combo_orders.setMinimumHeight(56)
        self.combo_orders.currentIndexChanged.connect(self._order_selected)
        
        btn_refresh = QPushButton("ðŸ”„ Aktualisieren")
        btn_refresh.setProperty("class", "retro-btn")
        btn_refresh.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_refresh.clicked.connect(self._load_pending_orders)
        
        order_layout.addWidget(lbl_order)
        order_layout.addWidget(self.combo_orders, stretch=1)
        order_layout.addWidget(btn_refresh)
        main_layout.addLayout(order_layout)

        preview_frame = QFrame()
        preview_frame.setStyleSheet("QFrame { background-color: #1a1b26; border: 1px solid #30374d; border-radius: 8px; }")
        preview_layout = QHBoxLayout(preview_frame)
        preview_layout.setContentsMargins(10, 8, 10, 8)
        preview_layout.setSpacing(10)

        self.order_visual_preview = CompactOrderVisualWidget(self.order_visuals, self)
        self.order_visual_preview.hide()
        self.lbl_order_visual_info = QLabel("Noch keine Bestellung ausgewaehlt.")
        self.lbl_order_visual_info.setStyleSheet("color: #a9b1d6; font-size: 13px;")
        self.lbl_order_visual_info.setWordWrap(True)

        preview_layout.addWidget(self.order_visual_preview, 0, Qt.AlignmentFlag.AlignVCenter)
        preview_layout.addWidget(self.lbl_order_visual_info, 1)
        main_layout.addWidget(preview_frame)
        
        # --- MODUS AUSWAHL ---
        mode_frame = QFrame()
        mode_frame.setStyleSheet("QFrame { background-color: #1a1b26; border: 1px solid #414868; border-radius: 6px; }")
        mode_layout = QHBoxLayout(mode_frame)
        
        lbl_mode = QLabel("PrÃ¼f-Modus:")
        lbl_mode.setStyleSheet("color: #7aa2f7; font-weight: bold; font-size: 14px; border: none;")
        
        self.radio_auge = QRadioButton("ðŸ‘€ PrÃ¼fung per Auge")
        self.radio_auge.setStyleSheet("color: #a9b1d6; font-size: 14px; border: none;")
        self.radio_auge.setChecked(True)
        
        self.radio_scanner = QRadioButton("ðŸ“Ÿ PrÃ¼fung per Scanner")
        self.radio_scanner.setStyleSheet("color: #a9b1d6; font-size: 14px; border: none;")
        
        self.mode_group = QButtonGroup(self)
        self.mode_group.addButton(self.radio_auge)
        self.mode_group.addButton(self.radio_scanner)
        self.mode_group.buttonClicked.connect(self._on_mode_changed)
        
        mode_layout.addWidget(lbl_mode)
        mode_layout.addWidget(self.radio_auge)
        mode_layout.addWidget(self.radio_scanner)
        mode_layout.addStretch()
        
        main_layout.addWidget(mode_frame)
        
        # --- SCANNER BEREICH (anfangs versteckt) ---
        self.scanner_container = QWidget()
        scanner_layout = QHBoxLayout(self.scanner_container)
        scanner_layout.setContentsMargins(0, 0, 0, 0)
        
        self.txt_scanner = QLineEdit()
        self.txt_scanner.setPlaceholderText("EAN oder Barcode hier scannen... (Auto-Enter)")
        self.txt_scanner.setStyleSheet("""
            QLineEdit { background-color: #24283b; color: #9ece6a; font-size: 18px; padding: 10px; border: 2px solid #7aa2f7; border-radius: 4px; font-weight: bold; }
        """)
        self.txt_scanner.returnPressed.connect(self._handle_scan)
        
        scanner_layout.addWidget(self.txt_scanner)
        self.scanner_container.hide() # Nur im Scanner Modus zeigen
        main_layout.addWidget(self.scanner_container)
        
        # --- TABELLE FÃœR POSITIONEN ---
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["ID", "Produkt", "EAN", "Erwartet", "Geliefert", "Aktionen (Auge)"])
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setStyleSheet("""
            QTableWidget { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868; border-radius: 4px; gridline-color: #414868; font-size: 13px; }
            QHeaderView::section { background-color: #1f2335; color: #7aa2f7; font-weight: bold; padding: 5px; border: 1px solid #414868; }
        """)
        main_layout.addWidget(self.table)
        
        # --- ABSCHLUSS BEREICH ---
        bottom_layout = QHBoxLayout()
        
        self.lbl_status = QLabel("")
        self.lbl_status.setStyleSheet("font-size: 14px; font-weight: bold;")
        
        self.btn_finish = QPushButton("âœ… Wareneingang abschlieÃŸen")
        self.btn_finish.setStyleSheet("""
            QPushButton { background-color: #1a1b26; color: #565f89; border: 2px solid #414868; border-radius: 6px; padding: 15px; font-size: 16px; font-weight: bold; }
            QPushButton:enabled { background-color: #9ece6a; color: #1a1b26; border: none; }
            QPushButton:enabled:hover { background-color: #b9f27c; }
        """)
        self.btn_finish.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_finish.setEnabled(False) # Erst aktiv, wenn alles grÃ¼n
        self.btn_finish.clicked.connect(self._finish_inbound)
        
        bottom_layout.addWidget(self.lbl_status)
        bottom_layout.addStretch()
        bottom_layout.addWidget(self.btn_finish)
        
        main_layout.addLayout(bottom_layout)

    def _on_mode_changed(self):
        """Wechselt zwischen Auge und Scanner"""
        if self.radio_scanner.isChecked():
            self.scanner_container.show()
            self.txt_scanner.setFocus()
            self.table.hideColumn(5) # "Aktionen" Spalte verstecken
        else:
            self.scanner_container.hide()
            self.table.showColumn(5)
            
    def _load_pending_orders(self):
        """LÃ¤dt alle EinkÃ¤ufe, die noch nicht geliefert sind und schon in Zustellung waren."""
        self.combo_orders.blockSignals(True)
        self.combo_orders.clear()
        self.combo_orders.addItem("Bitte wÃ¤hlen...", None)
        self._update_order_visual_panel()
        
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor(dictionary=True)
            # Hole EinkÃ¤ufe (sendungsstatus != geliefert)
            query = """
                SELECT id, bestellnummer, shop_name, tracking_nummer_einkauf
                FROM einkauf_bestellungen
                WHERE sendungsstatus != %s
                ORDER BY id DESC
            """
            cursor.execute(query, (shipment_db_value(ShipmentStatus.DELIVERED),))
            for row in cursor.fetchall():
                trk = row['tracking_nummer_einkauf']
                trk_str = f" | Trk: {trk}" if trk else ""
                label = f"#{row['id']} - {row['shop_name']} ({row['bestellnummer']}){trk_str}"
                preview = self.order_visuals.build_order_preview(row['id'], shop_name=row.get('shop_name', ''))
                self.combo_orders.addItem(self.order_visuals.render_visual_icon(preview), label, row['id'])
                item_index = self.combo_orders.count() - 1
                self.combo_orders.setItemData(item_index, preview, Qt.ItemDataRole.UserRole + 1)
                self.combo_orders.setItemData(item_index, self.order_visuals.build_tooltip(preview), Qt.ItemDataRole.ToolTipRole)
                
            cursor.close()
            conn.close()
        except Exception as e:
            log_exception(__name__, e)
            QMessageBox.critical(self, "Fehler", f"Konnte Bestellungen nicht laden: {e}")
            
        self.combo_orders.blockSignals(False)

    def _update_order_visual_panel(self, preview=None, title_text="Noch keine Bestellung ausgewaehlt."):
        preview = preview if isinstance(preview, dict) else {}
        if preview:
            tooltip = self.order_visuals.build_tooltip(preview)
            self.order_visual_preview.show()
            self.order_visual_preview.set_visual_preview(preview, tooltip=tooltip)
            self.lbl_order_visual_info.setText(str(title_text or tooltip or "Bestellung ausgewaehlt").strip())
            self.lbl_order_visual_info.setToolTip(tooltip)
        else:
            self.order_visual_preview.clear()
            self.order_visual_preview.hide()
            self.lbl_order_visual_info.setText(str(title_text or "Noch keine Bestellung ausgewaehlt.").strip())
            self.lbl_order_visual_info.setToolTip("")

    def _order_selected(self):
        """Eine Bestellung wurde aus dem Dropdown gewÃ¤hlt -> Positionen laden"""
        order_id = self.combo_orders.currentData()
        self.current_order_id = order_id
        preview = self.combo_orders.currentData(Qt.ItemDataRole.UserRole + 1)
        current_text = self.combo_orders.currentText().strip()
        
        if not order_id:
            self.positionen = []
            self.table.setRowCount(0)
            self._update_finish_button()
            self._update_order_visual_panel()
            return
            
        try:
            self.db.ensure_order_positions_are_unitized(order_id)
            conn = self.db._get_connection()
            cursor = conn.cursor(dictionary=True)
            query = """
                SELECT id, produkt_name, varianten_info, ean, menge, menge_geliefert
                FROM waren_positionen
                WHERE einkauf_id = %s
            """
            cursor.execute(query, (order_id,))
            self.positionen = cursor.fetchall()
            cursor.close()
            conn.close()
            
            self._render_table()
            self._update_order_visual_panel(preview=preview, title_text=current_text)
        except Exception as e:
            log_exception(__name__, e)
            QMessageBox.critical(self, "Fehler", f"Konnte Positionen nicht laden: {e}")

    def _on_visuals_invalidated(self, payload):
        self._pending_visual_refresh = True
        if not self.isVisible():
            return
        logging.debug("Wareneingang-Visual-Refresh eingeplant: %s", (payload or {}).get("reason", "unknown"))
        self._visual_refresh_timer.start(120)

    def _reload_order_visuals(self):
        selected_order_id = self.current_order_id
        self._pending_visual_refresh = False
        self._load_pending_orders()
        if not selected_order_id:
            return
        for index in range(self.combo_orders.count()):
            if self.combo_orders.itemData(index) == selected_order_id:
                self.combo_orders.setCurrentIndex(index)
                return
        self._update_order_visual_panel(title_text="Bestellung nicht mehr in der offenen Liste.")

    def showEvent(self, event):
        super().showEvent(event)
        if self._pending_visual_refresh:
            self._visual_refresh_timer.start(0)
    def _render_table(self):
        self.table.setRowCount(len(self.positionen))
        
        for idx, pos in enumerate(self.positionen):
            self.table.setItem(idx, 0, QTableWidgetItem(str(pos['id'])))
            
            name = pos['produkt_name']
            if pos['varianten_info']: name += f" [{pos['varianten_info']}]"
            self.table.setItem(idx, 1, QTableWidgetItem(name))
            
            self.table.setItem(idx, 2, QTableWidgetItem(str(pos['ean'] if pos['ean'] else "")))
            
            menge_erwartet = pos['menge']
            menge_geliefert = pos['menge_geliefert']
            
            item_erwartet = QTableWidgetItem(str(menge_erwartet))
            item_erwartet.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self.table.setItem(idx, 3, item_erwartet)
            
            item_geliefert = QTableWidgetItem(str(menge_geliefert))
            item_geliefert.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            
            # Farbe je nach Fortschritt
            if menge_geliefert == menge_erwartet:
                item_geliefert.setForeground(QColor("#9ece6a")) # GrÃ¼n
                item_geliefert.setFont(QFont("Arial", 10, QFont.Weight.Bold))
            elif menge_geliefert > 0:
                item_geliefert.setForeground(QColor("#e0af68")) # Orange (Teil)
            else:
                item_geliefert.setForeground(QColor("#f7768e")) # Rot
            self.table.setItem(idx, 4, item_geliefert)
            
            # Auge Aktionen
            action_widget = QWidget()
            h_layout = QHBoxLayout(action_widget)
            h_layout.setContentsMargins(2, 2, 2, 2)
            
            btn_minus = QPushButton(" - ")
            btn_minus.setCursor(Qt.CursorShape.PointingHandCursor)
            btn_minus.setStyleSheet("background-color: #f7768e; color: #1a1b26; font-weight: bold; border-radius: 3px;")
            btn_minus.clicked.connect(lambda checked, idx=idx: self._change_amount(idx, -1))
            
            btn_plus = QPushButton(" + ")
            btn_plus.setCursor(Qt.CursorShape.PointingHandCursor)
            btn_plus.setStyleSheet("background-color: #7aa2f7; color: #1a1b26; font-weight: bold; border-radius: 3px;")
            btn_plus.clicked.connect(lambda checked, idx=idx: self._change_amount(idx, 1))
            
            btn_all = QPushButton(" âœ” Alles ")
            btn_all.setCursor(Qt.CursorShape.PointingHandCursor)
            btn_all.setStyleSheet("background-color: #9ece6a; color: #1a1b26; font-weight: bold; border-radius: 3px;")
            btn_all.clicked.connect(lambda checked, idx=idx: self._set_amount_to_max(idx))
            
            h_layout.addWidget(btn_minus)
            h_layout.addWidget(btn_plus)
            h_layout.addWidget(btn_all)
            
            self.table.setCellWidget(idx, 5, action_widget)
            
        self.table.resizeColumnsToContents()
        self._update_finish_button()

    def _change_amount(self, row_idx, delta):
        pos = self.positionen[row_idx]
        new_amount = pos['menge_geliefert'] + delta
        if new_amount < 0: new_amount = 0
        if new_amount > pos['menge']: new_amount = pos['menge']
        
        pos['menge_geliefert'] = new_amount
        self._save_position_state(pos)
        self._render_table()

    def _set_amount_to_max(self, row_idx):
        pos = self.positionen[row_idx]
        pos['menge_geliefert'] = pos['menge']
        self._save_position_state(pos)
        self._render_table()

    def _save_position_state(self, pos):
        """Speichert die zwischenzeitliche Menge sofort in die DB, falls das Fenster geschlossen wird"""
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE waren_positionen SET menge_geliefert = %s WHERE id = %s", (pos['menge_geliefert'], pos['id']))
            conn.commit()
            cursor.close()
            conn.close()
        except: pass

    def _handle_scan(self):
        """Wenn eine EAN gescannt wurde"""
        ean = self.txt_scanner.text().strip()
        self.txt_scanner.clear()
        
        if not ean or not self.current_order_id: return
        
        # Suche nach der EAN in den aktuellen Positionen
        found_idx = -1
        for idx, pos in enumerate(self.positionen):
            # Grober Match (falls EAN als Integer in DB vs String im Scanner)
            db_ean = str(pos['ean']).strip().lower() if pos['ean'] else ""
            if ean.lower() == db_ean or ean in db_ean:
                # Bevorzuge Positionen, die noch nicht voll sind
                if pos['menge_geliefert'] < pos['menge']:
                    found_idx = idx
                    break
        
        if found_idx == -1:
            self.lbl_status.setText("âŒ EAN nicht gefunden oder bereits vollzÃ¤hlig!")
            self.lbl_status.setStyleSheet("color: #f7768e; font-size: 14px; font-weight: bold;")
        else:
            self.lbl_status.setText(f"âœ… EAN erkannt! {self.positionen[found_idx]['produkt_name']}")
            self.lbl_status.setStyleSheet("color: #9ece6a; font-size: 14px; font-weight: bold;")
            self._change_amount(found_idx, 1)

    def _update_finish_button(self):
        if not self.positionen:
            self.btn_finish.setEnabled(False)
            return
            
        all_done = True
        for pos in self.positionen:
            if pos['menge_geliefert'] < pos['menge']:
                all_done = False
                break
                
        self.btn_finish.setEnabled(all_done)

    def _finish_inbound(self):
        """Alle Haken sitzen -> Bestellung als Geliefert markieren und Positionen auf IN_STOCK"""
        if not self.current_order_id: return
        
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor()
            
            # 1. Bestellung abhaken
            cursor.execute("UPDATE einkauf_bestellungen SET sendungsstatus = %s WHERE id = %s", (shipment_db_value(ShipmentStatus.DELIVERED), self.current_order_id))
            
            # 2. Positionen als auf Lager markieren
            cursor.execute("UPDATE waren_positionen SET status = %s WHERE einkauf_id = %s", (InventoryStatus.IN_STOCK.value, self.current_order_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            QMessageBox.information(self, "Erfolg", "Das Paket wurde erfolgreich vereinnahmt und eingelagert!")
            
            # Reset UI
            self.lbl_status.setText("")
            self._load_pending_orders()
            
        except Exception as e:
            log_exception(__name__, e)
            QMessageBox.critical(self, "Fehler", f"Fehler beim Abschluss:\n{e}")







