"""
modul_tracker.py
Zentrales Tracking-Modul fuer Inbound (Einkauf) und Outbound (Ausgangs-Pakete).
Zeigt eine tabellarische Uebersicht, generiert Deep-Links zu Tracking-Providern
und erlaubt schnelle Status-Aenderungen.
"""

import logging

from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QTabWidget,
    QMenu,
    QMessageBox,
    QInputDialog,
    QAbstractItemView,
    QComboBox,
    QFrame,
)
from PyQt6.QtCore import Qt, QUrl, QTimer
from PyQt6.QtGui import QDesktopServices, QIcon, QAction
from module.database_manager import DatabaseManager
from module.status_model import ShipmentStatus, normalize_shipment_status, shipment_db_value
from module.order_visual_ui import CompactOrderVisualWidget, OrderVisualResolver
from module.order_visual_state import OrderVisualState

from module.crash_logger import log_exception


class TrackerApp(QWidget):
    def __init__(self, settings_manager):
        super().__init__()
        self.settings = settings_manager
        self.db = DatabaseManager(self.settings)
        self.order_visuals = OrderVisualResolver(self.settings)
        self._pending_visual_refresh = False
        self._visual_refresh_timer = QTimer(self)
        self._visual_refresh_timer.setSingleShot(True)
        self._visual_refresh_timer.timeout.connect(self._reload_visual_tables)
        OrderVisualState.bus().visualsInvalidated.connect(self._on_visuals_invalidated)

        self._build_ui()

        QTimer.singleShot(100, self.refresh_data)

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.tabs = QTabWidget()
        self.tabs.setStyleSheet(
            """
            QTabWidget::pane { border: 1px solid #414868; background-color: #1a1b26; border-radius: 4px; }
            QTabBar::tab { background: #24283b; color: #a9b1d6; padding: 10px 20px; margin-right: 2px; border-top-left-radius: 4px; border-top-right-radius: 4px; }
            QTabBar::tab:selected { background: #3b4261; color: #7aa2f7; font-weight: bold; }
        """
        )

        self.tab_inbound = QWidget()
        self.tab_outbound = QWidget()

        self._build_inbound_tab()
        self._build_outbound_tab()

        self.tabs.addTab(self.tab_inbound, "Inbound Pakete")
        self.tabs.addTab(self.tab_outbound, "Outbound Pakete")

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
        layout.addWidget(self.tabs)

    def _build_inbound_tab(self):
        layout = QVBoxLayout(self.tab_inbound)

        self.table_inbound = QTableWidget()
        self.table_inbound.setColumnCount(9)
        self.table_inbound.setHorizontalHeaderLabels(
            ["Visual", "ID", "Bestell-Nr", "Kaufdatum", "Shop", "Logistik", "Tracking-Nr", "Status", "Aktion"]
        )

        header = self.table_inbound.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)

        self.table_inbound.verticalHeader().setDefaultSectionSize(58)
        self.table_inbound.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_inbound.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table_inbound.setStyleSheet(
            """
            QTableWidget { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868; border-radius: 4px; gridline-color: #414868; }
            QHeaderView::section { background-color: #1f2335; color: #7aa2f7; font-weight: bold; padding: 5px; border: 1px solid #414868; }
            QTableWidget::item:selected { background-color: #3d59a1; }
        """
        )
        self.table_inbound.setColumnWidth(0, 92)
        self.table_inbound.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_inbound.customContextMenuRequested.connect(self._show_inbound_context_menu)

        layout.addWidget(self.table_inbound)

    def _build_outbound_tab(self):
        layout = QVBoxLayout(self.tab_outbound)

        self.table_outbound = QTableWidget()
        self.table_outbound.setColumnCount(7)
        self.table_outbound.setHorizontalHeaderLabels(
            ["Visual", "ID", "Versanddatum", "Logistik", "Tracking-Nr", "Status", "Aktion"]
        )

        header = self.table_outbound.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)

        self.table_outbound.verticalHeader().setDefaultSectionSize(58)
        self.table_outbound.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_outbound.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table_outbound.setStyleSheet(
            """
            QTableWidget { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868; border-radius: 4px; gridline-color: #414868; }
            QHeaderView::section { background-color: #1f2335; color: #7aa2f7; font-weight: bold; padding: 5px; border: 1px solid #414868; }
            QTableWidget::item:selected { background-color: #3d59a1; }
        """
        )
        self.table_outbound.setColumnWidth(0, 92)
        self.table_outbound.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_outbound.customContextMenuRequested.connect(self._show_outbound_context_menu)

        layout.addWidget(self.table_outbound)

    def _build_visual_cell(self, preview):
        widget = CompactOrderVisualWidget(self.order_visuals, self)
        widget.set_visual_preview(preview)
        return widget

    def _on_visuals_invalidated(self, payload):
        self._pending_visual_refresh = True
        if not self.isVisible():
            return
        logging.debug("Tracker-Visual-Refresh eingeplant: %s", (payload or {}).get("reason", "unknown"))
        self._visual_refresh_timer.start(150)

    def _reload_visual_tables(self):
        self._pending_visual_refresh = False
        self.refresh_data()

    def showEvent(self, event):
        super().showEvent(event)
        if self._pending_visual_refresh:
            self._visual_refresh_timer.start(0)

    def refresh_data(self):
        self._load_inbound()
        self._load_outbound()

    def _load_inbound(self):
        self.table_inbound.setRowCount(0)
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor(dictionary=True)
            query = """
                SELECT id, bestellnummer, kaufdatum, shop_name, paketdienst, tracking_nummer_einkauf, sendungsstatus
                FROM einkauf_bestellungen
                WHERE tracking_nummer_einkauf IS NOT NULL
                  AND tracking_nummer_einkauf != ''
                  AND sendungsstatus != %s
                ORDER BY kaufdatum DESC
            """
            cursor.execute(query, (shipment_db_value(ShipmentStatus.DELIVERED),))
            rows = cursor.fetchall()

            self.table_inbound.setRowCount(len(rows))
            for row_idx, row in enumerate(rows):
                preview = self.order_visuals.build_order_preview(row["id"], shop_name=row.get("shop_name", ""))
                tooltip = self.order_visuals.build_tooltip(preview)
                self.table_inbound.setCellWidget(row_idx, 0, self._build_visual_cell(preview))
                self.table_inbound.setRowHeight(row_idx, 58)
                self.table_inbound.setItem(row_idx, 1, QTableWidgetItem(str(row["id"])))
                bestell_item = QTableWidgetItem(str(row["bestellnummer"]))
                bestell_item.setToolTip(tooltip)
                self.table_inbound.setItem(row_idx, 2, bestell_item)

                date_str = str(row["kaufdatum"])[:10] if row["kaufdatum"] else ""
                self.table_inbound.setItem(row_idx, 3, QTableWidgetItem(date_str))

                shop_item = QTableWidgetItem(str(row["shop_name"]))
                shop_item.setToolTip(tooltip)
                self.table_inbound.setItem(row_idx, 4, shop_item)
                pd = str(row["paketdienst"]) if row["paketdienst"] else "Unbekannt"
                self.table_inbound.setItem(row_idx, 5, QTableWidgetItem(pd))

                trk = str(row["tracking_nummer_einkauf"])
                self.table_inbound.setItem(row_idx, 6, QTableWidgetItem(trk))

                raw_status = row["sendungsstatus"] if row["sendungsstatus"] else shipment_db_value(ShipmentStatus.NOT_DISPATCHED)
                status = shipment_db_value(normalize_shipment_status(raw_status))
                status_item = QTableWidgetItem(status)
                norm_status = normalize_shipment_status(status)
                if norm_status == ShipmentStatus.ISSUE_DELAYED:
                    status_item.setForeground(Qt.GlobalColor.red)
                elif norm_status in (ShipmentStatus.IN_TRANSIT, ShipmentStatus.OUT_FOR_DELIVERY):
                    status_item.setForeground(Qt.GlobalColor.yellow)

                self.table_inbound.setItem(row_idx, 7, status_item)

                btn_widget = QWidget()
                btn_layout = QHBoxLayout(btn_widget)
                btn_layout.setContentsMargins(0, 0, 0, 0)
                btn_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

                btn_track = QPushButton("Tracken")
                btn_track.setCursor(Qt.CursorShape.PointingHandCursor)
                btn_track.setStyleSheet(
                    "background-color: #3b4261; color: white; border: none; border-radius: 3px; padding: 4px 8px;"
                )
                btn_track.clicked.connect(lambda checked, p=pd, t=trk: self._open_tracking_url(p, t))

                btn_layout.addWidget(btn_track)
                self.table_inbound.setCellWidget(row_idx, 8, btn_widget)

            self.table_inbound.resizeColumnsToContents()
            self.table_inbound.setColumnWidth(0, 92)
            cursor.close()
            conn.close()
        except Exception as exc:
            log_exception(__name__, exc)
            QMessageBox.critical(self, "Fehler", f"Fehler beim Laden der Inbound-Daten:\n{exc}")

    def _load_outbound(self):
        self.table_outbound.setRowCount(0)
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor(dictionary=True)
            query = """
                SELECT id, versanddatum, tracking_nummer, paketdienst, sendungsstatus
                FROM ausgangs_pakete
                WHERE sendungsstatus != %s
                ORDER BY versanddatum DESC
            """
            cursor.execute(query, (shipment_db_value(ShipmentStatus.DELIVERED),))
            rows = cursor.fetchall()

            self.table_outbound.setRowCount(len(rows))
            for row_idx, row in enumerate(rows):
                preview = self.order_visuals.build_package_preview(row["id"])
                tooltip = self.order_visuals.build_tooltip(preview)
                self.table_outbound.setCellWidget(row_idx, 0, self._build_visual_cell(preview))
                self.table_outbound.setRowHeight(row_idx, 58)
                self.table_outbound.setItem(row_idx, 1, QTableWidgetItem(str(row["id"])))

                date_str = str(row["versanddatum"])[:10] if row["versanddatum"] else ""
                self.table_outbound.setItem(row_idx, 2, QTableWidgetItem(date_str))

                pd = str(row["paketdienst"]) if row["paketdienst"] else "Unbekannt"
                pd_item = QTableWidgetItem(pd)
                pd_item.setToolTip(tooltip)
                self.table_outbound.setItem(row_idx, 3, pd_item)

                trk = str(row["tracking_nummer"])
                trk_item = QTableWidgetItem(trk)
                trk_item.setToolTip(tooltip)
                self.table_outbound.setItem(row_idx, 4, trk_item)

                if row["sendungsstatus"]:
                    status = shipment_db_value(normalize_shipment_status(row["sendungsstatus"]))
                else:
                    status = shipment_db_value(ShipmentStatus.NOT_DISPATCHED)
                status_item = QTableWidgetItem(status)
                self.table_outbound.setItem(row_idx, 5, status_item)

                btn_widget = QWidget()
                btn_layout = QHBoxLayout(btn_widget)
                btn_layout.setContentsMargins(0, 0, 0, 0)
                btn_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

                btn_track = QPushButton("Tracken")
                btn_track.setCursor(Qt.CursorShape.PointingHandCursor)
                btn_track.setStyleSheet(
                    "background-color: #3b4261; color: white; border: none; border-radius: 3px; padding: 4px 8px;"
                )
                btn_track.clicked.connect(lambda checked, p=pd, t=trk: self._open_tracking_url(p, t))

                btn_layout.addWidget(btn_track)
                self.table_outbound.setCellWidget(row_idx, 6, btn_widget)

            self.table_outbound.resizeColumnsToContents()
            self.table_outbound.setColumnWidth(0, 92)
            cursor.close()
            conn.close()
        except Exception as exc:
            log_exception(__name__, exc)
            if "Unknown column" not in str(exc):
                print(f"Fehler Tracker Outbound: {exc}")

    def _open_tracking_url(self, paketdienst, tracking_nummer):
        url = ""
        pd = str(paketdienst).lower().strip()

        if "dhl" in pd or "deutsche post" in pd:
            url = f"https://www.dhl.de/de/privatkunden/pakete-empfangen/verfolgen.html?piececode={tracking_nummer}"
        elif "dpd" in pd:
            url = f"https://tracking.dpd.de/status/de_DE/parcel/{tracking_nummer}"
        elif "gls" in pd:
            url = f"https://gls-group.eu/DE/de/paketverfolgung?match={tracking_nummer}"
        elif "hermes" in pd:
            url = f"https://www.myhermes.de/empfangen/sendungsverfolgung/sendungsinformation/{tracking_nummer}"
        elif "ups" in pd:
            url = f"https://www.ups.com/track?loc=de_DE&tracknum={tracking_nummer}"
        elif "amazon" in pd or "swiship" in pd:
            url = f"https://www.swiship.de/track-and-trace?t={tracking_nummer}"
        else:
            url = f"https://www.google.com/search?q={tracking_nummer}"

        print(f"Oeffne Browser mit: {url}")
        QDesktopServices.openUrl(QUrl(url))

    def _show_inbound_context_menu(self, position):
        row = self.table_inbound.itemAt(position).row() if self.table_inbound.itemAt(position) else -1
        if row < 0:
            return

        db_id = self.table_inbound.item(row, 1).text()

        menu = QMenu()
        menu.setStyleSheet(
            "QMenu { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868; } QMenu::item:selected { background-color: #3b4261; }"
        )

        action_geliefert = QAction("Status: Markieren als 'Geliefert'", self)
        action_geliefert.triggered.connect(
            lambda: self._update_status(
                "einkauf_bestellungen", db_id, shipment_db_value(ShipmentStatus.DELIVERED)
            )
        )
        menu.addAction(action_geliefert)

        action_unterwegs = QAction("Status: Markieren als 'Unterwegs'", self)
        action_unterwegs.triggered.connect(
            lambda: self._update_status(
                "einkauf_bestellungen", db_id, shipment_db_value(ShipmentStatus.IN_TRANSIT)
            )
        )
        menu.addAction(action_unterwegs)

        action_problem = QAction("Status: Markieren als 'Problem/Verzoegert'", self)
        action_problem.triggered.connect(
            lambda: self._update_status(
                "einkauf_bestellungen", db_id, shipment_db_value(ShipmentStatus.ISSUE_DELAYED)
            )
        )
        menu.addAction(action_problem)

        menu.exec(self.table_inbound.viewport().mapToGlobal(position))

    def _show_outbound_context_menu(self, position):
        row = self.table_outbound.itemAt(position).row() if self.table_outbound.itemAt(position) else -1
        if row < 0:
            return

        db_id = self.table_outbound.item(row, 1).text()

        menu = QMenu()
        menu.setStyleSheet(
            "QMenu { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868; } QMenu::item:selected { background-color: #3b4261; }"
        )

        action_geliefert = QAction("Status: Markieren als 'Geliefert'", self)
        action_geliefert.triggered.connect(
            lambda: self._update_status(
                "ausgangs_pakete", db_id, shipment_db_value(ShipmentStatus.DELIVERED)
            )
        )
        menu.addAction(action_geliefert)

        menu.exec(self.table_outbound.viewport().mapToGlobal(position))

    def _update_status(self, table, db_id, new_status):
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"UPDATE {table} SET sendungsstatus = %s WHERE id = %s", (new_status, db_id))
            conn.commit()
            cursor.close()
            conn.close()
            self.refresh_data()
        except Exception as exc:
            log_exception(__name__, exc)
            QMessageBox.critical(self, "DB Fehler", f"Konnte Status nicht aktualisieren:\n{exc}")




