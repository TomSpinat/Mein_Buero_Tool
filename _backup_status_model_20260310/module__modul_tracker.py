"""
modul_tracker.py
Zentrales Tracking-Modul fÃ¼r Inbound (Einkauf) und Outbound (Ausgangs-Pakete).
Zeigt eine tabellarische Ãœbersicht, generiert Deep-Links zu Tracking-Providern
und erlaubt schnelle Status-Ã„nderungen.
"""

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
    QPushButton, QTableWidget, QTableWidgetItem, QHeaderView,
    QTabWidget, QMenu, QMessageBox, QInputDialog, QAbstractItemView,
    QComboBox, QFrame
)
from PyQt6.QtCore import Qt, QUrl, QTimer
from PyQt6.QtGui import QDesktopServices, QIcon, QAction
from module.database_manager import DatabaseManager

from module.crash_logger import log_exception
class TrackerApp(QWidget):
    def __init__(self, settings_manager):
        super().__init__()
        self.settings = settings_manager
        self.db = DatabaseManager(self.settings)
        
        self._build_ui()
        
        # Initialer Daten-Load
        QTimer.singleShot(100, self.refresh_data)

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Tabs
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabWidget::pane { border: 1px solid #414868; background-color: #1a1b26; border-radius: 4px; }
            QTabBar::tab { background: #24283b; color: #a9b1d6; padding: 10px 20px; margin-right: 2px; border-top-left-radius: 4px; border-top-right-radius: 4px; }
            QTabBar::tab:selected { background: #3b4261; color: #7aa2f7; font-weight: bold; }
        """)
        
        self.tab_inbound = QWidget()
        self.tab_outbound = QWidget()
        
        self._build_inbound_tab()
        self._build_outbound_tab()
        
        self.tabs.addTab(self.tab_inbound, "ðŸ“¦ Eingehende Pakete (Inbound)")
        self.tabs.addTab(self.tab_outbound, "ðŸš€ Ausgehende Pakete (Outbound)")
        
        # Top-Bar (Aktualisieren)
        top_bar = QHBoxLayout()
        lbl_title = QLabel("Tracking Radar")
        lbl_title.setStyleSheet("font-size: 20px; font-weight: bold; color: #7aa2f7;")
        
        btn_refresh = QPushButton(" ðŸ”„ Aktualisieren ")
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
        self.table_inbound.setColumnCount(8)
        self.table_inbound.setHorizontalHeaderLabels([
            "ID", "Bestell-Nr", "Kaufdatum", "Shop", "Logistik", "Tracking-Nr", "Status", "Aktion"
        ])
        
        header = self.table_inbound.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)
        
        self.table_inbound.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_inbound.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table_inbound.setStyleSheet("""
            QTableWidget { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868; border-radius: 4px; gridline-color: #414868; }
            QHeaderView::section { background-color: #1f2335; color: #7aa2f7; font-weight: bold; padding: 5px; border: 1px solid #414868; }
            QTableWidget::item:selected { background-color: #3d59a1; }
        """)
        # Context-Menu
        self.table_inbound.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_inbound.customContextMenuRequested.connect(self._show_inbound_context_menu)
        
        layout.addWidget(self.table_inbound)

    def _build_outbound_tab(self):
        layout = QVBoxLayout(self.tab_outbound)
        
        self.table_outbound = QTableWidget()
        self.table_outbound.setColumnCount(6)
        self.table_outbound.setHorizontalHeaderLabels([
            "ID", "Versanddatum", "Logistik", "Tracking-Nr", "Status", "Aktion"
        ])
        
        header = self.table_outbound.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(True)
        
        self.table_outbound.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_outbound.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table_outbound.setStyleSheet("""
            QTableWidget { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868; border-radius: 4px; gridline-color: #414868; }
            QHeaderView::section { background-color: #1f2335; color: #7aa2f7; font-weight: bold; padding: 5px; border: 1px solid #414868; }
            QTableWidget::item:selected { background-color: #3d59a1; }
        """)
        # Context-Menu
        self.table_outbound.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_outbound.customContextMenuRequested.connect(self._show_outbound_context_menu)
        
        layout.addWidget(self.table_outbound)

    def refresh_data(self):
        self._load_inbound()
        self._load_outbound()
        
    def _load_inbound(self):
        self.table_inbound.setRowCount(0)
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor(dictionary=True)
            # Hole alle EinkÃ¤ufe, bei denen das Lieferungdatum in der Zukunft liegt ODER der Status nicht Geliefert ist, 
            # und wo es Ã¼berhaupt eine Tracking ID gibt.
            query = """
                SELECT id, bestellnummer, kaufdatum, shop_name, paketdienst, tracking_nummer_einkauf, sendungsstatus
                FROM einkauf_bestellungen
                WHERE tracking_nummer_einkauf IS NOT NULL 
                  AND tracking_nummer_einkauf != ''
                  AND LOWER(sendungsstatus) != 'geliefert'
                ORDER BY kaufdatum DESC
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            self.table_inbound.setRowCount(len(rows))
            for row_idx, row in enumerate(rows):
                self.table_inbound.setItem(row_idx, 0, QTableWidgetItem(str(row['id'])))
                self.table_inbound.setItem(row_idx, 1, QTableWidgetItem(str(row['bestellnummer'])))
                
                date_str = str(row['kaufdatum'])[:10] if row['kaufdatum'] else ""
                self.table_inbound.setItem(row_idx, 2, QTableWidgetItem(date_str))
                
                self.table_inbound.setItem(row_idx, 3, QTableWidgetItem(str(row['shop_name'])))
                pd = str(row['paketdienst']) if row['paketdienst'] else "Unbekannt"
                self.table_inbound.setItem(row_idx, 4, QTableWidgetItem(pd))
                
                trk = str(row['tracking_nummer_einkauf'])
                self.table_inbound.setItem(row_idx, 5, QTableWidgetItem(trk))
                
                status = str(row['sendungsstatus']) if row['sendungsstatus'] else "Offen"
                status_item = QTableWidgetItem(status)
                if "problem" in status.lower() or "verzÃ¶gert" in status.lower():
                    status_item.setForeground(Qt.GlobalColor.red)
                elif "unterwegs" in status.lower() or "auslieferung" in status.lower():
                    status_item.setForeground(Qt.GlobalColor.yellow)
                    
                self.table_inbound.setItem(row_idx, 6, status_item)
                
                # Button: Link Ã¶ffnen
                btn_widget = QWidget()
                btn_layout = QHBoxLayout(btn_widget)
                btn_layout.setContentsMargins(0, 0, 0, 0)
                btn_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
                
                btn_track = QPushButton("ðŸ”— Tracken")
                btn_track.setCursor(Qt.CursorShape.PointingHandCursor)
                btn_track.setStyleSheet("background-color: #3b4261; color: white; border: none; border-radius: 3px; padding: 4px 8px;")
                btn_track.clicked.connect(lambda checked, p=pd, t=trk: self._open_tracking_url(p, t))
                
                btn_layout.addWidget(btn_track)
                self.table_inbound.setCellWidget(row_idx, 7, btn_widget)
                
            self.table_inbound.resizeColumnsToContents()
            cursor.close()
            conn.close()
        except Exception as e:
            log_exception(__name__, e)
            QMessageBox.critical(self, "Fehler", f"Fehler beim Laden der Inbound-Daten:\n{e}")

    def _load_outbound(self):
        self.table_outbound.setRowCount(0)
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor(dictionary=True)
            # Info: ausgangs_pakete benÃ¶tigt noch paketdienst und status!
            query = """
                SELECT id, versanddatum, tracking_nummer, paketdienst, sendungsstatus
                FROM ausgangs_pakete
                WHERE LOWER(sendungsstatus) != 'geliefert'
                ORDER BY versanddatum DESC
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            self.table_outbound.setRowCount(len(rows))
            for row_idx, row in enumerate(rows):
                self.table_outbound.setItem(row_idx, 0, QTableWidgetItem(str(row['id'])))
                
                date_str = str(row['versanddatum'])[:10] if row['versanddatum'] else ""
                self.table_outbound.setItem(row_idx, 1, QTableWidgetItem(date_str))
                
                pd = str(row['paketdienst']) if row['paketdienst'] else "Unbekannt"
                self.table_outbound.setItem(row_idx, 2, QTableWidgetItem(pd))
                
                trk = str(row['tracking_nummer'])
                self.table_outbound.setItem(row_idx, 3, QTableWidgetItem(trk))
                
                status = str(row['sendungsstatus']) if row['sendungsstatus'] else "Noch nicht los"
                status_item = QTableWidgetItem(status)
                self.table_outbound.setItem(row_idx, 4, status_item)
                
                # Button: Link Ã¶ffnen
                btn_widget = QWidget()
                btn_layout = QHBoxLayout(btn_widget)
                btn_layout.setContentsMargins(0, 0, 0, 0)
                btn_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
                
                btn_track = QPushButton("ðŸ”— Tracken")
                btn_track.setCursor(Qt.CursorShape.PointingHandCursor)
                btn_track.setStyleSheet("background-color: #3b4261; color: white; border: none; border-radius: 3px; padding: 4px 8px;")
                btn_track.clicked.connect(lambda checked, p=pd, t=trk: self._open_tracking_url(p, t))
                
                btn_layout.addWidget(btn_track)
                self.table_outbound.setCellWidget(row_idx, 5, btn_widget)
                
            self.table_outbound.resizeColumnsToContents()
            cursor.close()
            conn.close()
        except Exception as e:
            log_exception(__name__, e)
            # Wenn Spalten fehlen (Schema noch nicht ganz aktuell), fangen wir den Fehler ab
            if "Unknown column" in str(e):
                pass 
            else:
                print(f"Fehler Tracker Outbound: {e}")

    def _open_tracking_url(self, paketdienst, tracking_nummer):
        """Generiert den Deeplink anhand des Namens und Ã¶ffnet ihn im Standardbrowser"""
        url = ""
        pd = str(paketdienst).lower().strip()
        
        # Regex oder simple StartsWith/Contains logik
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
            # Fallback: Google Suche nach der Trackingnummer
            url = f"https://www.google.com/search?q={tracking_nummer}"
            
        print(f"Ã–ffne Browser mit: {url}")
        QDesktopServices.openUrl(QUrl(url))

    def _show_inbound_context_menu(self, position):
        """Rechtsklick Menu fÃ¼r Inbound Tabelle (Status manuell Ã¤ndern)"""
        row = self.table_inbound.itemAt(position).row() if self.table_inbound.itemAt(position) else -1
        if row < 0: return
        
        db_id = self.table_inbound.item(row, 0).text()
        
        menu = QMenu()
        menu.setStyleSheet("QMenu { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868; } QMenu::item:selected { background-color: #3b4261; }")
        
        action_geliefert = QAction("Status: Markieren als 'Geliefert'", self)
        action_geliefert.triggered.connect(lambda: self._update_status("einkauf_bestellungen", db_id, "Geliefert"))
        menu.addAction(action_geliefert)
        
        action_unterwegs = QAction("Status: Markieren als 'Unterwegs'", self)
        action_unterwegs.triggered.connect(lambda: self._update_status("einkauf_bestellungen", db_id, "Unterwegs"))
        menu.addAction(action_unterwegs)
        
        action_problem = QAction("Status: Markieren als 'Problem/VerzÃ¶gert'", self)
        action_problem.triggered.connect(lambda: self._update_status("einkauf_bestellungen", db_id, "Problem/VerzÃ¶gert"))
        menu.addAction(action_problem)
        
        menu.exec(self.table_inbound.viewport().mapToGlobal(position))

    def _show_outbound_context_menu(self, position):
        """Rechtsklick Menu fÃ¼r Outbound Tabelle"""
        row = self.table_outbound.itemAt(position).row() if self.table_outbound.itemAt(position) else -1
        if row < 0: return
        
        db_id = self.table_outbound.item(row, 0).text()
        
        menu = QMenu()
        menu.setStyleSheet("QMenu { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868; } QMenu::item:selected { background-color: #3b4261; }")
        
        action_geliefert = QAction("Status: Markieren als 'Geliefert'", self)
        action_geliefert.triggered.connect(lambda: self._update_status("ausgangs_pakete", db_id, "Geliefert"))
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
            
            # Tabelle direkt neuladen, damit die bearbeitete Reihe je nach Filter evtl. verschwindet (z.B. wenn Geliefert)
            self.refresh_data()
        except Exception as e:
            log_exception(__name__, e)
            QMessageBox.critical(self, "DB Fehler", f"Konnte Status nicht aktualisieren:\n{e}")
