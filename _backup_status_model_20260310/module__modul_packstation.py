"""
modul_packstation.py
UI-Modul zur Abwicklung des Outbounds (Warenausgang).
Workflow: 
1. Tracking-Nummer des leeren Kartons scannen (legt neues Paket an).
2. Artikel EAN scannen (sucht Artikel im Bestand).
3. Seriennummer scannen (speichert S/N, verknÃ¼pft Artikel mit Paket, Ã¤ndert Status).
"""

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, 
    QTableWidget, QTableWidgetItem, QHeaderView, 
    QLineEdit, QMessageBox, QFrame, QAbstractItemView
)
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QColor, QFont
from datetime import datetime

from module.database_manager import DatabaseManager

from module.crash_logger import log_exception
class PackstationApp(QWidget):
    def __init__(self, settings_manager):
        super().__init__()
        self.settings = settings_manager
        self.db = DatabaseManager(self.settings)
        
        # State Machine fÃ¼r den Scan-Modus
        # "WAITING_FOR_TRACKING" -> "WAITING_FOR_EAN" -> "WAITING_FOR_SERIAL"
        self.scan_state = "WAITING_FOR_TRACKING"
        
        self.current_tracking = None
        self.current_paket_id = None
        self.current_item_id = None # ID in waren_positionen (IN_STOCK)
        self.current_item_name = None
        
        self.scanned_items_in_box = [] # FÃ¼r die UI-Tabelle (Logs)
        
        self._build_ui()

    def _build_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)
        
        # --- TOP HEADER ---
        lbl_title = QLabel("Packstation / Outbound")
        lbl_title.setStyleSheet("font-size: 24px; font-weight: bold; color: #7aa2f7;")
        
        btn_reset = QPushButton("ðŸ”„ Vorgang abbrechen / Neues Paket")
        btn_reset.setProperty("class", "retro-btn")
        btn_reset.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_reset.clicked.connect(self._reset_workflow)
        
        top_layout = QHBoxLayout()
        top_layout.addWidget(lbl_title)
        top_layout.addStretch()
        top_layout.addWidget(btn_reset)
        main_layout.addLayout(top_layout)
        
        # --- STATUS BEREICH ---
        status_frame = QFrame()
        status_frame.setStyleSheet("QFrame { background-color: #1a1b26; border: 1px solid #414868; border-radius: 6px; }")
        status_layout = QVBoxLayout(status_frame)
        status_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.lbl_instruction = QLabel("Schritt 1: Scanne die TRACKING-NUMMER vom Versandetikett des Pakets")
        self.lbl_instruction.setStyleSheet("font-size: 22px; font-weight: bold; color: #e0af68; border: none; padding: 10px;")
        self.lbl_instruction.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.lbl_current_box = QLabel("Aktuelles Paket: Keine Zuweisung")
        self.lbl_current_box.setStyleSheet("font-size: 14px; color: #a9b1d6; border: none;")
        self.lbl_current_box.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        status_layout.addWidget(self.lbl_instruction)
        status_layout.addWidget(self.lbl_current_box)
        main_layout.addWidget(status_frame)
        
        # --- SCANNER EINGABE ---
        self.txt_scanner = QLineEdit()
        self.txt_scanner.setPlaceholderText("Barcode scannen... (Auto-Enter)")
        self.txt_scanner.setStyleSheet("""
            QLineEdit { background-color: #24283b; color: #9ece6a; font-size: 28px; padding: 15px; border: 2px solid #7aa2f7; border-radius: 6px; font-weight: bold; }
        """)
        self.txt_scanner.returnPressed.connect(self._handle_scan)
        # Immer fokussiert halten
        self.txt_scanner.editingFinished.connect(lambda: QTimer.singleShot(10, self.txt_scanner.setFocus))
        main_layout.addWidget(self.txt_scanner)
        
        # --- LOG TABELLE ---
        lbl_log = QLabel("Inhalt im aktuellen Karton:")
        lbl_log.setStyleSheet("font-size: 16px; font-weight: bold; color: #7aa2f7;")
        main_layout.addWidget(lbl_log)
        
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["Aktion Zeit", "Produkt", "EAN", "Seriennummer"])
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
        
        # Initialer Fokus
        QTimer.singleShot(500, self.txt_scanner.setFocus)

    def _reset_workflow(self):
        """Setzt alles auf Anfang zurÃ¼ck fÃ¼r ein neues Paket"""
        self.scan_state = "WAITING_FOR_TRACKING"
        self.current_tracking = None
        self.current_paket_id = None
        self.current_item_id = None
        self.current_item_name = None
        self.scanned_items_in_box = []
        
        self.lbl_instruction.setText("Schritt 1: Scanne die TRACKING-NUMMER vom Versandetikett des Pakets")
        self.lbl_instruction.setStyleSheet("font-size: 22px; font-weight: bold; color: #e0af68; border: none; padding: 10px;")
        self.lbl_current_box.setText("Aktuelles Paket: Keine Zuweisung")
        
        self.table.setRowCount(0)
        self.txt_scanner.clear()
        self.txt_scanner.setFocus()

    def _handle_scan(self):
        code = self.txt_scanner.text().strip()
        self.txt_scanner.clear()
        if not code: return
        
        if self.scan_state == "WAITING_FOR_TRACKING":
            self._process_tracking(code)
        elif self.scan_state == "WAITING_FOR_EAN":
            self._process_ean(code)
        elif self.scan_state == "WAITING_FOR_SERIAL":
            self._process_serial(code)

    def _process_tracking(self, tracking_code):
        """Legt ein neues Paket in der DB an oder nutzt ein existierendes offenes"""
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor(dictionary=True)
            
            # Schauen, ob dieses Paket schon existiert und noch nicht los ist
            cursor.execute("SELECT id FROM ausgangs_pakete WHERE tracking_nummer = %s", (tracking_code,))
            res = cursor.fetchone()
            
            if res:
                self.current_paket_id = res['id']
            else:
                # Neues Paket anlegen
                now = datetime.now().strftime('%Y-%m-%d')
                cursor.execute("""
                    INSERT INTO ausgangs_pakete (tracking_nummer, versanddatum, sendungsstatus)
                    VALUES (%s, %s, 'Noch nicht los')
                """, (tracking_code, now))
                self.current_paket_id = cursor.lastrowid
                
            conn.commit()
            cursor.close()
            conn.close()
            
            self.current_tracking = tracking_code
            self.lbl_current_box.setText(f"Aktuelles Paket: {tracking_code} (Gespeichert in Datenbank)")
            
            # Weiter zum nÃ¤chsten Schritt
            self.scan_state = "WAITING_FOR_EAN"
            self.lbl_instruction.setText("Schritt 2: Scanne die EAN (Barcode) des Artikels, der ins Paket kommt")
            self.lbl_instruction.setStyleSheet("font-size: 22px; font-weight: bold; color: #7aa2f7; border: none; padding: 10px;")
            
        except Exception as e:
            log_exception(__name__, e)
            QMessageBox.critical(self, "DB Fehler", f"Konnte Paket nicht anlegen:\n{e}")

    def _process_ean(self, ean_code):
        """Sucht nach einem IN_STOCK Artikel in der DB mit dieser EAN"""
        try:
            self.db.ensure_open_positions_unitized()
            conn = self.db._get_connection()
            cursor = conn.cursor(dictionary=True)
            
            # WICHTIG: Wir prÃ¼fen, ob der Artikel IN_STOCK ist und noch KEINEM ausgangs_paket zugeordnet ist.
            # (Limit 1, wir ignorieren vorerst Menge > 1 Logik pro Eintrag, falls wir Seriennummern pro Eintrag fordern).
            # Um es sicher zu machen: Die Tabelle waren_positionen sollte fÃ¼r Seriennummern ideally StÃ¼ckelung 1 haben,
            # aber falls menge > 1, ziehen wir 1 ab oder binden die Seriennummer an die Posiotion.
            # FÃ¼rs erste binden wir die Seriennummer an die Position. Wenn menge > 1, wird es komplex.
            
            query = """
                SELECT id, produkt_name, ean 
                FROM waren_positionen 
                WHERE ean = %s AND status = 'IN_STOCK' AND (ausgangs_paket_id IS NULL OR ausgangs_paket_id = 0)
                LIMIT 1
            """
            cursor.execute(query, (ean_code,))
            res = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if not res:
                QMessageBox.warning(self, "Nicht gefunden", f"Kein passender oder unzugewiesener Artikel (IN STOCK) zur EAN '{ean_code}' gefunden!\n\nHast du den Wareneingang dafÃ¼r gemacht?")
                return
                
            self.current_item_id = res['id']
            self.current_item_name = res['produkt_name']
            
            # NÃ¤chster Schritt: Seriennummer
            self.scan_state = "WAITING_FOR_SERIAL"
            self.lbl_instruction.setText(f"Gefunden: {self.current_item_name}!\nSchritt 3: Scanne jetzt die SERIENNUMMER des Artikels")
            self.lbl_instruction.setStyleSheet("font-size: 22px; font-weight: bold; color: #f7768e; border: none; padding: 10px;")
            
        except Exception as e:
            log_exception(__name__, e)
            QMessageBox.critical(self, "DB Fehler", f"Fehler bei EAN-Suche:\n{e}")

    def _process_serial(self, serial_code):
        """Speichert die Seriennummer beim Artikel, verknÃ¼pft das Paket und Ã¤ndert den Status"""
        try:
            conn = self.db._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE waren_positionen 
                SET seriennummern = %s, ausgangs_paket_id = %s, status = 'SHIPPED'
                WHERE id = %s
            """, (serial_code, self.current_paket_id, self.current_item_id))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Log Erfolg in die Tabelle
            self._add_to_log_table(self.current_item_name, "Gescannte EAN", serial_code)
            
            # ZurÃ¼ck zu EAN scannen (fÃ¼r den nÃ¤chsten Artikel ins gleiche Paket)
            self.scan_state = "WAITING_FOR_EAN"
            self.lbl_instruction.setText("Gespeichert! âœ”\nScanne NÃ„CHSTE EAN oder wÃ¤hle 'Neues Paket'")
            self.lbl_instruction.setStyleSheet("font-size: 22px; font-weight: bold; color: #9ece6a; border: none; padding: 10px;")
            
            self.current_item_id = None
            self.current_item_name = None
            
        except Exception as e:
            log_exception(__name__, e)
            QMessageBox.critical(self, "DB Fehler", f"Fehler bei Seriennummer-VerknÃ¼pfung:\n{e}")

    def _add_to_log_table(self, product_name, ean, serial):
        row_pos = self.table.rowCount()
        self.table.insertRow(row_pos)
        
        now = datetime.now().strftime('%H:%M:%S')
        
        self.table.setItem(row_pos, 0, QTableWidgetItem(now))
        self.table.setItem(row_pos, 1, QTableWidgetItem(product_name))
        self.table.setItem(row_pos, 2, QTableWidgetItem(ean))
        self.table.setItem(row_pos, 3, QTableWidgetItem(serial))
        
        self.table.resizeColumnsToContents()
        self.table.scrollToBottom()
