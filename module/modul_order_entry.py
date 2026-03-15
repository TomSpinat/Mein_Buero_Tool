"""
modul_order_entry.py
Das neue Clipboard- & Rechnungsscanner-Modul.
Ersetzt die alte scanner_app.py und integriert die Normalisierungs-Schranke 
(mapping.json) sowie die Speicherung in einkauf_bestellungen und waren_positionen.
"""

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
    QPushButton, QLineEdit, QFormLayout, QFrame, 
    QSizePolicy, QMessageBox, QApplication, QTextEdit,
    QTableWidget, QTableWidgetItem, QHeaderView, QRadioButton, QButtonGroup,
    QTabWidget, QComboBox, QFileDialog
)
from PyQt6.QtCore import Qt, pyqtSignal, QMimeData, QSize, QThread
from PyQt6.QtGui import QPixmap, QImage, QClipboard
import os
import tempfile
import shutil
import traceback

from module.gemini_api import process_receipt_with_gemini
from module.scan_input_preprocessing import prepare_order_entry_scan
from module.database_manager import DatabaseManager
from module.custom_msgbox import CustomMsgBox
from module.einkauf_pipeline import EinkaufPipeline
from module.ean_service import EanService
from module.ean_lookup_dialog import EanLookupDialog
from module.ean_search_worker import EanLookupWorker
from module.shop_logo_search_service import ShopLogoSearchService
from module.product_image_search_service import ProductImageSearchService
from module.media.media_grid_selection_dialog import MediaGridSelectionDialog
from module.media.media_keys import build_shop_key, build_product_key

from module.crash_logger import (
    AppError,
    classify_gemini_error,
    log_classified_error,
    log_exception,
)
class GeminiWorker(QThread):
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, api_key, prepared_scan):
        super().__init__()
        self.api_key = api_key
        self.prepared_scan = prepared_scan

    def run(self):
        try:
            scan_decision_dict = self.prepared_scan.scan_decision.to_dict() if getattr(self.prepared_scan, "scan_decision", None) else None
            result = process_receipt_with_gemini(
                api_key=self.api_key,
                image_path=self.prepared_scan.gemini_image_path,
                custom_text=self.prepared_scan.gemini_custom_text,
                scan_mode=self.prepared_scan.scan_mode,
                prompt_plan=self.prepared_scan.prompt_plan,
                scan_decision=scan_decision_dict,
            )
            self.finished_signal.emit(result or {})
        except Exception as e:
            app_error = e if isinstance(e, AppError) else classify_gemini_error(e, phase="order_entry_scan")
            log_classified_error(
                f"{__name__}.GeminiWorker.run",
                app_error.category if isinstance(app_error, AppError) else "unknown",
                app_error.user_message if isinstance(app_error, AppError) else str(e),
                status_code=app_error.status_code if isinstance(app_error, AppError) else None,
                service=app_error.service if isinstance(app_error, AppError) else "gemini",
                exc=e,
                extra={
                    "scan_mode": self.prepared_scan.scan_mode,
                    "image_path": str(self.prepared_scan.gemini_image_path or ""),
                    "source_types": [source.source_type for source in self.prepared_scan.sources],
                },
            )
            self.error_signal.emit(app_error.user_message if isinstance(app_error, AppError) else str(e))


class ImageDropBox(QFrame):
    """
    Drag & Drop Box und Ctrl+V Überwachung.
    """
    image_loaded = pyqtSignal(QPixmap)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setMinimumSize(300, 300)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        layout = QVBoxLayout(self)
        self.lbl_text = QLabel("Drag & Drop hier\noder Strg+V / Datei-Auswahl (Bild/PDF)")
        self.lbl_text.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_text.setObjectName("drop_text")
        layout.addWidget(self.lbl_text)

        self.current_pixmap = None
        self.current_source_path = None

    def dragEnterEvent(self, event):
        if event.mimeData().hasImage() or event.mimeData().hasUrls():
            event.acceptProposedAction()
            self.setStyleSheet("background-color: rgba(90, 107, 125, 0.5);")

    def dragLeaveEvent(self, event):
        self.setStyleSheet("")

    def dropEvent(self, event):
        self.setStyleSheet("")
        if event.mimeData().hasUrls():
            filepath = event.mimeData().urls()[0].toLocalFile()
            if filepath.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp')):
                self.load_image(filepath)
                event.acceptProposedAction()

    def keyPressEvent(self, event):
        if (event.modifiers() & Qt.KeyboardModifier.ControlModifier) and event.key() == Qt.Key.Key_V:
            clipboard = QApplication.clipboard()
            mime_data = clipboard.mimeData()
            
            if mime_data.hasImage():
                image = clipboard.image()
                pixmap = QPixmap.fromImage(image)
                self.set_image(pixmap, source_path=None)
            elif mime_data.hasUrls():
                filepath = mime_data.urls()[0].toLocalFile()
                if filepath.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp')):
                    self.load_image(filepath)

    def load_image(self, filepath):
        pixmap = QPixmap(filepath)
        if not pixmap.isNull():
            self.set_image(pixmap, source_path=filepath)

    def set_image(self, pixmap, source_path=None):
        self.current_pixmap = pixmap
        self.current_source_path = str(source_path or "").strip() or None
        scaled_pixmap = pixmap.scaled(
            self.size() - QSize(20, 20),
            Qt.AspectRatioMode.KeepAspectRatio, 
            Qt.TransformationMode.SmoothTransformation
        )
        self.lbl_text.setPixmap(scaled_pixmap)
        self.image_loaded.emit(pixmap)

    def mousePressEvent(self, event):
        self.setFocus()
        super().mousePressEvent(event)


class OrderEntryApp(QWidget):
    """
    Das Modul für die Bestellerfassung per Bild/Clipboard (Rechnungsscanner).
    """
    def __init__(self, settings_manager, parent=None):
        super().__init__(parent)
        self.settings_manager = settings_manager
        self.ean_service = EanService(self.settings_manager)
        self.ean_lookup_worker = None
        self._pending_ean_lookup_context = None
        self.setWindowTitle("Order Entry (Scanner)")
        self.current_gemini_data = {} # Speichert das komplette Dictionary zur Verarbeitung
        self.scan_mode = "einkauf" # 'einkauf' oder 'verkauf'
        self.selected_document_path = None  # Optional: manuell geladene Datei (z.B. PDF)
        self.scan_temp_file_path = None    # Temp-Datei fuer KI-Upload
        
        self.logo_search_service = ShopLogoSearchService(self.settings_manager)
        self.image_search_service = ProductImageSearchService(self.settings_manager)
        
        self.main_layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabBar::tab { background: #242535; color: #a9b1d6; padding: 10px 20px; font-weight: bold; border-top-left-radius: 4px; border-top-right-radius: 4px; }
            QTabBar::tab:selected { background: #6e3dd1; color: white; }
            QTabWidget::pane { border: 1px solid #414868; }
        """)
        self.main_layout.addWidget(self.tabs)
        
        # --- TAB 1: Scanner ---
        self.tab_scanner = QWidget()
        self.scanner_layout = QHBoxLayout(self.tab_scanner)
        self.tabs.addTab(self.tab_scanner, "1. KI Scanner")
        
        self._build_left_side()
        self._build_right_side()
        
        # --- TAB 2: Datenbank Ansicht ---
        self.tab_db = QWidget()
        self.db_layout = QVBoxLayout(self.tab_db)
        self.tabs.addTab(self.tab_db, "2. Datenbank-Tabellen (Editieren)")
        self._build_db_tab()
        
        self.tabs.currentChanged.connect(self._on_tab_changed)

    def _on_tab_changed(self, index):
        if index == 1:
            self._load_db_data()

    def _build_left_side(self):
        left_layout = QVBoxLayout()
        left_layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        # --- MODUS TOGGLE ---
        mode_frame = QFrame()
        mode_frame.setStyleSheet("QFrame { background-color: #1a1b26; border: 1px solid #414868; border-radius: 6px; padding: 5px; }")
        mode_layout = QHBoxLayout(mode_frame)
        
        lbl_mode = QLabel("Scan-Modus:")
        lbl_mode.setStyleSheet("color: #7aa2f7; font-weight: bold; border: none;")
        
        self.radio_einkauf = QRadioButton("🛒 Einkauf (Rechnung)")
        self.radio_einkauf.setChecked(True)
        
        self.radio_verkauf = QRadioButton("🏷️ Verkauf (Discord-Ticket)")
        
        self.mode_group = QButtonGroup(self)
        self.mode_group.addButton(self.radio_einkauf)
        self.mode_group.addButton(self.radio_verkauf)
        self.mode_group.buttonClicked.connect(self._on_mode_changed)
        
        mode_layout.addWidget(lbl_mode)
        mode_layout.addWidget(self.radio_einkauf)
        mode_layout.addWidget(self.radio_verkauf)
        left_layout.addWidget(mode_frame)

        lbl_anweisung = QLabel("<h3>1. Beleg einfuegen</h3><p>Drag & Drop, Strg+V oder Datei-Auswahl nutzen.</p>")
        left_layout.addWidget(lbl_anweisung)

        self.btn_upload_file = QPushButton("Datei aus Ordner auswaehlen")
        self.btn_upload_file.setObjectName("ScannerBtn")
        self.btn_upload_file.clicked.connect(self._upload_document)
        left_layout.addWidget(self.btn_upload_file)

        self.drop_box = ImageDropBox()
        self.drop_box.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        left_layout.addWidget(self.drop_box)

        lbl_freitext = QLabel("<h2>Freitext</h2>")
        left_layout.addWidget(lbl_freitext)
        
        self.txt_anweisung = QTextEdit()
        self.txt_anweisung.setPlaceholderText("Freitext-Notizen hier...")
        self.txt_anweisung.setFixedHeight(80)
        self.txt_anweisung.textChanged.connect(self._check_scan_ready)
        left_layout.addWidget(self.txt_anweisung)

        self.btn_scan = QPushButton("Scannen")
        self.btn_scan.setObjectName("ScannerBtn")
        self.btn_scan.setEnabled(False)
        self.btn_scan.clicked.connect(self._start_scan)
        left_layout.addWidget(self.btn_scan)

        self.drop_box.image_loaded.connect(self._on_image_loaded)
        self.scanner_layout.addLayout(left_layout, stretch=1)

    def _on_mode_changed(self):
        """Reagiert auf den Wechsel von Einkauf/Verkauf."""
        if self.radio_einkauf.isChecked():
            self.scan_mode = "einkauf"
            self.lbl_form.setText("<h3>2. Kopfdaten (Einkauf)</h3>")
            self.btn_save_db.setText("💾 Bestellung speichern")
        else:
            self.scan_mode = "verkauf"
            self.lbl_form.setText("<h3>2. Ticketdaten (Verkauf)</h3>")
            self.btn_save_db.setText("🚀 Discord-Ticket speichern / ticket folgt")
            
        self._build_dynamic_form()
        self._reset_form()

    def _build_right_side(self):
        right_layout = QVBoxLayout()
        right_layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        self.lbl_form = QLabel("<h3>2. Kopfdaten (Einkauf)</h3>")
        right_layout.addWidget(self.lbl_form)

        self.form_layout = QFormLayout()
        right_layout.addLayout(self.form_layout)
        
        # Tabelle für Warenpositionen (Einkauf oder Verkauf)
        lbl_waren = QLabel("<h3>3. Erfasste Artikel</h3>")
        right_layout.addWidget(lbl_waren)
        
        self.table_waren = QTableWidget()
        self.table_waren.setColumnCount(6) # +1 für Bild-Suche Button
        # Erlaube dem Nutzer die Spaltenbreite manuell anzupassen
        self.table_waren.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table_waren.horizontalHeader().setStretchLastSection(True) # Die letzte Spalte füllt den Rest auf
        self.table_waren.verticalHeader().setDefaultSectionSize(45) # Größer wegen dem QLineEdit Padding
        right_layout.addWidget(self.table_waren)


        ean_row = QHBoxLayout()
        self.btn_ean_lookup = QPushButton("EAN suchen (markierte Zeile)")
        self.btn_ean_lookup.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_ean_lookup.clicked.connect(self._lookup_ean_for_selected_row)
        ean_row.addWidget(self.btn_ean_lookup)
        ean_row.addStretch()
        right_layout.addLayout(ean_row)
        # Initial die Tabelle und das Formular aufbauen
        self._build_dynamic_form()

        # Buttons
        button_layout = QHBoxLayout()
        
        self.btn_reset = QPushButton("🗑️ Formular leeren")
        self.btn_reset.clicked.connect(self._reset_form)
        button_layout.addWidget(self.btn_reset)

        self.btn_save_db = QPushButton("💾 Bestellung speichern")
        self.btn_save_db.setObjectName("ScannerBtn")
        self.btn_save_db.setEnabled(False)
        self.btn_save_db.clicked.connect(self._save_to_database)
        button_layout.addWidget(self.btn_save_db)

        right_layout.addLayout(button_layout)
        self.scanner_layout.addLayout(right_layout, stretch=2)

    def _build_dynamic_form(self):
        # Alle bestehenden Widgets aus dem form_layout entfernen
        while self.form_layout.count():
            item = self.form_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

        self.inputs = {}
        if self.scan_mode == "einkauf":
            fields = [
                ("bestellnummer", "Bestellnummer:"),
                ("kaufdatum", "Kaufdatum:"),
                ("shop_name", "Shop-Name (Normiert):"),
                ("bestell_email", "Bestell-Email:"),
                ("tracking_nummer_einkauf", "Tracking Code:"),
                ("sendungsstatus", "Sendungsstatus:"),
                ("lieferdatum", "Lieferdatum:"),
                ("gesamt_ekp_brutto", "Gesamtpreis (Brutto):"),
                ("versandkosten_brutto", "Versandkosten (Brutto):"),
                ("nebenkosten_brutto", "Nebenkosten (Brutto):"),
                ("rabatt_brutto", "Rabatt/Gutschrift (Brutto):"),
                ("ust_satz", "USt.-Satz:"),
                ("zahlungsart", "Zahlungsart (Normiert):")
            ]
            self.table_waren.setHorizontalHeaderLabels(["Produkt", "Variante", "EAN", "Menge", "Stückpreis"])
        else:
            fields = [
                ("ticket_name", "Ticket-Name:"),
                ("kaeufer", "Käufer/Nutzername:"),
                ("zahlungsziel", "Zahlungsziel:")
            ]
            self.table_waren.setHorizontalHeaderLabels(["Produkt", "EAN", "Menge", "VK Brutto", "Marge gesamt"])

        for db_key, label_text in fields:
            le = QLineEdit()
            le.setPlaceholderText("Warte auf KI...")
            self.inputs[db_key] = le
            self.form_layout.addRow(label_text, le)

    def _upload_document(self):
        """Erlaubt das Auswaehlen einer Beleg-Datei aus einem Ordner (Bild oder PDF)."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Beleg-Datei auswaehlen",
            os.path.expanduser("~"),
            "Belege (*.png *.jpg *.jpeg *.bmp *.webp *.pdf);;Bilddateien (*.png *.jpg *.jpeg *.bmp *.webp);;PDF Dateien (*.pdf);;Alle Dateien (*.*)"
        )

        if not file_path:
            return

        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".pdf":
            self.selected_document_path = file_path
            self.drop_box.current_pixmap = None
            self.drop_box.lbl_text.setPixmap(QPixmap())
            self.drop_box.lbl_text.setText(f"PDF geladen:\n{os.path.basename(file_path)}")
            self._check_scan_ready()
            self.drop_box.setFocus()
            return

        pixmap = QPixmap(file_path)
        if pixmap.isNull():
            CustomMsgBox.warning(
                self,
                "Datei nicht lesbar",
                "Die ausgewaehlte Datei konnte nicht als Bild geladen werden. Bitte nutze PNG/JPG/BMP/WEBP/PDF."
            )
            return

        self.selected_document_path = None
        self.drop_box.load_image(file_path)
        self.drop_box.setFocus()

    def _on_image_loaded(self, pixmap=None):
        # Sobald ein Bild geladen wird, ist es die aktive Quelle (statt evtl. zuvor geladener PDF)
        self.selected_document_path = None
        self._check_scan_ready(pixmap)

    def _check_scan_ready(self, pixmap=None):
        has_image = (self.drop_box.current_pixmap is not None)
        has_file = bool(self.selected_document_path)
        has_text = (len(self.txt_anweisung.toPlainText().strip()) > 0)
        self.btn_scan.setEnabled(has_image or has_file or has_text)

    def _start_scan(self):
        api_key = self.settings_manager.get("gemini_api_key", "")
        if not api_key:
            CustomMsgBox.warning(self, "API Key fehlt", "Bitte trage deinen Gemini API Key in den Einstellungen ein!")
            return

        custom_text = self.txt_anweisung.toPlainText().strip()
        temp_img_path = None
        original_name = ""
        mime_type = ""

        if self.drop_box.current_pixmap is not None:
            temp_file, temp_img_path = tempfile.mkstemp(suffix=".png")
            os.close(temp_file)
            self.drop_box.current_pixmap.save(temp_img_path, "PNG")
            original_name = os.path.basename(self.drop_box.current_source_path) if self.drop_box.current_source_path else "clipboard_capture.png"
            mime_type = "image/png"
        elif self.selected_document_path and os.path.exists(self.selected_document_path):
            ext = os.path.splitext(self.selected_document_path)[1].lower() or ".tmp"
            temp_file, temp_img_path = tempfile.mkstemp(suffix=ext)
            os.close(temp_file)
            shutil.copy2(self.selected_document_path, temp_img_path)
            original_name = os.path.basename(self.selected_document_path)
            mime_type = "application/pdf" if ext == ".pdf" else ""

        prepared_scan = prepare_order_entry_scan(
            scan_mode=self.scan_mode,
            file_path=temp_img_path,
            original_name=original_name,
            mime_type=mime_type,
            custom_text=custom_text,
        )
        self.scan_temp_file_path = prepared_scan.gemini_image_path

        self.btn_scan.setText("🔄 Analyse läuft...")
        self.btn_scan.setEnabled(False)

        if not hasattr(self, 'loading_overlay'):
            from module.loading_overlay import LoadingOverlay
            self.loading_overlay = LoadingOverlay(self)
        self.loading_overlay.start("Dokument wird analysiert...")

        self.gemini_worker = GeminiWorker(api_key, prepared_scan)
        self.gemini_worker.finished_signal.connect(self._on_ai_finished)
        self.gemini_worker.error_signal.connect(self._on_ai_error)
        self.gemini_worker.start()

    def _on_ai_finished(self, result_dict):
        if hasattr(self, 'loading_overlay'):
            self.loading_overlay.stop()
        self.btn_scan.setText("Scannen")
        self.btn_scan.setEnabled(True)
        
        temp_img_path = self.scan_temp_file_path
        if temp_img_path and os.path.exists(temp_img_path):
            os.remove(temp_img_path)
        self.scan_temp_file_path = None
            
        if result_dict:
            try:
                if self.scan_mode == "einkauf":
                    result_dict = EinkaufPipeline.normalize_einkauf_result(self, result_dict)
                
                self.current_gemini_data = result_dict
                self._fill_ui()
            except Exception as e:
                log_exception(__name__, e)
                CustomMsgBox.critical(self, "Fehler bei Nachbearbeitung", f"{str(e)}")

    def _on_ai_error(self, err_msg):
        if hasattr(self, 'loading_overlay'):
            self.loading_overlay.stop()
        self.btn_scan.setText("Scannen")
        self.btn_scan.setEnabled(True)
        
        temp_img_path = self.scan_temp_file_path
        if temp_img_path and os.path.exists(temp_img_path):
            os.remove(temp_img_path)
        self.scan_temp_file_path = None
            
        CustomMsgBox.critical(self, "Fehler", f"Fehler bei KI Analyse:\\n{err_msg}")

    def _fill_ui(self):
        """Füllt das GUI aus dem current_gemini_data Dictionary."""
        for db_key, line_edit in self.inputs.items():
            value = str(self.current_gemini_data.get(db_key, ""))
            if value == "None" or not value:
                value = ""
            line_edit.setText(value)
            
        waren = self.current_gemini_data.get("waren", [])
        self.table_waren.setRowCount(len(waren))
        
        
        for row, item in enumerate(waren):
            produkt_name = str(item.get("produkt_name", "")).strip()
            ean = str(item.get("ean", "")).strip()
            
            # Falls die EAN leer ist, fragen wir die Datenbank!
            if not ean and produkt_name:
                ean = self.ean_service.find_best_local_ean_by_name(produkt_name, str(item.get("varianten_info", "")))

            if self.scan_mode == "einkauf":
                self.table_waren.setItem(row, 0, QTableWidgetItem(produkt_name))
                self.table_waren.setItem(row, 1, QTableWidgetItem(str(item.get("varianten_info", ""))))
                self.table_waren.setItem(row, 2, QTableWidgetItem(ean))
                self.table_waren.setItem(row, 3, QTableWidgetItem(str(item.get("menge", "1"))))
                self.table_waren.setItem(row, 4, QTableWidgetItem(str(item.get("ekp_brutto", "0.00"))))
            else:
                self.table_waren.setItem(row, 0, QTableWidgetItem(produkt_name))
                self.table_waren.setItem(row, 1, QTableWidgetItem(ean))
                self.table_waren.setItem(row, 2, QTableWidgetItem(str(item.get("menge", "1"))))
                self.table_waren.setItem(row, 3, QTableWidgetItem(str(item.get("vk_brutto", "0.00"))))
                self.table_waren.setItem(row, 4, QTableWidgetItem(str(item.get("marge_gesamt", "0.00"))))

        self.table_waren.resizeColumnsToContents()
        self.btn_save_db.setEnabled(True)
    def _lookup_ean_for_selected_row(self):
        if self.ean_lookup_worker is not None and self.ean_lookup_worker.isRunning():
            CustomMsgBox.information(self, "EAN Suche", "Es laeuft bereits eine EAN-Suche im Hintergrund.")
            return

        row = self.table_waren.currentRow()
        if row < 0:
            CustomMsgBox.information(self, "EAN Suche", "Bitte zuerst eine Artikelzeile markieren.")
            return

        produkt_name = ""
        varianten_info = ""
        ean_col = 2 if self.scan_mode == "einkauf" else 1

        if self.table_waren.item(row, 0):
            produkt_name = self.table_waren.item(row, 0).text().strip()
        if self.scan_mode == "einkauf" and self.table_waren.item(row, 1):
            varianten_info = self.table_waren.item(row, 1).text().strip()

        if not produkt_name:
            CustomMsgBox.warning(self, "EAN Suche", "In der markierten Zeile fehlt der Produktname.")
            return

        self._pending_ean_lookup_context = {
            "row": row,
            "ean_col": ean_col,
            "produkt_name": produkt_name,
            "varianten_info": varianten_info,
        }
        self.btn_ean_lookup.setEnabled(False)
        self.btn_ean_lookup.setText("EAN Suche laeuft...")

        self.ean_lookup_worker = EanLookupWorker(
            self.settings_manager,
            produkt_name,
            varianten_info=varianten_info,
            limit=25,
            allow_api_fallback=True,
        )
        self.ean_lookup_worker.result_signal.connect(self._on_ean_lookup_finished)
        self.ean_lookup_worker.error_signal.connect(self._on_ean_lookup_error)
        self.ean_lookup_worker.start()

    def _finish_ean_lookup_ui(self):
        self.btn_ean_lookup.setEnabled(True)
        self.btn_ean_lookup.setText("EAN suchen (markierte Zeile)")
        self.ean_lookup_worker = None

    def _on_ean_lookup_finished(self, payload):
        context = dict(self._pending_ean_lookup_context or {})
        self._pending_ean_lookup_context = None
        self._finish_ean_lookup_ui()

        candidates = payload.get("candidates", []) if isinstance(payload, dict) else []
        error_payload = payload.get("error", {}) if isinstance(payload, dict) else {}
        if not candidates:
            api_msg = ""
            if isinstance(error_payload, dict):
                api_msg = str(error_payload.get("user_message", "")).strip()
            if api_msg:
                CustomMsgBox.warning(
                    self,
                    "EAN Suche",
                    "Lokal gab es keine Treffer und die API-Suche ist fehlgeschlagen:\n\n" + api_msg,
                )
            else:
                CustomMsgBox.information(
                    self,
                    "Keine Treffer",
                    "Es wurden weder lokal noch ueber die API passende EAN-Vorschlaege gefunden."
                )
            return

        produkt_name = str(context.get("produkt_name", "")).strip()
        selected = EanLookupDialog.choose(produkt_name, candidates, parent=self)
        if not selected:
            return

        chosen_ean = str(selected.get("ean", "")).strip()
        if not chosen_ean:
            CustomMsgBox.warning(self, "EAN Suche", "Der gewaehlte Eintrag hat keine gueltige EAN.")
            return

        row = int(context.get("row", -1) or -1)
        ean_col = int(context.get("ean_col", 2) or 2)
        if row < 0 or row >= self.table_waren.rowCount():
            CustomMsgBox.warning(self, "EAN Suche", "Die bearbeitete Tabellenzeile existiert nicht mehr.")
            return

        self.table_waren.setItem(row, ean_col, QTableWidgetItem(chosen_ean))
        self.ean_service.remember_candidate_selection(
            produkt_name,
            selected,
            varianten_info=str(context.get("varianten_info", "")).strip(),
        )

    def _on_ean_lookup_error(self, err_msg):
        self._pending_ean_lookup_context = None
        self._finish_ean_lookup_ui()
        msg = str(err_msg or "").strip() or "Unbekannter Fehler bei der EAN-Suche."
        CustomMsgBox.warning(self, "EAN Suche", f"Die EAN-Suche ist fehlgeschlagen:\n{msg}")

    def _reset_form(self):
        self.drop_box.current_pixmap = None
        self.drop_box.current_source_path = None
        self.selected_document_path = None
        self.scan_temp_file_path = None
        self.drop_box.lbl_text.setPixmap(QPixmap())
        self.drop_box.lbl_text.setText("Drag & Drop hier\noder Strg+V / Datei-Auswahl (Bild/PDF)")
        
        self.txt_anweisung.clear()
        for le in self.inputs.values():
            le.clear()
            
        self.table_waren.setRowCount(0)
        self.current_gemini_data = {}
        
        self.btn_scan.setEnabled(False)
        self.btn_save_db.setEnabled(False)

    def _save_to_database(self):
        # UI Änderungen zurück in das gemini dict spielen
        for db_key, line_edit in self.inputs.items():
            self.current_gemini_data[db_key] = line_edit.text()
            
        # Tabelle auslesen
        rows = self.table_waren.rowCount()
        waren_liste = []
        for r in range(rows):
            if self.scan_mode == "einkauf":
                w = {
                    "produkt_name": self.table_waren.item(r, 0).text(),
                    "varianten_info": self.table_waren.item(r, 1).text(),
                    "ean": self.table_waren.item(r, 2).text(),
                    "menge": self.table_waren.item(r, 3).text(),
                    "ekp_brutto": self.table_waren.item(r, 4).text()
                }
            else:
                w = {
                    "produkt_name": self.table_waren.item(r, 0).text(),
                    "ean": self.table_waren.item(r, 1).text(),
                    "menge": self.table_waren.item(r, 2).text(),
                    "vk_brutto": self.table_waren.item(r, 3).text(),
                    "marge_gesamt": self.table_waren.item(r, 4).text()
                }
            waren_liste.append(w)
        
        self.current_gemini_data["waren"] = waren_liste

        if self.scan_mode == "einkauf":
            def _on_order_number_changed(new_no):
                self.current_gemini_data["bestellnummer"] = new_no
                if "bestellnummer" in self.inputs:
                    self.inputs["bestellnummer"].setText(new_no)

            try:
                save_result = EinkaufPipeline.confirm_and_save_single(
                    self,
                    self.settings_manager,
                    self.current_gemini_data,
                    on_order_number_changed=_on_order_number_changed,
                    show_new_number_info=True,
                    db=None
                )

                if save_result.get("status") != "saved":
                    return

                match_result = EinkaufPipeline.confirm_and_apply_pending_matches(
                    self,
                    self.settings_manager,
                    db=save_result.get("db")
                )
                title, text = EinkaufPipeline.build_match_result_message(match_result)
                CustomMsgBox.information(self, title, text)
                self._reset_form()
            except Exception as e:
                log_exception(__name__, e)
                CustomMsgBox.critical(self, "Datenbank-Fehler", str(e))
        else:
            if not self.current_gemini_data.get("ticket_name", "").strip():
                CustomMsgBox.warning(self, "Fehler", "Ticket-Name fehlt!")
                return

            try:
                db = DatabaseManager(self.settings_manager)
                matched_items, pending_units, pending_summary = db.preview_verkauf_discord(self.current_gemini_data)

                msg_parts = []
                if matched_items:
                    msg_parts.append(f"{len(matched_items)} Stück werden sofort mit vorhandenen Bestellungen verknüpft:\n")
                    for match in matched_items[:8]:
                        match_date = match["kaufdatum"].strftime("%d.%m.%Y") if match.get("kaufdatum") else "?"
                        msg_parts.append(f"- {match['bestellnummer']} ({match_date}) | {match['produkt_name']}")
                    if len(matched_items) > 8:
                        msg_parts.append(f"- ... und {len(matched_items) - 8} weitere")

                if pending_units:
                    if msg_parts:
                        msg_parts.append("")
                    msg_parts.append(f"{len(pending_units)} Stück bleiben als 'ticket folgt' offen und werden später automatisch nachverknüpft:\n")
                    for line in pending_summary[:8]:
                        msg_parts.append(f"- {line}")
                    if len(pending_summary) > 8:
                        msg_parts.append(f"- ... und {len(pending_summary) - 8} weitere")

                if not msg_parts:
                    msg_parts.append("Für dieses Ticket wurden aktuell keine verwertbaren Positionen erkannt.")

                msg_parts.append("")
                msg_parts.append("Matching jetzt anwenden?")
                msg_parts.append("Yes = Ticket speichern + Matching anwenden")
                msg_parts.append("No = Ticket speichern ohne Matching (alles bleibt ticket folgt)")
                msg_parts.append("Cancel = Abbrechen")

                reply = CustomMsgBox.question(
                    self,
                    "Discord-Ticket speichern",
                    "\n".join(msg_parts),
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No | QMessageBox.StandardButton.Cancel,
                    QMessageBox.StandardButton.Yes
                )
                if reply == QMessageBox.StandardButton.Cancel:
                    return

                if reply == QMessageBox.StandardButton.No:
                    forced_pending = list(pending_units)
                    for match in matched_items:
                        forced_pending.append({
                            "ware_index": match.get("ware_index", 0),
                            "unit_index": 0,
                            "produkt_name": match.get("ticket_produkt", match.get("produkt_name", "")),
                            "ean": match.get("ean", ""),
                            "vk_brutto": match.get("vk_brutto", 0.0),
                            "marge_gesamt": match.get("marge_gesamt", 0.0)
                        })
                    matched_items = []
                    pending_units = forced_pending

                result = db.confirm_verkauf_discord(self.current_gemini_data, matched_items, pending_units)
                if result.get("pending_count", 0) > 0:
                    CustomMsgBox.information(
                        self,
                        "Gespeichert",
                        f"Ticket gespeichert. {result.get('matched_count', 0)} Stück direkt verknüpft, {result.get('pending_count', 0)} Stück warten als 'ticket folgt'."
                    )
                else:
                    CustomMsgBox.information(
                        self,
                        "Erfolg",
                        f"Ticket gespeichert und {result.get('matched_count', 0)} Stück direkt verknüpft."
                    )
                self._reset_form()

            except Exception as e:
                log_exception(__name__, e)
                CustomMsgBox.critical(self, "Datenbank Fehler", f"Speichern fehlgeschlagen:\n{e}")

    def _build_db_tab(self):
        top_layout = QHBoxLayout()
        lbl = QLabel("Wähle Tabelle:")
        lbl.setStyleSheet("font-weight: bold; color: #a9b1d6;")
        
        self.combo_table = QComboBox()
        self.combo_table.addItems(["waren_positionen", "einkauf_bestellungen", "verkauf_tickets", "ausgangs_pakete"])
        self.combo_table.setStyleSheet("padding: 5px; background: #242535; color: #a9b1d6; border: 1px solid #414868; border-radius: 4px;")
        self.combo_table.currentTextChanged.connect(self._load_db_data)
        
        top_layout.addWidget(lbl)
        top_layout.addWidget(self.combo_table)
        top_layout.addStretch()
        
        btn_refresh = QPushButton("🔄 Aktualisieren")
        btn_refresh.setObjectName("ScannerBtn")
        btn_refresh.setFixedSize(150, 40)
        btn_refresh.clicked.connect(self._load_db_data)
        top_layout.addWidget(btn_refresh)
        
        self.db_layout.addLayout(top_layout)
        
        self.table_db = QTableWidget()
        self.table_db.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table_db.horizontalHeader().setStretchLastSection(True)
        self.table_db.verticalHeader().setDefaultSectionSize(35)
        self.table_db.itemChanged.connect(self._on_db_item_changed)
        self.db_layout.addWidget(self.table_db)

        self._loading_db = False

    def _load_db_data(self):
        table_name = self.combo_table.currentText()
        if not table_name: return
        
        try:
            self._loading_db = True
            db = DatabaseManager(self.settings_manager)
            conn = db._get_connection()
            cursor = conn.cursor()
            
            # Hole Spaltennamen
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = [col[0] for col in cursor.fetchall()]
            
            # Hole Daten
            cursor.execute(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT 500")
            data = cursor.fetchall()
            
            self.table_db.setColumnCount(len(columns))
            self.table_db.setHorizontalHeaderLabels(columns)
            self.table_db.setRowCount(len(data))
            
            for row, row_data in enumerate(data):
                for col, value in enumerate(row_data):
                    item = QTableWidgetItem(str(value) if value is not None else "")
                    # Die ID sollte nicht editierbar sein
                    if columns[col] == "id":
                        item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                    self.table_db.setItem(row, col, item)
            
            self.table_db.resizeColumnsToContents()
            cursor.close()
            conn.close()
            self._loading_db = False
        except Exception as e:
            log_exception(__name__, e)
            self._loading_db = False
            CustomMsgBox.critical(self, "Fehler", f"Konnte Tabelle {table_name} nicht laden:\n{e}")

    def _on_db_item_changed(self, item):
        """Wird ausgelöst, sobald eine Zelle händisch bearbeitet wurde."""
        if self._loading_db:
            return
            
        row = item.row()
        col = item.column()
        
        id_item = self.table_db.item(row, 0)
        if not id_item: return
        row_id = id_item.text()
        
        column_name = self.table_db.horizontalHeaderItem(col).text()
        new_value = item.text()
        table_name = self.combo_table.currentText()
        
        try:
            db = DatabaseManager(self.settings_manager)
            conn = db._get_connection()
            cursor = conn.cursor()
            query = f"UPDATE {table_name} SET {column_name} = %s WHERE id = %s"
            cursor.execute(query, (new_value if new_value else None, row_id))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            log_exception(__name__, e)
            CustomMsgBox.critical(self, "DB Speicherfehler", f"Fehler beim Speichern der Zeile {row_id}:\n{str(e)}")














