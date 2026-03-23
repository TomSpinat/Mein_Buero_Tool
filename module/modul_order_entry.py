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
    QTabWidget, QComboBox, QFileDialog, QMenu, QDialog, QDialogButtonBox, QSpinBox,
    QScrollArea, QSplitter,
)
from PyQt6.QtCore import Qt, pyqtSignal, QMimeData, QSize, QThread
from PyQt6.QtGui import QPixmap, QImage, QClipboard, QPainter
import os
import tempfile
import shutil
import traceback

from module.gemini_api import classify_ai_provider_error, process_receipt_with_gemini
from module.ai.provider_settings import get_ai_provider_label
from module.scan_input_preprocessing import prepare_order_entry_scan
from module.database_manager import DatabaseManager
from module.custom_msgbox import CustomMsgBox
from module.einkauf_pipeline import EinkaufPipeline
from module.ean_service import EanService
from module.ean_lookup_dialog import EanLookupDialog
from module.ean_search_worker import EanLookupWorker
from module.shop_logo_search_service import ShopLogoSearchService
from module.product_image_search_service import ProductImageSearchService
from module.product_image_search_worker import ProductImageSearchWorker
from module.media.media_grid_selection_dialog import MediaGridSelectionDialog
from module.media.media_service import MediaService

from module.shared_einkauf_review import (
    collect_einkauf_payload,
    check_einkauf_save_ready,
    clear_einkauf_review_data,
    refresh_summen_banner,
    apply_einkauf_review_workflow,
    refresh_einkauf_review_workflow,
    prepare_and_save_einkauf_workflow,
    reset_einkauf_review_workflow,
)
from module.shared_search_workflows import (
    create_logo_search_worker,
    reset_logo_search_button,
    handle_logo_search_result,
    handle_logo_search_error,
    create_ean_lookup_worker,
    reset_ean_lookup_button,
    handle_ean_lookup_result,
    handle_ean_lookup_error,
)

from module.crash_logger import (
    AppError,
    classify_gemini_error,
    log_classified_error,
    log_exception,
)
from module.lookup_service import LookupService
from module.lookup_results import FieldState, FieldType, LookupSource
from module.field_lookup_binding import FieldLookupBinding, create_bindings
from module.einkauf_ui import EinkaufHeadFormWidget, EinkaufItemsTableWidget, SummenBannerWidget, OrderReviewPanelWidget, set_field_state
from module.money_tooltips import calculate_purchase_payload_breakdown
from module.normalization_dialog import NormalizationPanel
from module.amazon_country_dialog import AmazonCountryPanel, SPECIFIC_AMAZON_VALUES
class GeminiWorker(QThread):
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)

    def __init__(self, api_key, prepared_scan, provider_name="gemini", provider_profile_name="", provider_profile_overrides=None):
        super().__init__()
        self.api_key = api_key
        self.prepared_scan = prepared_scan
        self.provider_name = str(provider_name or "gemini")
        self.provider_profile_name = str(provider_profile_name or "")
        self.provider_profile_overrides = dict(provider_profile_overrides or {}) if isinstance(provider_profile_overrides, dict) else {}

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
                provider_name=self.provider_name,
                provider_profile_name=self.provider_profile_name,
                provider_profile_overrides=self.provider_profile_overrides,
            )
            self.finished_signal.emit(result or {})
        except Exception as e:
            app_error = e if isinstance(e, AppError) else classify_ai_provider_error(e, provider_name=self.provider_name, phase="order_entry_scan")
            log_classified_error(
                f"{__name__}.GeminiWorker.run",
                app_error.category if isinstance(app_error, AppError) else "unknown",
                app_error.user_message if isinstance(app_error, AppError) else str(e),
                status_code=app_error.status_code if isinstance(app_error, AppError) else None,
                service=app_error.service if isinstance(app_error, AppError) else self.provider_name,
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
        self.logo_search_worker = None
        self.product_image_search_worker = None
        self._pending_image_search_context = None
        self.setWindowTitle("Order Entry (Scanner)")
        self.current_gemini_data = {} # Speichert das komplette Dictionary zur Verarbeitung
        self.scan_mode = "einkauf" # 'einkauf' oder 'verkauf'
        self.selected_document_path = None  # Optional: manuell geladene Datei (z.B. PDF)
        self.scan_temp_file_path = None    # Temp-Datei fuer KI-Upload
        
        self.logo_search_service = ShopLogoSearchService(self.settings_manager)
        self.image_search_service = ProductImageSearchService(self.settings_manager)

        # --- Zentraler LookupService ---
        self._lookup_db = DatabaseManager(self.settings_manager)
        self._lookup_service = LookupService(self._lookup_db)
        self._lookup_bindings: dict[str, FieldLookupBinding] = {}
        self._mapping_state = None
        self._mapping_done = True
        self._mapping_prompted = False

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
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(6)

        # Nicht sichtbares Label fuer Legacy-Kompatibilitaet (_on_mode_changed setText)
        self.lbl_form = QLabel()
        self.lbl_form.setVisible(False)

        # ── Mittelbereich + rechtes Mapping-Fach ──────────────────────────────
        self.content_splitter = QSplitter(Qt.Orientation.Horizontal, self)
        self.content_splitter.setChildrenCollapsible(False)
        right_layout.addWidget(self.content_splitter, 1)

        # ── Tab-Widget ─────────────────────────────────────────────────────────
        self.data_tabs = QTabWidget()
        self.data_tabs.setStyleSheet(
            "QTabWidget::pane { border: 1px solid #414868; border-radius: 6px; background: transparent; }"
            "QTabBar::tab { background: #1a1b26; color: #a9b1d6; padding: 8px 18px; border: 1px solid #414868;"
            " border-bottom: none; border-top-left-radius: 6px; border-top-right-radius: 6px; margin-right: 2px; }"
            "QTabBar::tab:selected { background: #1f2335; color: #7aa2f7; font-weight: bold; }"
            "QTabBar::tab:hover { background: #292e42; }"
        )

        # --- Tab 1: Kopfdaten ---
        kopf_scroll = QScrollArea()
        kopf_scroll.setWidgetResizable(True)
        kopf_scroll.setFrameShape(QFrame.Shape.NoFrame)
        kopf_scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")
        kopf_panel = QWidget()
        kopf_box = QVBoxLayout(kopf_panel)
        kopf_box.setContentsMargins(0, 8, 8, 0)
        kopf_box.setSpacing(8)

        self.einkauf_form_widget = EinkaufHeadFormWidget(self, logo_search_mode="direct")
        self.einkauf_form_widget.logoSearchRequested.connect(self._start_logo_search_from_context)
        self.einkauf_form_widget.valueChanged.connect(self._on_einkauf_form_value_changed)
        self.einkauf_form_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self._einkauf_form_scroll = self.einkauf_form_widget
        kopf_box.addWidget(self._einkauf_form_scroll)

        # Legacy QFormLayout fuer Verkauf-Modus
        self.form_layout_frame = QWidget()
        _ff_layout = QVBoxLayout(self.form_layout_frame)
        _ff_layout.setContentsMargins(0, 0, 0, 0)
        self.form_layout = QFormLayout()
        _ff_layout.addLayout(self.form_layout)
        self.form_layout_frame.setVisible(False)
        kopf_box.addWidget(self.form_layout_frame)

        kopf_scroll.setWidget(kopf_panel)
        self.data_tabs.addTab(kopf_scroll, "Kopfdaten")

        # --- Tab 2: Artikel ---
        artikel_panel = QWidget()
        artikel_box = QVBoxLayout(artikel_panel)
        artikel_box.setContentsMargins(0, 8, 8, 0)
        artikel_box.setSpacing(8)

        waren_header_row = QHBoxLayout()
        waren_header_row.addStretch()

        self.btn_add_row = QPushButton("+ Zeile")
        self.btn_add_row.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_add_row.setStyleSheet(
            "QPushButton { background-color: #203225; color: #9ece6a; border: 1px solid #9ece6a;"
            " border-radius: 4px; padding: 3px 10px; font-size: 12px; }"
            "QPushButton:hover { background-color: #2a4a35; }"
        )
        self.btn_add_row.clicked.connect(self._add_table_row)
        waren_header_row.addWidget(self.btn_add_row)

        self.btn_delete_row = QPushButton("- Markierte loeschen")
        self.btn_delete_row.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_delete_row.setStyleSheet(
            "QPushButton { background-color: #3c2020; color: #f7768e; border: 1px solid #f7768e;"
            " border-radius: 4px; padding: 3px 10px; font-size: 12px; }"
            "QPushButton:hover { background-color: #4a2a2a; }"
        )
        self.btn_delete_row.clicked.connect(self._delete_selected_rows)
        waren_header_row.addWidget(self.btn_delete_row)
        artikel_box.addLayout(waren_header_row)

        # --- Einkauf: EinkaufItemsTableWidget (10-Spalten-Widget mit Review, Bild-Management) ---
        self.einkauf_items_widget = EinkaufItemsTableWidget(self)
        self.einkauf_items_widget.eanLookupRequested.connect(self._on_einkauf_items_ean_lookup)
        self.einkauf_items_widget.imageSearchRequested.connect(self._on_einkauf_items_image_search)
        self.einkauf_items_widget.table.itemChanged.connect(self._on_waren_table_changed)
        self.einkauf_items_widget.setVisible(False)
        artikel_box.addWidget(self.einkauf_items_widget, 1)

        # --- Verkauf: Legacy QTableWidget (5 Spalten) ---
        self.table_waren = QTableWidget()
        self.table_waren.setColumnCount(7)
        self.table_waren.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table_waren.horizontalHeader().setStretchLastSection(True)
        self.table_waren.verticalHeader().setDefaultSectionSize(45)
        self.table_waren.setVisible(False)
        artikel_box.addWidget(self.table_waren, 1)

        ean_row = QHBoxLayout()
        self.btn_ean_lookup = QPushButton("EAN suchen (markierte Zeile)")
        self.btn_ean_lookup.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_ean_lookup.clicked.connect(self._lookup_ean_for_selected_row)
        ean_row.addWidget(self.btn_ean_lookup)
        ean_row.addStretch()
        artikel_box.addLayout(ean_row)

        self.summen_banner = SummenBannerWidget()
        artikel_box.addWidget(self.summen_banner)

        self.table_waren.itemChanged.connect(self._on_waren_table_changed)
        self.data_tabs.addTab(artikel_panel, "Artikel")

        # --- Tab 3: Uebersicht ---
        uebersicht_scroll = QScrollArea()
        uebersicht_scroll.setWidgetResizable(True)
        uebersicht_scroll.setFrameShape(QFrame.Shape.NoFrame)
        uebersicht_scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")
        uebersicht_panel = QWidget()
        uebersicht_box = QVBoxLayout(uebersicht_panel)
        uebersicht_box.setContentsMargins(0, 8, 8, 0)
        uebersicht_box.setSpacing(12)

        self.order_review_widget = OrderReviewPanelWidget(self)
        self.order_review_widget.setMinimumHeight(96)
        uebersicht_box.addWidget(self.order_review_widget)

        lbl_mapping_log = QLabel("Auto-Mapping")
        lbl_mapping_log.setStyleSheet("font-size: 13px; font-weight: bold; color: #7aa2f7;")
        uebersicht_box.addWidget(lbl_mapping_log)
        self.lbl_auto_mapping_log = QLabel("Keine automatischen Mappings.")
        self.lbl_auto_mapping_log.setWordWrap(True)
        self.lbl_auto_mapping_log.setStyleSheet(
            "font-size: 12px; color: #a9b1d6; background-color: #1f2335;"
            " border: 1px solid #414868; border-radius: 6px; padding: 8px;"
        )
        uebersicht_box.addWidget(self.lbl_auto_mapping_log)

        lbl_warnings_title = QLabel("Warnungen")
        lbl_warnings_title.setStyleSheet("font-size: 13px; font-weight: bold; color: #f7c66f;")
        uebersicht_box.addWidget(lbl_warnings_title)
        self.lbl_warnings = QLabel("Keine Warnungen.")
        self.lbl_warnings.setWordWrap(True)
        self.lbl_warnings.setStyleSheet(
            "font-size: 12px; color: #a9b1d6; background-color: #1f2335;"
            " border: 1px solid #414868; border-radius: 6px; padding: 8px;"
        )
        uebersicht_box.addWidget(self.lbl_warnings)

        lbl_validation_title = QLabel("Validierung")
        lbl_validation_title.setStyleSheet("font-size: 13px; font-weight: bold; color: #9ece6a;")
        uebersicht_box.addWidget(lbl_validation_title)
        self.lbl_validation_checklist = QLabel("")
        self.lbl_validation_checklist.setWordWrap(True)
        self.lbl_validation_checklist.setStyleSheet(
            "font-size: 12px; color: #c0caf5; background-color: #1f2335;"
            " border: 1px solid #414868; border-radius: 6px; padding: 8px;"
        )
        uebersicht_box.addWidget(self.lbl_validation_checklist)

        uebersicht_box.addStretch(1)
        uebersicht_scroll.setWidget(uebersicht_panel)
        self.data_tabs.addTab(uebersicht_scroll, "Uebersicht")

        self.content_splitter.addWidget(self.data_tabs)

        self.mapping_panel_widget = QWidget(self)
        self.mapping_panel_widget.setMinimumWidth(260)
        self.mapping_panel_widget.setMaximumWidth(420)
        mapping_box = QVBoxLayout(self.mapping_panel_widget)
        mapping_box.setContentsMargins(0, 0, 0, 0)
        mapping_box.setSpacing(12)

        lbl_mapping = QLabel("Mapping und Normalisierung")
        lbl_mapping.setStyleSheet("font-size: 14px; font-weight: bold;")
        mapping_box.addWidget(lbl_mapping)

        self.lbl_mapping_state = QLabel("Kein Mapping offen")
        self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #6b7280;")
        self.btn_run_mapping = QPushButton("Keine Aktion offen")
        self.btn_run_mapping.setEnabled(False)
        self.btn_run_mapping.clicked.connect(self._on_run_mapping_clicked)

        mapping_state_row = QHBoxLayout()
        mapping_state_row.addWidget(self.lbl_mapping_state, 1)
        mapping_state_row.addWidget(self.btn_run_mapping)
        mapping_box.addLayout(mapping_state_row)

        self.lbl_mapping_side_hint = QLabel(
            "Offene Zuordnungen erscheinen hier rechts, waehrend du links die Daten weiter pruefen kannst."
        )
        self.lbl_mapping_side_hint.setWordWrap(True)
        self.lbl_mapping_side_hint.setStyleSheet("font-size: 12px; color: #a9b1d6;")
        mapping_box.addWidget(self.lbl_mapping_side_hint)

        self.mapping_frame = QFrame()
        self.mapping_frame.setStyleSheet(
            "QFrame { background-color: #202233; border: 1px solid #414868; border-radius: 6px; }"
        )
        mapping_layout = QVBoxLayout(self.mapping_frame)
        mapping_layout.setContentsMargins(14, 14, 14, 14)
        mapping_layout.setSpacing(10)

        self.lbl_mapping_panel_title = QLabel("Mapping-Bereich")
        self.lbl_mapping_panel_title.setStyleSheet("font-size: 14px; font-weight: bold; color: #7aa2f7;")
        mapping_layout.addWidget(self.lbl_mapping_panel_title)

        self.lbl_mapping_panel_hint = QLabel("")
        self.lbl_mapping_panel_hint.setWordWrap(True)
        self.lbl_mapping_panel_hint.setStyleSheet("font-size: 12px; color: #a9b1d6;")
        mapping_layout.addWidget(self.lbl_mapping_panel_hint)

        self.mapping_panel_host = QVBoxLayout()
        self.mapping_panel_host.setContentsMargins(0, 0, 0, 0)
        mapping_layout.addLayout(self.mapping_panel_host)
        mapping_box.addWidget(self.mapping_frame, 1)
        mapping_box.addStretch(1)

        self.content_splitter.addWidget(self.mapping_panel_widget)
        self.content_splitter.setStretchFactor(0, 7)
        self.content_splitter.setStretchFactor(1, 3)
        self._active_mapping_panel = None
        self._active_mapping_context = None
        self._set_mapping_panel_collapsed(True)

        # Initial die Tabelle und das Formular aufbauen
        self._build_dynamic_form()

        # Footer-Buttons (ausserhalb der Tabs)
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

    def _clear_mapping_panel(self):
        while self.mapping_panel_host.count():
            item = self.mapping_panel_host.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()
        self._active_mapping_panel = None

    def _set_mapping_panel_collapsed(self, collapsed):
        sizes = self.content_splitter.sizes()
        if len(sizes) < 2:
            return
        total = max(1, sum(sizes))
        if collapsed:
            panel_width = 210
            self.mapping_panel_widget.setMinimumWidth(190)
            self.mapping_panel_widget.setMaximumWidth(220)
            self.mapping_frame.setVisible(False)
            self.lbl_mapping_side_hint.setVisible(False)
            self.content_splitter.setSizes([max(260, total - panel_width), panel_width])
        else:
            panel_width = min(360, max(280, int(total * 0.34)))
            self.mapping_panel_widget.setMinimumWidth(280)
            self.mapping_panel_widget.setMaximumWidth(420)
            self.mapping_frame.setVisible(True)
            self.lbl_mapping_side_hint.setVisible(True)
            self.content_splitter.setSizes([max(420, total - panel_width), panel_width])

    def _clear_embedded_mapping_request(self):
        self._active_mapping_context = None
        self._mapping_state = None
        self._mapping_done = True
        self._mapping_prompted = False
        self._clear_mapping_panel()
        self.lbl_mapping_state.setText("Kein Mapping offen")
        self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #6b7280;")
        self.btn_run_mapping.setText("Keine Aktion offen")
        self.btn_run_mapping.setEnabled(False)
        self.lbl_mapping_panel_title.setText("Mapping-Bereich")
        self.lbl_mapping_panel_hint.setText("")
        self._set_mapping_panel_collapsed(True)

    def _show_embedded_mapping_panel(self, panel, *, title, hint, context):
        self._clear_mapping_panel()
        self._active_mapping_context = dict(context or {})
        self._active_mapping_panel = panel
        if hasattr(panel, "selection_confirmed"):
            panel.selection_confirmed.connect(self._on_mapping_panel_completed)
        if hasattr(panel, "cancel_requested"):
            panel.cancel_requested.connect(self._clear_embedded_mapping_request)
        self.lbl_mapping_panel_title.setText(str(title or "Mapping-Bereich"))
        self.lbl_mapping_panel_hint.setText(str(hint or ""))
        self.mapping_panel_host.addWidget(panel)
        field_label = str(self._active_mapping_context.get("field_label", "Wert") or "Wert")
        self.lbl_mapping_state.setText(f"Zuordnung offen: {field_label}")
        self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #f7c66f;")
        self.btn_run_mapping.setText("Rechts anzeigen")
        self.btn_run_mapping.setEnabled(True)
        self._set_mapping_panel_collapsed(False)

    def _open_shop_amazon_mapping_panel(self, raw_name):
        raw_text = str(raw_name or "Amazon").strip() or "Amazon"
        panel = AmazonCountryPanel(raw_value=raw_text, mode="embedded", parent=self.mapping_frame)
        self._show_embedded_mapping_panel(
            panel,
            title="Shop-Zuordnung: Amazon-Land",
            hint=f"Die KI hat '{raw_text}' erkannt. Bitte waehle rechts den passenden Amazon-Shop.",
            context={
                "kind": "shop_amazon",
                "field_key": "shop_name",
                "field_label": "Shop-Name",
                "raw_value": raw_text,
            },
        )

    def _open_normalization_mapping_panel(self, *, field_key, field_label, category, raw_value):
        raw_text = str(raw_value or "").strip()
        if not raw_text:
            return
        panel = NormalizationPanel(category, raw_text, mode="embedded", parent=self.mapping_frame)
        self._show_embedded_mapping_panel(
            panel,
            title=f"{field_label} zuordnen",
            hint=f"Der erkannte Rohwert lautet '{raw_text}'. Rechts kannst du ihn uebernehmen oder einem Standard zuordnen.",
            context={
                "kind": "normalization",
                "field_key": str(field_key or ""),
                "field_label": str(field_label or field_key or "Wert"),
                "category": str(category or ""),
                "raw_value": raw_text,
            },
        )

    def _on_run_mapping_clicked(self):
        self._set_mapping_panel_collapsed(False)
        self._run_mapping_for_current_scan(show_feedback=True, rebuild=True)

    def _clear_inline_suggestions(self):
        for widget in self.inputs.values():
            if hasattr(widget, "clear_suggestions"):
                widget.clear_suggestions()

    def _trigger_review_lookups_only(self):
        if self.scan_mode != "einkauf":
            return
        for key in ("bestellnummer", "kaufdatum"):
            binding = self._lookup_bindings.get(key)
            widget = self.inputs.get(key)
            if not binding or not widget:
                continue
            text = str(widget.text() or "").strip()
            if text:
                binding.set_state(FieldState.AI_SUGGESTED)
                binding.trigger_lookup(text)
            else:
                binding.set_state(FieldState.EMPTY)

    def _apply_payload_to_current_scan(self, payload):
        merged = dict(self.current_gemini_data or {})
        if isinstance(payload, dict):
            merged.update(payload)
        self.current_gemini_data = merged
        self._clear_inline_suggestions()
        apply_einkauf_review_workflow(
            self.einkauf_form_widget,
            self.einkauf_items_widget,
            self.summen_banner,
            merged,
            ean_callback=self.ean_service.find_best_local_ean_by_name,
        )
        self._update_save_button_state()
        self._trigger_review_lookups_only()

    def _build_live_einkauf_payload(self):
        payload = collect_einkauf_payload(
            self.einkauf_form_widget,
            self.einkauf_items_widget,
            self.current_gemini_data,
        )
        money_meta = calculate_purchase_payload_breakdown(payload, settings=self.settings_manager)
        for key in ("warenwert_brutto", "einstand_gesamt_brutto", "einstand_gesamt_netto", "ust_satz"):
            if key in money_meta:
                if key == "ust_satz":
                    payload[key] = f"{float(money_meta.get(key) or 0.0):.2f}".rstrip("0").rstrip(".")
                else:
                    payload[key] = money_meta.get(key)
        return payload

    def _refresh_einkauf_live_calculations(self, update_logo=False):
        if self.scan_mode != "einkauf":
            return
        payload = self._build_live_einkauf_payload()
        merged = dict(self.current_gemini_data or {})
        merged.update(payload)
        self.current_gemini_data = merged
        self.einkauf_form_widget.refresh_payload_context(merged, update_logo=update_logo)
        self.einkauf_items_widget.set_payload_context(merged)
        refresh_summen_banner(self.summen_banner, self.einkauf_items_widget, merged)

    def _on_einkauf_form_value_changed(self, field_key, _value):
        if self.scan_mode != "einkauf":
            return
        update_logo = str(field_key or "") in ("shop_name", "bestell_email")
        self._refresh_einkauf_live_calculations(update_logo=update_logo)
        self._update_save_button_state()

    def _ensure_mapping_state(self, rebuild=False, source_payload=None):
        if not rebuild and isinstance(self._mapping_state, dict):
            return self._mapping_state
        workflow = EinkaufPipeline.prepare_mapping_workflow(source_payload or self.current_gemini_data or {})
        self._mapping_state = {
            "payload": dict(workflow.get("payload", {}) or {}),
            "tasks": list(workflow.get("tasks", []) or []),
            "task_index": 0,
        }
        self.current_gemini_data.update(self._mapping_state["payload"])
        self._mapping_done = len(self._mapping_state["tasks"]) == 0
        return self._mapping_state

    def _current_mapping_state(self):
        return self._mapping_state if isinstance(self._mapping_state, dict) else None

    def _get_current_task_index(self):
        state = self._current_mapping_state()
        if not state:
            return 0
        return int(state.get("task_index", 0) or 0)

    def _set_current_task_index(self, task_index):
        state = self._current_mapping_state()
        if state:
            state["task_index"] = int(task_index or 0)

    def _has_pending_mapping_tasks(self):
        state = self._current_mapping_state()
        if not state:
            return False
        return self._get_current_task_index() < len(list(state.get("tasks", []) or []))

    def _advance_to_next_task(self):
        state = self._current_mapping_state()
        if not state:
            return
        next_index = self._get_current_task_index() + 1
        self._set_current_task_index(next_index)
        self._mapping_done = next_index >= len(list(state.get("tasks", []) or []))

    def _mapping_task_hint_text(self, task):
        raw_value = str((task or {}).get("raw_value", "") or "").strip() or "-"
        if (task or {}).get("task_type") == "amazon_country":
            return f"Erkannter Rohwert: {raw_value}. Bitte waehle jetzt rechts den passenden Amazon-Shop."
        return f"Erkannter Rohwert: {raw_value}. Du kannst ihn rechts uebernehmen oder einem Standard zuordnen."

    def _render_mapping_panel_for_current_scan(self):
        state = self._current_mapping_state()
        if not state or not self._has_pending_mapping_tasks():
            self.mapping_frame.setVisible(False)
            self._clear_mapping_panel()
            return

        tasks = list(state.get("tasks", []) or [])
        task_index = self._get_current_task_index()
        task = tasks[task_index]
        self._clear_mapping_panel()
        self.mapping_frame.setVisible(True)
        self.lbl_mapping_panel_title.setText(
            f"Mapping-Schritt {task_index + 1}/{len(tasks)}: {str(task.get('label', 'Mapping') or 'Mapping')}"
        )
        self.lbl_mapping_panel_hint.setText(self._mapping_task_hint_text(task))

        if task.get("task_type") == "amazon_country":
            panel = AmazonCountryPanel(raw_value=task.get("raw_value", "Amazon"), mode="embedded", parent=self.mapping_frame)
        else:
            panel = NormalizationPanel(task.get("category", "shops"), task.get("raw_value", ""), mode="embedded", parent=self.mapping_frame)
        panel.selection_confirmed.connect(self._on_mapping_panel_completed)
        if hasattr(panel, "cancel_requested"):
            panel.cancel_requested.connect(self._clear_embedded_mapping_request)
        self.mapping_panel_host.addWidget(panel)
        self._active_mapping_panel = panel

    def _update_mapping_state_ui(self):
        state = self._current_mapping_state() or {}
        tasks = list(state.get("tasks", []) or []) if isinstance(state, dict) else []
        remaining = max(0, len(tasks) - self._get_current_task_index())

        if self._mapping_done:
            self.lbl_mapping_state.setText("Mapping: erledigt")
            self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #9ece6a;")
            self.btn_run_mapping.setText("Mapping erneut pruefen")
            self.btn_run_mapping.setEnabled(True)
            self._set_mapping_panel_collapsed(True)
        elif remaining > 0:
            self.lbl_mapping_state.setText(f"Mapping: {remaining} Schritt(e) offen")
            self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #f7c66f;")
            self.btn_run_mapping.setText("Mapping-Bereich anzeigen")
            self.btn_run_mapping.setEnabled(True)
            self._set_mapping_panel_collapsed(False)
        else:
            self.lbl_mapping_state.setText("Mapping: offen")
            self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #f7c66f;")
            self.btn_run_mapping.setText("Mapping jetzt starten")
            self.btn_run_mapping.setEnabled(True)
            self._set_mapping_panel_collapsed(True)

    def _refresh_mapping_panel_and_ui(self):
        self._render_mapping_panel_for_current_scan()
        self._update_mapping_state_ui()

    def _run_mapping_for_current_scan(self, show_feedback=True, rebuild=False):
        try:
            payload = collect_einkauf_payload(
                self.einkauf_form_widget,
                self.einkauf_items_widget,
                self.current_gemini_data,
            )
            state = self._ensure_mapping_state(rebuild=rebuild, source_payload=payload)
            self._apply_payload_to_current_scan(state.get("payload", {}))

            if not self._has_pending_mapping_tasks():
                self._mapping_done = True
                self._clear_mapping_panel()
                if show_feedback:
                    self.mapping_frame.setVisible(True)
                    self.lbl_mapping_panel_title.setText("Keine offene Mapping-Aufgabe")
                    self.lbl_mapping_panel_hint.setText("Fuer diesen Beleg ist aktuell kein weiterer Mapping-Schritt noetig.")
                else:
                    self.mapping_frame.setVisible(False)
                self._update_mapping_state_ui()
                return

            self._mapping_done = False
            self._mapping_prompted = True
            self._refresh_mapping_panel_and_ui()
        except Exception as e:
            log_exception(__name__, e)
            CustomMsgBox.critical(self, "Mapping-Fehler", f"Das Mapping konnte nicht vorbereitet werden:\n{e}")

    def _on_mapping_panel_completed(self, selected_value):
        state = self._current_mapping_state()
        if not state:
            context = dict(self._active_mapping_context or {})
            field_key = str(context.get("field_key", "") or "").strip()
            if not field_key or field_key not in self.inputs:
                self._clear_embedded_mapping_request()
                return
            normalized = str(selected_value or "").strip()
            self.inputs[field_key].setText(normalized)
            self.current_gemini_data[field_key] = normalized
            binding = self._lookup_bindings.get(field_key)
            if binding:
                binding.set_state(FieldState.USER_CONFIRMED)
                if normalized:
                    binding.trigger_lookup(normalized)
            self._clear_embedded_mapping_request()
            return

        tasks = list(state.get("tasks", []) or [])
        task_index = self._get_current_task_index()
        if task_index >= len(tasks):
            self._clear_embedded_mapping_request()
            return

        task = tasks[task_index]
        state["payload"] = EinkaufPipeline.apply_mapping_decision(state.get("payload", {}), task, selected_value)
        self._mapping_state = state
        self._apply_payload_to_current_scan(state.get("payload", {}))
        field_key = str(task.get("field", "") or "").strip()
        binding = self._lookup_bindings.get(field_key)
        if field_key and binding and field_key in self.inputs:
            text = str(self.inputs[field_key].text() or "").strip()
            if text:
                binding.set_state(FieldState.USER_CONFIRMED)
                binding.trigger_lookup(text)
        self._mapping_prompted = True
        self._advance_to_next_task()

        if self._has_pending_mapping_tasks():
            self._refresh_mapping_panel_and_ui()
        else:
            self.mapping_frame.setVisible(False)
            self._clear_mapping_panel()
            self._update_mapping_state_ui()

    def _build_dynamic_form(self):
        self.inputs = {}
        if self.scan_mode == "einkauf":
            # EinkaufHeadFormWidget anzeigen, Legacy-Layout ausblenden
            self._einkauf_form_scroll.setVisible(True)
            self.form_layout_frame.setVisible(False)

            # inputs direkt aus EinkaufHeadFormWidget uebernehmen (InlineChangeFieldRow)
            self.inputs = self.einkauf_form_widget.inputs

            # lbl_shop_logo / btn_logo_search als Properties weiterleiten (Legacy-Kompatibilitaet)
            self.lbl_shop_logo = self.einkauf_form_widget.lbl_shop_logo
            self.btn_logo_search = self.einkauf_form_widget.btn_logo_search

            # Einkauf: EinkaufItemsTableWidget anzeigen, Legacy-Tabelle + externe EAN-/Zeilen-Buttons ausblenden
            self.einkauf_items_widget.setVisible(True)
            self.table_waren.setVisible(False)
            self.btn_ean_lookup.setVisible(False)
            self.btn_add_row.setVisible(False)
            self.btn_delete_row.setVisible(False)

            # Bestellnummer-Feld: textChanged → Save-Button-State aktualisieren
            bestnr_widget = self.inputs.get("bestellnummer")
            if bestnr_widget:
                # InlineChangeFieldRow hat normal_input (QLineEdit) mit textChanged
                inner = getattr(bestnr_widget, "normal_input", bestnr_widget)
                if hasattr(inner, "textChanged"):
                    # Alte Verbindung trennen (verhindert Mehrfach-Connects bei Moduswechsel)
                    try:
                        inner.textChanged.disconnect()
                    except TypeError:
                        pass  # Noch keine Verbindung vorhanden
                    inner.textChanged.connect(lambda _: self._update_save_button_state())
            self._clear_embedded_mapping_request()
        else:
            # Legacy-QFormLayout fuer Verkauf-Modus
            self._einkauf_form_scroll.setVisible(False)
            self.form_layout_frame.setVisible(True)
            self._clear_embedded_mapping_request()

            # Verkauf: Legacy-Tabelle + externe Buttons anzeigen, EinkaufItemsTableWidget ausblenden
            self.einkauf_items_widget.setVisible(False)
            self.table_waren.setVisible(True)
            self.btn_ean_lookup.setVisible(True)
            self.btn_add_row.setVisible(True)
            self.btn_delete_row.setVisible(True)

            # Alte Widgets entfernen
            while self.form_layout.count():
                item = self.form_layout.takeAt(0)
                widget = item.widget()
                if widget:
                    widget.deleteLater()

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

        # --- Lookup-Bindings fuer bekannte Felder erstellen ---
        self._setup_lookup_bindings()

    def _upload_document(self):
        """Erlaubt das Auswaehlen einer Beleg-Datei aus einem Ordner (Bild oder PDF)."""
        start_dir = self.settings_manager.get_last_dir("upload_beleg") if self.settings_manager else os.path.expanduser("~")
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Beleg-Datei auswaehlen",
            start_dir,
            "Belege (*.png *.jpg *.jpeg *.bmp *.webp *.pdf);;Bilddateien (*.png *.jpg *.jpeg *.bmp *.webp);;PDF Dateien (*.pdf);;Alle Dateien (*.*)"
        )

        if not file_path:
            return
        if self.settings_manager:
            self.settings_manager.set_last_dir("upload_beleg", file_path)

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
        provider_name = self.settings_manager.get_active_ai_provider()
        api_key = self.settings_manager.get_active_ai_api_key()
        if not api_key:
            provider_label = get_ai_provider_label(provider_name)
            CustomMsgBox.warning(self, "API Key fehlt", f"Bitte trage deinen {provider_label} API Key in den Einstellungen ein!")
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

        self.gemini_worker = GeminiWorker(
            api_key,
            prepared_scan,
            provider_name=provider_name,
            provider_profile_name=self.settings_manager.get_ai_profile_name(provider_name),
            provider_profile_overrides=self.settings_manager.get_ai_profile_overrides(provider_name),
        )
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
        if self.scan_mode == "einkauf":
            self._fill_einkauf_ui()  # inkl. Summen-Banner via Shared-Apply-Workflow
        else:
            self._fill_verkauf_ui()
            self._update_summen_banner()
            self._trigger_post_fill_lookups()

        self._update_save_button_state()

    def _fill_einkauf_ui(self):
        """Einkauf-Pfad: Kopfdaten + Artikel + Summen-Banner ueber Shared-Phase befuellen."""
        self._clear_inline_suggestions()
        apply_einkauf_review_workflow(
            self.einkauf_form_widget,
            self.einkauf_items_widget,
            self.summen_banner,
            self.current_gemini_data,
            ean_callback=self.ean_service.find_best_local_ean_by_name,
        )
        self._mapping_state = None
        self._mapping_done = True
        self._mapping_prompted = False
        self._run_mapping_for_current_scan(show_feedback=False, rebuild=True)

    def _fill_verkauf_ui(self):
        """Verkauf-Pfad (Legacy): Kopfdaten aus QLineEdits, Artikel aus QTableWidget befuellen."""
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
            if not ean and produkt_name:
                ean = self.ean_service.find_best_local_ean_by_name(produkt_name, str(item.get("varianten_info", "")))
            self.table_waren.setItem(row, 0, QTableWidgetItem(produkt_name))
            self.table_waren.setItem(row, 1, QTableWidgetItem(ean))
            self.table_waren.setItem(row, 2, QTableWidgetItem(str(item.get("menge", "1"))))
            self.table_waren.setItem(row, 3, QTableWidgetItem(str(item.get("vk_brutto", "0.00"))))
            self.table_waren.setItem(row, 4, QTableWidgetItem(str(item.get("marge_gesamt", "0.00"))))
        self.table_waren.resizeColumnsToContents()

    # ── Zentraler LookupService: Setup + Handler ──────────────────────

    def _setup_lookup_bindings(self):
        """Richtet Lookup-Bindings fuer shop_name und zahlungsart ein."""
        # Alte Bindings aufräumen
        for binding in self._lookup_bindings.values():
            binding.deleteLater()
        self._lookup_bindings.clear()

        if self.scan_mode != "einkauf":
            return

        self._lookup_bindings = create_bindings(
            widgets=self.inputs,
            lookup_service=self._lookup_service,
            result_handler=self._on_lookup_result,
            parent_widget=self,
        )

    def _trigger_post_fill_lookups(self):
        """Triggert Lookups nach KI-Fill fuer alle relevanten Felder."""
        if self.scan_mode != "einkauf":
            return

        for key, binding in self._lookup_bindings.items():
            text = str(self.inputs.get(key, QLineEdit()).text()).strip()
            if text:
                binding.set_state(FieldState.AI_SUGGESTED)
                binding.trigger_lookup(text)
            else:
                binding.set_state(FieldState.EMPTY)

    def _on_lookup_result(self, result):
        """Zentraler Handler fuer alle Lookup-Ergebnisse."""
        try:
            if result.field_type == FieldType.SHOP_NAME:
                self._handle_shop_lookup_result(result)
            elif result.field_type == FieldType.ZAHLUNGSART:
                self._handle_zahlungsart_lookup_result(result)
            elif result.field_type == FieldType.BESTELLNUMMER:
                self._handle_bestellnummer_lookup_result(result)
            # KAUFDATUM: FieldLookupBinding setzt den State direkt – kein extra Handler noetig
        except Exception as exc:
            log_exception(__name__, exc)

    def _handle_shop_lookup_result(self, result):
        """Verarbeitet Shop-Lookup-Ergebnis: Logo anzeigen, eingebettetes Mapping oeffnen."""
        # Logo anzeigen wenn gefunden – nutzt set_shop_logo_path damit der Frame sichtbar wird
        if result.has_logo and hasattr(self, "einkauf_form_widget"):
            self.einkauf_form_widget.set_shop_logo_path(result.logo_path)

        # Amazon-Zuordnung rechts einblenden wenn noetig
        if result.source == LookupSource.AMAZON_DIALOG and result.needs_confirm:
            raw_name = result.data.get("raw_shop_name", "Amazon")
            self._open_shop_amazon_mapping_panel(raw_name)
            return

        # Allgemeine Shop-Zuordnung rechts einblenden wenn noetig
        elif result.source == LookupSource.NORMALIZATION_DIALOG and result.needs_confirm:
            raw_name = result.data.get("raw_shop_name", "")
            # Spezifische Amazon-Shops (z.B. "Amazon DE") nicht nochmal normalisieren
            if raw_name and raw_name.lower() not in SPECIFIC_AMAZON_VALUES:
                self._open_normalization_mapping_panel(
                    field_key="shop_name",
                    field_label="Shop-Name",
                    category="shops",
                    raw_value=raw_name,
                )
                return

        # Normalisierten Wert eintragen wenn vorhanden
        elif result.normalized_value and result.found:
            current_text = str(self.inputs.get("shop_name", QLineEdit()).text()).strip()
            if result.normalized_value != current_text:
                self.inputs["shop_name"].setText(result.normalized_value)
            self.current_gemini_data["shop_name"] = str(self.inputs["shop_name"].text() or "").strip()
            if self._active_mapping_context and not self._current_mapping_state():
                self._clear_embedded_mapping_request()
        elif self._active_mapping_context and not self._current_mapping_state():
            self._clear_embedded_mapping_request()

    def _handle_zahlungsart_lookup_result(self, result):
        """Verarbeitet Zahlungsart-Lookup-Ergebnis."""
        if result.source == LookupSource.NORMALIZATION_DIALOG and result.needs_confirm:
            raw_value = result.data.get("raw_zahlungsart", "")
            if raw_value:
                self._open_normalization_mapping_panel(
                    field_key="zahlungsart",
                    field_label="Zahlungsart",
                    category="zahlungsarten",
                    raw_value=raw_value,
                )
                return

        elif result.normalized_value and result.found:
            current_text = str(self.inputs.get("zahlungsart", QLineEdit()).text()).strip()
            if result.normalized_value != current_text:
                self.inputs["zahlungsart"].setText(result.normalized_value)
            self.current_gemini_data["zahlungsart"] = str(self.inputs["zahlungsart"].text() or "").strip()
            if self._active_mapping_context and not self._current_mapping_state():
                self._clear_embedded_mapping_request()
        elif self._active_mapping_context and not self._current_mapping_state():
            self._clear_embedded_mapping_request()

    def _handle_bestellnummer_lookup_result(self, result):
        """Verarbeitet Bestellnummer-Lookup: OVERWRITE (gelb) wenn schon in DB.
        Bei OVERWRITE werden alle vorhandenen DB-Daten geladen und in der
        Split-View (InlineChangeFieldRow) angezeigt.
        """
        widget = self.inputs.get("bestellnummer")
        if not widget:
            return
        if result.state == FieldState.OVERWRITE:
            bestellnummer = str(widget.text() or "").strip()
            widget.setToolTip(
                f"Bestellnummer '{bestellnummer}' "
                f"ist bereits in der Datenbank gespeichert.\n"
                f"Beim Speichern wird der bestehende Eintrag aktualisiert."
            )
            try:
                payload = collect_einkauf_payload(self.einkauf_form_widget, self.einkauf_items_widget)
                review_result = refresh_einkauf_review_workflow(
                    self.einkauf_form_widget,
                    self.einkauf_items_widget,
                    self.settings_manager,
                    payload,
                    db=self._lookup_db,
                    hydrate_existing_order=True,
                    ean_callback=self.ean_service.find_best_local_ean_by_name,
                    payload_target=self.current_gemini_data,
                )
                self._lookup_db = review_result["db"]
                if review_result["status"] == "ok" and review_result["order_exists"]:
                    self.btn_save_db.setEnabled(True)
            except Exception as exc:
                log_exception(__name__, exc)
        else:
            widget.setToolTip("Neue Bestellnummer – noch nicht gespeichert.")
            # Review-Ansicht zuruecksetzen wenn Bestellnummer neu ist
            if hasattr(self, "einkauf_form_widget"):
                clear_einkauf_review_data(self.einkauf_form_widget, self.einkauf_items_widget)

    def _start_logo_search_from_context(self, context):
        """Slot fuer EinkaufHeadFormWidget.logoSearchRequested – delegiert an _start_logo_search()."""
        self._start_logo_search()

    # ── EAN-Lookup: Einkauf (EinkaufItemsTableWidget) ──────────────────

    def _on_einkauf_items_ean_lookup(self, context):
        """Signal-Handler: EAN-Suche aus EinkaufItemsTableWidget heraus (delegiert an shared workflow)."""
        ctx = dict(context or {})
        worker = create_ean_lookup_worker(
            parent_widget=self,
            settings_manager=self.settings_manager,
            context=ctx,
            current_worker=self.ean_lookup_worker,
            ean_button=self.einkauf_items_widget.btn_ean_lookup,
            on_finished_callback=self._on_einkauf_ean_finished,
            on_error_callback=self._on_einkauf_ean_error,
        )
        if worker is not None:
            self._pending_ean_lookup_context = ctx
            self.ean_lookup_worker = worker

    def _on_einkauf_ean_finished(self, payload):
        """Einkauf-Pfad: EAN-Ergebnis ueber shared workflow verarbeiten."""
        context = dict(self._pending_ean_lookup_context or {})
        self._pending_ean_lookup_context = None
        reset_ean_lookup_button(self.einkauf_items_widget.btn_ean_lookup)
        self.ean_lookup_worker = None

        def _write_ean(row, ean):
            self.einkauf_items_widget.set_ean_for_row(row, ean)

        handle_ean_lookup_result(
            parent_widget=self,
            payload=payload,
            context=context,
            ean_service=self.ean_service,
            on_ean_selected=_write_ean,
        )

    def _on_einkauf_ean_error(self, err_msg):
        """Einkauf-Pfad: EAN-Fehler anzeigen."""
        self._pending_ean_lookup_context = None
        reset_ean_lookup_button(self.einkauf_items_widget.btn_ean_lookup)
        self.ean_lookup_worker = None
        handle_ean_lookup_error(parent_widget=self, err_msg=err_msg)

    # ── EAN-Lookup: Verkauf (Legacy QTableWidget) ────────────────────

    def _lookup_ean_for_selected_row(self):
        """Verkauf-Modus: EAN-Suche ueber Legacy-Tabelle."""
        if self.ean_lookup_worker is not None and self.ean_lookup_worker.isRunning():
            CustomMsgBox.information(self, "EAN Suche", "Es laeuft bereits eine EAN-Suche im Hintergrund.")
            return

        row = self.table_waren.currentRow()
        if row < 0:
            CustomMsgBox.information(self, "EAN Suche", "Bitte zuerst eine Artikelzeile markieren.")
            return

        produkt_name = ""
        varianten_info = ""
        # Verkauf: Produkt(0), EAN(1)
        name_col = 0
        ean_col = 1

        if self.table_waren.item(row, name_col):
            produkt_name = self.table_waren.item(row, name_col).text().strip()

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
        self.ean_lookup_worker.result_signal.connect(self._on_verkauf_ean_finished)
        self.ean_lookup_worker.error_signal.connect(self._on_verkauf_ean_error)
        self.ean_lookup_worker.start()

    def _on_verkauf_ean_finished(self, payload):
        """Verkauf-Pfad: EAN-Ergebnis in Legacy-QTableWidget schreiben."""
        context = dict(self._pending_ean_lookup_context or {})
        self._pending_ean_lookup_context = None
        reset_ean_lookup_button(self.btn_ean_lookup)
        self.ean_lookup_worker = None

        candidates = payload.get("candidates", []) if isinstance(payload, dict) else []
        error_payload = payload.get("error", {}) if isinstance(payload, dict) else {}
        if not candidates:
            api_msg = ""
            if isinstance(error_payload, dict):
                api_msg = str(error_payload.get("user_message", "")).strip()
            if api_msg:
                CustomMsgBox.warning(
                    self, "EAN Suche",
                    "Lokal gab es keine Treffer und die API-Suche ist fehlgeschlagen:\n\n" + api_msg,
                )
            else:
                CustomMsgBox.information(
                    self, "Keine Treffer",
                    "Es wurden weder lokal noch ueber die API passende EAN-Vorschlaege gefunden.",
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

    def _on_verkauf_ean_error(self, err_msg):
        """Verkauf-Pfad: EAN-Fehler anzeigen."""
        self._pending_ean_lookup_context = None
        reset_ean_lookup_button(self.btn_ean_lookup)
        self.ean_lookup_worker = None
        handle_ean_lookup_error(parent_widget=self, err_msg=err_msg)

    # ── Produktbild-Suche: Einkauf (EinkaufItemsTableWidget) ────────────

    def _on_einkauf_items_image_search(self, context):
        """Signal-Handler: Produktbild-Suche aus EinkaufItemsTableWidget heraus."""
        row = int(context.get("source_row_index", -1) or -1)
        if row < 0:
            return
        self._start_product_image_search_for_widget(row, context)

    def _start_product_image_search_for_widget(self, row, context):
        """Startet Produktbild-Suche fuer eine Zeile im EinkaufItemsTableWidget."""
        if self.product_image_search_worker is not None and self.product_image_search_worker.isRunning():
            CustomMsgBox.information(self, "Bildsuche", "Es laeuft bereits eine Bildsuche im Hintergrund.")
            return

        produkt_name = str(context.get("produkt_name", "") or "").strip()
        varianten_info = str(context.get("varianten_info", "") or "").strip()
        ean = str(context.get("ean", "") or "").strip()

        if not produkt_name:
            CustomMsgBox.warning(self, "Bildsuche", "In der markierten Zeile fehlt der Produktname.")
            return

        self._pending_image_search_context = {
            "row": row,
            "produkt_name": produkt_name,
            "varianten_info": varianten_info,
            "ean": ean,
        }

        self.product_image_search_worker = ProductImageSearchWorker(
            self.settings_manager,
            produkt_name,
            varianten_info=varianten_info,
            ean=ean,
            limit=6,
        )
        self.product_image_search_worker.result_signal.connect(self._on_product_image_search_finished)
        self.product_image_search_worker.error_signal.connect(self._on_product_image_search_error)
        self.product_image_search_worker.start()

    # ── Shop-Logo-Suche (Google Custom Search API, 100 Gratis/Tag) ───

    def _start_logo_search(self):
        shop_name = str(self.inputs.get("shop_name", QLineEdit()).text()).strip()
        sender_domain = ""
        bestell_email = str(self.inputs.get("bestell_email", QLineEdit()).text()).strip()
        if "@" in bestell_email:
            sender_domain = bestell_email.split("@", 1)[1].strip().lower()

        worker = create_logo_search_worker(
            parent_widget=self,
            settings_manager=self.settings_manager,
            shop_name=shop_name,
            sender_domain=sender_domain,
            current_worker=self.logo_search_worker,
            logo_button=self.btn_logo_search,
            on_finished_callback=self._on_logo_search_finished,
            on_error_callback=self._on_logo_search_error,
        )
        if worker is not None:
            self.logo_search_worker = worker

    def _finish_logo_search_ui(self):
        reset_logo_search_button(self.btn_logo_search)
        self.logo_search_worker = None

    def _on_logo_search_finished(self, result_dict):
        shop_name = str(self.inputs.get("shop_name", QLineEdit()).text()).strip()
        self._finish_logo_search_ui()

        def _on_logo_saved(saved_shop_name):
            binding = self._lookup_bindings.get("shop_name")
            if binding:
                binding.trigger_lookup(saved_shop_name)

        handle_logo_search_result(
            parent_widget=self,
            settings_manager=self.settings_manager,
            result_dict=result_dict,
            shop_name=shop_name,
            source_module="modul_order_entry",
            form_widget=getattr(self, "einkauf_form_widget", None),
            on_complete=_on_logo_saved,
        )

    def _on_logo_search_error(self, err_msg):
        self._finish_logo_search_ui()
        handle_logo_search_error(parent_widget=self, err_msg=err_msg)

    # ── Produktbild-Suche (Einkauf-Pfad via EinkaufItemsTableWidget) ──

    def _on_product_image_search_finished(self, result_dict):
        context = dict(self._pending_image_search_context or {})
        self._pending_image_search_context = None
        self.product_image_search_worker = None

        candidates = result_dict.get("candidates", []) if isinstance(result_dict, dict) else []
        produkt_name = str(context.get("produkt_name", "")).strip()

        if not candidates:
            CustomMsgBox.information(self, "Bildsuche", "Es wurden keine passenden Produktbilder gefunden.")
            return

        selected = MediaGridSelectionDialog.choose(
            produkt_name or "Produktbild",
            candidates,
            search_type="Produktbild",
            parent=self,
        )
        if not selected:
            return

        image_url = str(selected.get("image_url", "") or selected.get("thumbnail_url", "") or "").strip()
        if not image_url:
            CustomMsgBox.warning(self, "Bildsuche", "Der gewaehlte Eintrag hat keine gueltige Bild-URL.")
            return

        try:
            db = DatabaseManager(self.settings_manager)
            media_service = MediaService(db)
            media_service.register_remote_product_image(
                product_name=produkt_name,
                image_url=image_url,
                ean=str(context.get("ean", "") or "").strip(),
                variant_text=str(context.get("varianten_info", "") or "").strip(),
                source_module="modul_order_entry",
                source_kind="manual_web_selection",
                source_ref=str(selected.get("source_page_url", "") or "").strip(),
            )
            # EinkaufItemsTableWidget: Tabelle neu zeichnen, damit Bild-Preview aktualisiert wird
            target_row = context.get("row", -1)
            if target_row >= 0:
                self.einkauf_items_widget.refresh_display(select_source_index=target_row)
            CustomMsgBox.information(self, "Bildsuche", f"Produktbild fuer '{produkt_name}' wurde gespeichert.")
        except Exception as exc:
            log_exception(__name__, exc)
            CustomMsgBox.warning(self, "Bildsuche", f"Das Produktbild konnte nicht gespeichert werden:\n{exc}")

    def _on_product_image_search_error(self, err_msg):
        self._pending_image_search_context = None
        self.product_image_search_worker = None
        msg = str(err_msg or "").strip() or "Unbekannter Fehler bei der Bildsuche."
        CustomMsgBox.warning(self, "Bildsuche", f"Die Bildsuche ist fehlgeschlagen:\n{msg}")

    # ── Live-Validierung & Summen-Kontrolle ─────────────────────────────

    def _on_waren_table_changed(self, item):
        """Wird bei jeder Aenderung in der Artikel-Tabelle aufgerufen."""
        self._refresh_einkauf_live_calculations()
        self._update_save_button_state()

    def _update_summen_banner(self):
        """Aktualisiert das Summen-Banner mit berechnetem Warenwert vs. KI-Gesamtpreis."""
        if self.scan_mode != "einkauf":
            self.summen_banner.setVisible(False)
            return
        self._refresh_einkauf_live_calculations()

    def _update_save_button_state(self):
        """Aktiviert/deaktiviert den Save-Button basierend auf Pflichtfeld-Validierung."""
        if self.scan_mode == "einkauf":
            self.btn_save_db.setEnabled(check_einkauf_save_ready(
                self.einkauf_form_widget,
                self.einkauf_items_widget,
                mark_fields=True,
                tooltip="Pflichtfeld: Bestellnummer muss ausgefuellt sein",
            ))
        else:
            self.btn_save_db.setEnabled(self._check_verkauf_save_ready())

    def _check_verkauf_save_ready(self):
        """Verkauf-Pfad (Legacy): Ticket-Name + mindestens 1 Zeile in der Tabelle."""
        ticket_widget = self.inputs.get("ticket_name")
        ticket_name = str(ticket_widget.text()).strip() if ticket_widget else ""
        return bool(ticket_name) and self.table_waren.rowCount() > 0

    # ── Zeilen-Management (+ Zeile / - Markierte) ──────────────────────

    def _add_table_row(self):
        """Fuegt eine leere Zeile am Ende der Artikel-Tabelle hinzu (nur Verkauf-Modus)."""
        row = self.table_waren.rowCount()
        self.table_waren.insertRow(row)

        self.table_waren.setItem(row, 0, QTableWidgetItem(""))
        self.table_waren.setItem(row, 1, QTableWidgetItem(""))
        self.table_waren.setItem(row, 2, QTableWidgetItem("1"))
        self.table_waren.setItem(row, 3, QTableWidgetItem("0.00"))
        self.table_waren.setItem(row, 4, QTableWidgetItem("0.00"))

        # Neue Zeile direkt zum Editieren auswaehlen
        self.table_waren.setCurrentCell(row, 0)
        self.table_waren.editItem(self.table_waren.item(row, 0))
        self._update_summen_banner()
        self._update_save_button_state()

    def _delete_selected_rows(self):
        """Loescht die markierten Zeilen aus der Artikel-Tabelle (mit Bestaetigung)."""
        selected_rows = sorted(set(idx.row() for idx in self.table_waren.selectedIndexes()), reverse=True)
        if not selected_rows:
            CustomMsgBox.information(self, "Loeschen", "Bitte zuerst eine Zeile in der Tabelle markieren.")
            return

        count = len(selected_rows)
        if count > 1:
            reply = CustomMsgBox.question(
                self, "Zeilen loeschen",
                f"{count} Zeilen wirklich loeschen?",
            )
            if reply != QMessageBox.StandardButton.Yes:
                return

        for row in selected_rows:
            self.table_waren.removeRow(row)
        self._update_summen_banner()
        self._update_save_button_state()

    def _reset_form(self):
        # Gemeinsamer Reset: Scan-Eingaben leeren
        self.drop_box.current_pixmap = None
        self.drop_box.current_source_path = None
        self.selected_document_path = None
        self.scan_temp_file_path = None
        self.drop_box.lbl_text.setPixmap(QPixmap())
        self.drop_box.lbl_text.setText("Drag & Drop hier\noder Strg+V / Datei-Auswahl (Bild/PDF)")
        self.txt_anweisung.clear()

        # Modus-spezifische Felder/Tabellen leeren
        if self.scan_mode == "einkauf":
            reset_einkauf_review_workflow(
                self.einkauf_form_widget,
                self.einkauf_items_widget,
                self.summen_banner,
            )
        else:
            self._reset_verkauf_state()

        # Gemeinsamer Reset: Zustand zuruecksetzen
        self.current_gemini_data = {}
        self.summen_banner.setVisible(False)
        self.btn_scan.setEnabled(False)
        self.btn_save_db.setEnabled(False)
        self.logo_search_worker = None
        self.product_image_search_worker = None
        self._pending_image_search_context = None
        self._clear_embedded_mapping_request()

    def _reset_verkauf_state(self):
        """Verkauf-Pfad (Legacy): QLineEdits + QTableWidget leeren."""
        for le in self.inputs.values():
            le.clear()
        self.table_waren.setRowCount(0)

    def _save_to_database(self):
        """Dispatcher: leitet je nach Modus an den passenden Save-Workflow weiter."""
        if self.scan_mode == "einkauf":
            self._save_einkauf()
        else:
            self._save_verkauf()

    # ── Einkauf-Save ─────────────────────────────────────────────────

    def _save_einkauf(self):
        """Einkauf-Pfad: Payload aus Widgets aufbauen, Save-Workflow, Reset."""
        try:
            def _apply_source(payload):
                has_scan = bool(payload.get("_scan_sources") or payload.get("_provider_meta"))
                payload["quelle"] = "modul1_scan" if has_scan else "modul1_manual"
                return payload

            result = prepare_and_save_einkauf_workflow(
                self,
                self.settings_manager,
                self.einkauf_form_widget,
                self.einkauf_items_widget,
                self.inputs,
                base_payload=self.current_gemini_data,
                payload_enricher=_apply_source,
            )
            self.current_gemini_data = result["payload"]
            if result["issues"]:
                CustomMsgBox.warning(self, "Validierung", result["issues"][0])
                return
            if result["status"] == "saved":
                self._reset_form()
        except Exception as e:
            log_exception(__name__, e)
            CustomMsgBox.critical(self, "Datenbank-Fehler", str(e))

    # ── Verkauf-Save (Legacy) ────────────────────────────────────────

    def _save_verkauf(self):
        """Verkauf-Pfad (Legacy): Payload aus QLineEdits/QTableWidget, Discord-Matching, Speichern."""
        # Kopfdaten aus Legacy-QLineEdits uebernehmen
        for db_key, line_edit in self.inputs.items():
            self.current_gemini_data[db_key] = line_edit.text()

        if not self.current_gemini_data.get("ticket_name", "").strip():
            CustomMsgBox.warning(self, "Fehler", "Ticket-Name fehlt!")
            return

        # Waren aus Legacy-QTableWidget auslesen + validieren
        rows = self.table_waren.rowCount()
        if rows == 0:
            CustomMsgBox.warning(self, "Validierung", "Keine Artikel vorhanden. Bitte mindestens eine Position hinzufuegen.")
            return
        if not any(
            self.table_waren.item(r, 0) and str(self.table_waren.item(r, 0).text()).strip()
            for r in range(rows)
        ):
            CustomMsgBox.warning(self, "Validierung", "Mindestens eine Position muss einen Produktnamen haben.")
            return

        waren_liste = []
        for r in range(rows):
            waren_liste.append({
                "produkt_name": self.table_waren.item(r, 0).text() if self.table_waren.item(r, 0) else "",
                "ean": self.table_waren.item(r, 1).text() if self.table_waren.item(r, 1) else "",
                "menge": self.table_waren.item(r, 2).text() if self.table_waren.item(r, 2) else "1",
                "vk_brutto": self.table_waren.item(r, 3).text() if self.table_waren.item(r, 3) else "0.00",
                "marge_gesamt": self.table_waren.item(r, 4).text() if self.table_waren.item(r, 4) else "0.00",
            })
        self.current_gemini_data["waren"] = waren_liste

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
                QMessageBox.StandardButton.Yes,
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
                        "marge_gesamt": match.get("marge_gesamt", 0.0),
                    })
                matched_items = []
                pending_units = forced_pending

            result = db.confirm_verkauf_discord(self.current_gemini_data, matched_items, pending_units)
            if result.get("pending_count", 0) > 0:
                CustomMsgBox.information(
                    self,
                    "Gespeichert",
                    f"Ticket gespeichert. {result.get('matched_count', 0)} Stück direkt verknüpft, {result.get('pending_count', 0)} Stück warten als 'ticket folgt'.",
                )
            else:
                CustomMsgBox.information(
                    self,
                    "Erfolg",
                    f"Ticket gespeichert und {result.get('matched_count', 0)} Stück direkt verknüpft.",
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
        self.table_db.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table_db.customContextMenuRequested.connect(self._on_db_context_menu)
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
            
            menge_col = columns.index("menge") if "menge" in columns else -1
            storno_menge_col = columns.index("storno_menge") if "storno_menge" in columns else -1

            for row, row_data in enumerate(data):
                is_storniert = False
                is_teilstorno = False
                if table_name == "waren_positionen" and menge_col >= 0 and storno_menge_col >= 0:
                    try:
                        menge_val = int(row_data[menge_col] or 0)
                        storno_val = int(row_data[storno_menge_col] or 0)
                        if menge_val > 0 and storno_val >= menge_val:
                            is_storniert = True
                        elif storno_val > 0:
                            is_teilstorno = True
                    except Exception:
                        pass

                for col, value in enumerate(row_data):
                    item = QTableWidgetItem(str(value) if value is not None else "")
                    # Die ID sollte nicht editierbar sein
                    if columns[col] == "id":
                        item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                    if is_storniert:
                        from PyQt6.QtGui import QFont, QBrush, QColor
                        font = item.font()
                        font.setStrikeOut(True)
                        item.setFont(font)
                        item.setForeground(QBrush(QColor("#f7768e")))
                    elif is_teilstorno:
                        from PyQt6.QtGui import QBrush, QColor
                        item.setForeground(QBrush(QColor("#e0af68")))
                    self.table_db.setItem(row, col, item)
            
            self.table_db.resizeColumnsToContents()
            cursor.close()
            conn.close()
            self._loading_db = False
        except Exception as e:
            log_exception(__name__, e)
            self._loading_db = False
            CustomMsgBox.critical(self, "Fehler", f"Konnte Tabelle {table_name} nicht laden:\n{e}")

    def _on_db_context_menu(self, pos):
        if self.combo_table.currentText() != "waren_positionen":
            return
        item = self.table_db.itemAt(pos)
        if item is None:
            return

        menu = QMenu(self)
        action_storno = menu.addAction("Position stornieren...")
        action = menu.exec(self.table_db.viewport().mapToGlobal(pos))

        if action == action_storno:
            self._storniere_position_fuer_zeile(item.row())

    def _storniere_position_fuer_zeile(self, row):
        columns = [self.table_db.horizontalHeaderItem(c).text() for c in range(self.table_db.columnCount())]
        try:
            position_id = int(self.table_db.item(row, columns.index("id")).text())
            menge = int(self.table_db.item(row, columns.index("menge")).text() or 1)
            storno_bisher = int(self.table_db.item(row, columns.index("storno_menge")).text() or 0) if "storno_menge" in columns else 0
        except (ValueError, AttributeError):
            CustomMsgBox.warning(self, "Storno", "Konnte Positionsdaten nicht lesen.")
            return

        offen = menge - storno_bisher
        if offen <= 0:
            CustomMsgBox.information(self, "Storno", "Diese Position ist bereits vollstaendig storniert.")
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("Position stornieren")
        layout = QVBoxLayout(dialog)

        lbl = QLabel(f"Wie viele Einheiten sollen storniert werden?\n(max: {offen} von {menge})")
        lbl.setWordWrap(True)
        layout.addWidget(lbl)

        spinbox = QSpinBox()
        spinbox.setMinimum(1)
        spinbox.setMaximum(offen)
        spinbox.setValue(offen)
        layout.addWidget(spinbox)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        if dialog.exec() != QDialog.DialogCode.Accepted:
            return

        neue_storno_menge = storno_bisher + spinbox.value()
        try:
            db = DatabaseManager(self.settings_manager)
            db.storniere_waren_position(position_id, neue_storno_menge)
            self._load_db_data()
        except Exception as exc:
            log_exception(__name__, exc)
            CustomMsgBox.critical(self, "Storno-Fehler", f"Stornierung fehlgeschlagen:\n{exc}")

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














