import json
import os

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtWidgets import (
    QComboBox,
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from config import resource_path
from module.crash_logger import log_exception

MAPPING_FILE = resource_path("mapping.json")


def load_mapping():
    if not os.path.exists(MAPPING_FILE):
        return {"shops": {}, "zahlungsarten": {}}
    try:
        with open(MAPPING_FILE, "r", encoding="utf-8-sig") as file_handle:
            return json.load(file_handle)
    except Exception as exc:
        log_exception(__name__, exc)
        return {"shops": {}, "zahlungsarten": {}}


def save_mapping(mapping_data):
    try:
        with open(MAPPING_FILE, "w", encoding="utf-8") as file_handle:
            json.dump(mapping_data, file_handle, indent=4, ensure_ascii=False)
        return True
    except Exception as exc:
        log_exception(__name__, exc)
        return False


def resolve_known_mapping(category, raw_value):
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return ""

    mapping_data = load_mapping()
    known_mappings = mapping_data.get(category, {}) or {}
    if raw_text in known_mappings:
        return known_mappings[raw_text]

    lowered = raw_text.lower()
    for key, standard in known_mappings.items():
        if str(key or "").strip().lower() == lowered:
            return standard
    return None


def remember_mapping(category, raw_value, standard_value):
    raw_text = str(raw_value or "").strip()
    standard_text = str(standard_value or "").strip()
    if not raw_text or not standard_text:
        return False

    mapping_data = load_mapping()
    mapping_data.setdefault(category, {})
    mapping_data[category][raw_text] = standard_text
    return save_mapping(mapping_data)


class NormalizationPanel(QWidget):
    selection_confirmed = pyqtSignal(str)
    cancel_requested = pyqtSignal()

    def __init__(self, category="shops", unknown_value="", mode="dialog", parent=None):
        super().__init__(parent)
        self.category = "shops"
        self.unknown_value = ""
        self.mode = str(mode or "dialog").strip().lower()
        self.mapping_data = load_mapping()
        self._build_ui()
        self.set_context(category, unknown_value)

    def _build_ui(self):
        try:
            from module.style_manager import StyleManager

            global_style = StyleManager.get_global_stylesheet()
            self.setStyleSheet(global_style + "\nQLabel { color: #E2E2E6; }")
        except Exception:
            self.setStyleSheet("QLabel { color: #E2E2E6; }")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        self.info_label = QLabel("")
        self.info_label.setWordWrap(True)
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.info_label)

        self.context_label = QLabel("")
        self.context_label.setWordWrap(True)
        self.context_label.setStyleSheet("color: #565f89; font-size: 12px;")
        self.context_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.context_label)

        self.entry_new = QLineEdit()
        self.entry_new.setPlaceholderText("Neuen Standard eintragen...")
        self.entry_new.setStyleSheet("font-size: 15px; padding: 8px; font-weight: bold;")
        self.entry_new.textChanged.connect(self._on_entry_changed)
        layout.addWidget(self.entry_new)

        self.combo_standards = QComboBox()
        self.combo_standards.setStyleSheet("color: #a9b1d6; padding: 5px;")
        self.combo_standards.currentIndexChanged.connect(self._on_combo_changed)
        layout.addWidget(self.combo_standards)

        self.lbl_error = QLabel("")
        self.lbl_error.setWordWrap(True)
        self.lbl_error.setStyleSheet("color: #f7768e; font-size: 12px;")
        self.lbl_error.hide()
        layout.addWidget(self.lbl_error)

        button_row = QHBoxLayout()
        button_row.addStretch()

        self.btn_secondary = QPushButton("Abbrechen")
        self.btn_secondary.clicked.connect(self._on_secondary_clicked)
        button_row.addWidget(self.btn_secondary)

        self.btn_primary = QPushButton("Mapping speichern")
        self.btn_primary.setObjectName("ScannerBtn")
        self.btn_primary.setMinimumHeight(40)
        self.btn_primary.clicked.connect(self._on_primary_clicked)
        button_row.addWidget(self.btn_primary)

        layout.addLayout(button_row)
        self._apply_mode_labels()

    def _apply_mode_labels(self):
        if self.mode == "embedded":
            self.btn_secondary.setText("Rohwert uebernehmen")
        else:
            self.btn_secondary.setText("Abbrechen")

    def _category_label(self):
        return "Shop-Name" if self.category == "shops" else "Zahlungsart"

    def _refresh_info(self):
        cat_name = self._category_label()
        safe_unknown = self.unknown_value or "-"
        self.info_label.setText(
            f"Neuer Begriff fuer <b>{cat_name}</b> erkannt:<br><br>"
            f"<span style='color:#7aa2f7; font-size:18px; font-weight:bold;'>{safe_unknown}</span>"
        )
        if self.mode == "embedded":
            self.context_label.setText(
                "Du kannst den Rohwert direkt uebernehmen oder ihn einem Standard zuordnen. "
                "Die Mail- und PDF-Vorschau bleibt waehrenddessen links offen."
            )
        else:
            self.context_label.setText(
                "Speichere den Begriff als neuen Standard oder ordne ihn einem bestehenden Standard zu."
            )

    def _refresh_standards(self):
        self.mapping_data = load_mapping()
        standards = sorted(set((self.mapping_data.get(self.category, {}) or {}).values()))
        self.combo_standards.blockSignals(True)
        self.combo_standards.clear()
        self.combo_standards.addItem("--- Bestehenden Standard auswaehlen ---")
        self.combo_standards.addItems(standards)
        self.combo_standards.blockSignals(False)

    def set_context(self, category, unknown_value):
        self.category = str(category or "shops").strip() or "shops"
        self.unknown_value = str(unknown_value or "").strip()
        self.entry_new.blockSignals(True)
        self.entry_new.setText(self.unknown_value)
        self.entry_new.blockSignals(False)
        self._refresh_standards()
        self._set_error("")
        self._refresh_info()

    def _set_error(self, message):
        text = str(message or "").strip()
        self.lbl_error.setText(text)
        self.lbl_error.setVisible(bool(text))

    def _on_entry_changed(self):
        if self.entry_new.text().strip():
            self.combo_standards.blockSignals(True)
            self.combo_standards.setCurrentIndex(0)
            self.combo_standards.blockSignals(False)
        self._set_error("")

    def _on_combo_changed(self, index):
        if index > 0:
            self.entry_new.blockSignals(True)
            self.entry_new.clear()
            self.entry_new.blockSignals(False)
        self._set_error("")

    def save_selection(self):
        new_value = self.entry_new.text().strip()
        combo_value = self.combo_standards.currentText().strip() if self.combo_standards.currentIndex() > 0 else ""
        final_standard = new_value or combo_value
        if not final_standard:
            self._set_error("Bitte waehle einen Standard oder trage einen neuen ein.")
            return None

        if not remember_mapping(self.category, self.unknown_value, final_standard):
            self._set_error("Mapping konnte nicht gespeichert werden.")
            return None
        self._set_error("")
        return final_standard

    def _on_primary_clicked(self):
        final_standard = self.save_selection()
        if final_standard:
            self.selection_confirmed.emit(final_standard)

    def _on_secondary_clicked(self):
        if self.mode == "embedded":
            self._set_error("")
            self.selection_confirmed.emit(self.unknown_value)
        else:
            self.cancel_requested.emit()


class NormalizationDialog(QDialog):
    """
    Dialog-Wrapper fuer Modul 1. Der eigentliche Inhalt lebt in NormalizationPanel.
    """

    def __init__(self, category, unknown_value, parent=None):
        super().__init__(parent)
        self.category = category
        self.unknown_value = unknown_value
        self.selected_standard = None

        self.setWindowTitle("KI Mapping - Neuer Begriff erkannt")
        self.setModal(True)
        self.resize(520, 320)
        self.setObjectName("NormDialog")

        try:
            from module.style_manager import StyleManager

            global_style = StyleManager.get_global_stylesheet()
            self.setStyleSheet(global_style + "\nQDialog#NormDialog { background-color: #1a1b26; }")
        except Exception:
            self.setStyleSheet("QDialog#NormDialog { background-color: #1a1b26; }")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(25, 25, 25, 25)
        self.panel = NormalizationPanel(category, unknown_value, mode="dialog", parent=self)
        self.panel.selection_confirmed.connect(self._on_selection_confirmed)
        self.panel.cancel_requested.connect(self.reject)
        layout.addWidget(self.panel)

    def _on_selection_confirmed(self, final_value):
        self.selected_standard = str(final_value or "").strip()
        self.accept()


def normalize_value(category, raw_value, parent_widget=None):
    """
    Popup-Wrapper fuer den bisherigen Modul-1-Pfad.
    Bekannte Werte werden direkt aufgeloest, unbekannte ueber den Dialog.
    """
    raw_text = str(raw_value or "").strip()
    if not raw_text:
        return ""

    known_value = resolve_known_mapping(category, raw_text)
    if known_value is not None:
        return known_value

    dialog = NormalizationDialog(category, raw_text, parent_widget)
    if dialog.exec() == QDialog.DialogCode.Accepted:
        return dialog.selected_standard
    return raw_text
