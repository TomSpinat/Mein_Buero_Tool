import os

from PyQt6.QtCore import Qt, QSize, pyqtSignal
from PyQt6.QtGui import QIcon
from PyQt6.QtWidgets import QButtonGroup, QDialog, QGridLayout, QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget

from config import resource_path
from module.crash_logger import log_exception
from module.style_manager import StyleManager

AMAZON_OPTIONS = [
    ("Amazon DE", "de"),
    ("Amazon FR", "fr"),
    ("Amazon IT", "it"),
    ("Amazon ESP", "es"),
    ("Amazon UK", "gb"),
    ("Amazon US", "us"),
    ("Anderer Amazon Shop", None),
]
SPECIFIC_AMAZON_VALUES = {label.lower(): label for label, _flag in AMAZON_OPTIONS if label.startswith("Amazon ") and label != "Anderer Amazon Shop"}


def normalize_amazon_shop_value(raw_value):
    text = str(raw_value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in SPECIFIC_AMAZON_VALUES:
        return SPECIFIC_AMAZON_VALUES[lowered]
    for key, canonical in SPECIFIC_AMAZON_VALUES.items():
        if key == lowered:
            return canonical
    return text


def is_generic_amazon_shop(raw_value):
    text = str(raw_value or "").strip()
    lowered = text.lower()
    if "amazon" not in lowered:
        return False
    return lowered not in SPECIFIC_AMAZON_VALUES


class AmazonCountryPanel(QWidget):
    selection_confirmed = pyqtSignal(str)
    cancel_requested = pyqtSignal()

    def __init__(self, raw_value="Amazon", mode="dialog", parent=None):
        super().__init__(parent)
        self.mode = str(mode or "dialog").strip().lower()
        self.raw_value = str(raw_value or "Amazon").strip() or "Amazon"
        self.selected_country = "Amazon DE"
        self._build_ui()
        self.set_raw_value(self.raw_value)

    def _build_ui(self):
        style = """
        QLabel { color: #E2E2E6; font-size: 15px; }
        QPushButton[is_country_btn="true"] {
            background-color: #242535;
            border: 2px solid #33354C;
            border-radius: 12px;
            color: #E2E2E6;
            padding: 12px;
            text-align: left;
            font-size: 15px;
            font-weight: bold;
        }
        QPushButton[is_country_btn="true"]:hover {
            background-color: #2F3146;
            border: 2px solid #6e3dd1;
        }
        QPushButton[is_country_btn="true"]:checked {
            background-color: rgba(110, 61, 209, 0.25);
            border: 2px solid #a561ff;
            color: #ffffff;
        }
        """
        try:
            global_style = StyleManager.get_global_stylesheet()
            self.setStyleSheet(global_style + "\n" + style)
        except Exception as exc:
            log_exception(__name__, exc)
            self.setStyleSheet(style)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        self.title_lbl = QLabel("")
        self.title_lbl.setWordWrap(True)
        self.title_lbl.setStyleSheet("font-size: 18px; font-weight: bold; color: #E2E2E6;")
        layout.addWidget(self.title_lbl)

        self.context_lbl = QLabel("")
        self.context_lbl.setWordWrap(True)
        self.context_lbl.setStyleSheet("color: #565f89; font-size: 12px;")
        layout.addWidget(self.context_lbl)

        self.radio_group = QButtonGroup(self)
        self.radio_layout = QGridLayout()
        self.radio_layout.setSpacing(10)
        layout.addLayout(self.radio_layout)

        row = 0
        col = 0
        for index, (text, flag_code) in enumerate(AMAZON_OPTIONS):
            btn = QPushButton(text)
            btn.setCheckable(True)
            btn.setProperty("is_country_btn", True)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            if flag_code:
                icon_path = resource_path(f"assets/flags/{flag_code}.png")
                if os.path.exists(icon_path):
                    btn.setIcon(QIcon(icon_path))
                    btn.setIconSize(QSize(28, 20))
            self.radio_layout.addWidget(btn, row, col)
            self.radio_group.addButton(btn, index)
            if index == 0:
                btn.setChecked(True)
            col += 1
            if col >= 3:
                col = 0
                row += 1

        button_row = QHBoxLayout()
        button_row.addStretch()
        self.btn_secondary = QPushButton("Abbrechen")
        self.btn_secondary.clicked.connect(self._on_secondary_clicked)
        self.btn_secondary.setVisible(self.mode == "dialog")
        button_row.addWidget(self.btn_secondary)

        self.btn_primary = QPushButton("Uebernehmen")
        self.btn_primary.setObjectName("ScannerBtn")
        self.btn_primary.setMinimumHeight(42)
        self.btn_primary.clicked.connect(self._on_primary_clicked)
        button_row.addWidget(self.btn_primary)
        layout.addLayout(button_row)

    def set_raw_value(self, raw_value):
        self.raw_value = str(raw_value or "Amazon").strip() or "Amazon"
        self.title_lbl.setText(
            "Die KI hat eine Amazon-Bestellung erkannt.\nAus welchem Land wurde diese bestellt?"
        )
        if self.mode == "embedded":
            self.context_lbl.setText(
                f"Erkannter Rohwert: {self.raw_value}. Die Vorschau bleibt links weiter nutzbar."
            )
        else:
            self.context_lbl.setText(f"Erkannter Rohwert: {self.raw_value}")

    def current_selection(self):
        selected_button = self.radio_group.checkedButton()
        if not selected_button:
            return "Amazon"
        raw_text = selected_button.text().strip()
        if "Anderer" in raw_text:
            return "Amazon"
        return raw_text or "Amazon"

    def _on_primary_clicked(self):
        self.selected_country = self.current_selection()
        self.selection_confirmed.emit(self.selected_country)

    def _on_secondary_clicked(self):
        self.cancel_requested.emit()


class AmazonCountryDialog(QDialog):
    """
    Dialog-Wrapper fuer Modul 1. Der eigentliche Inhalt lebt in AmazonCountryPanel.
    """

    def __init__(self, parent=None, raw_value="Amazon"):
        super().__init__(parent)
        self.setWindowTitle("Amazon Shop Auswahl")
        self.setModal(True)
        self.setMinimumSize(640, 320)
        self.selected_country = "Amazon DE"
        self.setObjectName("AmazonCountryDialog")

        try:
            global_style = StyleManager.get_global_stylesheet()
            self.setStyleSheet(global_style + "\nQDialog#AmazonCountryDialog { background-color: #1a1b26; }")
        except Exception as exc:
            log_exception(__name__, exc)
            self.setStyleSheet("QDialog#AmazonCountryDialog { background-color: #1a1b26; }")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        self.panel = AmazonCountryPanel(raw_value=raw_value, mode="dialog", parent=self)
        self.panel.selection_confirmed.connect(self._on_selection_confirmed)
        self.panel.cancel_requested.connect(self.reject)
        layout.addWidget(self.panel)

    def _on_selection_confirmed(self, selected_country):
        self.selected_country = str(selected_country or "Amazon").strip() or "Amazon"
        self.accept()
