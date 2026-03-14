from PyQt6.QtWidgets import QDialog, QVBoxLayout, QLabel, QPushButton, QButtonGroup, QGridLayout
from PyQt6.QtCore import Qt, QSize
from PyQt6.QtGui import QIcon
import os

from module.style_manager import StyleManager
from config import resource_path


from module.crash_logger import log_exception
class AmazonCountryDialog(QDialog):
    """
    Fragt den Amazon-Laendershop ab, wenn die KI nur "Amazon" liefern konnte.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Amazon Shop Auswahl")
        self.setModal(True)
        self.setMinimumSize(600, 300)
        self.selected_country = "Amazon DE"
        self.setObjectName("AmazonCountryDialog")

        style = """
        QDialog#AmazonCountryDialog { background-color: #1a1b26; }
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
        except Exception as e:
            log_exception(__name__, e)
            self.setStyleSheet(style)

        layout = QVBoxLayout(self)

        title_lbl = QLabel("Die KI hat eine Amazon-Bestellung erkannt.\nAus welchem Land wurde diese bestellt?")
        title_lbl.setStyleSheet("font-size: 18px; font-weight: bold; margin-bottom: 15px; color: #E2E2E6;")
        layout.addWidget(title_lbl)

        self.radio_group = QButtonGroup(self)
        self.radio_layout = QGridLayout()
        self.radio_layout.setSpacing(10)

        options = [
            ("Amazon DE", "de"),
            ("Amazon FR", "fr"),
            ("Amazon IT", "it"),
            ("Amazon ESP", "es"),
            ("Amazon UK", "gb"),
            ("Amazon US", "us"),
            ("Anderer Amazon Shop", None),
        ]

        row = 0
        col = 0
        for index, (text, flag_code) in enumerate(options):
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

        layout.addLayout(self.radio_layout)
        layout.addSpacing(15)

        btn_ok = QPushButton("Uebernehmen")
        btn_ok.setObjectName("ScannerBtn")
        btn_ok.setMinimumHeight(45)
        btn_ok.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_ok.clicked.connect(self.save)
        layout.addWidget(btn_ok)

    def save(self):
        selected_button = self.radio_group.checkedButton()
        if not selected_button:
            self.selected_country = "Amazon"
            self.accept()
            return

        raw_text = selected_button.text()
        if "Anderer" in raw_text:
            self.selected_country = "Amazon"
        else:
            self.selected_country = raw_text
        self.accept()
