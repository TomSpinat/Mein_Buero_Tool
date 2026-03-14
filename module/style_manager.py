import os
from config import resource_path

from module.crash_logger import log_exception
class StyleManager:
    """
    Diese Klasse verwaltet das globale Styling (CSS/QSS) für das gesamte Bürokratie-Tool.
    Modernes, cleanes Cyberpunk/Dark GUI - inspiriert von aktuellen Web-Trends (siehe Vorgabe).
    """
    
    @staticmethod
    def get_global_stylesheet():
        """
        Gibt den kompletten CSS-String für alle Widgets zurück im modernen 'Dark/Neon'-Look.
        """
        return """
        /* =========================================
           MODERN DARK THEME (Sleek UI)
           ========================================= */

        /* Hauptfenster, Dialoge & Pop-Ups */
        QMainWindow, QDialog, QMessageBox, #CentralWidget {
            background-color: #1a1b26; /* Tiefes Dunkelblau/Grau */
        }

        QWidget {
            background-color: transparent;
            color: #E2E2E6;
            font-family: 'Segoe UI', 'Roboto', 'Inter', sans-serif;
            font-size: 14px;
        }

        /* Kopfleiste (TopBar) */
        QFrame#TopBar {
            background-color: #202130;
            border-bottom: 2px solid #2B2D42;
        }

        /* Panels (Inhaltsboxen) */
        QFrame.Panel, QFrame#Panel {
            background-color: #242535;
            border-radius: 16px;
            border: 1px solid #33354C;
        }

        /* Labels */
        QLabel {
            color: #E2E2E6;
            font-weight: 500;
        }

        QLabel#TitleLabel, QLabel.title {
            font-size: 22px;
            font-weight: bold;
            color: #00E4FF; /* Cyan Accent für Überschriften */
        }

        /* Standard Buttons */
        QPushButton, QPushButton.retro-btn {
            background-color: qlineargradient(x1: 0, y1: 0, x2: 1, y2: 0, stop: 0 #6e3dd1, stop: 1 #a561ff); /* Lila Gradient */
            color: #ffffff;
            border: none;
            border-radius: 20px; /* Pillen-Form (pill-shaped) */
            padding: 10px 24px;
            font-size: 14px;
            font-weight: bold;
            letter-spacing: 0.5px;
        }

        QPushButton:hover, QPushButton.retro-btn:hover {
            background-color: qlineargradient(x1: 0, y1: 0, x2: 1, y2: 0, stop: 0 #7b45e8, stop: 1 #b475ff);
        }

        QPushButton:pressed, QPushButton.retro-btn:pressed {
            background-color: #592ab0;
            padding-top: 12px;
            padding-bottom: 8px; /* leichter Drück-Effekt */
        }

        QPushButton:disabled {
            background-color: #31334A;
            color: #6A6C84;
        }

        /* Hervorgehobener Button (z.B. Scanner) */
        QPushButton#ScannerBtn {
            background-color: qlineargradient(x1: 0, y1: 0, x2: 1, y2: 0, stop: 0 #00c6ff, stop: 1 #0072ff); /* Cyan/Blau Gradient */
            font-size: 16px;
            letter-spacing: 1px;
            border-radius: 22px;
            padding: 12px 28px;
        }

        QPushButton#ScannerBtn:hover {
            background-color: qlineargradient(x1: 0, y1: 0, x2: 1, y2: 0, stop: 0 #15d4ff, stop: 1 #1783ff);
        }

        QPushButton#ScannerBtn:pressed {
            background-color: #0060d1;
            padding-top: 14px;
            padding-bottom: 10px;
        }

        /* Eingabefelder (LineEdits) / Textfelder */
        QLineEdit, QTextEdit, QPlainTextEdit {
            background-color: #171824;
            color: #ffffff;
            border: 1px solid #33354C;
            border-radius: 12px;
            padding: 10px 14px;
            font-size: 14px;
            selection-background-color: #6e3dd1;
        }

        QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus {
            border: 1px solid #a561ff; /* Lila Akzent bei Fokus */
            background-color: #1E1F2D;
        }

        /* Comboboxen (Dropdowns) */
        QComboBox {
            background-color: #171824;
            color: #ffffff;
            border: 1px solid #33354C;
            border-radius: 12px;
            padding: 10px 14px;
            selection-background-color: #6e3dd1;
        }
        
        QComboBox:focus {
            border: 1px solid #00E4FF;
        }

        QComboBox::drop-down {
            border: none;
            width: 30px;
        }
        
        QComboBox QAbstractItemView {
            background-color: #242535;
            color: #ffffff;
            selection-background-color: #6e3dd1;
            selection-color: #ffffff;
            border: 1px solid #a561ff;
            outline: none;
            border-radius: 8px;
        }

        /* Checkboxen */
        QCheckBox {
            color: #ffffff;
            spacing: 12px;
        }
        QCheckBox::indicator {
            width: 22px;
            height: 22px;
            border-radius: 6px;
            border: 1px solid #33354C;
            background-color: #171824;
        }
        QCheckBox::indicator:hover {
            border: 1px solid #6e3dd1;
        }
        QCheckBox::indicator:checked {
            background-color: #00E4FF;
            border: 1px solid #00E4FF;
        }

        /* Scrollbars */
        QScrollBar:vertical {
            border: none;
            background: #1a1b26;
            width: 8px;
            margin: 0px;
            border-radius: 4px;
        }
        QScrollBar::handle:vertical {
            background: #33354C;
            min-height: 30px;
            border-radius: 4px;
        }
        QScrollBar::handle:vertical:hover {
            background: #a561ff;
        }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
            height: 0px;
        }
        
        QScrollBar:horizontal {
            border: none;
            background: #1a1b26;
            height: 8px;
            margin: 0px;
            border-radius: 4px;
        }
        QScrollBar::handle:horizontal {
            background: #33354C;
            min-width: 30px;
            border-radius: 4px;
        }
        QScrollBar::handle:horizontal:hover {
            background: #a561ff;
        }
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
            width: 0px;
        }

        /* Tabellen und Listen */
        QTableView, QTableWidget, QListView, QListWidget {
            background-color: #171824;
            color: #E2E2E6;
            border: 1px solid #33354C;
            border-radius: 12px;
            gridline-color: #33354C;
            selection-background-color: #6e3dd1;
            selection-color: #ffffff;
        }
        QHeaderView::section {
            background-color: #242535;
            color: #A0A2B8;
            padding: 10px;
            border: none;
            border-right: 1px solid #33354C;
            border-bottom: 1px solid #33354C;
            font-weight: bold;
        }

        /* --- NEU: Sidebar / To-Do --- */
        #SidePanel {
            background-color: #1a1b26;
            border-left: 2px solid #2B2D42;
        }
        
        QFrame.todo-card {
            background-color: #242535;
            border: 1px solid #33354C;
            border-radius: 8px;
        }
        
        QFrame.todo-card:hover {
            border: 1px solid #6e3dd1;
            background-color: #2A2B3D;
        }

        /* --- NEU: Radio Buttons wie High-Contrast Buttons stylen --- */
        QRadioButton {
            background-color: #242535;
            border: 2px solid #33354C;
            border-radius: 12px;
            color: #E2E2E6;
            padding: 10px 15px;
            font-size: 14px;
            font-weight: bold;
        }
        
        QRadioButton::indicator {
            width: 0px;  /* Blendet den eigentlichen kleinen runden Punkt aus! */
            height: 0px;
        }
        
        QRadioButton:hover {
            background-color: #2F3146;
            border: 2px solid #6e3dd1;
        }
        
        QRadioButton:checked {
            background-color: rgba(110, 61, 209, 0.25);
            border: 2px solid #a561ff;
            color: #ffffff;
        }
        """
