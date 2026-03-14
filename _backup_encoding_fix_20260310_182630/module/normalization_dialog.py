import json
import os
from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
                             QPushButton, QComboBox, QLineEdit, QMessageBox)
from PyQt6.QtCore import Qt
from config import resource_path

# Pfad zur Mapping-Datei
from module.crash_logger import log_exception
MAPPING_FILE = resource_path("mapping.json")

def load_mapping():
    if not os.path.exists(MAPPING_FILE):
        return {"shops": {}, "zahlungsarten": {}}
    try:
        with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {"shops": {}, "zahlungsarten": {}}

def save_mapping(mapping_data):
    try:
        with open(MAPPING_FILE, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, indent=4)
        return True
    except Exception as e:
        log_exception(__name__, e)
        print(f"Fehler beim Speichern des Mappings: {e}")
        return False

class NormalizationDialog(QDialog):
    """
    Ein Dialog, der aufpoppt, wenn die KI einen unbekannten Shop oder eine unbekannte Zahlungsart liefert.
    Der Nutzer kann den Wert dann einem bestehenden Standard zuweisen oder als neuen Standard speichern.
    """
    def __init__(self, category, unknown_value, parent=None):
        super().__init__(parent)
        self.category = category # 'shops' oder 'zahlungsarten'
        self.unknown_value = unknown_value
        self.selected_standard = None
        self.mapping_data = load_mapping()
        
        self.setWindowTitle("KI Mapping - Neuer Begriff erkannt")
        self.setModal(True)
        self.resize(500, 320)
        self.setObjectName("NormDialog")
        
        try:
            from module.style_manager import StyleManager
            global_style = StyleManager.get_global_stylesheet()
            self.setStyleSheet(global_style + "\nQDialog#NormDialog { background-color: #1a1b26; } QLabel { color: #E2E2E6; }")
        except:
            self.setStyleSheet("QDialog#NormDialog { background-color: #1a1b26; } QLabel { color: #E2E2E6; }")
            
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(25, 25, 25, 25)
        layout.setSpacing(15)
        
        # Hinweis Label
        cat_name = "Shop-Name" if self.category == 'shops' else "Zahlungsart"
        info_lbl = QLabel(f"Neuer Begriff fÃ¼r <b>{cat_name}</b> erkannt:\n"
                          f"<h2 style='color:#7aa2f7; text-align:center; margin-top: 10px;'>{self.unknown_value}</h2>")
        info_lbl.setWordWrap(True)
        info_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(info_lbl)
        
        layout.addSpacing(10)
        
        # PrimÃ¤re Option: Als neuen Standard speichern
        lbl_new = QLabel("<b>PrimÃ¤re Auswahl:</b> Diesen Begriff als neuen Standard speichern")
        lbl_new.setStyleSheet("color: #9ece6a; font-size: 14px;")
        layout.addWidget(lbl_new)
        
        self.entry_new = QLineEdit()
        self.entry_new.setText(self.unknown_value)
        self.entry_new.setPlaceholderText("Neuen Standard eintragen...")
        self.entry_new.setStyleSheet("font-size: 16px; padding: 8px; font-weight: bold;")
        layout.addWidget(self.entry_new)
        
        layout.addSpacing(15)
        
        # SekundÃ¤re Option: Bestehenden Standard wÃ¤hlen
        lbl_old = QLabel("<i>Oder einem bestehenden Standard zuweisen:</i>")
        lbl_old.setStyleSheet("color: #565f89; font-size: 12px;")
        layout.addWidget(lbl_old)
        
        self.combo_standards = QComboBox()
        standards = list(set(self.mapping_data.get(self.category, {}).values()))
        standards.sort()
        self.combo_standards.addItem("--- Nicht Ã¤ndern, sondern obigen Text beibehalten ---")
        self.combo_standards.addItems(standards)
        self.combo_standards.setStyleSheet("color: #a9b1d6; padding: 5px;")
        layout.addWidget(self.combo_standards)
        
        # Wenn der User in das neue Feld tippt, setzen wir die Combo zurÃ¼ck
        self.entry_new.textChanged.connect(lambda: self.combo_standards.setCurrentIndex(0))
        # Wenn der User die Combo Ã¤ndert, leeren wir das neue Feld
        self.combo_standards.currentIndexChanged.connect(self._combo_changed)
        
        layout.addStretch()
        
        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.setAlignment(Qt.AlignmentFlag.AlignRight)
        
        btn_cancel = QPushButton("Abbrechen")
        btn_cancel.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_cancel.clicked.connect(self.reject)
        
        btn_save = QPushButton("Mapping speichern")
        btn_save.setObjectName("ScannerBtn") # Nutzen wir den Button-Style aus dem Theme
        btn_save.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_save.setMinimumHeight(40)
        btn_save.clicked.connect(self.save_selection)
        
        btn_layout.addWidget(btn_cancel)
        btn_layout.addWidget(btn_save)
        
        layout.addLayout(btn_layout)
        self.setLayout(layout)

    def _combo_changed(self, index):
        if index > 0:
            self.entry_new.clear()

    def save_selection(self):
        new_val = self.entry_new.text().strip()
        combo_val = self.combo_standards.currentText()
        
        final_standard = ""
        
        if new_val:
            final_standard = new_val
        elif self.combo_standards.currentIndex() > 0:
            final_standard = combo_val
            
        if not final_standard:
            QMessageBox.warning(self, "Eingabe fehlgeschlagen", "Bitte wÃ¤hle einen Standard oder trage einen neuen ein.")
            return
            
        # Mapping aktualisieren
        if self.category not in self.mapping_data:
            self.mapping_data[self.category] = {}
            
        self.mapping_data[self.category][self.unknown_value] = final_standard
        save_mapping(self.mapping_data)
        
        self.selected_standard = final_standard
        self.accept()

def normalize_value(category, raw_value, parent_widget=None):
    """
    Hilfsfunktion, die aufgerufen wird, um einen Roh-Wert (z.B. Shop-Namen) zu checken.
    Ist er im mapping.json, wird der Standard zurÃ¼ckgegeben.
    Ist er unbekannt, poppt der Dialog auf.
    
    :param category: 'shops' oder 'zahlungsarten'
    :param raw_value: Der von der KI extrahierte rohe String
    :param parent_widget: Optionales QWidget, Ã¼ber dem der Dialog zentriert wird
    :return: Der standardisierte Wert (oder der raw_value, wenn abgebrochen wird)
    """
    if not raw_value:
        return ""
        
    mapping_data = load_mapping()
    known_mappings = mapping_data.get(category, {})
    
    # Check ob exakt bekannt
    if raw_value in known_mappings:
        return known_mappings[raw_value]
        
    # Check Case-Insensitive
    for key, standard in known_mappings.items():
        if key.lower() == raw_value.lower():
            return standard
            
    # Unbekannt -> Schranke Ã¶ffnen
    dialog = NormalizationDialog(category, raw_value, parent_widget)
    if dialog.exec() == QDialog.DialogCode.Accepted:
        return dialog.selected_standard
    
    # Falls abgebrochen, geben wir un-normalisiert zurÃ¼ck
    return raw_value
