from PyQt6.QtWidgets import QMessageBox, QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton
from PyQt6.QtGui import QPixmap
from PyQt6.QtCore import Qt
from module.style_manager import StyleManager
from config import resource_path

from module.crash_logger import log_exception
class CustomSuccessDialog(QDialog):
    def __init__(self, parent, title, text):
        super().__init__(parent)
        self.setWindowTitle(title)
        
        self.setStyleSheet(StyleManager.get_global_stylesheet() + """
            QDialog { background-color: #1a1b26; }
            QLabel#MessageText { font-size: 20px; font-weight: bold; color: #E2E2E6; }
            QPushButton { 
                background-color: #6e3dd1; 
                color: white; 
                border-radius: 8px; 
                padding: 10px 40px; 
                font-size: 16px; 
                font-weight: bold;
            }
            QPushButton:hover { background-color: #8c5cd6; }
        """)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(40, 40, 40, 40)
        layout.setSpacing(40)
        
        icon_label = QLabel()
        pixmap = QPixmap(resource_path("assets/icon_success.png"))
        if not pixmap.isNull():
            pixmap = pixmap.scaled(200, 200, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
            icon_label.setPixmap(pixmap)
            
        layout.addWidget(icon_label, alignment=Qt.AlignmentFlag.AlignTop)
        
        right_layout = QVBoxLayout()
        right_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        text_label = QLabel(text)
        text_label.setObjectName("MessageText")
        text_label.setWordWrap(True)
        text_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        ok_btn = QPushButton("OK")
        ok_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        ok_btn.clicked.connect(self.accept)
        
        right_layout.addStretch()
        right_layout.addWidget(text_label, alignment=Qt.AlignmentFlag.AlignCenter)
        right_layout.addSpacing(30)
        right_layout.addWidget(ok_btn, alignment=Qt.AlignmentFlag.AlignCenter)
        right_layout.addStretch()
        
        layout.addLayout(right_layout)
        self.resize(500, 250)

class CustomMsgBox:
    """
    Eine Wrapper-Klasse für QMessageBox, die sicherstellt, 
    dass der Stylesheet (Dark Theme) auch wirklich zwingend 
    auf native Windows-Dialoge angewendet wird, anstatt
    teilweise das OS-Design zu verwenden.
    """
    @staticmethod
    def _create_styled_box(parent, title, text, icon_or_pixmap, buttons, default_btn=QMessageBox.StandardButton.NoButton):
        msg = QMessageBox(parent)
        msg.setWindowTitle(title)
        msg.setText(text)
        
        if isinstance(icon_or_pixmap, QPixmap):
            msg.setIconPixmap(icon_or_pixmap)
        else:
            msg.setIcon(icon_or_pixmap)
            
        if buttons:
            msg.setStandardButtons(buttons)
        if default_btn != QMessageBox.StandardButton.NoButton:
            msg.setDefaultButton(default_btn)
            
        # Den globalen Style explizit auf diese eine Instanz nochmals zwingen
        style = StyleManager.get_global_stylesheet()
        
        # Ein kleiner Zusatz, falls Windows die Labels weiterhin im "black theme" hält
        # Wir zwingen die QDialog Box und das Label direkt nochmals um:
        style += """
        QMessageBox { background-color: #1a1b26; }
        QMessageBox QLabel { 
            color: #E2E2E6; 
            background: transparent; 
            font-size: 16px; 
            font-weight: bold;
            padding: 5px;
        }
        """
        msg.setStyleSheet(style)
        return msg

    @staticmethod
    def information(parent, title, text, buttons=QMessageBox.StandardButton.Ok, defaultButton=QMessageBox.StandardButton.NoButton):
        # Nutzt den vollständig anpassbaren, großen QDialog
        dialog = CustomSuccessDialog(parent, title, text)
        return dialog.exec()

    @staticmethod
    def warning(parent, title, text, buttons=QMessageBox.StandardButton.Ok, defaultButton=QMessageBox.StandardButton.NoButton):
        msg = CustomMsgBox._create_styled_box(parent, title, text, QMessageBox.Icon.Warning, buttons, defaultButton)
        return msg.exec()

    @staticmethod
    def critical(parent, title, text, buttons=QMessageBox.StandardButton.Ok, defaultButton=QMessageBox.StandardButton.NoButton):
        msg = CustomMsgBox._create_styled_box(parent, title, text, QMessageBox.Icon.Critical, buttons, defaultButton)
        return msg.exec()

    @staticmethod
    def question(parent, title, text, buttons=QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, defaultButton=QMessageBox.StandardButton.NoButton):
        msg = CustomMsgBox._create_styled_box(parent, title, text, QMessageBox.Icon.Question, buttons, defaultButton)
        return msg.exec()
