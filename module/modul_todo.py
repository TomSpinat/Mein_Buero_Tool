"""
modul_todo.py
Erstellt das Sidebar-Widget für die To-Do-Liste, das permanent im Dashboard angezeigt wird.
"""

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QFrame, QScrollArea, 
    QMessageBox
)
from PyQt6.QtCore import Qt, pyqtSignal
from module.database_manager import DatabaseManager

from module.crash_logger import log_exception
class ToDoCard(QFrame):
    """Eine einzelne Aufgabe als Karteikarte"""
    
    # Signal, wenn die Karte geklickt wird. Wir senden die empfohlene "Action" mit.
    clicked = pyqtSignal(str)
    
    def __init__(self, data):
        super().__init__()
        self.data = data
        self.setProperty("class", "todo-card")
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(8)
        
        # Titel (Farbkodiert je nach Typ)
        lbl_title = QLabel(data.get("title", "Aufgabe"))
        title_color = "#a9b1d6" # Default (info)
        t_type = data.get("type", "info")
        if t_type == "warning":
            title_color = "#e0af68" # Orange
        elif t_type == "success":
            title_color = "#9ece6a" # Grün
        elif t_type == "danger":
            title_color = "#f7768e" # Rot
            
        lbl_title.setStyleSheet(f"font-size: 15px; font-weight: bold; color: {title_color}; border: none;")
        
        # Beschreibung (mit Zeilenumbruch)
        lbl_desc = QLabel(data.get("desc", ""))
        lbl_desc.setWordWrap(True)
        lbl_desc.setStyleSheet("font-size: 13px; color: #a9b1d6; border: none;")
        
        layout.addWidget(lbl_title)
        layout.addWidget(lbl_desc)

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit(self.data.get("action", ""))
        super().mousePressEvent(event)


class ToDoWidget(QWidget):
    """Das eigentliche Sidebar-Widget, welches die Todo-Karten beherbergt."""
    
    # Leitet das Klick-Signal ans Dashboard weiter
    action_requested = pyqtSignal(str)
    
    def __init__(self, settings_manager):
        super().__init__()
        self.settings = settings_manager
        self.db = DatabaseManager(self.settings)
        self.setObjectName("SidePanel")
        
        # Flexible Breite für Skalierung, statt fester 300px
        self.setMinimumWidth(250)
        self.setMaximumWidth(600)
        
        self._build_ui()
        self.refresh_todos()
        
    def _build_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Header (Top-Bereich des To-Do Widgets)
        header_frame = QFrame()
        header_frame.setStyleSheet("background-color: transparent; border-bottom: 1px solid #33354C;")
        header_layout = QVBoxLayout(header_frame)
        header_layout.setContentsMargins(20, 20, 20, 15)
        
        lbl_header = QLabel("🔥 Action-Items")
        lbl_header.setStyleSheet("font-size: 18px; font-weight: bold; color: #7aa2f7;")
        header_layout.addWidget(lbl_header)
        main_layout.addWidget(header_frame)
        
        # Scroll-Bereich für die Karten
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        scroll_area.setStyleSheet("background-color: transparent;")
        
        self.content_widget = QWidget()
        self.content_widget.setStyleSheet("background-color: transparent;")
        self.content_layout = QVBoxLayout(self.content_widget)
        self.content_layout.setContentsMargins(15, 15, 15, 15)
        self.content_layout.setSpacing(12)
        self.content_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        scroll_area.setWidget(self.content_widget)
        main_layout.addWidget(scroll_area)
        
    def refresh_todos(self):
        """Holt die frischen Daten aus der DB und baut das Layout neu auf."""
        # Altes Layout leeren
        for i in reversed(range(self.content_layout.count())): 
            widget = self.content_layout.itemAt(i).widget()
            if widget is not None:
                widget.setParent(None)
                
        # Neue holen
        items = self.db.get_todo_items()
        
        if not items:
            lbl_empty = QLabel("Nichts zu tun! Geh einen Kaffee trinken. ☕")
            lbl_empty.setWordWrap(True)
            lbl_empty.setStyleSheet("color: #565f89; font-size: 14px; text-align: center; margin-top: 20px;")
            lbl_empty.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.content_layout.addWidget(lbl_empty)
            return

        for item in items:
            card = ToDoCard(item)
            # Wenn Karte geklickt wird, geben wir das Signal weiter
            card.clicked.connect(self.action_requested.emit)
            self.content_layout.addWidget(card)
