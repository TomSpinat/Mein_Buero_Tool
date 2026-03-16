import math
from PyQt6.QtWidgets import QWidget
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QPainter, QColor, QFont

from module.crash_logger import log_exception
class LoadingOverlay(QWidget):
    """
    Ein dunkles Overlay mit einem sich drehenden Ladekreis (Spinner),
    das den Benutzer blockiert und anzeigt, dass im Hintergrund (z.B. KI Analyse)
    gearbeitet wird.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, False) # Klicks blockieren
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.hide()
        
        self.angle = 0
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.rotate)
        self.text = "Bitte warten..."

    def rotate(self):
        self.angle = (self.angle + 30) % 360
        self.update()

    def start(self, text="Bitte warten..."):
        self.text = text
        if self.parent():
            self.resize(self.parent().size())
            self.move(0, 0)
        self.timer.start(50)
        self.show()
        self.raise_()  # raise_() erst NACH show() – vorher hat es keine Wirkung

    def stop(self):
        self.timer.stop()
        self.hide()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Hintergrund stark abdunkeln
        painter.fillRect(self.rect(), QColor(0, 0, 0, 180))
        
        center = self.rect().center()
        radius = 50
        
        # Rotierender Kreis aus 12 Punkten
        for i in range(12):
            point_angle = (i * 30 + self.angle) % 360
            rad = math.radians(point_angle)
            x = center.x() + int(math.cos(rad) * radius)
            y = center.y() + int(math.sin(rad) * radius) - 30
            
            # Farbe ausblenden basierend auf Position (Schweif-Effekt)
            alpha = max(20, 255 - (i * 20))
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor(110, 61, 209, alpha)) # Marken-Lila
            painter.drawEllipse(x - 8, y - 8, 16, 16)
            
        # Lade-Text unter dem Spinner
        painter.setPen(QColor("#E2E2E6"))
        font = QFont("Arial", 16, QFont.Weight.Bold)
        painter.setFont(font)
        
        text_rect = self.rect()
        text_rect.translate(0, 60)
        painter.drawText(text_rect, Qt.AlignmentFlag.AlignCenter, self.text)
        
        painter.end()
