import sys
import time
from PyQt6.QtWidgets import QApplication, QMessageBox
import os

app = QApplication(sys.argv)
app.setStyleSheet("""
    QMessageBox { background-color: #1a1b26; }
    QMessageBox QLabel { color: #E2E2E6; background: transparent; }
    #CustomMsgBox { background-color: #1a1b26; }
    QDialog { background-color: #1a1b26; }
    QWidget { background-color: #1a1b26; color: white; }
""")

# Wir versuchen es manuell
msg = QMessageBox()
msg.setObjectName("CustomMsgBox")
msg.setWindowTitle("Test")
msg.setText("Funktioniert das Blau?")
msg.setStandardButtons(QMessageBox.StandardButton.Ok)

# Anstatt exec blockierend, zeigen wir es und machen Screenshot
msg.show()

QApplication.processEvents()

# Mache nach 500ms einen Screenshot
def take_ss():
    pixmap = msg.grab()
    pixmap.save("C:/Users/timth/Desktop/Mein_Buero_Tool/test_msg.png")
    QApplication.quit()

from PyQt6.QtCore import QTimer
QTimer.singleShot(500, take_ss)

app.exec()
