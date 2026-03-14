import sys
from PyQt6.QtWidgets import QApplication, QMessageBox, QWidget

app = QApplication(sys.argv)

def my_info(parent, title, text, buttons=QMessageBox.StandardButton.Ok, defaultButton=QMessageBox.StandardButton.NoButton):
    msg = QMessageBox(parent)
    msg.setWindowTitle(title)
    msg.setText(text)
    msg.setIcon(QMessageBox.Icon.Information)
    msg.setStandardButtons(buttons)
    # Style zwingen
    msg.setStyleSheet("QMessageBox { background-color: #1a1b26; } QLabel { color: white; }")
    return msg.exec()

# Monkeypatching
QMessageBox.information = my_info

w = QWidget()
QMessageBox.information(w, "Test Monkeypatch", "Ich bin blau!")

