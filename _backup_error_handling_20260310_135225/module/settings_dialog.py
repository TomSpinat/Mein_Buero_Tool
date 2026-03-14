"""
settings_dialog.py
Fenster fuer die allgemeinen Einstellungen.
Normale Werte liegen in settings.json, geheime Werte im Windows-Anmeldespeicher.
"""

from PyQt6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QVBoxLayout,
)
from PyQt6.QtCore import Qt

from config import SettingsManager
from module.background_tasks import BackgroundTask
from module.crash_logger import log_exception
from module.database_manager import DatabaseManager
from module.gemini_api import test_api_key
from module.secret_store import sanitize_text


def _run_settings_connection_checks(api_key, db_settings):
    api_ok = bool(test_api_key(api_key)) if api_key else False
    db_manager = DatabaseManager(db_settings)
    db_ok = bool(db_manager.test_connection())
    return {
        "api_ok": api_ok,
        "db_ok": db_ok,
    }


class SettingsDialog(QDialog):
    """Kleines Einstellungsfenster fuer allgemeine App-Werte und Secrets."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Einstellungen")
        self.setFixedSize(560, 560)

        self.settings_manager = SettingsManager()
        self._connection_test_task = None
        self._connection_test_token = 0

        self._create_widgets()
        self._setup_layout()
        self._load_current_settings()
        self._show_secret_warnings()

    def _create_widgets(self):
        self.lbl_secret_note = QLabel()
        self.lbl_secret_note.setWordWrap(True)

        self.lbl_api_key = QLabel("Gemini API Key:")
        self.entry_api_key = QLineEdit()
        self.entry_api_key.setEchoMode(QLineEdit.EchoMode.PasswordEchoOnEdit)
        self.entry_api_key.setPlaceholderText("Neuen API-Schluessel eingeben")
        self.btn_clear_api_key = QPushButton("API-Key loeschen")
        self.btn_clear_api_key.clicked.connect(self._delete_api_key)
        self.lbl_api_secret_state = QLabel()

        self.lbl_db_host = QLabel("MySQL Host:")
        self.entry_db_host = QLineEdit()
        self.entry_db_host.setPlaceholderText("z.B. 127.0.0.1")

        self.lbl_db_port = QLabel("MySQL Port:")
        self.entry_db_port = QLineEdit()
        self.entry_db_port.setPlaceholderText("z.B. 3306")

        self.lbl_db_user = QLabel("MySQL Benutzer:")
        self.entry_db_user = QLineEdit()
        self.entry_db_user.setPlaceholderText("z.B. root")

        self.lbl_db_pass = QLabel("MySQL Passwort:")
        self.entry_db_pass = QLineEdit()
        self.entry_db_pass.setEchoMode(QLineEdit.EchoMode.PasswordEchoOnEdit)
        self.entry_db_pass.setPlaceholderText("Neues Passwort eingeben")
        self.btn_clear_db_pass = QPushButton("DB-Passwort loeschen")
        self.btn_clear_db_pass.clicked.connect(self._delete_db_password)
        self.lbl_db_pass_state = QLabel()

        self.lbl_db_name = QLabel("Datenbank Name:")
        self.entry_db_name = QLineEdit()
        self.entry_db_name.setPlaceholderText("buchhaltung")

        self.lbl_api_status = QLabel("API Status: ungetestet")
        self.lbl_db_status = QLabel("DB Status: ungetestet")
        self.lbl_test_busy = QLabel("")
        self.lbl_test_busy.setStyleSheet("color: #7aa2f7; font-size: 12px;")
        self.lbl_test_busy.hide()

        self.progress_test = QProgressBar()
        self.progress_test.setVisible(False)
        self.progress_test.setTextVisible(False)
        self.progress_test.setFixedHeight(10)

        self.btn_test = QPushButton("Verbindungen testen")
        self.btn_test.clicked.connect(self.test_connections)

        self.btn_save = QPushButton("Speichern")
        self.btn_save.clicked.connect(self.save_settings)

        self.btn_cancel = QPushButton("Abbrechen")
        self.btn_cancel.clicked.connect(self.reject)

    def _setup_layout(self):
        main_layout = QVBoxLayout()
        main_layout.addWidget(self.lbl_secret_note)
        main_layout.addSpacing(8)

        main_layout.addWidget(self.lbl_api_key)
        api_row = QHBoxLayout()
        api_row.addWidget(self.entry_api_key)
        api_row.addWidget(self.btn_clear_api_key)
        main_layout.addLayout(api_row)
        main_layout.addWidget(self.lbl_api_secret_state)

        main_layout.addSpacing(10)

        row1 = QHBoxLayout()
        row1.addWidget(self.lbl_db_host)
        row1.addWidget(self.entry_db_host)
        row1.addWidget(self.lbl_db_port)
        row1.addWidget(self.entry_db_port)
        main_layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.addWidget(self.lbl_db_user)
        row2.addWidget(self.entry_db_user)
        row2.addWidget(self.lbl_db_name)
        row2.addWidget(self.entry_db_name)
        main_layout.addLayout(row2)

        main_layout.addWidget(self.lbl_db_pass)
        db_pass_row = QHBoxLayout()
        db_pass_row.addWidget(self.entry_db_pass)
        db_pass_row.addWidget(self.btn_clear_db_pass)
        main_layout.addLayout(db_pass_row)
        main_layout.addWidget(self.lbl_db_pass_state)

        main_layout.addSpacing(10)

        status_layout = QHBoxLayout()
        status_layout.addWidget(self.lbl_api_status)
        status_layout.addSpacing(20)
        status_layout.addWidget(self.lbl_db_status)
        status_layout.addStretch()
        main_layout.addLayout(status_layout)
        main_layout.addWidget(self.lbl_test_busy)
        main_layout.addWidget(self.progress_test)

        main_layout.addStretch()

        button_layout = QHBoxLayout()
        button_layout.setAlignment(Qt.AlignmentFlag.AlignRight)
        button_layout.addWidget(self.btn_test)
        button_layout.addWidget(self.btn_cancel)
        button_layout.addWidget(self.btn_save)
        main_layout.addLayout(button_layout)

        self.setLayout(main_layout)

    def _show_secret_warnings(self):
        warnings = self.settings_manager.consume_secret_warnings()
        if warnings:
            QMessageBox.warning(self, "Secret-Speicher", "\n\n".join(warnings))

    def _refresh_secret_states(self):
        store_ok = self.settings_manager.is_secret_store_available()
        if store_ok:
            self.lbl_secret_note.setText(
                "Geheime Werte werden getrennt im Windows-Anmeldespeicher abgelegt. "
                "Leere Felder bleiben beim Speichern unveraendert."
            )
        else:
            self.lbl_secret_note.setText(
                "Warnung: Der sichere Windows-Anmeldespeicher ist aktuell nicht verfuegbar. "
                "Neue Geheimwerte werden dann nicht dauerhaft gespeichert."
            )

        api_set = self.settings_manager.has_secret("gemini_api_key")
        db_set = self.settings_manager.has_secret("db_pass")

        self.lbl_api_secret_state.setText("Gemini API Key: gesetzt" if api_set else "Gemini API Key: nicht gesetzt")
        self.lbl_db_pass_state.setText("MySQL Passwort: gesetzt" if db_set else "MySQL Passwort: nicht gesetzt")

        self.entry_api_key.clear()
        self.entry_db_pass.clear()
        self.entry_api_key.setPlaceholderText(
            "Bereits gespeichert - nur fuellen, wenn du ersetzen willst" if api_set else "Neuen API-Schluessel eingeben"
        )
        self.entry_db_pass.setPlaceholderText(
            "Bereits gespeichert - nur fuellen, wenn du ersetzen willst" if db_set else "Neues Passwort eingeben"
        )

    def _load_current_settings(self):
        self.entry_db_host.setText(self.settings_manager.get("db_host", "127.0.0.1"))
        self.entry_db_port.setText(self.settings_manager.get("db_port", "3306"))
        self.entry_db_user.setText(self.settings_manager.get("db_user", "root"))
        self.entry_db_name.setText(self.settings_manager.get("db_name", "buchhaltung"))
        self._refresh_secret_states()

    def _delete_api_key(self):
        self.settings_manager.delete_secret("gemini_api_key")
        self._refresh_secret_states()
        QMessageBox.information(self, "Entfernt", "Der gespeicherte Gemini API Key wurde geloescht.")

    def _delete_db_password(self):
        self.settings_manager.delete_secret("db_pass")
        self._refresh_secret_states()
        QMessageBox.information(self, "Entfernt", "Das gespeicherte MySQL Passwort wurde geloescht.")

    def _set_test_busy(self, busy, text=""):
        self.btn_test.setEnabled(not busy)
        self.btn_save.setEnabled(not busy)
        self.btn_cancel.setEnabled(not busy)
        self.entry_api_key.setEnabled(not busy)
        self.entry_db_host.setEnabled(not busy)
        self.entry_db_port.setEnabled(not busy)
        self.entry_db_user.setEnabled(not busy)
        self.entry_db_pass.setEnabled(not busy)
        self.entry_db_name.setEnabled(not busy)
        self.btn_clear_api_key.setEnabled(not busy)
        self.btn_clear_db_pass.setEnabled(not busy)
        self.btn_test.setText("Teste..." if busy else "Verbindungen testen")
        self.progress_test.setVisible(busy)
        self.lbl_test_busy.setVisible(bool(text))
        self.lbl_test_busy.setText(text)
        if busy:
            self.progress_test.setRange(0, 0)
        else:
            self.progress_test.setRange(0, 1)
            self.progress_test.setValue(0)

    def test_connections(self):
        if self._connection_test_task is not None and self._connection_test_task.isRunning():
            return

        api_key = self.entry_api_key.text().strip() or self.settings_manager.get("gemini_api_key", "")
        temp_settings = {
            "db_host": self.entry_db_host.text().strip(),
            "db_port": self.entry_db_port.text().strip() or "3306",
            "db_user": self.entry_db_user.text().strip(),
            "db_pass": self.entry_db_pass.text().strip() or self.settings_manager.get("db_pass", ""),
            "db_name": self.entry_db_name.text().strip(),
        }

        self._connection_test_token += 1
        task_id = self._connection_test_token
        self.lbl_api_status.setText("API Status: teste...")
        self.lbl_db_status.setText("DB Status: teste...")
        self._set_test_busy(True, "Verbindungen werden im Hintergrund geprueft...")

        self._connection_test_task = BackgroundTask(
            _run_settings_connection_checks,
            api_key,
            temp_settings,
            task_id=task_id,
            parent=self,
        )
        self._connection_test_task.result_signal.connect(self._on_test_connections_result)
        self._connection_test_task.error_signal.connect(self._on_test_connections_error)
        self._connection_test_task.finished_signal.connect(self._on_test_connections_finished)
        self._connection_test_task.start()

    def _on_test_connections_result(self, task_id, result):
        if task_id != self._connection_test_token:
            return

        result = result if isinstance(result, dict) else {}
        self.lbl_api_status.setText("API Status: verbunden" if result.get("api_ok") else "API Status: Fehler")
        self.lbl_db_status.setText("DB Status: verbunden" if result.get("db_ok") else "DB Status: Fehler")

    def _on_test_connections_error(self, task_id, err_msg):
        if task_id != self._connection_test_token:
            return

        self.lbl_api_status.setText("API Status: Fehler")
        self.lbl_db_status.setText("DB Status: Fehler")
        QMessageBox.critical(self, "Test fehlgeschlagen", f"Die Verbindungspruefung ist fehlgeschlagen:\n{err_msg}")

    def _on_test_connections_finished(self, task_id):
        if task_id != self._connection_test_token:
            return
        self._connection_test_task = None
        self._set_test_busy(False)

    def save_settings(self):
        settings_dict = {
            "db_host": self.entry_db_host.text().strip(),
            "db_port": self.entry_db_port.text().strip() or "3306",
            "db_user": self.entry_db_user.text().strip(),
            "db_name": self.entry_db_name.text().strip(),
        }

        api_key = self.entry_api_key.text().strip()
        db_pass = self.entry_db_pass.text().strip()
        if api_key:
            settings_dict["gemini_api_key"] = api_key
        if db_pass:
            settings_dict["db_pass"] = db_pass

        try:
            self.settings_manager.save_settings(settings_dict)
            self._refresh_secret_states()
            self._show_secret_warnings()
            QMessageBox.information(self, "Erfolg", "Einstellungen wurden erfolgreich gespeichert.")
            self.accept()
        except Exception as exc:
            log_exception(__name__, exc)
            QMessageBox.critical(
                self,
                "Fehler",
                f"Beim Speichern trat ein Fehler auf:\n{sanitize_text(exc)}",
            )

    def closeEvent(self, event):
        if self._connection_test_task is not None and self._connection_test_task.isRunning():
            self._connection_test_task.cancel()
        super().closeEvent(event)
