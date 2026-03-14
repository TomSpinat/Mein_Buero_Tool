"""
modul_mail_scraper.py
Ein Modul zum Auslesen von E-Mails via IMAP (z.B. IONOS / 1und1).
Es sucht nach Bestell- und Zahlungsbestaetigungen, extrahiert die Anhaenge
oder wichtigen Textinhalte und leitet sie an die Gemini-Erkennung weiter.
"""

from PyQt6.QtWidgets import (
  QWidget, QVBoxLayout, QHBoxLayout, QLabel,
  QPushButton, QListWidget, QListWidgetItem, QFrame,
  QMessageBox, QProgressBar, QApplication, QDialog,
  QTableWidget, QTableWidgetItem, QHeaderView, QCheckBox,
  QComboBox, QLineEdit, QSizePolicy, QFormLayout, QTabWidget
)
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWebEngineCore import QWebEngineSettings
try:
  from PyQt6.QtPdf import QPdfDocument
  from PyQt6.QtPdfWidgets import QPdfView
  QT_PDF_AVAILABLE = True
except Exception:
  QPdfDocument = None
  QPdfView = None
  QT_PDF_AVAILABLE = False
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QUrl, QTimer, QSize
from PyQt6.QtGui import QIcon, QPixmap
import base64
import mimetypes
import imaplib
import email
from email.header import decode_header
import getpass
import time
import re
import urllib.request
import os
import hashlib
import json
import tempfile
import subprocess
import sys

from module.background_tasks import BackgroundTask
from module.gemini_api import process_receipt_with_gemini
from module.einkauf_pipeline import EinkaufPipeline
from module.normalization_dialog import NormalizationPanel
from module.amazon_country_dialog import AmazonCountryPanel
from module.ean_service import EanService
from module.ean_lookup_dialog import EanLookupDialog
from module.ean_search_worker import EanLookupWorker
from module.mail_screenshot_renderer import MailScreenshotRenderJob
from module.scan_input_preprocessing import prepare_mail_scan

from module.crash_logger import (
  AppError,
  classify_gemini_error,
  log_classified_error,
  log_exception,
)
from module.secret_store import sanitize_text
from module.safe_mail_renderer import SafeMailRenderer


class AddAccountDialog(QDialog):
  def __init__(self, parent=None):
    super().__init__(parent)
    self.setWindowTitle("E-Mail Konto hinzufuegen")
    self.setFixedSize(400, 300)
    self.setStyleSheet("background-color: #1a1b26; color: #a9b1d6;")

    layout = QVBoxLayout(self)

    self.entry_name = QLineEdit()
    self.entry_name.setPlaceholderText("Name (z.B. Haupt-Account)")
    layout.addWidget(QLabel("Konto-Name:"))
    layout.addWidget(self.entry_name)

    self.entry_host = QLineEdit()
    self.entry_host.setPlaceholderText("z.B. imap.ionos.de")
    layout.addWidget(QLabel("IMAP Server:"))
    layout.addWidget(self.entry_host)

    self.entry_port = QLineEdit()
    self.entry_port.setText("993")
    layout.addWidget(QLabel("IMAP Port:"))
    layout.addWidget(self.entry_port)

    self.entry_user = QLineEdit()
    self.entry_user.setPlaceholderText("E-Mail Adresse")
    layout.addWidget(QLabel("Benutzername / E-Mail:"))
    layout.addWidget(self.entry_user)

    self.entry_pwd = QLineEdit()
    self.entry_pwd.setEchoMode(QLineEdit.EchoMode.Password)
    self.entry_pwd.setPlaceholderText("Passwort")
    layout.addWidget(QLabel("Passwort:"))
    layout.addWidget(self.entry_pwd)

    btn_layout = QHBoxLayout()
    btn_save = QPushButton("Speichern")
    btn_save.clicked.connect(self.accept)
    btn_cancel = QPushButton("Abbrechen")
    btn_cancel.clicked.connect(self.reject)
    btn_layout.addWidget(btn_cancel)
    btn_layout.addWidget(btn_save)
    layout.addLayout(btn_layout)

  def get_data(self):
    return {
      "name": self.entry_name.text().strip(),
      "host": self.entry_host.text().strip(),
      "port": self.entry_port.text().strip() or "993",
      "user": self.entry_user.text().strip(),
      "pwd": self.entry_pwd.text().strip(),
      "last_mail_uid": 0,
    }


def _run_imap_connection_check(host, port, user, pwd, last_uid):
  mail = None
  try:
    mail = imaplib.IMAP4_SSL(host, int(port))
    mail.login(user, pwd)
    status, messages = mail.select("INBOX", readonly=True)

    total_mails_str = messages[0].decode("utf-8") if messages and messages[0] else "0"
    new_mail_count = 0
    if status == "OK":
      status_search, search_data = mail.uid('SEARCH', 'ALL')
      if status_search == "OK" and search_data and search_data[0]:
        mail_ids = search_data[0].split()
        new_mail_count = sum(1 for uid in mail_ids if int(uid) > int(last_uid or 0))

    return {
      "ok": status == "OK",
      "total_mails": total_mails_str,
      "new_mail_count": new_mail_count,
    }
  finally:
    if mail is not None:
      try:
        mail.logout()
      except Exception:
        pass


class MailScraperApp(QWidget):
  """
  UI und Steuerlogik fuer den E-Mail Postfach Scraper.
  """
  def __init__(self, settings_manager, parent=None):
    super().__init__(parent)
    self.settings_manager = settings_manager
    self._open_info_boxes = []
    self._connect_task = None
    self._connect_task_id = 0
    self._connect_task_origin = "manual"
    self._render_job = None
    self._active_scan_session_id = 0
    self.scraper_thread = None
    self.gemini_thread = None

    self.main_layout = QVBoxLayout(self)
    self.main_layout.setAlignment(Qt.AlignmentFlag.AlignTop)

    self._migrate_old_settings()
    self._build_ui()
    self._load_accounts()
    self._show_secret_warnings()
    QTimer.singleShot(0, self._auto_connect_if_possible)

  def _migrate_old_settings(self):
    accounts = self.settings_manager.get("mail_accounts", [])
    if not accounts:
      old_user = self.settings_manager.get("email_user", "")
      if old_user:
        migrated_acct = {
          "name": "Standard Account",
          "host": self.settings_manager.get("imap_server", "imap.ionos.de"),
          "port": self.settings_manager.get("imap_port", "993"),
          "user": old_user,
          "pwd": self.settings_manager.get("email_password", ""),
          "last_mail_uid": int(self.settings_manager.get("last_mail_uid", 0)),
        }
        accounts.append(migrated_acct)
        self.settings_manager.save_setting("mail_accounts", accounts)
        self.settings_manager.delete_secret("email_password")
        self.settings_manager.delete_secret("imap_pass")

  def _build_ui(self):
    lbl_title = QLabel("Automatische Beleg-Erfassung durchs E-Mail-Postfach")
    lbl_title.setStyleSheet("font-size: 22px; font-weight: bold; color: #00E4FF; margin-bottom: 5px;")
    self.main_layout.addWidget(lbl_title)

    lbl_safe = QLabel("Safe-Mode: Nur Lesezugriff.")
    lbl_safe.setStyleSheet("color: #4CAF50; font-size: 13px; font-weight: bold; margin-bottom: 10px;")
    self.main_layout.addWidget(lbl_safe)

    acc_layout = QHBoxLayout()
    self.account_combo = QComboBox()
    self.account_combo.setStyleSheet("background-color: #24283b; color: #a9b1d6; padding: 5px;")
    self.account_combo.setMinimumHeight(40)
    self.account_combo.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
    acc_layout.addWidget(self.account_combo)

    self.btn_add_account = QPushButton("+")
    self.btn_add_account.setFixedSize(40, 40)
    self.btn_add_account.setStyleSheet("font-size: 20px; font-weight: bold;")
    self.btn_add_account.clicked.connect(self._on_add_account)
    acc_layout.addWidget(self.btn_add_account)

    self.main_layout.addLayout(acc_layout)

    control_layout = QHBoxLayout()

    self.btn_connect = QPushButton("Mit Server verbinden")
    self.btn_connect.setMinimumHeight(45)
    self.btn_connect.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_connect.clicked.connect(self._test_connection)
    control_layout.addWidget(self.btn_connect)

    self.combo_limit = QComboBox()
    self.combo_limit.setMinimumHeight(45)
    self.combo_limit.setCursor(Qt.CursorShape.PointingHandCursor)
    self.combo_limit.addItems(["Alle seit letztem Scan", "Letzte 5 Mails", "Letzte 10 Mails", "Letzte 15 Mails", "Letzte 20 Mails", "Letzte 50 Mails", "Alle Mails"])
    self.combo_limit.setStyleSheet("background-color: #24283b; color: #a9b1d6; font-weight: bold; padding: 5px;")
    control_layout.addWidget(self.combo_limit)

    self.btn_scan = QPushButton("Postfach nach Belegen scannen")
    self.btn_scan.setObjectName("ScannerBtn")
    self.btn_scan.setMinimumHeight(45)
    self.btn_scan.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_scan.setEnabled(False)
    self.btn_scan.clicked.connect(self._start_scan)
    control_layout.addWidget(self.btn_scan)

    self.main_layout.addLayout(control_layout)

    self.main_layout.addSpacing(20)
    lbl_log = QLabel("<b>Aktivitaetsprotokoll:</b>")
    self.main_layout.addWidget(lbl_log)

    self.list_log = QListWidget()
    self.list_log.setStyleSheet("background-color: #171824; border: 1px solid #33354C; border-radius: 8px; padding: 5px;")
    self.main_layout.addWidget(self.list_log)

    self.progress_bar = QProgressBar()
    self.progress_bar.setVisible(False)
    self.main_layout.addWidget(self.progress_bar)

    self.lbl_busy_state = QLabel("")
    self.lbl_busy_state.setStyleSheet("color: #7aa2f7; font-size: 12px; padding-top: 4px;")
    self.lbl_busy_state.setVisible(False)
    self.main_layout.addWidget(self.lbl_busy_state)

    self._log("Warte auf Verbindung...")

  def _log(self, message):
    item = QListWidgetItem(str(message))
    self.list_log.addItem(item)
    self.list_log.scrollToBottom()

  def _set_busy_hint(self, text=""):
    text = str(text or "").strip()
    self.lbl_busy_state.setText(text)
    self.lbl_busy_state.setVisible(bool(text))

  def _show_non_blocking_info(self, title, text):
    parent = self.window() if self.window() else self
    msg = QMessageBox(parent)
    msg.setIcon(QMessageBox.Icon.Information)
    msg.setWindowTitle(title)
    msg.setText(text)
    msg.setStandardButtons(QMessageBox.StandardButton.Ok)
    msg.setWindowModality(Qt.WindowModality.NonModal)
    msg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)

    def _cleanup(_result, dlg=msg):
      if dlg in self._open_info_boxes:
        self._open_info_boxes.remove(dlg)

    msg.finished.connect(_cleanup)
    self._open_info_boxes.append(msg)
    msg.show()
    msg.raise_()
    msg.activateWindow()

  def _show_secret_warnings(self):
    warnings = []
    if hasattr(self.settings_manager, "consume_secret_warnings"):
      warnings = self.settings_manager.consume_secret_warnings()
    if warnings:
      QMessageBox.warning(self, "Secret-Speicher", "\n\n".join(warnings))

  def _load_accounts(self):
    self.account_combo.clear()
    accounts = self.settings_manager.get("mail_accounts", [])
    for acc in accounts:
      self.account_combo.addItem(acc.get("name", acc.get("user", "Unbekannt")))

  def _auto_connect_if_possible(self):
    if self._connect_task is not None and self._connect_task.isRunning():
      return
    if self.account_combo.count() <= 0:
      return
    host, port, user, pwd, _last_uid = self._get_imap_settings()
    if not host or not user or not pwd:
      return
    self._test_connection(auto=True)

  def _on_add_account(self):
    dialog = AddAccountDialog(self)
    if dialog.exec() == QDialog.DialogCode.Accepted:
      new_acc = dialog.get_data()
      if new_acc["user"] and new_acc["host"]:
        accounts = self.settings_manager.get("mail_accounts", [])
        accounts.append(new_acc)
        self.settings_manager.save_setting("mail_accounts", accounts)
        self._show_secret_warnings()
        self._load_accounts()
        self.account_combo.setCurrentIndex(len(accounts) - 1)
        QTimer.singleShot(0, self._auto_connect_if_possible)

  def _get_imap_settings(self):
    idx = self.account_combo.currentIndex()
    accounts = self.settings_manager.get("mail_accounts", [])
    if idx >= 0 and idx < len(accounts):
      acc = accounts[idx]
      return acc.get("host", ""), acc.get("port", "993"), acc.get("user", ""), acc.get("pwd", ""), int(acc.get("last_mail_uid", 0))
    return "", "993", "", "", 0

  def _test_connection(self, auto=False):
    if self._connect_task is not None and self._connect_task.isRunning():
      return

    host, port, user, pwd, last_uid = self._get_imap_settings()
    if not user or not pwd:
      if not auto:
        QMessageBox.warning(self, "Fehlende Daten", "Bitte waehle ein E-Mail Konto aus oder fuege eines ueber das '+' hinzu.")
      return

    self._log(f"Versuche Verbindung zu {host}:{port} fuer {user} herzustellen...")
    self._connect_task_id += 1
    task_id = self._connect_task_id
    self._connect_task_origin = "auto" if auto else "manual"
    self.btn_connect.setEnabled(False)
    self.btn_add_account.setEnabled(False)
    self.account_combo.setEnabled(False)
    self.btn_scan.setEnabled(False)
    self.btn_connect.setText("Verbinde...")
    self.progress_bar.setVisible(True)
    self.progress_bar.setRange(0, 0)
    self._set_busy_hint("Serververbindung wird geprueft...")

    self._connect_task = BackgroundTask(
      _run_imap_connection_check,
      host,
      port,
      user,
      pwd,
      last_uid,
      task_id=task_id,
      parent=self,
    )
    self._connect_task.result_signal.connect(self._on_connection_result)
    self._connect_task.error_signal.connect(self._on_connection_error)
    self._connect_task.finished_signal.connect(self._on_connection_finished)
    self._connect_task.start()

  def _on_connection_result(self, task_id, result):
    if task_id != self._connect_task_id:
      return

    result = result if isinstance(result, dict) else {}
    if result.get("ok"):
      total_mails_str = result.get("total_mails", "0")
      new_mail_count = int(result.get("new_mail_count", 0) or 0)
      self._log("Verbindung erfolgreich hergestellt!")
      self._log(f"Posteingang geoeffnet (sicherer Lese-Modus). {total_mails_str} Mails gefunden.")
      for i in range(self.combo_limit.count()):
        if "Alle seit letztem Scan" in self.combo_limit.itemText(i):
          self.combo_limit.setItemText(i, f"Alle seit letztem Scan ({new_mail_count} neu)")
          break
      self.btn_scan.setEnabled(True)
    else:
      self.btn_scan.setEnabled(False)
      if self._connect_task_origin == "manual":
        QMessageBox.critical(self, "IMAP Fehler", "Der Posteingang konnte nicht im sicheren Lese-Modus geoeffnet werden.")
      else:
        self._log("Auto-Verbindung fehlgeschlagen: Posteingang konnte nicht geoeffnet werden.")

  def _on_connection_error(self, task_id, err_msg):
    if task_id != self._connect_task_id:
      return
    self.btn_scan.setEnabled(False)
    self._log(f"Verbindungsfehler: {err_msg}")
    if self._connect_task_origin == "manual":
      QMessageBox.critical(self, "IMAP Fehler", f"Konnte keine Verbindung zum E-Mail Server herstellen:\n{err_msg}")

  def _on_connection_finished(self, task_id):
    if task_id != self._connect_task_id:
      return
    self._connect_task = None
    self.btn_connect.setEnabled(True)
    self.btn_add_account.setEnabled(True)
    self.account_combo.setEnabled(True)
    self.btn_connect.setText("Verbindung erneuern")
    self.progress_bar.setVisible(False)
    self._set_busy_hint("")

  def _start_scan(self):
    host, port, user, pwd, last_uid = self._get_imap_settings()
    api_key = self.settings_manager.get("gemini_api_key", "")
    account_idx = self.account_combo.currentIndex()

    if not user:
      QMessageBox.critical(self, "Fehler", "Kein E-Mail Konto ausgewaehlt!")
      return

    if not api_key:
      QMessageBox.critical(self, "Fehler", "Kein Gemini API Key gefunden! Bitte in den Einstellungen hinterlegen.")
      return

    self._active_scan_session_id += 1
    self._log("Starte Suchvorgang nach neuen E-Mail Belegen...")
    self.btn_scan.setEnabled(False)
    self.btn_connect.setEnabled(False)
    self.progress_bar.setVisible(True)
    self.progress_bar.setRange(0, 0)
    self._set_busy_hint("Postfach wird gelesen...")

    limit_text = self.combo_limit.currentText()
    if "Alle seit" in limit_text:
      mail_limit = "SINCE_LAST"
    elif "Alle" in limit_text:
      mail_limit = 99999
    else:
      try:
        mail_limit = int(''.join(filter(str.isdigit, limit_text)))
      except Exception:
        mail_limit = 5

    self.scraper_thread = MailScraperThread(host, port, user, pwd, mail_limit, last_uid, self.settings_manager, account_idx)
    self.scraper_thread.log_signal.connect(self._log)
    self.scraper_thread.progress_signal.connect(self._update_progress)
    self.scraper_thread.raw_signal.connect(self._on_raw_emails_fetched)
    self.scraper_thread.start()

  def _update_progress(self, current, total):
    self.progress_bar.setRange(0, total)
    self.progress_bar.setValue(current)
    if total:
      self._set_busy_hint(f"Postfach wird gelesen... ({current}/{total})")

  def _begin_screenshot_rendering(self, raw_emails):
    self._cleanup_render_job()
    self._log(f"Rendere {len(raw_emails)} E-Mail(s) als Screenshot fuer die KI...")
    self.progress_bar.setVisible(True)
    self.progress_bar.setRange(0, len(raw_emails))
    self.progress_bar.setValue(0)
    self._set_busy_hint("E-Mails werden fuer die KI vorbereitet...")

    self._render_job = MailScreenshotRenderJob(
      self._active_scan_session_id,
      raw_emails,
      self.settings_manager,
      parent=self,
    )
    self._render_job.progress_signal.connect(self._on_render_progress)
    self._render_job.finished_signal.connect(self._on_render_finished)
    self._render_job.error_signal.connect(self._on_render_error)
    self._render_job.start()

  def _cleanup_render_job(self):
    if self._render_job is not None:
      try:
        self._render_job.cancel()
      except Exception as exc:
        log_exception(__name__, exc)
      self._render_job = None

  def _on_render_progress(self, session_id, payload):
    if session_id != self._active_scan_session_id:
      return

    payload = payload if isinstance(payload, dict) else {}
    total = int(payload.get("total", 0) or 0)
    current = int(payload.get("current", 0) or 0)
    if total:
      self.progress_bar.setRange(0, total)
      self.progress_bar.setValue(current)
    status_text = str(payload.get("status_text", "") or "").strip()
    if status_text:
      self._set_busy_hint(status_text)
    log_message = str(payload.get("log_message", "") or "").strip()
    if log_message:
      self._log(log_message)

  def _on_render_finished(self, session_id, screenshot_paths):
    if session_id != self._active_scan_session_id:
      return

    self._render_job = None
    self.progress_bar.setRange(0, 0)
    self._set_busy_hint("Screenshots werden von der KI analysiert...")
    self._log(f"Sende {len(screenshot_paths)} Screenshot(s) an Gemini KI...")

    api_key = self.settings_manager.get("gemini_api_key", "")
    self.gemini_thread = GeminiEmailThread(api_key, screenshot_paths)
    self.gemini_thread.log_signal.connect(self._log)
    self.gemini_thread.result_signal.connect(self._on_scan_finished)
    self.gemini_thread.start()

  def _on_render_error(self, session_id, err_msg):
    if session_id != self._active_scan_session_id:
      return

    self._render_job = None
    self.progress_bar.setVisible(False)
    self.btn_scan.setEnabled(True)
    self.btn_connect.setEnabled(True)
    self._set_busy_hint("")
    QMessageBox.critical(self, "Screenshot-Fehler", f"Die E-Mail-Vorbereitung ist fehlgeschlagen:\n{err_msg}")

  def _on_raw_emails_fetched(self, raw_emails, highest_uid, account_idx):
    self.btn_connect.setEnabled(True)

    if highest_uid > 0 and account_idx >= 0:
      accounts = self.settings_manager.get("mail_accounts", [])
      if 0 <= account_idx < len(accounts):
        current_last = int(accounts[account_idx].get("last_mail_uid", 0))
        if highest_uid > current_last:
          accounts[account_idx]["last_mail_uid"] = highest_uid
          self.settings_manager.save_setting("mail_accounts", accounts)

    if not raw_emails:
      self.btn_scan.setEnabled(True)
      self.progress_bar.setVisible(False)
      self._set_busy_hint("")
      self._log("Suche beendet. Keine neuen/verwertbaren Belege gefunden.")
      QMessageBox.information(self, "Ergebnis", "Keine neuen Belege in den letzten E-Mails gefunden.")
      return

    self.btn_scan.setEnabled(False)
    self._begin_screenshot_rendering(raw_emails)

  def _on_scan_finished(self, extracted_data_list, highest_uid=-1, account_idx=-1):
    try:
      self.btn_scan.setEnabled(True)
      self.btn_connect.setEnabled(True)
      self.progress_bar.setVisible(False)
      self._set_busy_hint("")

      if not extracted_data_list:
        self._log("Suche beendet. Keine neuen/verwertbaren Belege gefunden.")
        QMessageBox.information(self, "Ergebnis", "Keine neuen Belege in den letzten E-Mails gefunden.")
        return

      self._log(f"Scanner fertig! {len(extracted_data_list)} Belege erfolgreich erkannt. Lade Ueberpruefungsfenster...")

      dialog = ScraperReviewWizardDialog(extracted_data_list, self.settings_manager, self)
      if dialog.exec() != QDialog.DialogCode.Accepted:
        self._log("Vorgang abgebrochen: Keine weiteren E-Mails gespeichert.")
        return

      summary = dialog.get_summary()
      saved = int(summary.get("saved", 0) or 0)
      skipped = int(summary.get("skipped", 0) or 0)
      discarded = int(summary.get("discarded", 0) or 0)
      renamed = int(summary.get("renamed", 0) or 0)

      if saved <= 0:
        self._log("Keine Belege gespeichert.")
        self._show_non_blocking_info("Ergebnis", "Es wurde kein Beleg gespeichert.")
        return

      info_lines = [f"Es wurden {saved} Beleg(e) gespeichert."]
      if skipped > 0:
        info_lines.append(f"Uebersprungen: {skipped}")
      if discarded > 0:
        info_lines.append(f"Verworfen: {discarded}")
      if renamed > 0:
        info_lines.append(f"Als neue Bestellung gespeichert (Duplikat): {renamed}")

      self._log("Vorgang erfolgreich abgeschlossen.")
      self._show_non_blocking_info("Ergebnis", "\n".join(info_lines))

    except Exception as fatal_err:
      app_error = fatal_err if isinstance(fatal_err, AppError) else classify_gemini_error(fatal_err, phase="mail_scraper_review")
      log_classified_error(
        f"{__name__}._on_scan_finished",
        app_error.category if isinstance(app_error, AppError) else "unknown",
        app_error.user_message if isinstance(app_error, AppError) else str(fatal_err),
        status_code=app_error.status_code if isinstance(app_error, AppError) else None,
        service=app_error.service if isinstance(app_error, AppError) else "mail_scraper",
        exc=fatal_err,
      )
      user_msg = app_error.user_message if isinstance(app_error, AppError) else "Unbekannter Fehler im Uebernehmen-Flow."
      self._log(f"Fehler im Uebernehmen-Flow: {user_msg}")
      QMessageBox.critical(
        self,
        "Kritischer Fehler",
        "Beim Uebernehmen ist ein Fehler aufgetreten:\n" + user_msg + "\n\nDetails stehen im zentralen Crash-Log.",
      )

  def closeEvent(self, event):
    if self._connect_task is not None and self._connect_task.isRunning():
      self._connect_task.cancel()
    self._cleanup_render_job()
    super().closeEvent(event)

class GeminiEmailThread(QThread):
  """Verarbeitet vorbereitete Mail-Quellen fuer Gemini, screenshot-first und tokenarm."""
  log_signal  = pyqtSignal(str)
  result_signal = pyqtSignal(list, int, int) # (results, -1, -1)

  def __init__(self, api_key, screenshot_paths):
    super().__init__()
    self.api_key = api_key
    self.screenshot_paths = screenshot_paths # list of (path_or_None, raw_dict)

  def run(self):
    extracted_data = []

    for i, (img_path, raw) in enumerate(self.screenshot_paths):
      sender = raw.get("sender", "")
      email_date_raw = raw.get("date", "")
      subject = raw.get("subject", "")
      body_html = raw.get("body_html", "")
      body_text = raw.get("body_text", "")
      prepared_scan = prepare_mail_scan(raw, screenshot_path=img_path, scan_mode="einkauf")
      self._log(f" ÃƒÂ°Ã…Â¸Ã‚Â¤Ã¢â‚¬â€œ [{i+1}/{len(self.screenshot_paths)}] KI analysiert: {str(subject)[:40]}...")

      max_retries = 3
      retry_count = 0

      while retry_count < max_retries:
        try:
          time.sleep(2)
          result = process_receipt_with_gemini(
            self.api_key,
            image_path=prepared_scan.gemini_image_path,
            custom_text=prepared_scan.gemini_custom_text,
            scan_mode=prepared_scan.scan_mode,
          )

          if result and isinstance(result, dict):
            tokens = result.pop("_token_count", "?")
            result["_original_email_html"] = body_html or body_text
            result["_original_email_text"] = body_text or ""
            result["_mail_cid_map"] = raw.get("cid_map", {}) or {}
            result["_email_sender"] = str(sender)
            result["_email_date"]  = str(email_date_raw)
            result["_scan_sources"] = [
              {
                "source_type": source.source_type,
                "original_name": source.original_name,
                "mime_type": source.mime_type,
              }
              for source in prepared_scan.sources
            ]
            result["_mail_tracking_links"] = list((prepared_scan.sources[0].extras.get("tracking_links", []) if prepared_scan.sources else []))
            primary_source = prepared_scan.primary_source
            primary_file_path = primary_source.file_path if primary_source else ""
            result["_mail_review_attachments"] = sorted(
              [
                {
                  "file_path": source.file_path,
                  "original_name": source.original_name,
                  "mime_type": source.mime_type,
                  "temp_file": bool(source.metadata.get("temp_file")),
                  "pdf_relevance_score": int(source.extras.get("pdf_relevance_score", 0) or 0),
                  "pdf_is_relevant": bool(source.extras.get("pdf_is_relevant", False)),
                  "pdf_relevance_reason": str(source.extras.get("pdf_relevance_reason", "") or ""),
                  "pdf_text_hint": str(source.extras.get("pdf_text_hint", "") or ""),
                  "used_for_ai": bool(primary_file_path and source.file_path == primary_file_path),
                }
                for source in prepared_scan.sources
                if source.source_type == "mail_attachment" and source.file_path
              ],
              key=lambda row: (
                1 if row.get("used_for_ai") else 0,
                1 if row.get("pdf_is_relevant") else 0,
                int(row.get("pdf_relevance_score", 0) or 0),
              ),
              reverse=True,
            )
            result["_mail_pdf_attachments"] = [
              dict(row)
              for row in result["_mail_review_attachments"]
              if str(row.get("mime_type", "")).lower() == "application/pdf"
              or str(row.get("original_name", "")).lower().endswith(".pdf")
              or str(row.get("file_path", "")).lower().endswith(".pdf")
            ]
            result["_primary_scan_source_type"] = primary_source.source_type if primary_source else ""
            result["_primary_scan_file_path"] = primary_file_path
            has_relevant_data = bool(
              result.get("shop_name")
              or result.get("gesamt_ekp_brutto")
              or result.get("bestellnummer")
              or result.get("tracking_nummer_einkauf")
              or (isinstance(result.get("waren"), list) and len(result.get("waren")) > 0)
            )

            if has_relevant_data:
              self.log_signal.emit(f" ÃƒÂ¢Ã…â€œÃ¢â‚¬Â¦ Erfolgreich extrahiert! (Tokens: {tokens})")
              extracted_data.append(result)
            else:
              self.log_signal.emit(f" (KI fand keine relevanten Daten. Tokens: {tokens})")

          break

        except Exception as e:
          app_error = e if isinstance(e, AppError) else classify_gemini_error(e, phase="mail_scraper_scan")
          log_classified_error(
            f"{__name__}.GeminiEmailThread.run",
            app_error.category if isinstance(app_error, AppError) else "unknown",
            app_error.user_message if isinstance(app_error, AppError) else str(e),
            status_code=app_error.status_code if isinstance(app_error, AppError) else None,
            service=app_error.service if isinstance(app_error, AppError) else "gemini",
            exc=e,
            extra={
              "mail_index": i + 1,
              "mail_total": len(self.screenshot_paths),
              "subject": str(subject or "")[:120],
              "sender": str(sender or "")[:120],
              "source_types": [source.source_type for source in prepared_scan.sources],
            },
          )
          user_msg = app_error.user_message if isinstance(app_error, AppError) else str(e)
          retryable = bool(isinstance(app_error, AppError) and app_error.retryable)
          if retryable and retry_count < (max_retries - 1):
            retry_count += 1
            wait_s = 60 if app_error.category == "rate_limit" else 15
            self.log_signal.emit(f"  KI temporaer nicht verfuegbar: {user_msg}")
            self.log_signal.emit(f"  Warte {wait_s}s und versuche erneut ({retry_count+1}/{max_retries})...")
            for t in range(wait_s, 0, -1):
              if t % 10 == 0 or t <= 5:
                self.log_signal.emit(f" ... {t}s verbleiben.")
              time.sleep(1)
            continue

          self.log_signal.emit(f"  KI Fehler: {user_msg}")
          break

      for temp_path in prepared_scan.iter_temporary_paths(cleanup_stage="after_gemini"):
        try:
          os.remove(temp_path)
        except Exception:
          pass

    self.result_signal.emit(extracted_data, -1, -1)

  def _log(self, msg):
    self.log_signal.emit(msg)


class MailScraperThread(QThread):
  """Holt E-Mails via IMAP (READ-ONLY) und gibt rohe HTML-Bodies zurÃƒÆ’Ã‚Â¼ck.
  Gemini-Verarbeitung ÃƒÆ’Ã‚Â¼bernimmt GeminiEmailThread."""
  log_signal   = pyqtSignal(str)
  progress_signal = pyqtSignal(int, int)
  raw_signal   = pyqtSignal(list, int, int) # (raw_emails, highest_uid, account_idx)

  def __init__(self, host, port, user, pwd, mail_limit=5, last_uid=0, settings_manager=None, account_idx=-1):
    super().__init__()
    self.host = host
    self.port = port
    self.user = user
    self.pwd = pwd
    self.mail_limit = mail_limit
    self.last_uid = int(last_uid or 0)
    self.settings_manager = settings_manager
    self.account_idx = account_idx

  def run(self):
    raw_emails = []
    highest_uid = 0

    try:
      self.log_signal.emit("Verbinde (Read-Only)...")
      mail = imaplib.IMAP4_SSL(self.host, int(self.port))
      mail.login(self.user, self.pwd)
      mail.select("INBOX", readonly=True)

      status, messages = mail.uid('SEARCH', 'ALL')
      if status != "OK":
        self.log_signal.emit(" Fehler beim Durchsuchen des Postfachs.")
        self.raw_signal.emit([], 0, self.account_idx)
        return

      mail_ids = messages[0].split()

      last_uid = self.last_uid

      if self.mail_limit == "SINCE_LAST":
        self.log_signal.emit(f"Suche alle neuen Mails seit UID > {last_uid}...")
        recent_ids = [uid for uid in mail_ids if int(uid) > last_uid]
        if not recent_ids:
          self.log_signal.emit("Keine neuen Mails seit dem letzten Scan gefunden.")
          self.raw_signal.emit([], 0, self.account_idx)
          return
      elif isinstance(self.mail_limit, int) and self.mail_limit >= len(mail_ids):
        self.log_signal.emit("Durchsuche das gesamte Postfach...")
        recent_ids = mail_ids
      else:
        self.log_signal.emit(f"Durchsuche die letzten {self.mail_limit} Mails...")
        recent_ids = mail_ids[-self.mail_limit:]

      recent_ids.reverse()
      total = len(recent_ids)
      self.progress_signal.emit(0, total)
      highest_uid = last_uid

      for idx, mail_id in enumerate(recent_ids):
        uid_int = int(mail_id)
        if uid_int > highest_uid:
          highest_uid = uid_int

        self.progress_signal.emit(idx, total)
        self.log_signal.emit(f"Lade E-Mail {idx+1}/{total}...")

        status, msg_data = mail.uid('fetch', mail_id, "(RFC822)")
        if status != "OK":
          continue

        for response_part in msg_data:
          if not isinstance(response_part, tuple):
            continue

          msg = email.message_from_bytes(response_part[1])

          subject, enc = decode_header(msg["Subject"])[0]
          if isinstance(subject, bytes):
            subject = subject.decode(enc if enc else "utf-8", errors="ignore")

          sender     = msg.get("From", "Unbekannt")
          email_date_raw = msg.get("Date", "")

          self.log_signal.emit(f"-> PrÃƒÆ’Ã‚Â¼fe: {str(subject)[:50]}")

          keywords = ["bestell", "rechnung", "order", "auftrag", "zahl",
                "amazon", "paypal", "ebay", "pedido", "versand",
                "encomenda", "commande", "confirma"]
          is_relevant = (any(kw in str(subject).lower() for kw in keywords) or
                  any(kw in str(sender).lower() for kw in keywords))
          if not is_relevant:
            self.log_signal.emit(" (Ignoriert - kein Beleg)")
            continue

          body_text = ""
          body_html = ""
          cid_map = {}
          attachments = []
          if msg.is_multipart():
            for part in msg.walk():
              ct = str(part.get_content_type())
              content_id = str(part.get("Content-ID") or "").strip().strip("<>").lower()
              content_disposition = str(part.get("Content-Disposition") or "").lower()
              file_name = str(part.get_filename() or "").strip()
              try:
                payload = part.get_payload(decode=True)
                if not payload:
                  continue

                if file_name or "attachment" in content_disposition:
                  guessed_from_name = mimetypes.guess_type(file_name)[0] or ""
                  is_pdf_attachment = (
                    ct == "application/pdf"
                    or guessed_from_name == "application/pdf"
                    or str(file_name).lower().endswith(".pdf")
                  )
                  is_image_attachment = ct.startswith("image/") or str(guessed_from_name).startswith("image/")
                  if is_pdf_attachment or is_image_attachment:
                    suffix = os.path.splitext(file_name)[1].lower()
                    if not suffix:
                      suffix = mimetypes.guess_extension(guessed_from_name or ct) or ".bin"
                    normalized_mime = "application/pdf" if is_pdf_attachment else (guessed_from_name or ct)
                    fd, attachment_path = tempfile.mkstemp(suffix=suffix)
                    os.close(fd)
                    with open(attachment_path, "wb") as file_handle:
                      file_handle.write(payload)
                    attachments.append({
                      "file_path": attachment_path,
                      "original_name": file_name or os.path.basename(attachment_path),
                      "mime_type": normalized_mime,
                      "temp_file": True,
                      "size_bytes": len(payload),
                    })
                  continue

                if content_id and ct.startswith("image/"):
                  encoded = base64.b64encode(payload).decode("ascii")
                  cid_map[content_id] = f"data:{ct};base64,{encoded}"
                  continue

                if ct in ("text/plain", "text/html"):
                  charset = part.get_content_charset() or "utf-8"
                  decoded = payload.decode(charset, errors="ignore")
                  if ct == "text/plain":
                    body_text += decoded
                  else:
                    body_html += decoded
              except Exception:
                pass
          else:
            try:
              payload = msg.get_payload(decode=True)
              if isinstance(payload, bytes):
                charset = msg.get_content_charset() or "utf-8"
                decoded = payload.decode(charset, errors="ignore")
                if str(msg.get_content_type()) == "text/html":
                  body_html = decoded
                else:
                  body_text = decoded
            except Exception:
              pass

          if not body_html and not body_text and not attachments:
            self.log_signal.emit(" (Kein auswertbarer Inhalt)")
            continue

          self.log_signal.emit(f" ÃƒÂ°Ã…Â¸Ã¢â‚¬Å“Ã‚Â¬ Mail eingesammelt fÃƒÆ’Ã‚Â¼r Screenshot-Rendering")
          raw_emails.append({
            "subject": str(subject),
            "sender": str(sender),
            "date": str(email_date_raw),
            "body_html": body_html,
            "body_text": body_text,
            "cid_map": cid_map,
            "attachments": attachments,
          })

      self.progress_signal.emit(total, total)
      mail.logout()

    except Exception as e:
      log_exception(__name__, e)
      self.log_signal.emit(f"Kritischer Thread-Fehler: {str(e)}")

    self.raw_signal.emit(raw_emails, highest_uid, self.account_idx)






class ClickableLabel(QLabel):
  """Ein QLabel das bei Klick ein vergrÃƒÆ’Ã‚Â¶ÃƒÆ’Ã…Â¸ertes Bild zeigt."""
  def __init__(self, full_pixmap=None, parent=None):
    super().__init__(parent)
    self.full_pixmap = full_pixmap
    self.setCursor(Qt.CursorShape.PointingHandCursor)
    
  def mousePressEvent(self, event):
    if self.full_pixmap and not self.full_pixmap.isNull():
      dlg = QDialog(self.window())
      dlg.setWindowTitle("Produktbild")
      dlg.setStyleSheet("background-color: #1a1b26;")
      layout = QVBoxLayout(dlg)
      lbl = QLabel()
      scaled = self.full_pixmap.scaled(400, 400, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
      lbl.setPixmap(scaled)
      lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
      layout.addWidget(lbl)
      btn = QPushButton("SchlieÃƒÆ’Ã…Â¸en")
      btn.clicked.connect(dlg.accept)
      layout.addWidget(btn)
      dlg.exec()
    super().mousePressEvent(event)


class ScraperReviewWizardDialog(QDialog):
  """
  Prueft erkannte E-Mails Schritt fuer Schritt.
  Links: Mail-Vorschau. Rechts: bearbeitbare Felder wie in Modul 1.
  """

  FIELD_DEFS = [
    ("bestellnummer", "Bestellnummer"),
    ("kaufdatum", "Kaufdatum"),
    ("shop_name", "Shop"),
    ("bestell_email", "Bestell-E-Mail"),
    ("tracking_nummer_einkauf", "Tracking"),
    ("sendungsstatus", "Sendungsstatus"),
    ("lieferdatum", "Lieferdatum"),
    ("gesamt_ekp_brutto", "Gesamtpreis (brutto)"),
    ("zahlungsart", "Zahlungsart"),
  ]

  def __init__(self, data_list, settings_manager, parent=None):
    super().__init__(parent)
    self.settings_manager = settings_manager
    self.ean_service = EanService(self.settings_manager)
    self.ean_lookup_worker = None
    self._pending_ean_lookup_context = None
    self.data_list = [dict(x) for x in (data_list or []) if isinstance(x, dict)]
    self.current_index = 0
    self._shared_db = None
    self._preview_processes = []
    self._mapping_done_by_index = {}
    self._mapping_prompted_by_index = {}
    self._mapping_state_by_index = {}
    self._active_mapping_panel = None
    self.summary = {
      "saved": 0,
      "skipped": 0,
      "discarded": 0,
      "renamed": 0,
    }

    self.setWindowTitle("E-Mails einzeln pruefen")
    self.resize(1680, 900)
    self.setMinimumSize(1100, 650)
    self.setStyleSheet("background-color: #1a1b26; color: #DADADA;")

    self._build_ui()

    if not self.data_list:
      QTimer.singleShot(0, self.reject)
      return

    self._load_current_mail()

  def _build_ui(self):
    layout = QVBoxLayout(self)

    self.lbl_progress = QLabel("")
    self.lbl_progress.setStyleSheet("font-size: 16px; font-weight: bold; color: #7aa2f7;")
    layout.addWidget(self.lbl_progress)

    self.lbl_hint = QLabel("Jede Mail wird einzeln geprueft. Speichern oeffnet die bekannten Matching-Dialoge.")
    self.lbl_hint.setStyleSheet("font-size: 13px; color: #a9b1d6;")
    layout.addWidget(self.lbl_hint)

    content = QHBoxLayout()
    left_box = QVBoxLayout()
    lbl_left = QLabel("Mail- und Anhangsvorschau")
    lbl_left.setStyleSheet("font-size: 14px; font-weight: bold;")
    left_box.addWidget(lbl_left)

    self.preview_tabs = QTabWidget(self)
    self.preview_tabs.setStyleSheet("QTabWidget::pane { border: 1px solid #414868; border-radius: 6px; } QTabBar::tab { background-color: #1f2335; padding: 8px 14px; } QTabBar::tab:selected { color: #7aa2f7; }")
    left_box.addWidget(self.preview_tabs, 1)

    mail_tab = QWidget()
    mail_tab_layout = QVBoxLayout(mail_tab)

    self.lbl_preview_notice = QLabel("")
    self.lbl_preview_notice.setWordWrap(True)
    self.lbl_preview_notice.setStyleSheet("font-size: 12px; color: #a9b1d6;")
    mail_tab_layout.addWidget(self.lbl_preview_notice)

    preview_action_row = QHBoxLayout()
    self.btn_load_external_preview = QPushButton("Bilder fuer diese Mail laden")
    self.btn_load_external_preview.clicked.connect(self._allow_external_for_current_mail)
    self.btn_trust_sender_preview = QPushButton("Absender vertrauen")
    self.btn_trust_sender_preview.clicked.connect(self._trust_current_sender)
    self.btn_trust_domain_preview = QPushButton("Domain vertrauen")
    self.btn_trust_domain_preview.clicked.connect(self._trust_current_domain)
    preview_action_row.addWidget(self.btn_load_external_preview)
    preview_action_row.addWidget(self.btn_trust_sender_preview)
    preview_action_row.addWidget(self.btn_trust_domain_preview)
    preview_action_row.addStretch()
    mail_tab_layout.addLayout(preview_action_row)

    self.preview_web = QWebEngineView(self)
    self.preview_web.setMinimumWidth(560)
    self.preview_web.setStyleSheet("background-color: #10111a; border: 1px solid #414868; border-radius: 6px;")
    mail_tab_layout.addWidget(self.preview_web, 1)

    self.btn_open_large_preview = QPushButton("In grosser Chrome-Vorschau oeffnen")
    self.btn_open_large_preview.clicked.connect(self._open_large_preview)
    mail_tab_layout.addWidget(self.btn_open_large_preview)
    self.preview_tabs.addTab(mail_tab, "E-Mail")

    attachment_tab = QWidget()
    attachment_layout = QVBoxLayout(attachment_tab)

    self.lbl_attachment_notice = QLabel("")
    self.lbl_attachment_notice.setWordWrap(True)
    self.lbl_attachment_notice.setStyleSheet("font-size: 12px; color: #a9b1d6;")
    attachment_layout.addWidget(self.lbl_attachment_notice)

    attachment_select_row = QHBoxLayout()
    attachment_select_row.addWidget(QLabel("PDF-Anhang:"))
    self.cmb_attachment_pdf = QComboBox()
    self.cmb_attachment_pdf.currentIndexChanged.connect(self._render_current_attachment_preview)
    attachment_select_row.addWidget(self.cmb_attachment_pdf, 1)
    attachment_layout.addLayout(attachment_select_row)

    self.pdf_preview_widget = None
    self.pdf_preview_web = None
    self.pdf_document = None
    if QT_PDF_AVAILABLE:
      self.pdf_document = QPdfDocument(self)
      try:
        self.pdf_document.statusChanged.connect(self._on_pdf_preview_status_changed)
      except Exception:
        pass
      self.pdf_preview_widget = QPdfView(self)
      self.pdf_preview_widget.setDocument(self.pdf_document)
      try:
        self.pdf_preview_widget.setZoomMode(QPdfView.ZoomMode.FitToWidth)
      except Exception:
        pass
      try:
        self.pdf_preview_widget.setPageMode(QPdfView.PageMode.MultiPage)
      except Exception:
        pass
      self.pdf_preview_widget.setMinimumWidth(560)
      self.pdf_preview_widget.setStyleSheet("background-color: #10111a; border: 1px solid #414868; border-radius: 6px;")
      attachment_layout.addWidget(self.pdf_preview_widget, 1)
    else:
      self.pdf_preview_web = QWebEngineView(self)
      self.pdf_preview_web.setMinimumWidth(560)
      self.pdf_preview_web.setStyleSheet("background-color: #10111a; border: 1px solid #414868; border-radius: 6px;")
      self._configure_pdf_preview_view(self.pdf_preview_web)
      self.pdf_preview_widget = self.pdf_preview_web
      attachment_layout.addWidget(self.pdf_preview_web, 1)

    self.btn_open_large_attachment_preview = QPushButton("PDF-Anhang gross oeffnen")
    self.btn_open_large_attachment_preview.clicked.connect(self._open_large_attachment_preview)
    attachment_layout.addWidget(self.btn_open_large_attachment_preview)
    self.preview_tabs.addTab(attachment_tab, "PDF-Anhang")

    right_box = QVBoxLayout()
    lbl_right = QLabel("Extrahierte Felder")
    lbl_right.setStyleSheet("font-size: 14px; font-weight: bold;")
    right_box.addWidget(lbl_right)

    map_row = QHBoxLayout()
    self.lbl_mapping_state = QLabel("")
    self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #f7c66f;")
    self.btn_run_mapping = QPushButton("Mapping jetzt starten")
    self.btn_run_mapping.clicked.connect(self._on_run_mapping_clicked)
    map_row.addWidget(self.lbl_mapping_state)
    map_row.addStretch()
    map_row.addWidget(self.btn_run_mapping)
    right_box.addLayout(map_row)

    self.mapping_frame = QFrame()
    self.mapping_frame.setStyleSheet("QFrame { background-color: #202233; border: 1px solid #414868; border-radius: 6px; }")
    mapping_layout = QVBoxLayout(self.mapping_frame)
    mapping_layout.setContentsMargins(14, 14, 14, 14)
    mapping_layout.setSpacing(10)

    self.lbl_mapping_panel_title = QLabel("Mapping-Bereich")
    self.lbl_mapping_panel_title.setStyleSheet("font-size: 14px; font-weight: bold; color: #7aa2f7;")
    mapping_layout.addWidget(self.lbl_mapping_panel_title)

    self.lbl_mapping_panel_hint = QLabel("")
    self.lbl_mapping_panel_hint.setWordWrap(True)
    self.lbl_mapping_panel_hint.setStyleSheet("font-size: 12px; color: #a9b1d6;")
    mapping_layout.addWidget(self.lbl_mapping_panel_hint)

    self.mapping_panel_host = QVBoxLayout()
    self.mapping_panel_host.setContentsMargins(0, 0, 0, 0)
    mapping_layout.addLayout(self.mapping_panel_host)
    self.mapping_frame.setVisible(False)
    right_box.addWidget(self.mapping_frame)

    form_frame = QFrame()
    form_frame.setStyleSheet("QFrame { background-color: #24283b; border: 1px solid #414868; border-radius: 6px; }")
    form_layout = QFormLayout(form_frame)
    form_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
    form_layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
    form_layout.setVerticalSpacing(8)

    self.inputs = {}
    for key, label in self.FIELD_DEFS:
      line = QLineEdit()
      line.setStyleSheet("QLineEdit { background-color: #171824; border: 1px solid #414868; border-radius: 4px; padding: 6px; }")
      self.inputs[key] = line
      form_layout.addRow(QLabel(label + ":"), line)

    right_box.addWidget(form_frame)

    lbl_items = QLabel("Artikel")
    lbl_items.setStyleSheet("font-size: 14px; font-weight: bold;")
    right_box.addWidget(lbl_items)

    self.table_waren = QTableWidget()
    self.table_waren.setColumnCount(5)
    self.table_waren.setHorizontalHeaderLabels(["Produkt", "Variante", "EAN", "Menge", "Stueckpreis"])
    self.table_waren.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
    self.table_waren.horizontalHeader().setStretchLastSection(True)
    self.table_waren.verticalHeader().setDefaultSectionSize(38)
    self.table_waren.setStyleSheet("""
      QTableWidget { background-color: #171824; border: 1px solid #414868; border-radius: 6px; gridline-color: #414868; }
      QHeaderView::section { background-color: #1f2335; color: #7aa2f7; font-weight: bold; padding: 5px; border: 1px solid #414868; }
    """)
    right_box.addWidget(self.table_waren, 1)


    ean_row = QHBoxLayout()
    self.btn_ean_lookup = QPushButton("EAN suchen (markierte Zeile)")
    self.btn_ean_lookup.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_ean_lookup.clicked.connect(self._lookup_ean_for_selected_row)
    ean_row.addWidget(self.btn_ean_lookup)
    ean_row.addStretch()
    right_box.addLayout(ean_row)
    content.addLayout(left_box, 6)
    content.addLayout(right_box, 7)
    layout.addLayout(content, 1)

    button_row = QHBoxLayout()
    self.btn_cancel = QPushButton("Wizard beenden")
    self.btn_cancel.clicked.connect(self._on_cancel)

    self.btn_discard = QPushButton("Mail verwerfen")
    self.btn_discard.clicked.connect(self._discard_current)

    self.btn_skip = QPushButton("Mail ueberspringen")
    self.btn_skip.clicked.connect(self._skip_current)

    self.btn_save_next = QPushButton("Speichern und weiter")
    self.btn_save_next.setProperty("class", "retro-btn-action")
    self.btn_save_next.clicked.connect(self._save_current_and_next)

    button_row.addWidget(self.btn_cancel)
    button_row.addStretch()
    button_row.addWidget(self.btn_discard)
    button_row.addWidget(self.btn_skip)
    button_row.addWidget(self.btn_save_next)
    layout.addLayout(button_row)

  def _set_progress_text(self):
    total = len(self.data_list)
    current_human = self.current_index + 1
    self.lbl_progress.setText(f"Mail {current_human}/{total}")
    if current_human >= total:
      self.btn_save_next.setText("Speichern und abschliessen")
    else:
      self.btn_save_next.setText("Speichern und weiter")

  def _safe_text(self, value):
    if value is None:
      return ""
    return str(value)

  def _set_form_fields_from_payload(self, payload):
    payload = payload if isinstance(payload, dict) else {}
    for key, _label in self.FIELD_DEFS:
      if key in self.inputs:
        self.inputs[key].setText(self._safe_text(payload.get(key, "")))

  def _apply_payload_to_current_mail(self, payload):
    merged = dict(self.data_list[self.current_index])
    if isinstance(payload, dict):
      merged.update(payload)
    self.data_list[self.current_index] = merged
    self._set_form_fields_from_payload(merged)
    self._populate_items_table(merged.get("waren", []))

  def _clear_mapping_panel(self):
    while self.mapping_panel_host.count():
      item = self.mapping_panel_host.takeAt(0)
      widget = item.widget()
      if widget is not None:
        widget.setParent(None)
        widget.deleteLater()
    self._active_mapping_panel = None

  def _ensure_mapping_state_for_index(self, idx, rebuild=False, source_payload=None):
    if not rebuild and idx in self._mapping_state_by_index:
      return self._mapping_state_by_index[idx]

    payload_source = dict(source_payload or self.data_list[idx] or {})
    workflow = EinkaufPipeline.prepare_mapping_workflow(payload_source)
    state = {
      "payload": dict(workflow.get("payload", {}) or {}),
      "tasks": list(workflow.get("tasks", []) or []),
      "task_index": 0,
    }
    self._mapping_state_by_index[idx] = state

    merged = dict(self.data_list[idx])
    merged.update(state["payload"])
    self.data_list[idx] = merged
    self._mapping_done_by_index[idx] = len(state["tasks"]) == 0
    return state

  def _current_mapping_state(self):
    return self._mapping_state_by_index.get(self.current_index)

  def _mapping_task_hint_text(self, task):
    raw_value = self._safe_text(task.get("raw_value", "")).strip() or "-"
    if task.get("task_type") == "amazon_country":
      return f"Erkannter Rohwert: {raw_value}. Waehle jetzt den Amazon-Shop. Links kannst du Mail und PDF weiter pruefen."
    return f"Erkannter Rohwert: {raw_value}. Du kannst ihn zuordnen oder unveraendert uebernehmen."

  def _render_mapping_panel_for_current_mail(self):
    state = self._current_mapping_state()
    if not isinstance(state, dict):
      self.mapping_frame.setVisible(False)
      self._clear_mapping_panel()
      return

    tasks = list(state.get("tasks", []) or [])
    task_index = int(state.get("task_index", 0) or 0)
    if task_index >= len(tasks):
      self.mapping_frame.setVisible(False)
      self._clear_mapping_panel()
      return

    task = tasks[task_index]
    self._clear_mapping_panel()
    self.mapping_frame.setVisible(True)
    self.lbl_mapping_panel_title.setText(f"Mapping-Schritt {task_index + 1}/{len(tasks)}: {self._safe_text(task.get('label', 'Mapping'))}")
    self.lbl_mapping_panel_hint.setText(self._mapping_task_hint_text(task))

    if task.get("task_type") == "amazon_country":
      panel = AmazonCountryPanel(raw_value=task.get("raw_value", "Amazon"), mode="embedded", parent=self.mapping_frame)
    else:
      panel = NormalizationPanel(task.get("category", "shops"), task.get("raw_value", ""), mode="embedded", parent=self.mapping_frame)

    panel.selection_confirmed.connect(self._on_mapping_panel_completed)
    self.mapping_panel_host.addWidget(panel)
    self._active_mapping_panel = panel

  def _on_mapping_panel_completed(self, selected_value):
    state = self._current_mapping_state()
    if not isinstance(state, dict):
      return

    tasks = list(state.get("tasks", []) or [])
    task_index = int(state.get("task_index", 0) or 0)
    if task_index >= len(tasks):
      return

    task = tasks[task_index]
    state["payload"] = EinkaufPipeline.apply_mapping_decision(state.get("payload", {}), task, selected_value)
    self._mapping_state_by_index[self.current_index] = state
    self._apply_payload_to_current_mail(state.get("payload", {}))

    state["task_index"] = task_index + 1
    self._mapping_prompted_by_index[self.current_index] = True
    if state["task_index"] >= len(tasks):
      self._mapping_done_by_index[self.current_index] = True
      self.mapping_frame.setVisible(False)
      self._clear_mapping_panel()
    else:
      self._mapping_done_by_index[self.current_index] = False
      self._render_mapping_panel_for_current_mail()
    self._update_mapping_state_ui()

  def _update_mapping_state_ui(self):
    state = self._current_mapping_state() or {}
    tasks = list(state.get("tasks", []) or []) if isinstance(state, dict) else []
    task_index = int(state.get("task_index", 0) or 0) if isinstance(state, dict) else 0
    remaining = max(0, len(tasks) - task_index)
    done = bool(self._mapping_done_by_index.get(self.current_index, False))

    if done:
      self.lbl_mapping_state.setText("Mapping: erledigt")
      self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #9ece6a;")
      self.btn_run_mapping.setText("Mapping erneut pruefen")
    elif remaining > 0:
      self.lbl_mapping_state.setText(f"Mapping: {remaining} Schritt(e) offen")
      self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #f7c66f;")
      self.btn_run_mapping.setText("Mapping-Bereich anzeigen")
    else:
      self.lbl_mapping_state.setText("Mapping: offen")
      self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #f7c66f;")
      self.btn_run_mapping.setText("Mapping jetzt starten")

  def _current_mail_item(self):
    return self.data_list[self.current_index]

  def _build_preview_render_result(self, item):
    return SafeMailRenderer.prepare_html(
      item.get("_original_email_html", ""),
      text_fallback=item.get("_original_email_text", ""),
      sender_text=item.get("_email_sender", ""),
      settings_manager=self.settings_manager,
      inline_cid_map=item.get("_mail_cid_map", {}),
      allow_external=bool(item.get("_allow_external_preview_once", False)),
    )

  def _update_preview_controls(self, render_result):
    self.lbl_preview_notice.setText(SafeMailRenderer.build_notice_text(render_result))
    self.btn_load_external_preview.setVisible(render_result.blocked_remote_images > 0 or render_result.blocked_remote_links > 0)
    self.btn_trust_sender_preview.setVisible(render_result.can_trust_sender)
    self.btn_trust_domain_preview.setVisible(render_result.can_trust_domain)

  def _render_current_preview(self):
    item = self._current_mail_item()
    render_result = self._build_preview_render_result(item)
    self._current_preview_render_result = render_result
    SafeMailRenderer.apply_to_view(self.preview_web, render_result)
    self._update_preview_controls(render_result)

  def _configure_pdf_preview_view(self, view):
    if view is None:
      return
    try:
      settings = view.settings()
      settings.setAttribute(QWebEngineSettings.WebAttribute.JavascriptEnabled, False)
      settings.setAttribute(QWebEngineSettings.WebAttribute.PluginsEnabled, False)
      settings.setAttribute(QWebEngineSettings.WebAttribute.FullScreenSupportEnabled, False)
      settings.setAttribute(QWebEngineSettings.WebAttribute.LocalStorageEnabled, False)
      settings.setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, False)
      settings.setAttribute(QWebEngineSettings.WebAttribute.HyperlinkAuditingEnabled, False)
      settings.setAttribute(QWebEngineSettings.WebAttribute.ErrorPageEnabled, False)
      settings.setAttribute(QWebEngineSettings.WebAttribute.PdfViewerEnabled, True)
    except Exception as e:
      log_exception(__name__, e)

  def _pdf_attachment_rows(self, item=None):
    item = item if isinstance(item, dict) else self._current_mail_item()
    rows = []
    for attachment in list(item.get("_mail_pdf_attachments", []) or []):
      if not isinstance(attachment, dict):
        continue
      path_value = self._safe_text(attachment.get("file_path", "")).strip()
      if not path_value or not os.path.exists(path_value):
        continue
      rows.append(dict(attachment))
    rows.sort(
      key=lambda row: (
        1 if row.get("used_for_ai") else 0,
        1 if row.get("pdf_is_relevant") else 0,
        int(row.get("pdf_relevance_score", 0) or 0),
      ),
      reverse=True,
    )
    return rows

  def _attachment_relevance_text(self, attachment):
    score = int(attachment.get("pdf_relevance_score", 0) or 0)
    is_relevant = bool(attachment.get("pdf_is_relevant", False))
    used_for_ai = bool(attachment.get("used_for_ai", False))
    reason = self._safe_text(attachment.get("pdf_relevance_reason", "")).strip()
    prefix = "relevant" if is_relevant else "eher unklar"
    suffix = " | wird fuer KI genutzt" if used_for_ai else ""
    if reason:
      return f"KI-Einstufung: {prefix} (Score {score}) - {reason}{suffix}"
    return f"KI-Einstufung: {prefix} (Score {score}){suffix}"

  def _on_pdf_preview_status_changed(self, status):
    if self.pdf_document is None:
      return
    try:
      if status == QPdfDocument.Status.Error:
        self._set_attachment_preview_placeholder("PDF wurde erkannt, konnte aber von Qt nicht geladen werden.")
      elif status == QPdfDocument.Status.Ready and self.pdf_document.pageCount() <= 0:
        self._set_attachment_preview_placeholder("PDF wurde erkannt, aber Qt hat keine Seiten gefunden.")
    except Exception as e:
      log_exception(__name__, e)

  def _load_pdf_into_embedded_preview(self, pdf_path):
    if self.pdf_document is not None:
      try:
        self.pdf_document.close()
      except Exception:
        pass
      try:
        status = self.pdf_document.load(pdf_path)
        if status == QPdfDocument.Status.Error:
          self._set_attachment_preview_placeholder("PDF konnte nicht geladen werden.")
          return False
      except Exception as e:
        log_exception(__name__, e, extra={"pdf_path": pdf_path})
        self._set_attachment_preview_placeholder("PDF konnte nicht geladen werden.")
        return False
      return True

    if self.pdf_preview_web is not None:
      self.pdf_preview_web.setUrl(QUrl.fromLocalFile(pdf_path))
      return True
    return False

  def _set_attachment_preview_placeholder(self, message):
    self.lbl_attachment_notice.setText(self._safe_text(message))
    if self.pdf_document is not None:
      try:
        self.pdf_document.close()
      except Exception:
        pass
    if self.pdf_preview_web is not None:
      self.pdf_preview_web.setHtml("<html><body style='background-color:#10111a;color:#a9b1d6;font-family:Segoe UI,sans-serif;'><div style='padding:18px;'>Keine PDF-Vorschau verfuegbar.</div></body></html>")

  def _populate_attachment_preview(self):
    attachments = self._pdf_attachment_rows(self._current_mail_item())
    self.cmb_attachment_pdf.blockSignals(True)
    self.cmb_attachment_pdf.clear()
    for index, attachment in enumerate(attachments):
      title = self._safe_text(attachment.get("original_name", "")).strip() or f"PDF {index + 1}"
      score = int(attachment.get("pdf_relevance_score", 0) or 0)
      if bool(attachment.get("used_for_ai", False)):
        title = f"{title} [fuer KI gewaehlt]"
      elif bool(attachment.get("pdf_is_relevant", False)):
        title = f"{title} [relevant {score}]"
      self.cmb_attachment_pdf.addItem(title, attachment)
    self.cmb_attachment_pdf.blockSignals(False)

    has_pdf = bool(attachments)
    self.cmb_attachment_pdf.setEnabled(has_pdf and len(attachments) > 1)
    self.btn_open_large_attachment_preview.setEnabled(has_pdf)
    if not has_pdf:
      self._set_attachment_preview_placeholder("Kein PDF-Anhang fuer diese Mail erkannt.")
      return

    self.cmb_attachment_pdf.setCurrentIndex(0)
    self._render_current_attachment_preview()

  def _render_current_attachment_preview(self):
    attachments = self._pdf_attachment_rows()
    if not attachments:
      self._set_attachment_preview_placeholder("Kein PDF-Anhang fuer diese Mail erkannt.")
      return

    attachment = self.cmb_attachment_pdf.currentData()
    if not isinstance(attachment, dict):
      attachment = attachments[0]
    pdf_path = self._safe_text(attachment.get("file_path", "")).strip()
    if not pdf_path or not os.path.exists(pdf_path):
      self._set_attachment_preview_placeholder("Der PDF-Anhang ist nicht mehr verfuegbar.")
      return

    name = self._safe_text(attachment.get("original_name", "")).strip() or os.path.basename(pdf_path)
    total = len(attachments)
    index = 0
    for pos, row in enumerate(attachments):
      if self._safe_text(row.get("file_path", "")).strip() == pdf_path:
        index = pos
        break

    headline = f"PDF-Anhang {index + 1}/{total}: {name}" if total > 1 else f"PDF-Anhang: {name}"
    self.lbl_attachment_notice.setText(headline + "\n" + self._attachment_relevance_text(attachment))
    self._load_pdf_into_embedded_preview(pdf_path)

  def _allow_external_for_current_mail(self):
    item = self._current_mail_item()
    item["_allow_external_preview_once"] = True
    self._render_current_preview()

  def _trust_current_sender(self):
    item = self._current_mail_item()
    SafeMailRenderer.trust_sender(self.settings_manager, item.get("_email_sender", ""))
    item["_allow_external_preview_once"] = True
    self._render_current_preview()

  def _trust_current_domain(self):
    item = self._current_mail_item()
    SafeMailRenderer.trust_domain(self.settings_manager, item.get("_email_sender", ""))
    item["_allow_external_preview_once"] = True
    self._render_current_preview()

  def _populate_items_table(self, waren):
    waren = waren if isinstance(waren, list) else []
    self.table_waren.setRowCount(len(waren))

    for row, ware in enumerate(waren):
      if not isinstance(ware, dict):
        ware = {}
      self.table_waren.setItem(row, 0, QTableWidgetItem(self._safe_text(ware.get("produkt_name", ""))))
      self.table_waren.setItem(row, 1, QTableWidgetItem(self._safe_text(ware.get("varianten_info", ""))))
      self.table_waren.setItem(row, 2, QTableWidgetItem(self._safe_text(ware.get("ean", ""))))
      self.table_waren.setItem(row, 3, QTableWidgetItem(self._safe_text(ware.get("menge", "1"))))
      self.table_waren.setItem(row, 4, QTableWidgetItem(self._safe_text(ware.get("ekp_brutto", ""))))

    self.table_waren.resizeColumnsToContents()

  def _load_current_mail(self):
    item = self.data_list[self.current_index]
    self._set_progress_text()

    state = self._ensure_mapping_state_for_index(self.current_index, rebuild=False, source_payload=item)
    self._apply_payload_to_current_mail(state.get("payload", {}))
    item = self._current_mail_item()

    self._render_current_preview()
    self._populate_attachment_preview()
    self._update_mapping_state_ui()
    if item.get("_primary_scan_source_type", "") == "mail_attachment" and self._pdf_attachment_rows(item):
      self.preview_tabs.setCurrentIndex(1)
    else:
      self.preview_tabs.setCurrentIndex(0)

    if self._mapping_done_by_index.get(self.current_index, False):
      self.mapping_frame.setVisible(False)
      self._clear_mapping_panel()
    else:
      self.mapping_frame.setVisible(False)
      self._clear_mapping_panel()

    idx = self.current_index
    QTimer.singleShot(0, lambda idx=idx: self._auto_prompt_mapping_for_index(idx))

  def _auto_prompt_mapping_for_index(self, idx):
    if idx != self.current_index:
      return
    if self._mapping_done_by_index.get(idx, False):
      return
    if self._mapping_prompted_by_index.get(idx, False):
      return

    self._mapping_prompted_by_index[idx] = True
    self._run_mapping_for_current_mail(show_feedback=False, rebuild=False)

  def _read_table_cell(self, row, col):
    cell = self.table_waren.item(row, col)
    if cell is None:
      return ""
    return self._safe_text(cell.text()).strip()

  def _collect_current_payload(self):
    base = dict(self.data_list[self.current_index])

    for key, _label in self.FIELD_DEFS:
      base[key] = self.inputs[key].text().strip()

    waren = []
    for row in range(self.table_waren.rowCount()):
      produkt = self._read_table_cell(row, 0)
      variante = self._read_table_cell(row, 1)
      ean = self._read_table_cell(row, 2)
      menge = self._read_table_cell(row, 3)
      ekp = self._read_table_cell(row, 4)

      if not any([produkt, variante, ean, menge, ekp]):
        continue

      waren.append({
        "produkt_name": produkt,
        "varianten_info": variante,
        "ean": ean,
        "menge": menge or "1",
        "ekp_brutto": ekp,
      })

    base["waren"] = waren
    return base
  def _lookup_ean_for_selected_row(self):
    if self.ean_lookup_worker is not None and self.ean_lookup_worker.isRunning():
      QMessageBox.information(self, "EAN Suche", "Es laeuft bereits eine EAN-Suche im Hintergrund.")
      return

    row = self.table_waren.currentRow()
    if row < 0:
      QMessageBox.information(self, "EAN Suche", "Bitte zuerst eine Artikelzeile markieren.")
      return

    produkt_name = self._read_table_cell(row, 0)
    varianten_info = self._read_table_cell(row, 1)
    if not produkt_name:
      QMessageBox.warning(self, "EAN Suche", "In der markierten Zeile fehlt der Produktname.")
      return

    self._pending_ean_lookup_context = {
      "row": row,
      "produkt_name": produkt_name,
      "varianten_info": varianten_info,
    }
    self.btn_ean_lookup.setEnabled(False)
    self.btn_ean_lookup.setText("EAN Suche laeuft...")

    self.ean_lookup_worker = EanLookupWorker(
      self.settings_manager,
      produkt_name,
      varianten_info=varianten_info,
      limit=25,
      allow_api_fallback=True,
    )
    self.ean_lookup_worker.result_signal.connect(self._on_ean_lookup_finished)
    self.ean_lookup_worker.error_signal.connect(self._on_ean_lookup_error)
    self.ean_lookup_worker.start()

  def _finish_ean_lookup_ui(self):
    self.btn_ean_lookup.setEnabled(True)
    self.btn_ean_lookup.setText("EAN suchen (markierte Zeile)")
    self.ean_lookup_worker = None

  def _on_ean_lookup_finished(self, payload):
    context = dict(self._pending_ean_lookup_context or {})
    self._pending_ean_lookup_context = None
    self._finish_ean_lookup_ui()

    candidates = payload.get("candidates", []) if isinstance(payload, dict) else []
    error_payload = payload.get("error", {}) if isinstance(payload, dict) else {}
    if not candidates:
      api_msg = ""
      if isinstance(error_payload, dict):
        api_msg = str(error_payload.get("user_message", "")).strip()
      if api_msg:
        QMessageBox.warning(
          self,
          "EAN Suche",
          "Lokal gab es keine Treffer und die API-Suche ist fehlgeschlagen:\n\n" + api_msg,
        )
      else:
        QMessageBox.information(
          self,
          "Keine Treffer",
          "Es wurden weder lokal noch ueber die API passende EAN-Vorschlaege gefunden."
        )
      return

    produkt_name = str(context.get("produkt_name", "")).strip()
    selected = EanLookupDialog.choose(produkt_name, candidates, parent=self)
    if not selected:
      return

    chosen_ean = str(selected.get("ean", "")).strip()
    if not chosen_ean:
      QMessageBox.warning(self, "EAN Suche", "Der gewaehlte Eintrag hat keine gueltige EAN.")
      return

    row = int(context.get("row", -1) or -1)
    if row < 0 or row >= self.table_waren.rowCount():
      QMessageBox.warning(self, "EAN Suche", "Die bearbeitete Tabellenzeile existiert nicht mehr.")
      return

    self.table_waren.setItem(row, 2, QTableWidgetItem(chosen_ean))
    self.ean_service.remember_candidate_selection(
      produkt_name,
      selected,
      varianten_info=str(context.get("varianten_info", "")).strip(),
    )

  def _on_ean_lookup_error(self, err_msg):
    self._pending_ean_lookup_context = None
    self._finish_ean_lookup_ui()
    msg = str(err_msg or "").strip() or "Unbekannter Fehler bei der EAN-Suche."
    QMessageBox.warning(self, "EAN Suche", f"Die EAN-Suche ist fehlgeschlagen:\n{msg}")

  def _on_run_mapping_clicked(self):
    self._run_mapping_for_current_mail(show_feedback=True, rebuild=True)

  def _run_mapping_for_current_mail(self, show_feedback=True, rebuild=False):
    try:
      payload = self._collect_current_payload()
      state = self._ensure_mapping_state_for_index(self.current_index, rebuild=rebuild, source_payload=payload)
      self._apply_payload_to_current_mail(state.get("payload", {}))

      tasks = list(state.get("tasks", []) or [])
      task_index = int(state.get("task_index", 0) or 0)
      if task_index >= len(tasks):
        self._mapping_done_by_index[self.current_index] = True
        self._clear_mapping_panel()
        if show_feedback:
          self.mapping_frame.setVisible(True)
          self.lbl_mapping_panel_title.setText("Keine offene Mapping-Aufgabe")
          self.lbl_mapping_panel_hint.setText("Fuer diese Mail ist aktuell kein weiterer Mapping-Schritt noetig.")
        else:
          self.mapping_frame.setVisible(False)
        self._update_mapping_state_ui()
        return

      self._mapping_done_by_index[self.current_index] = False
      self._mapping_prompted_by_index[self.current_index] = True
      self._update_mapping_state_ui()
      self._render_mapping_panel_for_current_mail()
    except Exception as e:
      log_exception(__name__, e)
      QMessageBox.critical(self, "Mapping-Fehler", f"Mapping fehlgeschlagen:\n{e}")

  def _save_current_and_next(self):
    try:
      if not self._mapping_done_by_index.get(self.current_index, False):
        QMessageBox.information(
          self,
          "Mapping offen",
          "Bitte zuerst den Mapping-Bereich rechts abschliessen.\n"
          "So bleiben Vorschau und Mapping im selben Wizard-Kontext."
        )
        return

      payload = self._collect_current_payload()
      self.data_list[self.current_index] = payload

      def _on_order_number_changed(new_no):
        if "bestellnummer" in self.inputs:
          self.inputs["bestellnummer"].setText(self._safe_text(new_no))
        payload["bestellnummer"] = self._safe_text(new_no)

      save_result = EinkaufPipeline.confirm_and_save_single(
        self,
        self.settings_manager,
        payload,
        on_order_number_changed=_on_order_number_changed,
        show_new_number_info=True,
        db=self._shared_db
      )
      self._shared_db = save_result.get("db", self._shared_db)

      if save_result.get("status") != "saved":
        return

      self.summary["saved"] += 1
      if save_result.get("renamed"):
        self.summary["renamed"] += 1

      match_result = EinkaufPipeline.confirm_and_apply_pending_matches(
        self,
        self.settings_manager,
        db=self._shared_db
      )
      self._shared_db = match_result.get("db", self._shared_db)

      title, text = EinkaufPipeline.build_match_result_message(match_result)
      QMessageBox.information(self, title, text)

      self._advance_to_next()
    except Exception as e:
      log_exception(__name__, e)
      QMessageBox.critical(self, "Speichern fehlgeschlagen", f"Fehler beim Speichern:\n{e}")

  def _skip_current(self):
    self.summary["skipped"] += 1
    self._advance_to_next()

  def _discard_current(self):
    reply = QMessageBox.question(
      self,
      "Mail verwerfen",
      "Diese Mail wirklich verwerfen und mit der naechsten weitermachen?",
      QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
      QMessageBox.StandardButton.No,
    )
    if reply != QMessageBox.StandardButton.Yes:
      return

    self.summary["discarded"] += 1
    self._advance_to_next()

  def _open_large_preview(self):
    item = self.data_list[self.current_index]
    payload = {
      "preview_kind": "mail",
      "shop_name": self._safe_text(item.get("shop_name", "Unbekannt")),
      "email_sender": self._safe_text(item.get("_email_sender", "Unbekannt")),
      "email_date": self._safe_text(item.get("_email_date", "")),
      "email_html": self._safe_text(item.get("_original_email_html", "<p>Kein Text verfuegbar.</p>") or ""),
      "email_text": self._safe_text(item.get("_original_email_text", "") or ""),
      "cid_map": item.get("_mail_cid_map", {}) or {},
      "allow_external_once": bool(item.get("_allow_external_preview_once", False)),
    }
    self._launch_preview_process(payload)

  def _open_large_attachment_preview(self):
    attachments = self._pdf_attachment_rows()
    if not attachments:
      QMessageBox.information(self, "PDF Vorschau", "Fuer diese Mail wurde kein PDF-Anhang erkannt.")
      return

    attachment = self.cmb_attachment_pdf.currentData()
    if not isinstance(attachment, dict):
      attachment = attachments[0]
    pdf_path = self._safe_text(attachment.get("file_path", "")).strip()
    if not pdf_path or not os.path.exists(pdf_path):
      QMessageBox.warning(self, "PDF Vorschau", "Der PDF-Anhang ist nicht mehr verfuegbar.")
      return

    item = self.data_list[self.current_index]
    payload = {
      "preview_kind": "pdf",
      "attachment_name": self._safe_text(attachment.get("original_name", "PDF-Anhang")) or "PDF-Anhang",
      "pdf_path": pdf_path,
      "email_sender": self._safe_text(item.get("_email_sender", "Unbekannt")),
      "email_date": self._safe_text(item.get("_email_date", "")),
    }
    self._launch_preview_process(payload)

  def _launch_preview_process(self, payload):
    self._cleanup_preview_processes()

    payload_path = ""
    try:
      tmp = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", prefix="mail_preview_", delete=False)
      json.dump(payload, tmp, ensure_ascii=False)
      tmp.close()
      payload_path = tmp.name

      project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
      helper_script = os.path.join(project_root, "module", "mail_preview_helper.py")
      if not os.path.exists(helper_script):
        raise FileNotFoundError(f"Helper fehlt: {helper_script}")

      creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
      cmd = [sys.executable, "-m", "module.mail_preview_helper", "--payload", payload_path]
      proc = subprocess.Popen(cmd, creationflags=creationflags, cwd=project_root)
      self._preview_processes.append(proc)
    except Exception as e:
      log_exception(__name__, e)
      try:
        if payload_path and os.path.exists(payload_path):
          os.remove(payload_path)
      except Exception as e2:
        log_exception(__name__, e2)
      QMessageBox.warning(self, "Vorschau-Fehler", f"Konnte Vorschau nicht starten:\n{e}")

  def _cleanup_preview_processes(self):
    alive = []
    for proc in self._preview_processes:
      try:
        if proc is not None and proc.poll() is None:
          alive.append(proc)
      except Exception as e:
        log_exception(__name__, e)
    self._preview_processes = alive

  def _close_preview_dialogs(self):
    self._cleanup_preview_processes()
    for proc in self._preview_processes:
      try:
        proc.terminate()
        try:
          proc.wait(timeout=1.5)
        except Exception:
          proc.kill()
      except Exception as e:
        log_exception(__name__, e)
    self._preview_processes = []

  def _cleanup_mail_assets(self, item):
    if not isinstance(item, dict) or item.get("_mail_assets_cleaned", False):
      return
    for attachment in list(item.get("_mail_review_attachments", []) or []):
      if not isinstance(attachment, dict):
        continue
      path_value = self._safe_text(attachment.get("file_path", "")).strip()
      if not path_value:
        continue
      try:
        if os.path.exists(path_value):
          os.remove(path_value)
      except Exception as e:
        log_exception(__name__, e, extra={"attachment_path": path_value})
    item["_mail_assets_cleaned"] = True

  def _cleanup_all_mail_assets(self):
    for item in self.data_list:
      self._cleanup_mail_assets(item)

  def _advance_to_next(self):
    current_item = self.data_list[self.current_index] if 0 <= self.current_index < len(self.data_list) else None
    self._close_preview_dialogs()
    self._cleanup_mail_assets(current_item)
    self.current_index += 1
    if self.current_index >= len(self.data_list):
      self.accept()
      return
    self._load_current_mail()

  def _on_cancel(self):
    reply = QMessageBox.question(
      self,
      "Wizard beenden",
      "Wizard jetzt beenden? Bereits gespeicherte Eintraege bleiben erhalten.",
      QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
      QMessageBox.StandardButton.No,
    )
    if reply == QMessageBox.StandardButton.Yes:
      self.reject()

  def get_summary(self):
    return dict(self.summary)

  def accept(self):
    self._close_preview_dialogs()
    self._cleanup_all_mail_assets()
    super().accept()

  def reject(self):
    self._close_preview_dialogs()
    self._cleanup_all_mail_assets()
    super().reject()

  def closeEvent(self, event):
    self._close_preview_dialogs()
    self._cleanup_all_mail_assets()
    super().closeEvent(event)





















