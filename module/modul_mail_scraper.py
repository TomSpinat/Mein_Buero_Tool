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
  QComboBox, QLineEdit, QSizePolicy, QFormLayout, QTabWidget, QScrollArea, QSplitter
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
import logging
import subprocess
import sys
import socket
import ssl

from module.background_tasks import BackgroundTask
from module.gemini_api import process_receipt_with_gemini
from module.einkauf_pipeline import EinkaufPipeline
from module.einkauf_ui import EinkaufHeadFormWidget, EinkaufItemsTableWidget, OrderReviewPanelWidget, SummenBannerWidget
from module.normalization_dialog import NormalizationPanel
from module.amazon_country_dialog import AmazonCountryPanel
from module.ean_service import EanService
from module.mail_screenshot_renderer import MailScreenshotRenderJob
from module.mail_pipeline_ui import MailPipelineDashboardWidget
from module.scan_input_preprocessing import prepare_mail_scan
from module.scan_profile_catalog import build_scan_decision_from_existing

from module.crash_logger import (
  AppError,
  classify_gemini_error,
  log_classified_error,
  log_exception,
)
from module.custom_msgbox import CustomMsgBox
from module.lookup_service import LookupService
from module.lookup_results import FieldState, FieldType, LookupSource
from module.secret_store import sanitize_text
from module.safe_mail_renderer import SafeMailRenderer
from module.shop_logo_search_service import ShopLogoSearchService
from module.shop_logo_search_worker import ShopLogoSearchWorker
from module.media.media_grid_selection_dialog import MediaGridSelectionDialog
from module.media.media_service import MediaService
from module.media.media_store import LocalMediaStore
from module.shared_search_workflows import (
    create_logo_search_worker,
    reset_logo_search_button,
    handle_logo_search_result,
    handle_logo_search_error,
    create_ean_lookup_worker,
    reset_ean_lookup_button,
    handle_ean_lookup_result,
    handle_ean_lookup_error,
)


class AddAccountDialog(QDialog):
  def __init__(self, parent=None):
    super().__init__(parent)
    self.setWindowTitle("E-Mail Konto hinzufuegen")
    self.resize(620, 460)
    self.setMinimumSize(560, 430)
    self.setSizeGripEnabled(True)
    self.setStyleSheet(
      "QDialog { background-color: #1a1b26; color: #a9b1d6; }"
      "QLabel { color: #d6deff; font-size: 14px; font-weight: bold; }"
      "QLineEdit { background-color: #10182b; color: #f2f5ff; border: 1px solid #384769; border-radius: 10px; padding: 10px 12px; font-size: 14px; }"
      "QLineEdit:focus { border: 1px solid #8b5cf6; }"
      "QPushButton { min-height: 42px; min-width: 150px; border-radius: 12px; font-size: 14px; font-weight: bold; padding: 8px 16px; }"
    )

    layout = QVBoxLayout(self)
    layout.setContentsMargins(22, 22, 22, 18)
    layout.setSpacing(14)

    lbl_hint = QLabel("Bitte die IMAP-Zugangsdaten des Postfachs eintragen. Die Eingaben bleiben hier nur bei diesem Konto.")
    lbl_hint.setWordWrap(True)
    lbl_hint.setStyleSheet("color: #9aa4c5; font-size: 12px; font-weight: normal;")
    layout.addWidget(lbl_hint)

    form_layout = QVBoxLayout()
    form_layout.setSpacing(8)

    self.entry_name = QLineEdit()
    self.entry_name.setPlaceholderText("Name (z.B. Haupt-Account)")
    self.entry_name.setMinimumHeight(44)
    form_layout.addWidget(QLabel("Konto-Name:"))
    form_layout.addWidget(self.entry_name)

    self.entry_host = QLineEdit()
    self.entry_host.setPlaceholderText("z.B. imap.ionos.de")
    self.entry_host.setMinimumHeight(44)
    form_layout.addWidget(QLabel("IMAP Server:"))
    form_layout.addWidget(self.entry_host)

    self.entry_port = QLineEdit()
    self.entry_port.setText("993")
    self.entry_port.setMinimumHeight(44)
    form_layout.addWidget(QLabel("IMAP Port:"))
    form_layout.addWidget(self.entry_port)

    self.entry_user = QLineEdit()
    self.entry_user.setPlaceholderText("E-Mail Adresse")
    self.entry_user.setMinimumHeight(44)
    form_layout.addWidget(QLabel("Benutzername / E-Mail:"))
    form_layout.addWidget(self.entry_user)

    self.entry_pwd = QLineEdit()
    self.entry_pwd.setEchoMode(QLineEdit.EchoMode.Password)
    self.entry_pwd.setPlaceholderText("Passwort")
    self.entry_pwd.setMinimumHeight(44)
    form_layout.addWidget(QLabel("Passwort:"))
    form_layout.addWidget(self.entry_pwd)

    layout.addLayout(form_layout)
    layout.addStretch(1)

    btn_layout = QHBoxLayout()
    btn_layout.setSpacing(10)
    btn_layout.addStretch(1)

    btn_cancel = QPushButton("Abbrechen")
    btn_cancel.setStyleSheet(
      "QPushButton { background-color: #20273d; color: #d6deff; border: 1px solid #3a4566; }"
      "QPushButton:hover { background-color: #27314b; border-color: #55658f; }"
    )
    btn_cancel.clicked.connect(self.reject)
    btn_layout.addWidget(btn_cancel)

    btn_save = QPushButton("Speichern")
    btn_save.setStyleSheet(
      "QPushButton { background-color: #7c3aed; color: white; border: 1px solid #8b5cf6; }"
      "QPushButton:hover { background-color: #8b5cf6; }"
    )
    btn_save.clicked.connect(self.accept)
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


def _emit_task_progress(progress_callback, text):
  if callable(progress_callback):
    progress_callback({"status_text": str(text or "").strip()})


MAIL_IMAP_TIMEOUT_SEC = 30
MAIL_IMAP_RETRY_TIMEOUT_SEC = 60
MAIL_IMAP_STARTTLS_PORT = 143


def _probe_tcp_connection(host, port, timeout_sec):
  sock = None
  try:
    sock = socket.create_connection((host, int(port)), timeout=timeout_sec)
    sock.settimeout(timeout_sec)
  finally:
    if sock is not None:
      try:
        sock.close()
      except Exception:
        pass


def _set_imap_socket_timeout(mail, timeout_sec):
  try:
    sock = getattr(mail, "sock", None)
    if sock is not None:
      sock.settimeout(timeout_sec)
  except Exception:
    pass


def _should_prefer_starttls(host, port):
  host_lower = str(host or "").strip().lower()
  return int(port or 0) == 993 and host_lower == "imap.ionos.de"


def _create_starttls_imap_client(host, port=MAIL_IMAP_STARTTLS_PORT, timeout_sec=MAIL_IMAP_TIMEOUT_SEC, progress_callback=None):
  timeout_sec = max(10, int(timeout_sec or MAIL_IMAP_TIMEOUT_SEC))
  host = str(host or "").strip()
  port = int(port or MAIL_IMAP_STARTTLS_PORT)
  _emit_task_progress(progress_callback, f"Verbinde ueber STARTTLS auf Port {port}...")
  mail = imaplib.IMAP4(host=host, port=port, timeout=timeout_sec)
  _set_imap_socket_timeout(mail, timeout_sec)
  mail.starttls(ssl_context=ssl.create_default_context())
  _set_imap_socket_timeout(mail, timeout_sec)
  setattr(mail, "_transport_text", f"STARTTLS ueber Port {port}")
  return mail


def _create_imap_client(host, port, timeout_sec=MAIL_IMAP_TIMEOUT_SEC, retry_timeout_sec=MAIL_IMAP_RETRY_TIMEOUT_SEC, progress_callback=None):
  host = str(host or "").strip()
  port = int(port)
  timeout_sec = max(10, int(timeout_sec or MAIL_IMAP_TIMEOUT_SEC))
  retry_timeout_sec = max(timeout_sec, int(retry_timeout_sec or MAIL_IMAP_RETRY_TIMEOUT_SEC))
  attempt_timeouts = [timeout_sec]
  if retry_timeout_sec > timeout_sec:
    attempt_timeouts.append(retry_timeout_sec)

  last_exc = None
  prefer_starttls = _should_prefer_starttls(host, port)
  if prefer_starttls:
    try:
      return _create_starttls_imap_client(host, MAIL_IMAP_STARTTLS_PORT, timeout_sec=timeout_sec, progress_callback=progress_callback)
    except Exception as exc:
      last_exc = exc
      _emit_task_progress(progress_callback, "STARTTLS-Versuch fehlgeschlagen. Direkter SSL-Port 993 wird prueft...")

  for attempt_index, attempt_timeout in enumerate(attempt_timeouts):
    try:
      ssl_context = ssl.create_default_context()
      mail = imaplib.IMAP4_SSL(
        host=host,
        port=port,
        ssl_context=ssl_context,
        timeout=attempt_timeout,
      )
      _set_imap_socket_timeout(mail, attempt_timeout)
      setattr(mail, "_transport_text", f"Direktes SSL ueber Port {port}")
      return mail
    except (ssl.SSLError, socket.timeout, TimeoutError, OSError) as exc:
      last_exc = exc
      detail_lower = sanitize_text(exc).lower()
      is_retryable_timeout = "timed out" in detail_lower or "timeout" in detail_lower or "handshake" in detail_lower
      has_retry_left = attempt_index < len(attempt_timeouts) - 1
      if has_retry_left and is_retryable_timeout:
        next_timeout = attempt_timeouts[attempt_index + 1]
        _emit_task_progress(progress_callback, f"Server reagiert langsam. Neuer SSL-Versuch mit {next_timeout} Sekunden...")
        continue
      if int(port) == 993:
        _emit_task_progress(progress_callback, f"Port 993 reagiert nicht. Fallback auf STARTTLS-Port {MAIL_IMAP_STARTTLS_PORT}...")
        try:
          return _create_starttls_imap_client(host, MAIL_IMAP_STARTTLS_PORT, timeout_sec=timeout_sec, progress_callback=progress_callback)
        except Exception as fallback_exc:
          last_exc = fallback_exc
      raise last_exc

  if last_exc is not None:
    raise last_exc
  raise RuntimeError("Unbekannter Fehler beim IMAP-Verbindungsaufbau.")


def _read_imap_banner(tls_socket, timeout_sec):
  tls_socket.settimeout(timeout_sec)
  chunks = []
  total = 0
  while total < 4096:
    data = tls_socket.recv(512)
    if not data:
      break
    chunks.append(data)
    total += len(data)
    if b"\n" in data:
      break
  return b"".join(chunks).decode("utf-8", errors="replace").strip()


def _raise_imap_stage_error(stage, host, port, exc=None, banner_text=""):
  host = str(host or "").strip() or "unbekannt"
  port_text = str(port or "").strip() or "?"
  detail = sanitize_text(exc) if exc is not None else ""
  detail_suffix = f" ({detail})" if detail else ""

  if stage == "resolve":
    raise RuntimeError(f"Der Servername {host} konnte nicht aufgeloest werden.{detail_suffix}") from exc
  if stage == "tcp":
    raise RuntimeError(f"Der Mailserver {host}:{port_text} antwortet nicht rechtzeitig oder ist lokal blockiert.{detail_suffix}") from exc
  if stage == "tls":
    raise RuntimeError(f"Die sichere SSL-Verbindung zu {host}:{port_text} konnte nicht aufgebaut werden.{detail_suffix}") from exc
  if stage == "banner":
    banner_preview = (banner_text or "").strip()
    if len(banner_preview) > 120:
      banner_preview = banner_preview[:117] + "..."
    extra = f" Antwort: {banner_preview}" if banner_preview else ""
    raise RuntimeError(f"Der Server antwortet nicht wie ein IMAP-Postfach.{extra}") from exc
  if stage == "login":
    raise RuntimeError(f"Der Login wurde vom Mailserver abgelehnt. Bitte Benutzername und Passwort pruefen.{detail_suffix}") from exc
  if stage == "inbox":
    raise RuntimeError(f"Der Posteingang konnte nach dem Login nicht geoeffnet werden.{detail_suffix}") from exc
  raise RuntimeError(detail or "Unbekannter IMAP-Fehler.") from exc


def _count_new_mail_uids(mail, last_uid, total_mails_hint=0):
  last_uid_int = int(last_uid or 0)
  if last_uid_int <= 0:
    return max(0, int(total_mails_hint or 0))

  try:
    status_search, search_data = mail.uid("SEARCH", None, "UID", f"{last_uid_int + 1}:*")
    if status_search == "OK":
      return len(search_data[0].split()) if search_data and search_data[0] else 0
  except Exception:
    pass

  status_search, search_data = mail.uid("SEARCH", None, "ALL")
  if status_search == "OK" and search_data and search_data[0]:
    return sum(1 for uid in search_data[0].split() if int(uid) > last_uid_int)
  return 0


def _run_imap_connection_check(host, port, user, pwd, last_uid, progress_callback=None, is_cancelled=None):
  host = str(host or "").strip()
  port = int(str(port or "993").strip() or "993")
  timeout_sec = max(10, int(MAIL_IMAP_TIMEOUT_SEC or 30))
  retry_timeout_sec = max(timeout_sec, int(MAIL_IMAP_RETRY_TIMEOUT_SEC or 60))
  mail = None
  try:
    _emit_task_progress(progress_callback, "Servername wird geprueft...")
    if callable(is_cancelled) and is_cancelled():
      return {"ok": False, "cancelled": True, "total_mails": "0", "new_mail_count": 0}

    try:
      socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
      _raise_imap_stage_error("resolve", host, port, exc=exc)

    _emit_task_progress(progress_callback, "SSL-Verbindung wird aufgebaut...")
    if callable(is_cancelled) and is_cancelled():
      return {"ok": False, "cancelled": True, "total_mails": "0", "new_mail_count": 0}

    try:
      mail = _create_imap_client(
        host,
        port,
        timeout_sec=timeout_sec,
        retry_timeout_sec=retry_timeout_sec,
        progress_callback=progress_callback,
      )
    except ssl.SSLError as exc:
      _raise_imap_stage_error("tls", host, port, exc=exc)
    except (socket.timeout, TimeoutError) as exc:
      try:
        _probe_tcp_connection(host, port, min(12, timeout_sec))
      except Exception as probe_exc:
        _raise_imap_stage_error("tcp", host, port, exc=probe_exc)
      _raise_imap_stage_error("tls", host, port, exc=exc)
    except OSError as exc:
      detail_lower = sanitize_text(exc).lower()
      if "handshake" in detail_lower or "ssl" in detail_lower or "tls" in detail_lower:
        _raise_imap_stage_error("tls", host, port, exc=exc)
      try:
        _probe_tcp_connection(host, port, min(12, timeout_sec))
      except Exception as probe_exc:
        _raise_imap_stage_error("tcp", host, port, exc=probe_exc)
      _raise_imap_stage_error("tls", host, port, exc=exc)

    banner_text = ""
    try:
      welcome = getattr(mail, "welcome", b"")
      if isinstance(welcome, bytes):
        banner_text = welcome.decode("utf-8", errors="replace").strip()
      else:
        banner_text = str(welcome or "").strip()
    except Exception:
      banner_text = ""

    if banner_text and not banner_text.startswith("*"):
      _raise_imap_stage_error("banner", host, port, banner_text=banner_text)

    if callable(is_cancelled) and is_cancelled():
      return {"ok": False, "cancelled": True, "total_mails": "0", "new_mail_count": 0}

    _emit_task_progress(progress_callback, "Anmeldung am Postfach...")
    try:
      mail.login(user, pwd)
    except imaplib.IMAP4.error as exc:
      _raise_imap_stage_error("login", host, port, exc=exc)
    except ssl.SSLError as exc:
      _raise_imap_stage_error("tls", host, port, exc=exc)
    except (socket.timeout, TimeoutError) as exc:
      _raise_imap_stage_error("tls", host, port, exc=exc)
    except OSError as exc:
      detail_lower = sanitize_text(exc).lower()
      if "handshake" in detail_lower or "ssl" in detail_lower or "tls" in detail_lower:
        _raise_imap_stage_error("tls", host, port, exc=exc)
      _raise_imap_stage_error("tcp", host, port, exc=exc)
    if callable(is_cancelled) and is_cancelled():
      return {"ok": False, "cancelled": True, "total_mails": "0", "new_mail_count": 0}

    _emit_task_progress(progress_callback, "Posteingang wird geoeffnet...")
    try:
      status, messages = mail.select("INBOX", readonly=True)
    except imaplib.IMAP4.error as exc:
      _raise_imap_stage_error("inbox", host, port, exc=exc)
    except (socket.timeout, TimeoutError) as exc:
      _raise_imap_stage_error("inbox", host, port, exc=exc)
    except OSError as exc:
      _raise_imap_stage_error("inbox", host, port, exc=exc)
    total_mails_str = messages[0].decode("utf-8") if messages and messages[0] else "0"
    total_mails_int = int(total_mails_str or 0)
    new_mail_count = 0

    if status == "OK":
      if int(last_uid or 0) > 0:
        _emit_task_progress(progress_callback, "Neue Mails seit dem letzten Scan werden geprueft...")
      else:
        _emit_task_progress(progress_callback, "Postfach verbunden. Uebersicht wird vorbereitet...")
      new_mail_count = _count_new_mail_uids(mail, last_uid, total_mails_int)

    return {
      "ok": status == "OK",
      "total_mails": total_mails_str,
      "new_mail_count": new_mail_count,
      "transport_text": str(getattr(mail, "_transport_text", "Direktes SSL ueber Port 993") or "Direktes SSL ueber Port 993"),
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

    self._migrate_old_settings()
    self._build_ui()
    self._load_accounts()
    self._show_secret_warnings()

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
    self.main_layout.setContentsMargins(18, 18, 18, 18)
    self.main_layout.setSpacing(14)

    hero_frame = QFrame(self)
    hero_frame.setObjectName("mailScraperHero")
    hero_frame.setStyleSheet(
      "QFrame#mailScraperHero { background-color: #171F33; border: 1px solid #28324E; border-radius: 24px; }"
    )
    hero_layout = QVBoxLayout(hero_frame)
    hero_layout.setContentsMargins(18, 18, 18, 18)
    hero_layout.setSpacing(12)

    lbl_title = QLabel("Automatische Beleg-Erfassung durchs E-Mail-Postfach")
    lbl_title.setStyleSheet("font-size: 24px; font-weight: bold; color: #00E4FF;")
    hero_layout.addWidget(lbl_title)

    lbl_safe = QLabel("Safe-Mode: Nur Lesezugriff.")
    lbl_safe.setStyleSheet("color: #4ADE80; font-size: 13px; font-weight: bold;")
    hero_layout.addWidget(lbl_safe)

    combo_style = (
      "QComboBox { background-color: #0F172A; color: #D6DEFF; border: 1px solid #304160; border-radius: 14px; padding: 10px 14px; }"
      "QComboBox::drop-down { border: none; width: 24px; }"
    )
    action_style = (
      "QPushButton { color: white; font-size: 15px; font-weight: bold; border-radius: 16px; padding: 12px 18px; border: 1px solid #43537A; }"
      "QPushButton:hover { border-color: #6C84BD; }"
      "QPushButton:disabled { color: #7B88A8; background-color: #222B44; border-color: #303A57; }"
    )

    acc_layout = QHBoxLayout()
    acc_layout.setSpacing(10)
    self.account_combo = QComboBox()
    self.account_combo.setStyleSheet(combo_style)
    self.account_combo.setMinimumHeight(48)
    self.account_combo.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
    acc_layout.addWidget(self.account_combo)

    self.btn_add_account = QPushButton("+")
    self.btn_add_account.setFixedSize(48, 48)
    self.btn_add_account.setStyleSheet(
      action_style +
      "QPushButton { background-color: #7C3AED; }"
      "QPushButton:hover { background-color: #8B5CF6; }"
    )
    self.btn_add_account.clicked.connect(self._on_add_account)
    acc_layout.addWidget(self.btn_add_account)
    hero_layout.addLayout(acc_layout)

    control_layout = QHBoxLayout()
    control_layout.setSpacing(10)

    self.btn_connect = QPushButton("Verbindung erneuern")
    self.btn_connect.setMinimumHeight(52)
    self.btn_connect.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_connect.setStyleSheet(
      action_style +
      "QPushButton { background-color: #6D28D9; }"
      "QPushButton:hover { background-color: #7C3AED; }"
    )
    self.btn_connect.clicked.connect(self._test_connection)
    control_layout.addWidget(self.btn_connect)

    self.combo_limit = QComboBox()
    self.combo_limit.setMinimumHeight(52)
    self.combo_limit.setCursor(Qt.CursorShape.PointingHandCursor)
    self.combo_limit.addItems(["Alle seit letztem Scan", "Letzte 5 Mails", "Letzte 10 Mails", "Letzte 15 Mails", "Letzte 20 Mails", "Letzte 50 Mails", "Alle Mails"])
    self.combo_limit.setStyleSheet(combo_style)
    control_layout.addWidget(self.combo_limit)

    self.btn_scan = QPushButton("Postfach nach Belegen scannen")
    self.btn_scan.setObjectName("ScannerBtn")
    self.btn_scan.setMinimumHeight(52)
    self.btn_scan.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_scan.setEnabled(False)
    self.btn_scan.setStyleSheet(
      action_style +
      "QPushButton { background-color: #0284C7; }"
      "QPushButton:hover { background-color: #0EA5E9; }"
    )
    self.btn_scan.clicked.connect(self._start_scan)
    control_layout.addWidget(self.btn_scan)

    hero_layout.addLayout(control_layout)
    self.main_layout.addWidget(hero_frame)

    self.pipeline_dashboard = MailPipelineDashboardWidget(self._mail_pipeline_icon_paths(), self)
    self.pipeline_dashboard.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
    self.main_layout.addWidget(self.pipeline_dashboard, 1)

    status_frame = QFrame(self)
    status_frame.setStyleSheet("QFrame { background-color: #141B2E; border: 1px solid #27304A; border-radius: 18px; }")
    status_layout = QVBoxLayout(status_frame)
    status_layout.setContentsMargins(16, 12, 16, 12)
    status_layout.setSpacing(8)

    self.lbl_busy_state = QLabel("")
    self.lbl_busy_state.setStyleSheet("color: #8FC7FF; font-size: 12px;")
    self.lbl_busy_state.setVisible(False)
    status_layout.addWidget(self.lbl_busy_state)

    self.progress_bar = QProgressBar()
    self.progress_bar.setVisible(False)
    self.progress_bar.setTextVisible(False)
    self.progress_bar.setFixedHeight(12)
    self.progress_bar.setStyleSheet(
      "QProgressBar { background-color: #0F172A; border: 1px solid #304160; border-radius: 6px; }"
      "QProgressBar::chunk { background-color: #00E4FF; border-radius: 6px; }"
    )
    status_layout.addWidget(self.progress_bar)
    self.main_layout.addWidget(status_frame)

    log_frame = QFrame(self)
    log_frame.setStyleSheet("QFrame { background-color: #12182A; border: 1px solid #222C45; border-radius: 18px; }")
    log_layout = QVBoxLayout(log_frame)
    log_layout.setContentsMargins(16, 14, 16, 14)
    log_layout.setSpacing(8)

    lbl_log = QLabel("Aktivitaetsprotokoll")
    lbl_log.setStyleSheet("font-size: 13px; font-weight: bold; color: #D6DEFF;")
    log_layout.addWidget(lbl_log)

    self.list_log = QListWidget()
    self.list_log.setMaximumHeight(150)
    self.list_log.setStyleSheet("background-color: #0F172A; border: 1px solid #26314D; border-radius: 14px; padding: 6px; color: #AAB4D6;")
    log_layout.addWidget(self.list_log)
    self.main_layout.addWidget(log_frame)

    self.pipeline_dashboard.reset("Bereit fuer einen neuen Scan.")
    self._log("Warte auf Verbindung...")

  def _log(self, message):
    item = QListWidgetItem(str(message))
    self.list_log.addItem(item)
    while self.list_log.count() > 250:
      self.list_log.takeItem(0)
    self.list_log.scrollToBottom()

  def _set_busy_hint(self, text=""):
    text = str(text or "").strip()
    self.lbl_busy_state.setText(text)
    self.lbl_busy_state.setVisible(bool(text))
    if hasattr(self, "pipeline_dashboard") and self.pipeline_dashboard is not None:
      if text:
        self.pipeline_dashboard.set_status_text(text)

  def _mail_pipeline_icon_paths(self):
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "assets", "icons"))
    return {
      "scan": os.path.join(base_dir, "mail_pipeline_scan.svg"),
      "screenshots": os.path.join(base_dir, "mail_pipeline_screenshots.svg"),
      "cloudscan": os.path.join(base_dir, "mail_pipeline_cloudscan.svg"),
    }

  def _reset_pipeline_visuals(self, message="Bereit fuer einen neuen Scan."):
    self.pipeline_dashboard.reset(message)

  def _on_mail_detected(self, raw_payload, current, total):
    raw_payload = raw_payload if isinstance(raw_payload, dict) else {}
    self.pipeline_dashboard.upsert_mail(raw_payload, state_key="scanned")
    if total:
      self.pipeline_dashboard.set_stage("scan", min(1.0, float(current) / float(total)))

  def _on_cloudscan_item_status(self, payload):
    payload = payload if isinstance(payload, dict) else {}
    raw_payload = payload.get("raw_email") if isinstance(payload.get("raw_email"), dict) else {}
    if not raw_payload:
      raw_payload = {
        "_pipeline_card_key": payload.get("mail_key", ""),
        "subject": payload.get("subject", ""),
        "sender": payload.get("sender", ""),
      }

    total = int(payload.get("total", 0) or 0)
    current = int(payload.get("current", 0) or 0)
    state = str(payload.get("state", "") or "").lower()

    if total:
      if state == "running":
        progress = max(0.0, float(max(0, current - 1)) / float(total))
      else:
        progress = min(1.0, float(current) / float(total))
      self.pipeline_dashboard.set_stage("cloudscan", progress)

    if state == "running":
      self.pipeline_dashboard.set_mail_state(raw_payload, "cloudscan")
    elif state == "done":
      self.pipeline_dashboard.set_mail_state(raw_payload, "done" if payload.get("success") else "error")
    elif state == "error":
      self.pipeline_dashboard.set_mail_state(raw_payload, "error")

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
        self._set_busy_hint("Konto gespeichert. Bitte Verbindung erneuern.")

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
    self._connect_task.progress_signal.connect(self._on_connection_progress)
    self._connect_task.result_signal.connect(self._on_connection_result)
    self._connect_task.error_signal.connect(self._on_connection_error)
    self._connect_task.finished_signal.connect(self._on_connection_finished)
    self._connect_task.start()

  def _on_connection_progress(self, task_id, payload):
    if task_id != self._connect_task_id:
      return

    if isinstance(payload, dict):
      text = str(payload.get("status_text", "") or "").strip()
    else:
      text = str(payload or "").strip()

    if text:
      self._set_busy_hint(text)

  def _on_connection_result(self, task_id, result):
    if task_id != self._connect_task_id:
      return

    result = result if isinstance(result, dict) else {}
    if result.get("ok"):
      total_mails_str = result.get("total_mails", "0")
      new_mail_count = int(result.get("new_mail_count", 0) or 0)
      transport_text = str(result.get("transport_text", "") or "").strip()
      self._log("Verbindung erfolgreich hergestellt!")
      if transport_text:
        self._log(f"Verbindungsweg: {transport_text}")
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

    err_msg = str(err_msg or "").strip()

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
    self._reset_pipeline_visuals("Postfachscan startet. Neue Mails erscheinen direkt als Karten.")
    self.pipeline_dashboard.set_stage("scan", 0.0)
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

    account_name = self.account_combo.currentText()
    self.scraper_thread = MailScraperThread(host, port, user, pwd, mail_limit, last_uid, self.settings_manager, account_idx, account_name)
    self.scraper_thread.log_signal.connect(self._log)
    self.scraper_thread.progress_signal.connect(self._update_progress)
    self.scraper_thread.mail_detected_signal.connect(self._on_mail_detected)
    self.scraper_thread.raw_signal.connect(self._on_raw_emails_fetched)
    self.scraper_thread.start()

  def _update_progress(self, current, total):
    self.progress_bar.setRange(0, total)
    self.progress_bar.setValue(current)
    if total:
      self.pipeline_dashboard.set_stage("scan", min(1.0, float(current) / float(total)))
      self._set_busy_hint(f"Postfach wird gelesen... ({current}/{total})")

  def _begin_screenshot_rendering(self, raw_emails):
    self._cleanup_render_job()
    self._log(f"Rendere {len(raw_emails)} E-Mail(s) als Screenshot fuer die KI...")
    self.progress_bar.setVisible(True)
    self.progress_bar.setRange(0, len(raw_emails))
    self.progress_bar.setValue(0)
    self.pipeline_dashboard.set_stage("screenshots", 0.0)
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
      self.pipeline_dashboard.set_stage("screenshots", min(1.0, float(current) / float(total)))
    status_text = str(payload.get("status_text", "") or "").strip()
    if status_text:
      self._set_busy_hint(status_text)
    log_message = str(payload.get("log_message", "") or "").strip()
    if log_message:
      self._log(log_message)

    raw_email = payload.get("raw_email") if isinstance(payload.get("raw_email"), dict) else None
    if raw_email:
      if payload.get("screenshot_path"):
        self.pipeline_dashboard.set_screenshot(raw_email, str(payload.get("screenshot_path") or ""))
      else:
        self.pipeline_dashboard.set_mail_state(raw_email, "rendering")

  def _on_render_finished(self, session_id, screenshot_paths):
    if session_id != self._active_scan_session_id:
      return

    self._render_job = None
    self.progress_bar.setRange(0, 0)
    self.pipeline_dashboard.set_stage("cloudscan", 0.0)
    self._set_busy_hint("Screenshots werden von der KI analysiert...")
    self._log(f"Sende {len(screenshot_paths)} Screenshot(s) an Gemini KI...")

    api_key = self.settings_manager.get("gemini_api_key", "")
    self.gemini_thread = GeminiEmailThread(api_key, screenshot_paths)
    self.gemini_thread.log_signal.connect(self._log)
    self.gemini_thread.item_status_signal.connect(self._on_cloudscan_item_status)
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
    self.pipeline_dashboard.set_status_text("Screenshot-Vorbereitung fehlgeschlagen.")
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
      self.pipeline_dashboard.reset("Keine neuen oder verwertbaren Mails gefunden.")
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
      self.pipeline_dashboard.finish_all()

      if not extracted_data_list:
        self.pipeline_dashboard.set_status_text("Cloudscan beendet, aber es wurden keine verwertbaren Belege erkannt.")
        self._log("Suche beendet. Keine neuen/verwertbaren Belege gefunden.")
        QMessageBox.information(self, "Ergebnis", "Keine neuen Belege in den letzten E-Mails gefunden.")
        return

      self.pipeline_dashboard.set_status_text(f"{len(extracted_data_list)} Beleg(e) wurden vorbereitet und koennen jetzt im Wizard geprueft werden.")
      self._log(f"Scanner fertig! {len(extracted_data_list)} Belege erfolgreich erkannt. Lade Ueberpruefungsfenster...")

      # --- Mail-UID Duplikat-Filter: bereits verarbeitete Mails ausfiltern ---
      filtered_data_list = extracted_data_list
      try:
        from module.database_manager import DatabaseManager
        dedup_db = DatabaseManager(self.settings_manager)
        original_count = len(extracted_data_list)
        filtered_data_list = []
        for mail_item in extracted_data_list:
          uid = str(mail_item.get("_mail_uid", "") or "").strip()
          if uid and dedup_db.mail_uid_exists(uid):
            continue
          filtered_data_list.append(mail_item)
        skipped_count = original_count - len(filtered_data_list)
        if skipped_count > 0:
          self._log(f"{skipped_count} von {original_count} Mails wurden bereits verarbeitet und uebersprungen.")
          if not filtered_data_list:
            QMessageBox.information(self, "Ergebnis", f"Alle {original_count} erkannten Mails wurden bereits verarbeitet.")
            return
      except Exception as dedup_err:
        log_exception(__name__, dedup_err)
        filtered_data_list = extracted_data_list

      dialog = ScraperReviewWizardDialog(list(reversed(filtered_data_list)), self.settings_manager, self)
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
  """Verarbeitet vorbereitete Mail-Quellen fuer Gemini, screenshot-first und sparsam."""
  log_signal = pyqtSignal(str)
  result_signal = pyqtSignal(list, int, int)
  item_status_signal = pyqtSignal(object)

  def __init__(self, api_key, screenshot_paths):
    super().__init__()
    self.api_key = api_key
    self.screenshot_paths = list(screenshot_paths or [])

  def _safe_text(self, value):
    return str(value or "").strip()

  def _dedupe_ordered(self, values):
    unique = []
    seen = set()
    for value in list(values or []):
      key = self._safe_text(value)
      if not key or key in seen:
        continue
      seen.add(key)
      unique.append(key)
    return unique

  def _has_meaningful_product(self, row):
    if not isinstance(row, dict):
      return False
    return bool(
      self._safe_text(row.get("produkt_name", ""))
      or self._safe_text(row.get("ean", ""))
      or self._safe_text(row.get("bild_url", ""))
      or self._safe_text(row.get("ekp_brutto", ""))
    )

  def _product_merge_key(self, row, fallback_index=None):
    if not isinstance(row, dict):
      return f"idx:{fallback_index}" if fallback_index is not None else ""
    produkt_name = self._safe_text(row.get("produkt_name", "")).lower()
    ean = self._safe_text(row.get("ean", "")).lower()
    bild_url = self._safe_text(row.get("bild_url", "")).lower()
    varianten = self._safe_text(row.get("varianten_info", "")).lower()
    if ean:
      return f"ean:{ean}"
    if produkt_name:
      return f"name:{produkt_name}|variant:{varianten}"
    if bild_url:
      return f"img:{bild_url}"
    return f"idx:{fallback_index}" if fallback_index is not None else ""

  def _detection_merge_key(self, row, fallback_index=None):
    if not isinstance(row, dict):
      return f"idx:{fallback_index}" if fallback_index is not None else ""
    parts = [
      self._safe_text(row.get("product_key", "")).lower(),
      self._safe_text(row.get("produkt_name_hint", "")).lower(),
      self._safe_text(row.get("ean", "")).lower(),
      self._safe_text(row.get("ware_index", row.get("waren_index", ""))),
      self._safe_text(row.get("x", "")),
      self._safe_text(row.get("y", "")),
      self._safe_text(row.get("width", "")),
      self._safe_text(row.get("height", "")),
    ]
    merged = "|".join(parts).strip("|")
    return merged or (f"idx:{fallback_index}" if fallback_index is not None else "")

  def _collect_missing_fields(self, result):
    result = result if isinstance(result, dict) else {}
    missing = []
    waren = result.get("waren") if isinstance(result.get("waren"), list) else []
    meaningful_rows = [row for row in waren if self._has_meaningful_product(row)]

    if not meaningful_rows:
      missing.append("waren")
    else:
      if all(not self._safe_text(row.get("produkt_name", "")) for row in meaningful_rows):
        missing.append("waren_unvollstaendig")

    if not self._safe_text(result.get("bestellnummer", "")):
      missing.append("bestellnummer")

    tracking = self._safe_text(result.get("tracking_nummer_einkauf", ""))
    paketdienst = self._safe_text(result.get("paketdienst", ""))
    if not tracking and not paketdienst:
      missing.append("tracking")

    has_sum = bool(self._safe_text(result.get("gesamt_ekp_brutto", "")))
    has_item_price = any(self._safe_text(row.get("ekp_brutto", "")) for row in meaningful_rows)
    if not has_sum and not has_item_price:
      missing.append("preise")

    return self._dedupe_ordered(missing)

  def _should_run_second_pass(self, prepared_scan, missing_fields):
    secondary_source = getattr(prepared_scan, "secondary_source", None)
    if not secondary_source:
      return False
    relevant_missing = set(self._dedupe_ordered(missing_fields))
    if not relevant_missing:
      return False

    source_type = str(secondary_source.source_type or "")
    relevant_by_source = {
      "mail_attachment": {"waren", "waren_unvollstaendig", "bestellnummer", "preise"},
      "email_message": {"waren", "waren_unvollstaendig", "bestellnummer", "tracking"},
      "mail_render_screenshot": {"waren", "waren_unvollstaendig", "bestellnummer", "tracking"},
    }
    useful_for = relevant_by_source.get(source_type, set())
    if not useful_for.intersection(relevant_missing):
      return False

    if source_type == "mail_attachment" and not self._safe_text(secondary_source.file_path):
      return False
    return True

  def _describe_scan_source(self, source):
    if source is None:
      return {}
    return {
      "source_type": str(getattr(source, "source_type", "") or ""),
      "original_name": self._safe_text(getattr(source, "original_name", "") or ""),
      "mime_type": self._safe_text(getattr(source, "mime_type", "") or ""),
      "file_path": self._safe_text(getattr(source, "file_path", "") or ""),
    }


  def _build_secondary_prompt_plan(self, prepared_scan, missing_fields):
    source = getattr(prepared_scan, "secondary_source", None)
    if source is None:
      return {}
    base_plan = dict(getattr(prepared_scan, "prompt_plan", {}) or {})
    extras = dict(base_plan.get("extras") or {})
    source_type = str(source.source_type or "")

    if source_type == "mail_attachment":
      prompt_class = "order_mail_pdf_primary"
      prompt_score = 86
    else:
      prompt_class = "order_mail_primary"
      prompt_score = 84 if source_type == "mail_render_screenshot" else 78

    extras.update({
      "secondary_pass": True,
      "secondary_source_type": source_type,
      "missing_fields": list(self._dedupe_ordered(missing_fields)),
    })
    base_plan.update({
      "prompt_class": prompt_class,
      "prompt_score": prompt_score,
      "prompt_reasoning_summary": "Ergaenzungspass fuer fehlende Felder mit der Sekundaerquelle.",
      "extras": extras,
    })
    return base_plan

  def _build_secondary_custom_text(self, prepared_scan, raw_email, missing_fields):
    source_plan = dict(getattr(prepared_scan, "source_plan", {}) or {})
    primary_source = getattr(prepared_scan, "primary_source", None)
    secondary_source = getattr(prepared_scan, "secondary_source", None)
    primary_name = self._safe_text(getattr(primary_source, "original_name", "") or getattr(primary_source, "source_type", ""))
    secondary_name = self._safe_text(getattr(secondary_source, "original_name", "") or getattr(secondary_source, "source_type", ""))
    tracking_links = []
    image_hints = []
    if getattr(prepared_scan, "sources", None):
      first_source = prepared_scan.sources[0]
      if isinstance(getattr(first_source, "extras", None), dict):
        tracking_links = list(first_source.extras.get("tracking_links", []) or [])
        image_hints = list(first_source.extras.get("image_hints", []) or [])

    parts = [
      "Dies ist ein gezielter Ergaenzungspass fuer eine Bestellmail.",
      f"Fehlende Felder aus Pass 1: {', '.join(self._dedupe_ordered(missing_fields))}.",
    ]
    if primary_name:
      parts.append(f"Pass 1 lief mit der Primaerquelle: {primary_name}.")
    if secondary_name:
      parts.append(f"Jetzt wird nur die Sekundaerquelle gelesen: {secondary_name}.")
    reasoning = self._safe_text(source_plan.get("source_reasoning_summary", ""))
    if reasoning:
      parts.append(f"Quellentscheidung: {reasoning}.")
    parts.append("Nutze diese Quelle nur zum Ergaenzen fehlender Felder. Bereits klare Werte aus Pass 1 nicht blind ueberschreiben.")

    if secondary_source and secondary_source.source_type == "mail_attachment":
      parts.append("Prioritaet in diesem Pass: Bestellnummer, Summen, Rechnungsdetails und fehlende Produktdetails aus der PDF.")
      pdf_hint = self._safe_text((getattr(secondary_source, "extras", {}) or {}).get("pdf_text_hint", ""))
      if pdf_hint:
        parts.append("PDF-Kurzinhalt:\n" + pdf_hint[:700])
    else:
      parts.append("Prioritaet in diesem Pass: Tracking, Versandhinweise, Buttons, Linktexte und sichtbare Produktbereiche aus der Mail.")
      mail_hint = self._safe_text(source_plan.get("mail_text_hint", ""))
      if mail_hint:
        parts.append("Mail-Kurzinhalt:\n" + mail_hint[:1500])
      if tracking_links:
        link_lines = []
        for link in tracking_links[:3]:
          href = self._safe_text(link.get("href", ""))
          text = self._safe_text(link.get("text", "") or "Link")
          if href:
            link_lines.append(f"- {text} -> {href}")
        if link_lines:
          parts.append("Moegliche Tracking-Links:\n" + "\n".join(link_lines))
      if image_hints:
        image_lines = []
        for hint in image_hints[:3]:
          src = self._safe_text(hint.get("src", ""))
          label = self._safe_text(hint.get("alt", "") or hint.get("title", "") or "Bild")
          if src:
            image_lines.append(f"- {label} -> {src}")
        if image_lines:
          parts.append("Moegliche Produktbild-Hinweise:\n" + "\n".join(image_lines))

    return "\n\n".join(part for part in parts if self._safe_text(part)).strip()

  def _merge_waren(self, primary_rows, secondary_rows):
    primary_list = [dict(row) for row in list(primary_rows or []) if isinstance(row, dict)]
    secondary_list = [dict(row) for row in list(secondary_rows or []) if isinstance(row, dict)]
    if not primary_list and not secondary_list:
      return [], []
    if not primary_list:
      adopted = [row for row in secondary_list if self._has_meaningful_product(row)]
      return adopted, (["waren"] if adopted else [])

    merged = list(primary_list)
    index_by_key = {}
    for idx, row in enumerate(merged):
      key = self._product_merge_key(row, idx)
      if key and key not in index_by_key:
        index_by_key[key] = idx

    changes = []
    for row in secondary_list:
      if not self._has_meaningful_product(row):
        continue
      key = self._product_merge_key(row)
      match_idx = index_by_key.get(key)
      if match_idx is None:
        row_ean = self._safe_text(row.get("ean", "")).lower()
        row_name = self._safe_text(row.get("produkt_name", "")).lower()
        for idx, existing in enumerate(merged):
          if row_ean and row_ean == self._safe_text(existing.get("ean", "")).lower():
            match_idx = idx
            break
          if row_name and row_name == self._safe_text(existing.get("produkt_name", "")).lower():
            match_idx = idx
            break

      if match_idx is None:
        merged.append(dict(row))
        new_idx = len(merged) - 1
        new_key = self._product_merge_key(row, new_idx)
        if new_key and new_key not in index_by_key:
          index_by_key[new_key] = new_idx
        changes.append("waren")
        continue

      target = merged[match_idx]
      row_changed = False
      for field_name in ("produkt_name", "varianten_info", "ean", "menge", "ekp_brutto", "bild_url"):
        if not self._safe_text(target.get(field_name, "")) and self._safe_text(row.get(field_name, "")):
          target[field_name] = row.get(field_name, "")
          row_changed = True
      if row_changed:
        changes.append("waren")

    return merged, self._dedupe_ordered(changes)

  def _merge_screenshot_detections(self, primary_rows, secondary_rows):
    primary_list = [dict(row) for row in list(primary_rows or []) if isinstance(row, dict)]
    secondary_list = [dict(row) for row in list(secondary_rows or []) if isinstance(row, dict)]
    if not primary_list and not secondary_list:
      return [], []
    if not primary_list:
      return secondary_list, (["screenshot_detections"] if secondary_list else [])

    merged = list(primary_list)
    known = set()
    for idx, row in enumerate(merged):
      key = self._detection_merge_key(row, idx)
      if key:
        known.add(key)

    added = False
    for row in secondary_list:
      key = self._detection_merge_key(row)
      if key and key in known:
        continue
      if key:
        known.add(key)
      merged.append(dict(row))
      added = True

    return merged, (["screenshot_detections"] if added else [])

  def _merge_second_pass(self, primary_result, secondary_result):
    merged = dict(primary_result or {})
    secondary_result = secondary_result if isinstance(secondary_result, dict) else {}
    added_fields = []

    fill_only_fields = (
      "bestellnummer",
      "kaufdatum",
      "shop_name",
      "gesamt_ekp_brutto",
      "versandkosten_brutto",
      "nebenkosten_brutto",
      "lieferdatum",
      "sendungsstatus",
    )
    for field_name in fill_only_fields:
      if not self._safe_text(merged.get(field_name, "")) and self._safe_text(secondary_result.get(field_name, "")):
        merged[field_name] = secondary_result.get(field_name, "")
        added_fields.append(field_name)

    if not self._safe_text(merged.get("tracking_nummer_einkauf", "")) and self._safe_text(secondary_result.get("tracking_nummer_einkauf", "")):
      merged["tracking_nummer_einkauf"] = secondary_result.get("tracking_nummer_einkauf", "")
      added_fields.append("tracking_nummer_einkauf")
    if not self._safe_text(merged.get("paketdienst", "")) and self._safe_text(secondary_result.get("paketdienst", "")):
      merged["paketdienst"] = secondary_result.get("paketdienst", "")
      added_fields.append("paketdienst")

    merged_waren, waren_changes = self._merge_waren(merged.get("waren", []), secondary_result.get("waren", []))
    if waren_changes:
      merged["waren"] = merged_waren
      added_fields.extend(waren_changes)

    merged_detections, detection_changes = self._merge_screenshot_detections(
      merged.get("screenshot_detections", []),
      secondary_result.get("screenshot_detections", []),
    )
    if detection_changes:
      merged["screenshot_detections"] = merged_detections
      added_fields.extend(detection_changes)

    return merged, self._dedupe_ordered(added_fields)

  def _run_optional_second_pass(self, raw, prepared_scan, primary_result):
    missing_fields = self._collect_missing_fields(primary_result)
    if not self._should_run_second_pass(prepared_scan, missing_fields):
      return primary_result, {
        "used": False,
        "missing_fields": missing_fields,
        "added_fields": [],
        "source_type": "",
        "prompt_class": "",
        "token_count": 0,
        "error": "",
      }

    secondary_source = prepared_scan.secondary_source
    secondary_prompt_plan = self._build_secondary_prompt_plan(prepared_scan, missing_fields)
    secondary_scan_decision = build_scan_decision_from_existing(
      secondary_prompt_plan,
      scan_mode=prepared_scan.scan_mode,
      source_plan=getattr(prepared_scan, "source_plan", {}) or {},
      primary_visual_source=self._describe_scan_source(secondary_source),
      secondary_context_source=self._describe_scan_source(getattr(prepared_scan, "primary_source", None)),
      should_allow_second_pass=False,
    ).to_dict()
    secondary_prompt_class = self._safe_text(secondary_prompt_plan.get("prompt_class", ""))
    secondary_custom_text = self._build_secondary_custom_text(prepared_scan, raw, missing_fields)
    secondary_image_path = self._safe_text(getattr(secondary_source, "file_path", "")) or None

    logging.info(
      "Gemini-Mail-Scan zweiter Pass: source_type=%s, prompt_class=%s, missing=%s",
      getattr(secondary_source, "source_type", ""),
      secondary_prompt_class,
      ",".join(missing_fields),
    )
    self._log(f" Ergaenzungspass mit {getattr(secondary_source, 'source_type', '')} fuer fehlende Felder: {', '.join(missing_fields)}")

    try:
      secondary_result = process_receipt_with_gemini(
        self.api_key,
        image_path=secondary_image_path,
        custom_text=secondary_custom_text,
        scan_mode=prepared_scan.scan_mode,
        prompt_profile=secondary_prompt_class,
        prompt_plan=secondary_prompt_plan,
        scan_decision=secondary_scan_decision,
      )
      secondary_tokens = int(secondary_result.pop("_token_count", 0) or 0) if isinstance(secondary_result, dict) else 0
      merged_result, added_fields = self._merge_second_pass(primary_result, secondary_result)
      logging.info(
        "Gemini-Mail-Scan Merge: source_type=%s, added=%s",
        getattr(secondary_source, "source_type", ""),
        ",".join(added_fields) or "none",
      )
      return merged_result, {
        "used": True,
        "missing_fields": missing_fields,
        "added_fields": added_fields,
        "source_type": getattr(secondary_source, "source_type", ""),
        "prompt_class": secondary_prompt_class,
        "scan_decision": secondary_scan_decision,
        "token_count": secondary_tokens,
        "error": "",
        "provider_meta": dict((secondary_result or {}).get("_provider_meta", {}) or {}) if isinstance(secondary_result, dict) else {},
      }
    except Exception as second_exc:
      app_error = second_exc if isinstance(second_exc, AppError) else classify_gemini_error(second_exc, phase="mail_scraper_second_pass")
      logging.warning(
        "Gemini-Mail-Scan zweiter Pass fehlgeschlagen: source_type=%s, reason=%s",
        getattr(secondary_source, "source_type", ""),
        app_error.user_message if isinstance(app_error, AppError) else str(second_exc),
      )
      self._log(" Ergaenzungspass fehlgeschlagen, Pass 1 bleibt massgeblich.")
      return primary_result, {
        "used": False,
        "missing_fields": missing_fields,
        "added_fields": [],
        "source_type": getattr(secondary_source, "source_type", ""),
        "prompt_class": secondary_prompt_class,
        "scan_decision": secondary_scan_decision,
        "token_count": 0,
        "error": app_error.user_message if isinstance(app_error, AppError) else str(second_exc),
      }

  def run(self):
    extracted_data = []

    for i, (img_path, raw) in enumerate(self.screenshot_paths):
      raw = raw if isinstance(raw, dict) else {}
      sender = raw.get("sender", "")
      email_date_raw = raw.get("date", "")
      subject = raw.get("subject", "")
      body_html = raw.get("body_html", "")
      body_text = raw.get("body_text", "")
      prepared_scan = prepare_mail_scan(raw, screenshot_path=img_path, scan_mode="einkauf")
      self.item_status_signal.emit({
        "state": "running",
        "current": i + 1,
        "total": len(self.screenshot_paths),
        "raw_email": dict(raw),
        "mail_key": str(raw.get("_pipeline_card_key", "") or ""),
        "subject": str(subject or ""),
        "sender": str(sender or ""),
      })
      self._log(f"[{i + 1}/{len(self.screenshot_paths)}] KI analysiert: {str(subject)[:40]}...")

      max_retries = 3
      retry_count = 0

      while retry_count < max_retries:
        try:
          time.sleep(2)
          logging.info(
            "Gemini-Mail-Scan startet: scan_mode=%s, source_mode=%s, prompt_class=%s, primary=%s, secondary=%s",
            prepared_scan.scan_mode,
            str((prepared_scan.source_plan or {}).get("scan_mode", "") or ""),
            str((prepared_scan.prompt_plan or {}).get("prompt_class", "") or ""),
            prepared_scan.primary_source.source_type if prepared_scan.primary_source else "",
            prepared_scan.secondary_source.source_type if prepared_scan.secondary_source else "",
          )
          primary_result = process_receipt_with_gemini(
            self.api_key,
            image_path=prepared_scan.gemini_image_path,
            custom_text=prepared_scan.gemini_custom_text,
            scan_mode=prepared_scan.scan_mode,
            prompt_profile=str((prepared_scan.prompt_plan or {}).get("prompt_class", "") or ""),
            prompt_plan=prepared_scan.prompt_plan,
            scan_decision=prepared_scan.scan_decision.to_dict() if getattr(prepared_scan, "scan_decision", None) else None,
          )

          if primary_result and isinstance(primary_result, dict):
            primary_tokens = int(primary_result.pop("_token_count", 0) or 0)
            merged_result, second_pass_info = self._run_optional_second_pass(raw, prepared_scan, primary_result)
            total_tokens = primary_tokens + int(second_pass_info.get("token_count", 0) or 0)

            merged_result["_original_email_html"] = body_html or body_text
            merged_result["_original_email_text"] = body_text or ""
            merged_result["_mail_cid_map"] = raw.get("cid_map", {}) or {}
            merged_result["_email_sender"] = str(sender)
            merged_result["_email_sender_domain"] = str(SafeMailRenderer.extract_sender_identity(sender).get("domain", "") or "")
            merged_result["_email_date"] = str(email_date_raw)
            merged_result["_scan_sources"] = [
              {
                "source_type": source.source_type,
                "original_name": source.original_name,
                "mime_type": source.mime_type,
                "file_path": source.file_path,
                "media_asset_id": source.metadata.get("media_asset_id") if isinstance(source.metadata, dict) else None,
                "media_key": source.metadata.get("media_key", "") if isinstance(source.metadata, dict) else "",
              }
              for source in prepared_scan.sources
            ]
            merged_result["_mail_tracking_links"] = list((prepared_scan.sources[0].extras.get("tracking_links", []) if prepared_scan.sources else []))
            merged_result["_mail_image_hints"] = list((prepared_scan.sources[0].extras.get("image_hints", []) if prepared_scan.sources else []))
            merged_result["_mail_logo_hints"] = list((prepared_scan.sources[0].extras.get("logo_hints", []) if prepared_scan.sources else []))
            primary_source = prepared_scan.primary_source
            primary_file_path = primary_source.file_path if primary_source else ""
            secondary_file_path = prepared_scan.secondary_source.file_path if prepared_scan.secondary_source else ""
            merged_result["_mail_review_attachments"] = sorted(
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
                  "pdf_classification": str(source.extras.get("pdf_classification", "") or ""),
                  "used_as_secondary_context": bool(prepared_scan.secondary_source and source.file_path and source.file_path == secondary_file_path),
                  "used_for_ai": bool(primary_file_path and source.file_path == primary_file_path),
                  "used_in_second_pass": bool(second_pass_info.get("used") and secondary_file_path and source.file_path == secondary_file_path),
                }
                for source in prepared_scan.sources
                if source.source_type == "mail_attachment" and source.file_path
              ],
              key=lambda row: (
                1 if row.get("used_for_ai") else 0,
                1 if row.get("used_in_second_pass") else 0,
                1 if row.get("pdf_is_relevant") else 0,
                int(row.get("pdf_relevance_score", 0) or 0),
              ),
              reverse=True,
            )
            merged_result["_mail_pdf_attachments"] = [
              dict(row)
              for row in merged_result["_mail_review_attachments"]
              if str(row.get("mime_type", "")).lower() == "application/pdf"
              or str(row.get("original_name", "")).lower().endswith(".pdf")
              or str(row.get("file_path", "")).lower().endswith(".pdf")
            ]
            merged_result["_primary_scan_source_type"] = primary_source.source_type if primary_source else ""
            merged_result["_primary_scan_file_path"] = primary_file_path
            merged_result["_primary_scan_media_asset_id"] = primary_source.metadata.get("media_asset_id") if primary_source and isinstance(primary_source.metadata, dict) else None
            merged_result["_secondary_scan_source_type"] = prepared_scan.secondary_source.source_type if prepared_scan.secondary_source else ""
            merged_result["_secondary_scan_file_path"] = secondary_file_path
            merged_result["_scan_source_plan"] = dict(prepared_scan.source_plan or {}) if isinstance(prepared_scan.source_plan, dict) else {}
            merged_result["_source_scan_mode"] = str((prepared_scan.source_plan or {}).get("scan_mode", "") or "")
            merged_result["_prompt_plan"] = dict(prepared_scan.prompt_plan or {}) if isinstance(prepared_scan.prompt_plan, dict) else {}
            merged_result["_prompt_class"] = str((prepared_scan.prompt_plan or {}).get("prompt_class", "") or "")
            merged_result["_scan_decision"] = prepared_scan.scan_decision.to_dict() if getattr(prepared_scan, "scan_decision", None) else {}
            merged_result["_scan_context"] = dict(getattr(prepared_scan, "scan_context", {}) or {})
            merged_result["_planner_info"] = dict(getattr(prepared_scan, "planner_info", {}) or {})
            merged_result["_second_pass_used"] = bool(second_pass_info.get("used", False))
            merged_result["_second_pass_missing_fields"] = list(second_pass_info.get("missing_fields", []) or [])
            merged_result["_second_pass_added_fields"] = list(second_pass_info.get("added_fields", []) or [])
            merged_result["_second_pass_source_type"] = str(second_pass_info.get("source_type", "") or "")
            merged_result["_second_pass_prompt_class"] = str(second_pass_info.get("prompt_class", "") or "")
            merged_result["_second_pass_scan_decision"] = dict(second_pass_info.get("scan_decision") or {}) if isinstance(second_pass_info, dict) else {}
            merged_result["_second_pass_error"] = str(second_pass_info.get("error", "") or "")
            if second_pass_info.get("provider_meta"):
              merged_result["_second_pass_provider_meta"] = dict(second_pass_info.get("provider_meta") or {})
            screenshot_source = next((source for source in prepared_scan.sources if source.source_type == "mail_render_screenshot" and isinstance(source.metadata, dict) and source.metadata.get("media_asset_id")), None)
            merged_result["_screenshot_media_asset_id"] = screenshot_source.metadata.get("media_asset_id") if screenshot_source else None
            merged_result["_pipeline_card_key"] = str(raw.get("_pipeline_card_key", "") or "")
            merged_result["_mail_uid"] = str(raw.get("_mail_uid", "") or "")
            merged_result["_mail_account"] = str(raw.get("_mail_account", "") or "")

            has_relevant_data = bool(
              merged_result.get("shop_name")
              or merged_result.get("gesamt_ekp_brutto")
              or merged_result.get("bestellnummer")
              or merged_result.get("tracking_nummer_einkauf")
              or (isinstance(merged_result.get("waren"), list) and len(merged_result.get("waren")) > 0)
            )

            if has_relevant_data:
              if second_pass_info.get("used"):
                added_text = ", ".join(second_pass_info.get("added_fields", []) or []) or "keine neuen Felder"
                self.log_signal.emit(f" Erfolgreich extrahiert (Tokens gesamt: {total_tokens}; Pass 2: {added_text})")
              else:
                self.log_signal.emit(f" Erfolgreich extrahiert (Tokens: {total_tokens})")
              extracted_data.append(merged_result)
            else:
              self.log_signal.emit(f" KI fand keine relevanten Daten (Tokens: {total_tokens})")

            self.item_status_signal.emit({
              "state": "done",
              "current": i + 1,
              "total": len(self.screenshot_paths),
              "raw_email": dict(raw),
              "mail_key": str(raw.get("_pipeline_card_key", "") or ""),
              "subject": str(subject or ""),
              "sender": str(sender or ""),
              "success": bool(has_relevant_data),
            })

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
              "scan_mode": str((prepared_scan.source_plan or {}).get("scan_mode", "") or ""),
              "prompt_class": str((prepared_scan.prompt_plan or {}).get("prompt_class", "") or ""),
              "source_types": [source.source_type for source in prepared_scan.sources],
            },
          )
          user_msg = app_error.user_message if isinstance(app_error, AppError) else str(e)
          retryable = bool(isinstance(app_error, AppError) and app_error.retryable)
          if retryable and retry_count < (max_retries - 1):
            retry_count += 1
            wait_s = 60 if app_error.category == "rate_limit" else 15
            self.log_signal.emit(f" KI temporaer nicht verfuegbar: {user_msg}")
            self.log_signal.emit(f" Warte {wait_s}s und versuche erneut ({retry_count + 1}/{max_retries})...")
            for t in range(wait_s, 0, -1):
              if t % 10 == 0 or t <= 5:
                self.log_signal.emit(f" ... {t}s verbleiben.")
              time.sleep(1)
            continue

          self.log_signal.emit(f" KI Fehler: {user_msg}")
          self.item_status_signal.emit({
            "state": "error",
            "current": i + 1,
            "total": len(self.screenshot_paths),
            "raw_email": dict(raw),
            "mail_key": str(raw.get("_pipeline_card_key", "") or ""),
            "subject": str(subject or ""),
            "sender": str(sender or ""),
            "success": False,
          })
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
  """Holt E-Mails im Lese-Modus und reicht nur relevante Rohdaten weiter."""
  log_signal = pyqtSignal(str)
  progress_signal = pyqtSignal(int, int)
  raw_signal = pyqtSignal(list, int, int)
  mail_detected_signal = pyqtSignal(dict, int, int)

  def __init__(self, host, port, user, pwd, mail_limit=5, last_uid=0, settings_manager=None, account_idx=-1, account_name=""):
    super().__init__()
    self.host = host
    self.port = port
    self.user = user
    self.pwd = pwd
    self.mail_limit = mail_limit
    self.last_uid = int(last_uid or 0)
    self.settings_manager = settings_manager
    self.account_idx = account_idx
    self.account_name = str(account_name or user or "")

  def run(self):
    raw_emails = []
    highest_uid = 0

    try:
      self.log_signal.emit("Verbinde (Read-Only)...")
      mail = _create_imap_client(self.host, self.port)
      mail.login(self.user, self.pwd)
      mail.select("INBOX", readonly=True)

      last_uid = self.last_uid
      if self.mail_limit == "SINCE_LAST" and last_uid > 0:
        status, messages = mail.uid("SEARCH", None, "UID", f"{last_uid + 1}:*")
        if status != "OK":
          status, messages = mail.uid("SEARCH", None, "ALL")
      else:
        status, messages = mail.uid("SEARCH", None, "ALL")
      if status != "OK":
        self.log_signal.emit(" Fehler beim Durchsuchen des Postfachs.")
        self.raw_signal.emit([], 0, self.account_idx)
        return

      mail_ids = messages[0].split() if messages and messages[0] else []

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
        self.log_signal.emit(f"Lade E-Mail {idx + 1}/{total}...")

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

          sender = msg.get("From", "Unbekannt")
          email_date_raw = msg.get("Date", "")
          self.log_signal.emit(f"-> Pruefe: {str(subject)[:50]}")

          keywords = [
            "bestell", "rechnung", "order", "auftrag", "zahl",
            "amazon", "paypal", "ebay", "pedido", "versand",
            "encomenda", "commande", "confirma",
          ]
          is_relevant = (
            any(kw in str(subject).lower() for kw in keywords)
            or any(kw in str(sender).lower() for kw in keywords)
          )
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

          self.log_signal.emit(" Mail eingesammelt fuer Screenshot-Rendering")
          mail_payload = {
            "subject": str(subject),
            "sender": str(sender),
            "date": str(email_date_raw),
            "body_html": body_html,
            "body_text": body_text,
            "cid_map": cid_map,
            "attachments": attachments,
            "_pipeline_card_key": f"mail-{uid_int}",
            "_mail_uid": str(uid_int),
            "_mail_account": str(self.account_name),
          }
          raw_emails.append(mail_payload)
          self.mail_detected_signal.emit(dict(mail_payload), idx + 1, total)

      self.progress_signal.emit(total, total)
      mail.logout()

    except Exception as e:
      log_exception(__name__, e)
      self.log_signal.emit(f"Kritischer Thread-Fehler: {str(e)}")

    self.raw_signal.emit(raw_emails, highest_uid, self.account_idx)






class ClickableLabel(QLabel):
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
      btn.clicked.connect(dlg.accept)
      layout.addWidget(btn)
      dlg.exec()
    super().mousePressEvent(event)


class ScraperReviewWizardDialog(QDialog):
  """
  Prueft erkannte E-Mails Schritt fuer Schritt.
  Links: Mail-Vorschau. Rechts: bearbeitbare Felder wie in Modul 1.
  """


  def __init__(self, data_list, settings_manager, parent=None):
    super().__init__(parent)
    self.settings_manager = settings_manager
    self.ean_service = EanService(self.settings_manager)
    self.ean_lookup_worker = None
    self._pending_ean_lookup_context = None
    self._logo_search_worker = None
    self._logo_search_service = ShopLogoSearchService(self.settings_manager)
    self.data_list = [dict(x) for x in (data_list or []) if isinstance(x, dict)]
    self.current_index = 0
    self._shared_db = None

    # --- Zentraler LookupService (fuer DB-Lookups VOR API-Calls) ---
    from module.database_manager import DatabaseManager
    self._lookup_db = DatabaseManager(self.settings_manager)
    self._lookup_service = LookupService(self._lookup_db)
    self._preview_processes = []
    self._mapping_done_by_index = {}
    self._mapping_prompted_by_index = {}
    self._mapping_state_by_index = {}
    self._active_mapping_panel = None
    self._current_order_review_bundle = None
    self._current_rechnung_pdf_path = ""
    self.summary = {
      "saved": 0,
      "skipped": 0,
      "discarded": 0,
      "renamed": 0,
    }

    self.setWindowTitle("E-Mails einzeln pruefen")
    self.resize(1680, 900)
    self.setMinimumSize(1280, 720)
    self.setStyleSheet("background-color: #1a1b26; color: #DADADA;")

    self._build_ui()

    if not self.data_list:
      QTimer.singleShot(0, self.reject)
      return

    self._load_current_mail()

  def _build_ui(self):
    layout = QVBoxLayout(self)
    layout.setContentsMargins(12, 8, 12, 8)
    layout.setSpacing(6)

    # ── Mail-Info-Banner: Absender, Betreff, Datum ─────────────────────
    self.mail_info_banner = QFrame()
    self.mail_info_banner.setStyleSheet(
        "QFrame { background-color: #1f2335; border: 1px solid #414868; border-radius: 8px; }"
    )
    banner_layout = QHBoxLayout(self.mail_info_banner)
    banner_layout.setContentsMargins(14, 8, 14, 8)
    banner_layout.setSpacing(12)

    # Fortschritt + Duplikat-Badge (links)
    progress_col = QVBoxLayout()
    progress_col.setSpacing(2)
    self.lbl_progress = QLabel("")
    self.lbl_progress.setStyleSheet("font-size: 15px; font-weight: bold; color: #7aa2f7; border: none;")
    progress_col.addWidget(self.lbl_progress)

    self.lbl_duplicate_badge = QLabel("")
    self.lbl_duplicate_badge.setStyleSheet(
        "font-size: 11px; font-weight: bold; color: #ff9e64; background-color: #3c2418;"
        " border: 1px solid #ff9e64; border-radius: 4px; padding: 2px 6px;"
    )
    self.lbl_duplicate_badge.setVisible(False)
    progress_col.addWidget(self.lbl_duplicate_badge)
    banner_layout.addLayout(progress_col)

    # Trennlinie
    sep1 = QFrame()
    sep1.setFrameShape(QFrame.Shape.VLine)
    sep1.setStyleSheet("color: #414868; border: none; background-color: #414868; max-width: 1px;")
    banner_layout.addWidget(sep1)

    # Mail-Metadaten (Mitte)
    mail_meta_col = QVBoxLayout()
    mail_meta_col.setSpacing(1)
    self.lbl_mail_sender = QLabel("")
    self.lbl_mail_sender.setStyleSheet("font-size: 13px; font-weight: bold; color: #c0caf5; border: none;")
    self.lbl_mail_sender.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
    mail_meta_col.addWidget(self.lbl_mail_sender)
    self.lbl_mail_subject = QLabel("")
    self.lbl_mail_subject.setStyleSheet("font-size: 12px; color: #a9b1d6; border: none;")
    self.lbl_mail_subject.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
    mail_meta_col.addWidget(self.lbl_mail_subject)
    banner_layout.addLayout(mail_meta_col, 1)

    # Datum (rechts)
    self.lbl_mail_date = QLabel("")
    self.lbl_mail_date.setStyleSheet("font-size: 12px; color: #7aa2f7; border: none;")
    self.lbl_mail_date.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
    banner_layout.addWidget(self.lbl_mail_date)

    # Mapping-Status-Badge (rechts aussen)
    self.lbl_mapping_badge = QLabel("")
    self.lbl_mapping_badge.setStyleSheet(
        "font-size: 11px; font-weight: bold; color: #9ece6a; background-color: #203225;"
        " border: 1px solid #9ece6a; border-radius: 4px; padding: 2px 8px;"
    )
    self.lbl_mapping_badge.setVisible(False)
    banner_layout.addWidget(self.lbl_mapping_badge)

    self.lbl_konfidenz_badge = QLabel("")
    self.lbl_konfidenz_badge.setStyleSheet(
        "font-size: 11px; font-weight: bold; color: #9ece6a; background-color: #203225;"
        " border: 1px solid #9ece6a; border-radius: 4px; padding: 2px 8px;"
    )
    self.lbl_konfidenz_badge.setVisible(False)
    banner_layout.addWidget(self.lbl_konfidenz_badge)

    self.lbl_absender_badge = QLabel("")
    self.lbl_absender_badge.setStyleSheet(
        "font-size: 11px; font-weight: bold; color: #a9b1d6; background-color: #2a2a3a;"
        " border: 1px solid #6b7280; border-radius: 4px; padding: 2px 8px;"
    )
    self.lbl_absender_badge.setVisible(False)
    banner_layout.addWidget(self.lbl_absender_badge)

    layout.addWidget(self.mail_info_banner)

    # ── Mail-Navigationsleiste: klickbare Status-Dots ──────────────────
    self.nav_bar = QFrame()
    self.nav_bar.setStyleSheet(
        "QFrame { background-color: #1a1b26; border: none; }"
    )
    nav_layout = QHBoxLayout(self.nav_bar)
    nav_layout.setContentsMargins(4, 2, 4, 2)
    nav_layout.setSpacing(4)

    self.btn_nav_prev = QPushButton("<")
    self.btn_nav_prev.setFixedSize(28, 24)
    self.btn_nav_prev.setStyleSheet(
        "QPushButton { background-color: #24283b; color: #7aa2f7; border: 1px solid #414868;"
        " border-radius: 4px; font-weight: bold; font-size: 13px; }"
        "QPushButton:hover { background-color: #2f3452; }"
        "QPushButton:disabled { color: #414868; }"
    )
    self.btn_nav_prev.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_nav_prev.clicked.connect(self._nav_go_prev)
    nav_layout.addWidget(self.btn_nav_prev)

    self._nav_dot_buttons = []
    self._nav_dots_container = QHBoxLayout()
    self._nav_dots_container.setSpacing(3)
    for idx in range(len(self.data_list)):
      dot = QPushButton("")
      dot.setFixedSize(24, 24)
      dot.setCursor(Qt.CursorShape.PointingHandCursor)
      dot.setToolTip(f"Mail {idx + 1}")
      dot.clicked.connect(lambda checked, i=idx: self._nav_go_to(i))
      self._nav_dot_buttons.append(dot)
      self._nav_dots_container.addWidget(dot)
    nav_layout.addLayout(self._nav_dots_container)

    self.btn_nav_next = QPushButton(">")
    self.btn_nav_next.setFixedSize(28, 24)
    self.btn_nav_next.setStyleSheet(
        "QPushButton { background-color: #24283b; color: #7aa2f7; border: 1px solid #414868;"
        " border-radius: 4px; font-weight: bold; font-size: 13px; }"
        "QPushButton:hover { background-color: #2f3452; }"
        "QPushButton:disabled { color: #414868; }"
    )
    self.btn_nav_next.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_nav_next.clicked.connect(self._nav_go_next)
    nav_layout.addWidget(self.btn_nav_next)
    nav_layout.addStretch()

    # Status-Tracking pro Mail
    self._mail_status = ["pending"] * len(self.data_list)

    layout.addWidget(self.nav_bar)

    self.content_splitter = QSplitter(Qt.Orientation.Horizontal, self)
    self.content_splitter.setChildrenCollapsible(False)
    layout.addWidget(self.content_splitter, 1)

    left_panel = QWidget(self)
    left_panel.setMinimumWidth(420)
    left_box = QVBoxLayout(left_panel)
    left_box.setContentsMargins(0, 0, 0, 0)
    left_box.setSpacing(10)
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
    self.preview_web.setMinimumWidth(440)
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
      self.pdf_preview_widget.setMinimumWidth(440)
      self.pdf_preview_widget.setStyleSheet("background-color: #10111a; border: 1px solid #414868; border-radius: 6px;")
      attachment_layout.addWidget(self.pdf_preview_widget, 1)
    else:
      self.pdf_preview_web = QWebEngineView(self)
      self.pdf_preview_web.setMinimumWidth(440)
      self.pdf_preview_web.setStyleSheet("background-color: #10111a; border: 1px solid #414868; border-radius: 6px;")
      self._configure_pdf_preview_view(self.pdf_preview_web)
      self.pdf_preview_widget = self.pdf_preview_web
      attachment_layout.addWidget(self.pdf_preview_web, 1)

    self.btn_open_large_attachment_preview = QPushButton("PDF-Anhang gross oeffnen")
    self.btn_open_large_attachment_preview.clicked.connect(self._open_large_attachment_preview)
    attachment_layout.addWidget(self.btn_open_large_attachment_preview)
    self.preview_tabs.addTab(attachment_tab, "PDF-Anhang")
    self.content_splitter.addWidget(left_panel)

    # ── Tab-basiertes Mittel-Panel ────────────────────────────────────
    self.data_tabs = QTabWidget(self)
    self.data_tabs.setMinimumWidth(480)
    self.data_tabs.setStyleSheet(
        "QTabWidget::pane { border: 1px solid #414868; border-radius: 6px; background: transparent; }"
        "QTabBar::tab { background: #1a1b26; color: #a9b1d6; padding: 8px 18px; border: 1px solid #414868; border-bottom: none; border-top-left-radius: 6px; border-top-right-radius: 6px; margin-right: 2px; }"
        "QTabBar::tab:selected { background: #1f2335; color: #7aa2f7; font-weight: bold; }"
        "QTabBar::tab:hover { background: #292e42; }"
    )

    # --- Tab 1: Kopfdaten ---
    kopf_scroll = QScrollArea()
    kopf_scroll.setWidgetResizable(True)
    kopf_scroll.setFrameShape(QFrame.Shape.NoFrame)
    kopf_scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")
    kopf_panel = QWidget()
    kopf_box = QVBoxLayout(kopf_panel)
    kopf_box.setContentsMargins(0, 8, 12, 0)
    kopf_box.setSpacing(12)

    self.einkauf_form_widget = EinkaufHeadFormWidget(self)
    self.einkauf_form_widget.logoSearchRequested.connect(self._on_manual_logo_search_requested)
    self.inputs = self.einkauf_form_widget.inputs
    self.einkauf_form_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Maximum)
    kopf_box.addWidget(self.einkauf_form_widget)

    # Rechnungs-Sektion (Platzhalter fuer Phase D1)
    self.rechnung_frame = QFrame()
    self.rechnung_frame.setStyleSheet("QFrame { background-color: #1f2335; border: 1px solid #414868; border-radius: 6px; }")
    rechnung_layout = QVBoxLayout(self.rechnung_frame)
    rechnung_layout.setContentsMargins(10, 8, 10, 8)
    rechnung_layout.setSpacing(6)
    lbl_rechnung_title = QLabel("Rechnung")
    lbl_rechnung_title.setStyleSheet("font-size: 13px; font-weight: bold; color: #7aa2f7; border: none;")
    rechnung_layout.addWidget(lbl_rechnung_title)
    self.chk_rechnung_vorhanden = QCheckBox("Rechnung vorhanden")
    self.chk_rechnung_vorhanden.setStyleSheet("color: #c0caf5; font-size: 12px; border: none;")
    rechnung_layout.addWidget(self.chk_rechnung_vorhanden)
    self.btn_rechnung_pdf = QPushButton("PDF als Rechnung verknuepfen")
    self.btn_rechnung_pdf.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_rechnung_pdf.setStyleSheet("font-size: 12px; padding: 4px 10px;")
    self.btn_rechnung_pdf.setVisible(False)
    self.btn_rechnung_pdf.clicked.connect(self._on_link_invoice_pdf)
    rechnung_layout.addWidget(self.btn_rechnung_pdf)
    self.lbl_rechnung_pdf_path = QLabel("")
    self.lbl_rechnung_pdf_path.setStyleSheet("font-size: 11px; color: #a9b1d6; border: none;")
    self.lbl_rechnung_pdf_path.setWordWrap(True)
    rechnung_layout.addWidget(self.lbl_rechnung_pdf_path)
    kopf_box.addWidget(self.rechnung_frame)

    kopf_box.addStretch(1)
    kopf_scroll.setWidget(kopf_panel)
    self.data_tabs.addTab(kopf_scroll, "Kopfdaten")

    # --- Tab 2: Artikel ---
    artikel_scroll = QScrollArea()
    artikel_scroll.setWidgetResizable(True)
    artikel_scroll.setFrameShape(QFrame.Shape.NoFrame)
    artikel_scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")
    artikel_panel = QWidget()
    artikel_box = QVBoxLayout(artikel_panel)
    artikel_box.setContentsMargins(0, 8, 12, 0)
    artikel_box.setSpacing(12)

    self.einkauf_items_widget = EinkaufItemsTableWidget(self)
    self.einkauf_items_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
    self.einkauf_items_widget.table.setMinimumHeight(280)
    self.table_waren = self.einkauf_items_widget.table
    artikel_box.addWidget(self.einkauf_items_widget, 1)

    self.summen_banner = SummenBannerWidget(self)
    artikel_box.addWidget(self.summen_banner)

    self.einkauf_items_widget.eanLookupRequested.connect(lambda _ctx: self._lookup_ean_for_selected_row())

    # Zeilen-Management-Buttons
    items_btn_row = QHBoxLayout()
    self.btn_add_row = QPushButton("+ Zeile")
    self.btn_add_row.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_add_row.setStyleSheet("font-size: 12px; padding: 4px 10px;")
    self.btn_add_row.clicked.connect(self._on_add_item_row)
    items_btn_row.addWidget(self.btn_add_row)
    self.btn_remove_row = QPushButton("- Zeile")
    self.btn_remove_row.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_remove_row.setStyleSheet("font-size: 12px; padding: 4px 10px;")
    self.btn_remove_row.clicked.connect(self._on_remove_item_row)
    items_btn_row.addWidget(self.btn_remove_row)
    items_btn_row.addStretch()
    artikel_box.addLayout(items_btn_row)

    artikel_scroll.setWidget(artikel_panel)
    self.data_tabs.addTab(artikel_scroll, "Artikel")

    # --- Tab 3: Uebersicht ---
    uebersicht_scroll = QScrollArea()
    uebersicht_scroll.setWidgetResizable(True)
    uebersicht_scroll.setFrameShape(QFrame.Shape.NoFrame)
    uebersicht_scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")
    uebersicht_panel = QWidget()
    uebersicht_box = QVBoxLayout(uebersicht_panel)
    uebersicht_box.setContentsMargins(0, 8, 12, 0)
    uebersicht_box.setSpacing(12)

    self.order_review_widget = OrderReviewPanelWidget(self)
    self.order_review_widget.setMinimumHeight(96)
    uebersicht_box.addWidget(self.order_review_widget)

    # Auto-Mapping-Log
    lbl_mapping_log = QLabel("Auto-Mapping")
    lbl_mapping_log.setStyleSheet("font-size: 13px; font-weight: bold; color: #7aa2f7;")
    uebersicht_box.addWidget(lbl_mapping_log)
    self.lbl_auto_mapping_log = QLabel("Keine automatischen Mappings.")
    self.lbl_auto_mapping_log.setWordWrap(True)
    self.lbl_auto_mapping_log.setStyleSheet("font-size: 12px; color: #a9b1d6; background-color: #1f2335; border: 1px solid #414868; border-radius: 6px; padding: 8px;")
    uebersicht_box.addWidget(self.lbl_auto_mapping_log)

    # Warnungen
    lbl_warnings_title = QLabel("Warnungen")
    lbl_warnings_title.setStyleSheet("font-size: 13px; font-weight: bold; color: #f7c66f;")
    uebersicht_box.addWidget(lbl_warnings_title)
    self.lbl_warnings = QLabel("Keine Warnungen.")
    self.lbl_warnings.setWordWrap(True)
    self.lbl_warnings.setStyleSheet("font-size: 12px; color: #a9b1d6; background-color: #1f2335; border: 1px solid #414868; border-radius: 6px; padding: 8px;")
    uebersicht_box.addWidget(self.lbl_warnings)

    # Validierungs-Checkliste
    lbl_validation_title = QLabel("Validierung")
    lbl_validation_title.setStyleSheet("font-size: 13px; font-weight: bold; color: #9ece6a;")
    uebersicht_box.addWidget(lbl_validation_title)
    self.lbl_validation_checklist = QLabel("")
    self.lbl_validation_checklist.setWordWrap(True)
    self.lbl_validation_checklist.setStyleSheet("font-size: 12px; color: #c0caf5; background-color: #1f2335; border: 1px solid #414868; border-radius: 6px; padding: 8px;")
    uebersicht_box.addWidget(self.lbl_validation_checklist)

    uebersicht_box.addStretch(1)
    uebersicht_scroll.setWidget(uebersicht_panel)
    self.data_tabs.addTab(uebersicht_scroll, "Uebersicht")

    self.content_splitter.addWidget(self.data_tabs)

    mapping_panel = QWidget(self)
    mapping_panel.setMinimumWidth(300)
    mapping_panel.setMaximumWidth(420)
    mapping_box = QVBoxLayout(mapping_panel)
    mapping_box.setContentsMargins(0, 0, 0, 0)
    mapping_box.setSpacing(12)

    lbl_mapping = QLabel("Mapping und Normalisierung")
    lbl_mapping.setStyleSheet("font-size: 14px; font-weight: bold;")
    mapping_box.addWidget(lbl_mapping)

    self.lbl_mapping_state = QLabel("")
    self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #f7c66f;")
    self.btn_run_mapping = QPushButton("Mapping jetzt starten")
    self.btn_run_mapping.clicked.connect(self._on_run_mapping_clicked)

    map_row = QHBoxLayout()
    map_row.addWidget(self.lbl_mapping_state, 1)
    map_row.addWidget(self.btn_run_mapping)
    mapping_box.addLayout(map_row)

    self.lbl_mapping_side_hint = QLabel("Hier bleibt das eingebettete Mapping sichtbar, waehrend du links Vorschau und mittig Daten pruefst.")
    self.lbl_mapping_side_hint.setWordWrap(True)
    self.lbl_mapping_side_hint.setStyleSheet("font-size: 12px; color: #a9b1d6;")
    mapping_box.addWidget(self.lbl_mapping_side_hint)

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
    mapping_box.addWidget(self.mapping_frame)
    mapping_box.addStretch(1)
    self.mapping_panel_widget = mapping_panel
    self.content_splitter.addWidget(mapping_panel)

    self.content_splitter.setStretchFactor(0, 5)
    self.content_splitter.setStretchFactor(1, 5)
    self.content_splitter.setStretchFactor(2, 3)
    self.content_splitter.setSizes([620, 560, 340])

    # ── Footer: Buttons + Live-Stats ─────────────────────────────────────
    footer_frame = QFrame()
    footer_frame.setStyleSheet(
        "QFrame { background-color: #1f2335; border: 1px solid #414868; border-radius: 8px; }"
    )
    footer_layout = QHBoxLayout(footer_frame)
    footer_layout.setContentsMargins(12, 6, 12, 6)
    footer_layout.setSpacing(8)

    self.btn_cancel = QPushButton("Wizard beenden")
    self.btn_cancel.setStyleSheet(
        "QPushButton { background-color: #24283b; color: #a9b1d6; border: 1px solid #414868;"
        " border-radius: 6px; padding: 8px 16px; font-size: 13px; }"
        "QPushButton:hover { background-color: #2f3452; border-color: #7aa2f7; }"
    )
    self.btn_cancel.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_cancel.clicked.connect(self._on_cancel)
    footer_layout.addWidget(self.btn_cancel)

    # Live-Statistiken
    self.lbl_stats = QLabel("")
    self.lbl_stats.setTextFormat(Qt.TextFormat.RichText)
    self.lbl_stats.setStyleSheet("font-size: 12px; color: #a9b1d6; border: none;")
    footer_layout.addWidget(self.lbl_stats, 1)

    self.btn_discard = QPushButton("Verwerfen")
    self.btn_discard.setStyleSheet(
        "QPushButton { background-color: #2d1f2b; color: #f7768e; border: 1px solid #f7768e;"
        " border-radius: 6px; padding: 8px 16px; font-size: 13px; font-weight: bold; }"
        "QPushButton:hover { background-color: #3c2433; }"
    )
    self.btn_discard.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_discard.setToolTip("Mail verwerfen (Ctrl+D)")
    self.btn_discard.clicked.connect(self._discard_current)
    footer_layout.addWidget(self.btn_discard)

    self.btn_skip = QPushButton("Ueberspringen")
    self.btn_skip.setStyleSheet(
        "QPushButton { background-color: #3a3117; color: #f7c66f; border: 1px solid #f7c66f;"
        " border-radius: 6px; padding: 8px 16px; font-size: 13px; font-weight: bold; }"
        "QPushButton:hover { background-color: #4a411f; }"
    )
    self.btn_skip.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_skip.setToolTip("Mail ueberspringen (Ctrl+K)")
    self.btn_skip.clicked.connect(self._skip_current)
    footer_layout.addWidget(self.btn_skip)

    self.btn_save_next = QPushButton("Speichern und weiter")
    self.btn_save_next.setStyleSheet(
        "QPushButton { background-color: #203225; color: #9ece6a; border: 1px solid #9ece6a;"
        " border-radius: 6px; padding: 8px 20px; font-size: 14px; font-weight: bold; }"
        "QPushButton:hover { background-color: #2a4232; }"
    )
    self.btn_save_next.setCursor(Qt.CursorShape.PointingHandCursor)
    self.btn_save_next.setToolTip("Speichern und weiter (Ctrl+S)")
    self.btn_save_next.clicked.connect(self._save_current_and_next)
    footer_layout.addWidget(self.btn_save_next)

    layout.addWidget(footer_frame)

    # ── Keyboard-Shortcuts ─────────────────────────────────────────────
    from PyQt6.QtGui import QShortcut, QKeySequence
    QShortcut(QKeySequence("Ctrl+S"), self).activated.connect(self._save_current_and_next)
    QShortcut(QKeySequence("Ctrl+K"), self).activated.connect(self._skip_current)
    QShortcut(QKeySequence("Ctrl+D"), self).activated.connect(self._discard_current)

  def _set_progress_text(self):
    total = len(self.data_list)
    current_human = self.current_index + 1
    self.lbl_progress.setText(f"Mail {current_human}/{total}")
    if current_human >= total:
      self.btn_save_next.setText("Speichern und abschliessen")
    else:
      self.btn_save_next.setText("Speichern und weiter")
    # Mail-Info-Banner aktualisieren
    self._update_mail_info_banner()

  def _update_mail_info_banner(self):
    """Aktualisiert Absender, Betreff, Datum im oberen Banner."""
    item = self._current_mail_record()
    sender = str(item.get("_email_sender", "") or "").strip()
    subject = str(item.get("subject", "") or item.get("_email_subject", "") or "").strip()
    date = str(item.get("_email_date", "") or "").strip()
    self.lbl_mail_sender.setText(sender or "Absender unbekannt")
    self.lbl_mail_subject.setText(subject or "Kein Betreff")
    self.lbl_mail_date.setText(date or "")

  # ── Navigationsleiste ──────────────────────────────────────────────────

  _NAV_DOT_STYLES = {
      "pending":   "background-color: #414868; border: 2px solid #414868; border-radius: 12px;",
      "current":   "background-color: #7aa2f7; border: 2px solid #7aa2f7; border-radius: 12px;",
      "saved":     "background-color: #9ece6a; border: 2px solid #9ece6a; border-radius: 12px;",
      "skipped":   "background-color: #f7c66f; border: 2px solid #f7c66f; border-radius: 12px;",
      "discarded": "background-color: #f7768e; border: 2px solid #f7768e; border-radius: 12px;",
  }

  def _update_nav_dots(self):
    """Aktualisiert Farben und Enabled-State aller Navigation-Dots."""
    for idx, dot in enumerate(self._nav_dot_buttons):
      if idx == self.current_index:
        style = self._NAV_DOT_STYLES["current"]
      else:
        style = self._NAV_DOT_STYLES.get(self._mail_status[idx], self._NAV_DOT_STYLES["pending"])
      dot.setStyleSheet(f"QPushButton {{ {style} }} QPushButton:hover {{ border-color: #7aa2f7; }}")
      # Bereits verarbeitete Mails sind nicht erneut ansteuerbar
      dot.setEnabled(self._mail_status[idx] == "pending" or idx == self.current_index)
    has_prev_pending = any(self._mail_status[i] == "pending" for i in range(self.current_index - 1, -1, -1))
    has_next_pending = any(self._mail_status[i] == "pending" for i in range(self.current_index + 1, len(self.data_list)))
    self.btn_nav_prev.setEnabled(has_prev_pending)
    self.btn_nav_next.setEnabled(has_next_pending)

  def _nav_go_to(self, idx):
    """Springt zu einer bestimmten Mail (nur wenn Status=pending)."""
    if idx == self.current_index:
      return
    if 0 <= idx < len(self.data_list) and self._mail_status[idx] == "pending":
      self.current_index = idx
      self._load_current_mail()

  def _nav_go_prev(self):
    """Zur vorherigen pending Mail."""
    for idx in range(self.current_index - 1, -1, -1):
      if self._mail_status[idx] == "pending":
        self._nav_go_to(idx)
        return

  def _nav_go_next(self):
    """Zur naechsten pending Mail."""
    for idx in range(self.current_index + 1, len(self.data_list)):
      if self._mail_status[idx] == "pending":
        self._nav_go_to(idx)
        return

  def _update_footer_stats(self):
    """Aktualisiert die Live-Statistiken im Footer."""
    total = len(self.data_list)
    saved = self.summary.get("saved", 0)
    skipped = self.summary.get("skipped", 0)
    discarded = self.summary.get("discarded", 0)
    pending = total - saved - skipped - discarded
    parts = []
    if saved:
      parts.append(f"<span style='color: #9ece6a;'>{saved} gespeichert</span>")
    if skipped:
      parts.append(f"<span style='color: #f7c66f;'>{skipped} uebersprungen</span>")
    if discarded:
      parts.append(f"<span style='color: #f7768e;'>{discarded} verworfen</span>")
    parts.append(f"<span style='color: #a9b1d6;'>{pending} offen</span>")
    self.lbl_stats.setText(" | ".join(parts))

  def _set_mapping_panel_collapsed(self, collapsed):
    """Klappt das rechte Mapping-Panel ein/aus und verteilt den Platz um."""
    sizes = self.content_splitter.sizes()
    if len(sizes) < 3:
      return
    total = sum(sizes)
    if collapsed:
      # Mapping-Panel einklappen: nur Status-Zeile + Button sichtbar
      mapping_min = 180
      remaining = total - mapping_min
      self.content_splitter.setSizes([remaining // 2, remaining - remaining // 2, mapping_min])
      self.mapping_panel_widget.setMaximumWidth(200)
      self.lbl_mapping_side_hint.setVisible(False)
      self.mapping_frame.setVisible(False)
    else:
      # Mapping-Panel aufklappen
      self.mapping_panel_widget.setMaximumWidth(420)
      self.mapping_panel_widget.setMinimumWidth(300)
      self.lbl_mapping_side_hint.setVisible(True)
      self.content_splitter.setSizes([int(total * 0.38), int(total * 0.38), int(total * 0.24)])

  def _safe_text(self, value):
    if value is None:
      return ""
    return str(value)

  def _set_form_fields_from_payload(self, payload):
    payload = payload if isinstance(payload, dict) else {}
    self.einkauf_form_widget.set_payload(payload)

  def _apply_payload_to_current_mail(self, payload):
    merged = dict(self._current_mail_record())
    if isinstance(payload, dict):
      merged.update(payload)
    self._set_current_mail_data(merged)
    self._set_form_fields_from_payload(merged)
    self._populate_items_table(merged.get("waren", []), payload=merged)
    self._refresh_order_review()

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

  # ── Mapping-State-Helfer (lokale Kapselung) ────────────────────

  def _get_current_task_index(self):
    """Gibt den aktuellen Task-Index der Mail zurueck (default: 0)."""
    state = self._current_mapping_state()
    if not isinstance(state, dict):
      return 0
    return int(state.get("task_index", 0) or 0)

  def _set_current_task_index(self, task_index):
    """Setzt den Task-Index der aktuellen Mail im State."""
    state = self._current_mapping_state()
    if isinstance(state, dict):
      state["task_index"] = int(task_index)
      self._mapping_state_by_index[self.current_index] = state

  def _has_pending_mapping_tasks(self):
    """Prueft, ob die aktuelle Mail noch offene Mapping-Tasks hat."""
    state = self._current_mapping_state()
    if not isinstance(state, dict):
      return False
    tasks = list(state.get("tasks", []) or [])
    task_index = self._get_current_task_index()
    return task_index < len(tasks)

  def _advance_to_next_task(self):
    """Erhoet task_index um 1 und synchronisiert _mapping_done_by_index."""
    state = self._current_mapping_state()
    if not isinstance(state, dict):
      return
    tasks = list(state.get("tasks", []) or [])
    task_index = self._get_current_task_index()
    self._set_current_task_index(task_index + 1)
    # Synchronisiere _mapping_done_by_index
    if task_index + 1 >= len(tasks):
      self._mapping_done_by_index[self.current_index] = True
    else:
      self._mapping_done_by_index[self.current_index] = False

  def _refresh_mapping_panel_and_ui(self):
    """Rendert Panel + aktualisiert UI-Labels (kombinierte Refresh)."""
    self._render_mapping_panel_for_current_mail()
    self._update_mapping_state_ui()

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
    if not self._has_pending_mapping_tasks():
      self.mapping_frame.setVisible(False)
      self._clear_mapping_panel()
      return

    task_index = self._get_current_task_index()
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
    task_index = self._get_current_task_index()
    if task_index >= len(tasks):
      return

    task = tasks[task_index]
    state["payload"] = EinkaufPipeline.apply_mapping_decision(state.get("payload", {}), task, selected_value)
    self._mapping_state_by_index[self.current_index] = state
    self._apply_payload_to_current_mail(state.get("payload", {}))

    self._mapping_prompted_by_index[self.current_index] = True
    self._advance_to_next_task()

    if self._has_pending_mapping_tasks():
      self._refresh_mapping_panel_and_ui()
    else:
      self.mapping_frame.setVisible(False)
      self._clear_mapping_panel()
      self._update_mapping_state_ui()

  def _update_mapping_state_ui(self):
    state = self._current_mapping_state() or {}
    tasks = list(state.get("tasks", []) or []) if isinstance(state, dict) else []
    task_index = self._get_current_task_index()
    remaining = max(0, len(tasks) - task_index)
    done = bool(self._mapping_done_by_index.get(self.current_index, False))

    if done:
      self.lbl_mapping_state.setText("Mapping: erledigt")
      self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #9ece6a;")
      self.btn_run_mapping.setText("Mapping erneut pruefen")
      self.lbl_mapping_badge.setText("Mapping OK")
      self.lbl_mapping_badge.setStyleSheet(
          "font-size: 11px; font-weight: bold; color: #9ece6a; background-color: #203225;"
          " border: 1px solid #9ece6a; border-radius: 4px; padding: 2px 8px;"
      )
      self.lbl_mapping_badge.setVisible(True)
      self._set_mapping_panel_collapsed(True)
    elif remaining > 0:
      self.lbl_mapping_state.setText(f"Mapping: {remaining} Schritt(e) offen")
      self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #f7c66f;")
      self.btn_run_mapping.setText("Mapping-Bereich anzeigen")
      self.lbl_mapping_badge.setText(f"{remaining} Mapping offen")
      self.lbl_mapping_badge.setStyleSheet(
          "font-size: 11px; font-weight: bold; color: #f7c66f; background-color: #3a3117;"
          " border: 1px solid #f7c66f; border-radius: 4px; padding: 2px 8px;"
      )
      self.lbl_mapping_badge.setVisible(True)
      self._set_mapping_panel_collapsed(False)
    else:
      self.lbl_mapping_state.setText("Mapping: offen")
      self.lbl_mapping_state.setStyleSheet("font-size: 12px; color: #f7c66f;")
      self.btn_run_mapping.setText("Mapping jetzt starten")
      self.lbl_mapping_badge.setVisible(False)
      self._set_mapping_panel_collapsed(True)

  # ── Aktuelle-Mail-Helfer (lokale Kapselung) ───────────────────

  def _has_current_mail(self):
    """Prueft, ob der aktuelle Index gueltig ist."""
    return 0 <= self.current_index < len(self.data_list)

  def _current_mail_record(self):
    """Gibt den aktuellen Mail-Record zurueck, default {}."""
    if self._has_current_mail():
      return self.data_list[self.current_index]
    return {}

  def _set_current_mail_data(self, data):
    """Schreibt den Datensatz fuer die aktuelle Mail zurueck in data_list."""
    if self._has_current_mail():
      self.data_list[self.current_index] = data

  # Alias: Abwaertskompatibilitaet – bevorzuge _current_mail_record().
  _current_mail_item = _current_mail_record

  def _update_current_mail_preview_and_attachments(self):
    """Rendert Preview und Anhaenge-Sektion fuer die aktuelle Mail."""
    self._render_current_preview()
    self._populate_attachment_preview()

  def _update_current_mail_badges(self):
    """Aktualisiert alle Status-Badges der aktuellen Mail."""
    self._check_duplicate_for_current()
    self._update_konfidenz_badge()
    self._update_absender_badge()

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
    item = self._current_mail_record()
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
    item = item if isinstance(item, dict) else self._current_mail_record()
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
    attachments = self._pdf_attachment_rows(self._current_mail_record())
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
    item = self._current_mail_record()
    item["_allow_external_preview_once"] = True
    self._render_current_preview()

  def _trust_current_sender(self):
    item = self._current_mail_record()
    SafeMailRenderer.trust_sender(self.settings_manager, item.get("_email_sender", ""))
    item["_allow_external_preview_once"] = True
    self._render_current_preview()

  def _trust_current_domain(self):
    item = self._current_mail_record()
    SafeMailRenderer.trust_domain(self.settings_manager, item.get("_email_sender", ""))
    item["_allow_external_preview_once"] = True
    self._render_current_preview()

  def _populate_items_table(self, waren, payload=None):
    self.einkauf_items_widget.set_items(
      waren,
      ean_fill_callback=self.ean_service.find_best_local_ean_by_name,
      payload=payload,
    )
    gesamt = (payload or {}).get("gesamt_ekp_brutto") if isinstance(payload, dict) else None
    self.summen_banner.update_from_items(waren or [], gesamt)

  def _load_current_mail(self):
    item = self._current_mail_record()
    self._set_progress_text()

    state = self._ensure_mapping_state_for_index(self.current_index, rebuild=False, source_payload=item)
    self._apply_payload_to_current_mail(state.get("payload", {}))
    item = self._current_mail_record()

    self._update_current_mail_preview_and_attachments()
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

    # --- Duplikat-Pre-Check + Konfidenz + Absender ---
    self._update_current_mail_badges()

    # --- Navigationsleiste + Stats aktualisieren ---
    self._update_nav_dots()
    self._update_footer_stats()

    # --- LookupService: Logo aus DB laden bevor API aufgerufen wird ---
    self._auto_lookup_shop_logo_from_db()

    # --- Paketdienst Auto-Detect ---
    self._auto_detect_paketdienst()

    # --- Uebersicht-Tab + Rechnungs-Sektion aktualisieren ---
    self._update_uebersicht_tab()
    self._update_rechnung_section()

    # --- Tab auf Kopfdaten zuruecksetzen ---
    self.data_tabs.setCurrentIndex(0)

  _CARRIER_PATTERNS = {
    "dhl": "DHL", "dpd": "DPD", "gls": "GLS",
    "ups": "UPS", "hermes": "Hermes", "amazon logistics": "Amazon Logistics",
    "deutsche post": "Deutsche Post", "fedex": "FedEx",
  }

  def _auto_detect_paketdienst(self):
    """Erkennt den Paketdienst aus Tracking-Nummer, Shop-Name oder Mail-Body."""
    try:
      paketdienst_widget = self.inputs.get("paketdienst")
      if not paketdienst_widget or not hasattr(paketdienst_widget, "text"):
        return
      if str(paketdienst_widget.text()).strip():
        return  # bereits ausgefuellt

      item = self._current_mail_record()
      search_texts = [
        str(item.get("tracking_nummer_einkauf", "") or "").lower(),
        str(item.get("shop_name", "") or "").lower(),
        str(item.get("subject", "") or item.get("_email_subject", "") or "").lower(),
      ]
      combined = " ".join(search_texts)

      for pattern, carrier in self._CARRIER_PATTERNS.items():
        if pattern in combined:
          paketdienst_widget.setText(carrier)
          return
    except Exception as e:
      log_exception(__name__, e)

  def _update_rechnung_section(self):
    """Aktualisiert die Rechnungs-Sektion basierend auf Mail-Anhaengen."""
    try:
      self._current_rechnung_pdf_path = ""
      self.lbl_rechnung_pdf_path.setText("")
      self.chk_rechnung_vorhanden.setChecked(False)
      item = self._current_mail_record()
      attachments = item.get("_attachments") or item.get("attachments") or []
      has_pdf = False
      auto_invoice = False
      for att in attachments:
        path = str(att.get("path", "") or att.get("file_path", "") or "").strip()
        if path.lower().endswith(".pdf"):
          has_pdf = True
          fname = os.path.basename(path).lower()
          if any(kw in fname for kw in ("rechnung", "invoice", "factura", "billing")):
            auto_invoice = True
            self._current_rechnung_pdf_path = path
            self.lbl_rechnung_pdf_path.setText(f"Auto-erkannt: {os.path.basename(path)}")
            self.chk_rechnung_vorhanden.setChecked(True)
            break
      self.btn_rechnung_pdf.setVisible(has_pdf)
    except Exception as e:
      log_exception(__name__, e)

  def _update_absender_badge(self):
    """Warnt dezent wenn die Absender-Domain nicht in den bekannten Mappings ist."""
    try:
      item = self._current_mail_record()
      sender = str(item.get("_email_sender", "") or item.get("bestell_email", "") or "").strip().lower()
      if not sender or "@" not in sender:
        self.lbl_absender_badge.setVisible(False)
        return

      domain = sender.split("@")[-1].strip()
      # Pruefen ob die Domain in mapping.json bekannt ist
      known = False
      try:
        mapping_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "mapping.json")
        if os.path.exists(mapping_path):
          with open(mapping_path, "r", encoding="utf-8") as f:
            mapping_data = json.load(f)
          shop_mappings = mapping_data.get("shop_name", {})
          known_values = set()
          for k, v in shop_mappings.items():
            known_values.add(k.lower())
            known_values.add(str(v).lower())
          for val in known_values:
            if domain in val or val in domain:
              known = True
              break
      except Exception:
        pass

      if not known:
        self.lbl_absender_badge.setText(f"Absender unbekannt ({domain})")
        self.lbl_absender_badge.setStyleSheet(
          "font-size: 11px; font-weight: bold; color: #f7c66f; background-color: #3a3117;"
          " border: 1px solid #f7c66f; border-radius: 4px; padding: 2px 8px;"
        )
        self.lbl_absender_badge.setVisible(True)
      else:
        self.lbl_absender_badge.setVisible(False)
    except Exception:
      self.lbl_absender_badge.setVisible(False)

  def _update_konfidenz_badge(self):
    """Zaehlt ausgefuellte Pflichtfelder und aktualisiert das Konfidenz-Badge."""
    try:
      filled = 0
      total = 5
      for key in ("bestellnummer", "shop_name", "bestell_datum", "gesamt_ekp_brutto"):
        w = self.inputs.get(key)
        if w and hasattr(w, "text") and str(w.text()).strip():
          if key == "gesamt_ekp_brutto":
            try:
              if float(str(w.text()).replace(",", ".")) > 0:
                filled += 1
            except (ValueError, TypeError):
              pass
          else:
            filled += 1
      # Min. 1 Artikel
      items = self.einkauf_items_widget.get_items()
      if items:
        filled += 1

      self.lbl_konfidenz_badge.setText(f"{filled}/{total} Pflichtfelder")
      if filled == total:
        self.lbl_konfidenz_badge.setStyleSheet(
          "font-size: 11px; font-weight: bold; color: #9ece6a; background-color: #203225;"
          " border: 1px solid #9ece6a; border-radius: 4px; padding: 2px 8px;"
        )
      else:
        self.lbl_konfidenz_badge.setStyleSheet(
          "font-size: 11px; font-weight: bold; color: #f7c66f; background-color: #3a3117;"
          " border: 1px solid #f7c66f; border-radius: 4px; padding: 2px 8px;"
        )
      self.lbl_konfidenz_badge.setVisible(True)
    except Exception:
      self.lbl_konfidenz_badge.setVisible(False)

  def _check_duplicate_for_current(self):
    """Prueft ob die Bestellnummer der aktuellen Mail bereits in der DB existiert."""
    try:
      item = self._current_mail_record()
      bestellnummer = str(item.get("bestellnummer", "") or "").strip()
      if not bestellnummer:
        self.lbl_duplicate_badge.setVisible(False)
        return
      db = self._shared_db
      if db is None:
        from module.database_manager import DatabaseManager
        db = DatabaseManager(self.settings_manager)
        self._shared_db = db
      exists = db.bestellnummer_exists(bestellnummer)
      if exists:
        self.lbl_duplicate_badge.setText(f"Bestellung \"{bestellnummer}\" existiert bereits in der DB")
        self.lbl_duplicate_badge.setVisible(True)
      else:
        self.lbl_duplicate_badge.setVisible(False)
    except Exception:
      self.lbl_duplicate_badge.setVisible(False)

  def _auto_lookup_shop_logo_from_db(self):
    """Prueft beim Laden einer Mail ob ein Shop-Logo in der lokalen DB existiert.

    Wird VOR dem Mapping aufgerufen. Wenn ein Logo lokal existiert, wird
    es sofort angezeigt – ohne Brave-API-Call.
    """
    try:
      item = self._current_mail_record()
      shop_name = str(item.get("shop_name", "") or "").strip()
      sender_domain = ""
      email = str(item.get("bestell_email", "") or "").strip()
      if "@" in email:
        sender_domain = email.split("@", 1)[1].strip().lower()

      if not shop_name and not sender_domain:
        return

      result = self._lookup_service.lookup_shop(
        shop_name=shop_name,
        sender_domain=sender_domain,
      )

      if result.has_logo and hasattr(self, "einkauf_form_widget"):
        self.einkauf_form_widget.set_shop_logo_path(result.logo_path)

    except Exception as exc:
      log_exception(__name__, exc)

  # ── Manuelle Logo-Suche (Fallback-Button im Logo-Frame) ──────────────

  def _on_manual_logo_search_requested(self, context):
    """Wird aufgerufen wenn der Nutzer manuell auf 'Logo suchen' klickt."""
    shop_name = str(context.get("canonical_shop_name", "") or "").strip()
    sender_domain = str(context.get("sender_domain", "") or "").strip()

    worker = create_logo_search_worker(
      parent_widget=self,
      settings_manager=self.settings_manager,
      shop_name=shop_name,
      sender_domain=sender_domain,
      current_worker=self._logo_search_worker,
      logo_button=self.einkauf_form_widget.btn_logo_search,
      on_finished_callback=lambda r: self._on_logo_search_finished(r, shop_name),
      on_error_callback=self._on_logo_search_error,
    )
    if worker is not None:
      self._logo_search_worker = worker

  def _finish_logo_search_ui(self):
    btn = getattr(self.einkauf_form_widget, "btn_logo_search", None) if hasattr(self, "einkauf_form_widget") else None
    reset_logo_search_button(btn)
    self._logo_search_worker = None

  def _on_logo_search_finished(self, result_dict, shop_name):
    self._finish_logo_search_ui()
    handle_logo_search_result(
      parent_widget=self,
      settings_manager=self.settings_manager,
      result_dict=result_dict,
      shop_name=shop_name,
      source_module="modul_mail_scraper",
      form_widget=getattr(self, "einkauf_form_widget", None),
    )

  def _on_logo_search_error(self, err_msg):
    self._finish_logo_search_ui()
    handle_logo_search_error(parent_widget=self, err_msg=err_msg)

  # ─────────────────────────────────────────────────────────────────────

  def _auto_prompt_mapping_for_index(self, idx):
    if idx != self.current_index:
      return
    if self._mapping_done_by_index.get(idx, False):
      return
    if self._mapping_prompted_by_index.get(idx, False):
      return

    self._mapping_prompted_by_index[idx] = True
    self._run_mapping_for_current_mail(show_feedback=False, rebuild=False)


  def _refresh_order_review(self):
    try:
      payload = self._collect_current_payload()
    except Exception:
      payload = dict(self._current_mail_record())
    bestellnummer = self._safe_text(payload.get("bestellnummer", "")).strip()
    if not bestellnummer:
      self._current_order_review_bundle = None
      self.einkauf_form_widget.clear_review_data()
      self.einkauf_items_widget.clear_review_data()
      self.order_review_widget.clear_review("Noch keine Bestellnummer erkannt. Die Pruefung startet, sobald eine Nummer vorhanden ist.")
      return None
    try:
      bundle = EinkaufPipeline.build_order_review_bundle(
        self.settings_manager,
        payload,
        db=self._shared_db,
      )
      self._shared_db = bundle.get("db", self._shared_db)
      self._current_order_review_bundle = bundle
      self.einkauf_form_widget.set_review_data(bundle)
      self.einkauf_items_widget.set_review_data(bundle)
      self.order_review_widget.set_review_data(bundle)
      return bundle
    except Exception as e:
      log_exception(__name__, e)
      self.einkauf_form_widget.clear_review_data()
      self.einkauf_items_widget.clear_review_data()
      self.order_review_widget.clear_review("Aenderungspruefung momentan nicht verfuegbar.")
      return None

  def _collect_current_payload(self):
    base = dict(self._current_mail_record())
    base = self.einkauf_form_widget.apply_to_payload(base)
    base["waren"] = self.einkauf_items_widget.get_items()
    return base
  def _lookup_ean_for_selected_row(self):
    """EAN-Suche fuer die selektierte Artikelzeile (delegiert an shared workflow)."""
    context = self.einkauf_items_widget.get_selected_context()
    if not isinstance(context, dict):
      CustomMsgBox.information(self, "EAN Suche", "Bitte zuerst eine Artikelzeile markieren.")
      return

    worker = create_ean_lookup_worker(
      parent_widget=self,
      settings_manager=self.settings_manager,
      context=context,
      current_worker=self.ean_lookup_worker,
      ean_button=self.einkauf_items_widget.btn_ean_lookup,
      on_finished_callback=self._on_ean_lookup_finished,
      on_error_callback=self._on_ean_lookup_error,
    )
    if worker is not None:
      self._pending_ean_lookup_context = dict(context)
      self.ean_lookup_worker = worker

  def _finish_ean_lookup_ui(self):
    reset_ean_lookup_button(self.einkauf_items_widget.btn_ean_lookup)
    self.ean_lookup_worker = None

  def _on_ean_lookup_finished(self, payload):
    context = dict(self._pending_ean_lookup_context or {})
    self._pending_ean_lookup_context = None
    self._finish_ean_lookup_ui()

    def _write_ean(row, ean):
      self.einkauf_items_widget.set_ean_for_row(row, ean)

    handle_ean_lookup_result(
      parent_widget=self,
      payload=payload,
      context=context,
      ean_service=self.ean_service,
      on_ean_selected=_write_ean,
    )

  def _on_ean_lookup_error(self, err_msg):
    self._pending_ean_lookup_context = None
    self._finish_ean_lookup_ui()
    handle_ean_lookup_error(parent_widget=self, err_msg=err_msg)

  def _on_run_mapping_clicked(self):
    self._set_mapping_panel_collapsed(False)
    self._run_mapping_for_current_mail(show_feedback=True, rebuild=True)

  def _run_mapping_for_current_mail(self, show_feedback=True, rebuild=False):
    try:
      payload = self._collect_current_payload()
      state = self._ensure_mapping_state_for_index(self.current_index, rebuild=rebuild, source_payload=payload)
      self._apply_payload_to_current_mail(state.get("payload", {}))

      if not self._has_pending_mapping_tasks():
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
      self._refresh_mapping_panel_and_ui()
    except Exception as e:
      log_exception(__name__, e)
      QMessageBox.critical(self, "Mapping-Fehler", f"Mapping fehlgeschlagen:\n{e}")

  # ── Artikel-Zeilen-Management ─────────────────────────────────────

  def _on_add_item_row(self):
    """Fuegt eine leere Artikelzeile zur Items-Tabelle hinzu."""
    try:
      items = self.einkauf_items_widget.get_items()
      items.append({"produkt_name": "", "varianten_info": "", "ean": "", "menge": "1", "ekp_brutto": "0.00"})
      self.einkauf_items_widget.set_items(items)
    except Exception as e:
      log_exception(__name__, e)

  def _on_remove_item_row(self):
    """Entfernt die ausgewaehlte Artikelzeile."""
    try:
      context = self.einkauf_items_widget.get_selected_context()
      if not context:
        QMessageBox.information(self, "Zeile entfernen", "Bitte zuerst eine Zeile auswaehlen.")
        return
      row = int(context.get("row", -1))
      items = self.einkauf_items_widget.get_items()
      if row < 0 or row >= len(items):
        return
      items.pop(row)
      self.einkauf_items_widget.set_items(items)
    except Exception as e:
      log_exception(__name__, e)

  # ── Rechnungs-PDF-Verknuepfung (Phase D1) ─────────────────────────

  def _on_link_invoice_pdf(self):
    """Verknuepft den ersten PDF-Anhang der aktuellen Mail als Rechnung."""
    try:
      item = self._current_mail_record()
      pdf_path = ""
      for attachment in (item.get("_attachments") or item.get("attachments") or []):
        path = str(attachment.get("path", "") or attachment.get("file_path", "") or "").strip()
        if path.lower().endswith(".pdf"):
          pdf_path = path
          break
      if pdf_path:
        self.lbl_rechnung_pdf_path.setText(f"Verknuepft: {os.path.basename(pdf_path)}")
        self._current_rechnung_pdf_path = pdf_path
        self.chk_rechnung_vorhanden.setChecked(True)
      else:
        QMessageBox.information(self, "Rechnung", "Kein PDF-Anhang in dieser Mail gefunden.")
    except Exception as e:
      log_exception(__name__, e)

  # ── Uebersicht-Tab aktualisieren ──────────────────────────────────

  def _update_uebersicht_tab(self):
    """Aktualisiert Auto-Mapping-Log, Warnungen und Validierungs-Checkliste."""
    try:
      item = self._current_mail_record()

      # Auto-Mapping-Log
      auto_mapped = item.get("_auto_mapped_fields", {})
      if auto_mapped:
        lines = []
        for key, info in auto_mapped.items():
          raw = str(info.get("raw", "") or "")
          resolved = str(info.get("resolved", "") or "")
          lines.append(f"{key}: \"{raw}\" \u2192 \"{resolved}\"")
        self.lbl_auto_mapping_log.setText("\n".join(lines))
      else:
        self.lbl_auto_mapping_log.setText("Keine automatischen Mappings.")

      # Warnungen
      warnings = []
      # Preisdelta
      items_list = self.einkauf_items_widget.get_items()
      warenwert = 0.0
      for art in items_list:
        try:
          m = float(str(art.get("menge", 1) or 1).replace(",", "."))
          p = float(str(art.get("ekp_brutto", 0) or 0).replace(",", "."))
          warenwert += m * p
        except (ValueError, TypeError):
          pass
      ki_gesamt = 0.0
      try:
        ki_gesamt = float(str(item.get("gesamt_ekp_brutto", 0) or 0).replace(",", "."))
      except (ValueError, TypeError):
        pass
      if ki_gesamt > 0 and abs(warenwert - ki_gesamt) > 0.02:
        warnings.append(f"Preisabweichung: Berechnet {warenwert:.2f} EUR vs. KI {ki_gesamt:.2f} EUR")

      # Fehlende Felder
      for key, label in [("bestellnummer", "Bestellnummer"), ("shop_name", "Shop"), ("bestell_datum", "Bestelldatum")]:
        widget = self.inputs.get(key)
        if widget and not str(widget.text()).strip():
          warnings.append(f"Feld \"{label}\" ist leer")

      if not items_list:
        warnings.append("Keine Artikelpositionen vorhanden")

      self.lbl_warnings.setText("\n".join(warnings) if warnings else "Keine Warnungen.")
      self.lbl_warnings.setStyleSheet(
        f"font-size: 12px; color: {'#f7c66f' if warnings else '#a9b1d6'}; background-color: #1f2335; border: 1px solid #414868; border-radius: 6px; padding: 8px;"
      )

      # Validierungs-Checkliste
      def _field_ok(key):
        w = self.inputs.get(key)
        if w and hasattr(w, "text"):
          return bool(str(w.text()).strip())
        return False

      checks = []
      checks.append(("\u2713" if _field_ok("bestellnummer") else "\u2717") + " Bestellnummer")
      checks.append(("\u2713" if _field_ok("shop_name") else "\u2717") + " Shop-Name")
      checks.append(("\u2713" if _field_ok("bestell_datum") else "\u2717") + " Bestelldatum")

      gesamt_ok = False
      try:
        g = self.inputs.get("gesamt_ekp_brutto")
        if g and hasattr(g, "text"):
          gesamt_ok = float(str(g.text()).replace(",", ".")) > 0
      except (ValueError, TypeError):
        pass
      checks.append(("\u2713" if gesamt_ok else "\u2717") + " Gesamtpreis")
      checks.append(("\u2713" if len(items_list) > 0 else "\u2717") + " Min. 1 Artikel")
      checks.append(("\u2713" if self._mapping_done_by_index.get(self.current_index, False) else "\u2717") + " Mapping erledigt")

      self.lbl_validation_checklist.setText("\n".join(checks))

    except Exception as e:
      log_exception(__name__, e)

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
      item = self._current_mail_record()
      payload["quelle"] = "mail_scraper"
      payload["mail_uid"] = str(item.get("_mail_uid", "") or "").strip()
      payload["mail_account"] = str(item.get("_mail_account", "") or "").strip()
      if self.chk_rechnung_vorhanden.isChecked():
        payload["rechnung_vorhanden"] = True
        if self._current_rechnung_pdf_path:
          payload["rechnung_pdf_pfad"] = self._current_rechnung_pdf_path
      self._set_current_mail_data(payload)

      def _on_order_number_changed(new_no):
        if "bestellnummer" in self.inputs:
          self.inputs["bestellnummer"].setText(self._safe_text(new_no))
        payload["bestellnummer"] = self._safe_text(new_no)

      review_bundle = self._refresh_order_review()
      save_result = EinkaufPipeline.confirm_and_save_single(
        self,
        self.settings_manager,
        payload,
        on_order_number_changed=_on_order_number_changed,
        show_new_number_info=True,
        db=self._shared_db,
        review_bundle=review_bundle,
        skip_existing_review_dialog=True,
      )
      self._shared_db = save_result.get("db", self._shared_db)

      if save_result.get("status") != "saved":
        return

      image_result = self.einkauf_items_widget.apply_saved_image_decisions(
        self._shared_db,
        save_result.get("einkauf_id"),
      )
      if image_result.get("reason") == "error":
        logging.warning("Bildentscheidungen aus Modul 2 konnten nicht angewendet werden: %s", image_result.get("message", ""))
        QMessageBox.warning(
          self,
          "Bildpflege",
          "Die Bestellung wurde gespeichert, aber die gemerkten Bildentscheidungen konnten noch nicht uebernommen werden."
        )

      self.summary["saved"] += 1
      self._mail_status[self.current_index] = "saved"
      if save_result.get("renamed"):
        self.summary["renamed"] += 1

      match_result = EinkaufPipeline.confirm_and_apply_pending_matches(
        self,
        self.settings_manager,
        db=self._shared_db
      )
      self._shared_db = match_result.get("db", self._shared_db)

      self._advance_to_next()
    except Exception as e:
      log_exception(__name__, e)
      QMessageBox.critical(self, "Speichern fehlgeschlagen", f"Fehler beim Speichern:\n{e}")

  def _skip_current(self):
    self.summary["skipped"] += 1
    self._mail_status[self.current_index] = "skipped"
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
    self._mail_status[self.current_index] = "discarded"
    self._advance_to_next()

  def _open_large_preview(self):
    item = self._current_mail_record()
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

    item = self._current_mail_record()
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
    current_item = self._current_mail_record() or None
    self._close_preview_dialogs()
    self._cleanup_mail_assets(current_item)
    # Naechste pending Mail suchen
    next_idx = -1
    for idx in range(self.current_index + 1, len(self.data_list)):
      if self._mail_status[idx] == "pending":
        next_idx = idx
        break
    # Falls keine nach current gefunden, auch davor suchen
    if next_idx < 0:
      for idx in range(0, self.current_index):
        if self._mail_status[idx] == "pending":
          next_idx = idx
          break
    if next_idx < 0:
      # Alle verarbeitet
      self.accept()
      return
    self.current_index = next_idx
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

































































