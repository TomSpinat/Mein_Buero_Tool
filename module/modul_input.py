"""Gemeinsames Input-Modul: Duenne UX-Fassade fuer Beleg-Scan und E-Mail-Postfach.

Bettet die bestehenden Fach-/UI-Kerne ein:
- OrderEntryApp  (module/modul_order_entry.py)  → "Beleg scannen"
- MailScraperApp  (module/modul_mail_scraper.py) → "E-Mail-Postfach"

Enthaelt KEINE Businesslogik – nur UX-Umschaltung.
"""

from PyQt6.QtWidgets import (
  QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QStackedWidget, QLabel, QFrame,
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QIcon

from config import resource_path
from module.modul_order_entry import OrderEntryApp
from module.modul_mail_scraper import MailScraperApp


class InputApp(QWidget):
  """Gemeinsamer Einstieg fuer alle Input-Wege (Beleg-Scan + E-Mail)."""

  def __init__(self, settings_manager, parent=None):
    super().__init__(parent)
    self.settings_manager = settings_manager

    # Fach-Kerne erstellen
    self.scanner_app = OrderEntryApp(settings_manager)
    self.mail_app = MailScraperApp(settings_manager)

    self._build_ui()
    self.show_scan_tab()

  # ── UI-Aufbau ────────────────────────────────────────────────

  def _build_ui(self):
    layout = QVBoxLayout(self)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(0)

    # Segment-Leiste
    layout.addWidget(self._build_segment_bar())

    # Content-Stack
    self._stack = QStackedWidget()
    self._stack.addWidget(self.scanner_app)
    self._stack.addWidget(self.mail_app)
    layout.addWidget(self._stack, 1)

  def _build_segment_bar(self):
    """Erzeugt die Umschalt-Leiste mit zwei Segment-Buttons."""
    bar = QFrame()
    bar.setObjectName("InputSegmentBar")
    bar.setStyleSheet(
      "QFrame#InputSegmentBar {"
      "  background-color: #1a1b26;"
      "  border-bottom: 1px solid #33354C;"
      "  padding: 0px;"
      "}"
    )

    bar_layout = QHBoxLayout(bar)
    bar_layout.setContentsMargins(18, 8, 18, 0)
    bar_layout.setSpacing(4)

    self._btn_scan = self._make_segment_button(
      "Beleg scannen",
      "assets/icons/dash_order_entry.png",
      self.show_scan_tab,
    )
    self._btn_mail = self._make_segment_button(
      "E-Mail-Postfach",
      "assets/icons/dash_mail_scraper.png",
      self.show_mail_tab,
    )

    bar_layout.addWidget(self._btn_scan)
    bar_layout.addWidget(self._btn_mail)
    bar_layout.addStretch()
    return bar

  def _make_segment_button(self, text, icon_path, slot):
    """Erzeugt einen einzelnen Segment-Button."""
    btn = QPushButton(f"  {text}")
    btn.setCursor(Qt.CursorShape.PointingHandCursor)
    btn.setIcon(QIcon(resource_path(icon_path)))
    btn.setFixedHeight(38)
    btn.clicked.connect(slot)
    return btn

  # ── Tab-Umschaltung ──────────────────────────────────────────

  def show_scan_tab(self):
    """Zeigt den Beleg-Scan-Bereich."""
    self._stack.setCurrentWidget(self.scanner_app)
    self._update_segment_styles()

  def show_mail_tab(self):
    """Zeigt den E-Mail-Postfach-Bereich."""
    self._stack.setCurrentWidget(self.mail_app)
    self._update_segment_styles()

  def _update_segment_styles(self):
    """Aktualisiert die Segment-Button-Styles basierend auf dem aktiven Tab."""
    active_widget = self._stack.currentWidget()

    for btn, widget in [(self._btn_scan, self.scanner_app), (self._btn_mail, self.mail_app)]:
      if widget is active_widget:
        btn.setStyleSheet(
          "QPushButton {"
          "  background-color: #24283b;"
          "  color: #7aa2f7;"
          "  border: 1px solid #7aa2f7;"
          "  border-bottom: 2px solid #7aa2f7;"
          "  border-radius: 6px 6px 0px 0px;"
          "  padding: 6px 18px;"
          "  font-size: 13px;"
          "  font-weight: bold;"
          "}"
        )
      else:
        btn.setStyleSheet(
          "QPushButton {"
          "  background-color: transparent;"
          "  color: #565f89;"
          "  border: 1px solid transparent;"
          "  border-bottom: 1px solid #33354C;"
          "  border-radius: 6px 6px 0px 0px;"
          "  padding: 6px 18px;"
          "  font-size: 13px;"
          "}"
          "QPushButton:hover {"
          "  color: #a9b1d6;"
          "  background-color: #1f2335;"
          "}"
        )
