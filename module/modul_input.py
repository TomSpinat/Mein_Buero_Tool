"""Gemeinsames Input-Modul: Routing- und Lifecycle-Owner fuer Beleg-Scan und E-Mail-Postfach.

Bettet die bestehenden Fach-/UI-Kerne ein:
- OrderEntryApp  (module/modul_order_entry.py)  → "Beleg scannen"
- MailScraperApp  (module/modul_mail_scraper.py) → "E-Mail-Postfach"

Verantwortlichkeiten dieser Shell:
- Routing:   activate(tab) als einziger externer Einstiegspunkt
- Ownership: haelt Submodule als feste Kinder-Widgets fuer ihre gesamte Lebenszeit
- State:     verfolgt den aktiven Tab (active_tab-Property)
- UX:        Segment-Bar und Tab-Stile

Enthaelt KEINE Businesslogik – delegiert vollstaendig an die Fach-Kerne.
"""

from PyQt6.QtWidgets import (
  QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QStackedWidget, QLabel, QFrame,
)
from PyQt6.QtCore import Qt, QSize
from PyQt6.QtGui import QIcon

from config import resource_path
from module.modul_order_entry import OrderEntryApp
from module.modul_mail_scraper import MailScraperApp


class InputApp(QWidget):
  """Gemeinsamer Einstieg fuer alle Input-Wege (Beleg-Scan + E-Mail).

  Routing-Owner: activate(tab) ist der einzige externe Einstiegspunkt.
  Lifecycle-Owner: Submodule werden im __init__ erstellt und bleiben fest.
  Business-Logik: keine – vollstaendige Delegation an Fach-Kerne.
  """

  # ── Tab-Konstanten ────────────────────────────────────────────
  # Alle externen Router (Dashboard, ToDo-Widget usw.) sollen diese
  # Konstanten statt Magic-Strings verwenden.
  TAB_SCAN = "scan"
  TAB_MAIL = "mail"

  def __init__(self, settings_manager, parent=None):
    super().__init__(parent)
    self.settings_manager = settings_manager

    # Fach-Kerne erstellen – bleiben fuer die gesamte Lebenszeit fest.
    # Kein Lazy-Init: beide Module fuehren in __init__ Accounts-Laden,
    # DB-Verbindungen und ggf. Warnungs-Dialoge aus → Timing wuerde sich
    # unvorhersehbar verschieben.
    self.scanner_app = OrderEntryApp(settings_manager)
    self.mail_app = MailScraperApp(settings_manager)

    # Interner Tab-Zustand – Quelle der Wahrheit fuer active_tab
    self._active_tab: str = self.TAB_SCAN

    self._build_ui()
    # Initialzustand sicherstellen (Styles + Stack)
    self.show_scan_tab()

  # ── Routing / Aktivierung ─────────────────────────────────────

  def activate(self, tab: str | None = None) -> None:
    """Aktiviert das Input-Modul auf dem gewuenschten Tab.

    Einziger externer Einstiegspunkt fuer das Dashboard (und andere Router).
    Unbekannte Tab-Namen fallen sicher auf TAB_SCAN zurueck.

    Args:
        tab: InputApp.TAB_SCAN (Standard) oder InputApp.TAB_MAIL.
    """
    if tab == self.TAB_MAIL:
      self.show_mail_tab()
    else:
      self.show_scan_tab()

  @property
  def active_tab(self) -> str:
    """Name des aktuell aktiven Tabs ('scan' oder 'mail')."""
    return self._active_tab

  # ── UI-Aufbau ────────────────────────────────────────────────

  def _build_ui(self) -> None:
    layout = QVBoxLayout(self)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(0)

    layout.addWidget(self._build_segment_bar())

    self._stack = QStackedWidget()
    self._stack.addWidget(self.scanner_app)
    self._stack.addWidget(self.mail_app)
    layout.addWidget(self._stack, 1)

  def _build_segment_bar(self) -> QFrame:
    """Erzeugt die Umschalt-Leiste mit Kontext-Label und zwei Segment-Buttons."""
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
    bar_layout.setContentsMargins(18, 6, 18, 0)
    bar_layout.setSpacing(6)

    context_icon = QLabel()
    context_icon.setPixmap(
      QIcon(resource_path("assets/icons/dash_order_entry.png")).pixmap(QSize(22, 22))
    )
    context_icon.setFixedSize(26, 26)
    context_icon.setStyleSheet("background: transparent; border: none; padding-top: 2px;")
    bar_layout.addWidget(context_icon)

    context_lbl = QLabel("Eingabekanal:")
    context_lbl.setStyleSheet(
      "color: #565f89; font-size: 12px; font-weight: bold;"
      "background: transparent; border: none; padding-top: 2px;"
    )
    bar_layout.addWidget(context_lbl)
    bar_layout.addSpacing(8)

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

  def _make_segment_button(self, text: str, icon_path: str, slot) -> QPushButton:
    """Erzeugt einen einzelnen Segment-Button."""
    btn = QPushButton(f"  {text}")
    btn.setCursor(Qt.CursorShape.PointingHandCursor)
    btn.setIcon(QIcon(resource_path(icon_path)))
    btn.setIconSize(QSize(18, 18))
    btn.setFixedHeight(36)
    btn.clicked.connect(slot)
    return btn

  # ── Tab-Umschaltung ──────────────────────────────────────────

  def show_scan_tab(self) -> None:
    """Zeigt den Beleg-Scan-Bereich und aktualisiert den Tab-Zustand."""
    self._active_tab = self.TAB_SCAN
    self._stack.setCurrentWidget(self.scanner_app)
    self._update_segment_styles()

  def show_mail_tab(self) -> None:
    """Zeigt den E-Mail-Postfach-Bereich und aktualisiert den Tab-Zustand."""
    self._active_tab = self.TAB_MAIL
    self._stack.setCurrentWidget(self.mail_app)
    self._update_segment_styles()

  # ── Segment-Styles ───────────────────────────────────────────

  _STYLE_ACTIVE = (
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

  _STYLE_INACTIVE = (
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

  def _update_segment_styles(self) -> None:
    """Aktualisiert die Segment-Button-Styles basierend auf dem aktiven Tab."""
    active_widget = self._stack.currentWidget()
    for btn, widget in [(self._btn_scan, self.scanner_app), (self._btn_mail, self.mail_app)]:
      btn.setStyleSheet(self._STYLE_ACTIVE if widget is active_widget else self._STYLE_INACTIVE)
