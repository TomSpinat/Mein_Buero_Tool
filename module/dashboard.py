"""
dashboard.py
Das Hauptfenster ("Dashboard") fuer unser Buerotool.
Wir designen es im 64-bit Pixel-Retro-Look durch QSS (Qt Style Sheets, quasi wie CSS fuer Websites).
Damit das Intro-Bild und dieses Fenster gut zusammenpassen, fokussieren wir uns auf
Dunkelgrau fuer den Hintergrund und auf haptisches Feedback (Hover / Active) bei den Buttons.
"""

from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QApplication, QScrollArea, QFrame, QSizePolicy, QStackedWidget, QGridLayout,
)
from PyQt6.QtGui import QIcon
from PyQt6.QtCore import Qt, QSize

from module.settings_dialog import SettingsDialog
from config import resource_path, SettingsManager
from module.style_manager import StyleManager
from module.modul_input import InputApp
from module.modul_tracker import TrackerApp
from module.modul_wareneingang import WareneingangApp
from module.modul_packstation import PackstationApp
from module.modul_finanzen import FinanzenApp
from module.modul_poms import PomsModule
from module.crash_logger import log_exception


class DashboardWindow(QMainWindow):
    """
    Das Dashboard ist unser riesiges Hauptfenster (QMainWindow).
    """
    def __init__(self, settings_manager=None):
        super().__init__()

        # Guard-Flag: verhindert Rekursion in resizeEvent
        self._in_resize = False

        self.settings_manager = settings_manager if settings_manager else SettingsManager()

        self.setWindowTitle("Just Business")
        self.setWindowIcon(QIcon(resource_path("assets/icons/app_icon.png")))

        # Mindestgroesse setzen – darunter erscheinen Scrollbars in den Modulen
        self.setMinimumSize(960, 640)

        # Stylesheets setzen (das Aussehen steuern)
        self._apply_stylesheets()

        # Den Inhaltsbereich des Fensters bauen
        central_widget = QWidget()
        central_widget.setObjectName("CentralWidget")
        self.setCentralWidget(central_widget)

        # Layout: Oben die Toolbar-Leiste, darunter der Inhalt
        self.main_layout = QVBoxLayout(central_widget)
        self.main_layout.setContentsMargins(0, 0, 0, 0)

        self._build_top_bar()
        self._build_content_area()

        # Startgroesse einmalig setzen – kein doppeltes resize mehr
        self.resize(1024, 700)

    # ------------------------------------------------------------------
    # Resize-Handling
    # ------------------------------------------------------------------

    def resizeEvent(self, event):
        """Fenster wurde skaliert – Dashboard-Kacheln dynamisch anpassen."""
        super().resizeEvent(event)

        # Guard: setFixedSize auf Buttons loest Layout-Updates aus, die erneut
        # resizeEvent triggern koennen → Rekursion / Flackern verhindern.
        if self._in_resize:
            return
        if not hasattr(self, 'btn_scanner'):
            return

        self._in_resize = True
        try:
            w = self.width()
            # Kachelgroesse skaliert mit der Fensterbreite, eingegrenzt auf 100–200px
            btn_size = max(100, min(200, int(w / 6.5)))
            icon_size = int(btn_size * 0.55)
            for btn in [
                self.btn_scanner, self.btn_mail, self.btn_tracker,
                self.btn_inbound, self.btn_packstation, self.btn_finances,
                self.btn_poms,
            ]:
                btn.setFixedSize(btn_size, btn_size)
                btn.setIconSize(QSize(icon_size, icon_size))
        finally:
            self._in_resize = False

    # ------------------------------------------------------------------
    # Aufbau
    # ------------------------------------------------------------------

    def _apply_stylesheets(self):
        style = StyleManager.get_global_stylesheet()
        self.setStyleSheet(style)

    def _build_top_bar(self):
        """Erzeugt die obere Leiste mit Titel und Settings-Icon."""
        top_bar = QFrame()
        top_bar.setObjectName("TopBar")
        top_bar.setFixedHeight(90)
        top_bar.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        bar_layout = QHBoxLayout(top_bar)
        bar_layout.setContentsMargins(35, 0, 35, 0)

        # Zurueck-Button
        self.btn_back = QPushButton()
        self.btn_back.setObjectName("BackBtn")
        self.btn_back.setStyleSheet("background: transparent; border: none; padding-top: 5px;")
        self.btn_back.setIcon(QIcon(resource_path("assets/icons/back_arrow.png")))
        self.btn_back.setIconSize(QSize(45, 45))
        self.btn_back.setFixedSize(50, 50)
        self.btn_back.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_back.hide()  # Standardmaessig versteckt
        self.btn_back.clicked.connect(self.return_to_main)

        # Titel
        self.title_lbl = QLabel("JUST BUSINESS")
        self.title_lbl.setStyleSheet("font-size: 20px; font-weight: bold; color: #DADADA;")

        # Zahnrad-Button
        self.btn_settings = QPushButton()
        self.btn_settings.setObjectName("SettingsBtn")
        self.btn_settings.setStyleSheet(
            "background: transparent; border: none; padding-bottom: 5px; padding-left: 5px;"
        )
        self.btn_settings.setIcon(QIcon(resource_path("assets/icons/icon_settings.png")))
        self.btn_settings.setIconSize(QSize(40, 40))
        self.btn_settings.setFixedSize(50, 50)
        self.btn_settings.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_settings.clicked.connect(self.open_settings)

        bar_layout.addWidget(self.btn_back)
        bar_layout.addWidget(self.title_lbl)
        bar_layout.addStretch()
        bar_layout.addWidget(self.btn_settings)

        self.main_layout.addWidget(top_bar)

    def _build_content_area(self):
        """Erzeugt den Bereich fuer die Desktop-Icons / Apps."""
        self.stacked_widget = QStackedWidget()

        # --- Karte 1: Das Hauptmenue (Dashboard Body) ---
        self.dashboard_body = QWidget()
        dash_h_layout = QHBoxLayout(self.dashboard_body)
        dash_h_layout.setContentsMargins(0, 0, 0, 0)

        apps_container = QWidget()
        content_layout = QVBoxLayout(apps_container)
        content_layout.setContentsMargins(20, 20, 20, 20)

        # Hilfsfunktion: erzeugt ein App-Icon-Kachel (Button + Label)
        def make_tile(btn_attr, icon_path, label_text, slot):
            wrapper = QWidget()
            layout = QVBoxLayout(wrapper)
            layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.setContentsMargins(0, 0, 0, 0)
            layout.setSpacing(2)

            btn = QPushButton()
            btn.setObjectName("ScannerBtn")
            btn.setProperty("class", "retro-btn")
            btn.setFixedSize(150, 150)
            btn.setIcon(QIcon(resource_path(icon_path)))
            btn.setIconSize(QSize(80, 80))
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.clicked.connect(slot)

            lbl = QLabel(label_text)
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl.setStyleSheet("font-size: 12px; font-weight: bold; color: #FFFFFF;")

            layout.addWidget(btn)
            layout.addWidget(lbl)
            setattr(self, btn_attr, btn)
            return wrapper

        apps_layout = QGridLayout()
        apps_layout.addWidget(
            make_tile("btn_input",       "assets/icons/dash_order_entry.png",  "Input",                 self.open_input),        0, 0)
        apps_layout.addWidget(
            make_tile("btn_tracker",     "assets/icons/dash_tracker.png",      "Tracking\nRadar",       self.open_tracker),      0, 1)
        apps_layout.addWidget(
            make_tile("btn_inbound",     "assets/icons/dash_inbound.png",      "Waren-\neingang",       self.open_inbound),      0, 2)
        apps_layout.addWidget(
            make_tile("btn_packstation", "assets/icons/dash_packstation.png",  "Packstation\nOutbound", self.open_packstation),  1, 0)
        apps_layout.addWidget(
            make_tile("btn_finances",    "assets/icons/dash_finances.png",     "Finanz-\nUebersicht",   self.open_finances),     1, 1)
        apps_layout.addWidget(
            make_tile("btn_poms",        "assets/icons/dash_poms.png",         "POMS\nUebersicht",      self.open_poms),         1, 2)

        content_layout.addLayout(apps_layout)
        dash_h_layout.addWidget(apps_container, stretch=3)

        # To-Do Sidebar – nur auf der Startseite sichtbar
        from module.modul_todo import ToDoWidget
        self.todo_widget = ToDoWidget(self.settings_manager)
        self.todo_widget.action_requested.connect(self._handle_todo_action)
        dash_h_layout.addWidget(self.todo_widget, stretch=1)

        self.stacked_widget.addWidget(self.dashboard_body)

        # --- Module ---
        self.input_app       = InputApp(self.settings_manager)
        self.scanner_app     = self.input_app.scanner_app   # Abwaertskompatibilitaet
        self.mail_app        = self.input_app.mail_app       # Abwaertskompatibilitaet
        self.tracker_app     = TrackerApp(self.settings_manager)
        self.inbound_app     = WareneingangApp(self.settings_manager)
        self.packstation_app = PackstationApp(self.settings_manager)
        self.finanzen_app    = FinanzenApp(self.settings_manager)

        from module.database_manager import DatabaseManager
        self.poms_app = PomsModule(DatabaseManager(self.settings_manager), self.settings_manager)

        for app in [
            self.input_app, self.tracker_app,
            self.inbound_app, self.packstation_app, self.finanzen_app, self.poms_app,
        ]:
            self.stacked_widget.addWidget(app)

        # --- ScrollArea: Scrollbars erscheinen automatisch wenn das Fenster
        #     kleiner als die Mindestgroesse des aktiven Moduls ist.
        #     setWidgetResizable(True) = Modul fuellt den verfuegbaren Platz aus,
        #     schrumpft aber nicht unter seine minimumSizeHint. ---
        self.module_scroll = QScrollArea()
        self.module_scroll.setWidgetResizable(True)
        self.module_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.module_scroll.setWidget(self.stacked_widget)
        # Kein eigener Hintergrund – Dark-Theme des zentralen Widgets scheint durch
        self.module_scroll.setStyleSheet(
            "QScrollArea { background: transparent; border: none; }"
        )
        self.module_scroll.viewport().setAutoFillBackground(False)

        self.main_layout.addWidget(self.module_scroll, stretch=1)

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def _handle_todo_action(self, action_str):
        if action_str == "open_scanner":
            self.open_scanner()
        elif action_str == "open_inbound":
            self.open_inbound()
        elif action_str == "open_packstation":
            self.open_packstation()

    def open_input(self, tab="scan"):
        """Oeffnet das Input-Modul, optional mit bestimmtem Tab."""
        if tab == "mail":
            self.input_app.show_mail_tab()
        else:
            self.input_app.show_scan_tab()
        self.stacked_widget.setCurrentWidget(self.input_app)
        self.title_lbl.setText("INPUT")
        self.btn_back.show()

    def open_scanner(self):
        """Abwaertskompatibilitaet: Oeffnet Input-Modul auf Beleg-Scan-Tab."""
        self.open_input(tab="scan")

    def open_mail_scraper(self):
        """Abwaertskompatibilitaet: Oeffnet Input-Modul auf E-Mail-Tab."""
        self.open_input(tab="mail")

    def open_tracker(self):
        self.stacked_widget.setCurrentWidget(self.tracker_app)
        self.tracker_app.refresh_data()
        self.title_lbl.setText("TRACKING RADAR")
        self.btn_back.show()

    def open_inbound(self):
        self.stacked_widget.setCurrentWidget(self.inbound_app)
        self.inbound_app._list_page.reload()
        self.title_lbl.setText("WARENEINGANG")
        self.btn_back.show()

    def open_packstation(self):
        self.stacked_widget.setCurrentWidget(self.packstation_app)
        self.packstation_app._reset_workflow()
        self.title_lbl.setText("PACKSTATION OUTBOUND")
        self.btn_back.show()

    def open_finances(self):
        self.stacked_widget.setCurrentWidget(self.finanzen_app)
        self.finanzen_app.refresh_data()
        self.title_lbl.setText("FINANZ-UEBERSICHT")
        self.btn_back.show()

    def open_poms(self):
        self.stacked_widget.setCurrentWidget(self.poms_app)
        self.poms_app.refresh_data()
        self.title_lbl.setText("POMS REBORN")
        self.btn_back.show()

    def return_to_main(self):
        self.stacked_widget.setCurrentWidget(self.dashboard_body)
        self.title_lbl.setText("JUST BUSINESS")
        self.btn_back.hide()
        if hasattr(self, 'todo_widget'):
            self.todo_widget.refresh_todos()

    def open_settings(self):
        dialog = SettingsDialog(self)
        dialog.exec()
