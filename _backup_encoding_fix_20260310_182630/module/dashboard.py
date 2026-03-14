"""
dashboard.py
Das Hauptfenster ("Dashboard") fÃ¼r unser BÃ¼rotoll.
Wir designen es im 64-bit Pixel-Retro-Look durch QSS (Qt Style Sheets, quasi wie CSS fÃ¼r Websites).
Damit das Intro-Bild und dieses Fenster gut zusammenpassen, fokussieren wir uns auf 
Dunkelgrau fÃ¼r den Hintergrund und auf haptisches Feedback (Hover / Active) bei den Buttons.
"""

from PyQt6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel
from PyQt6.QtGui import QIcon
from PyQt6.QtCore import Qt, QSize

# Wir benÃ¶tigen den SettingsDialog!
from module.settings_dialog import SettingsDialog
from config import resource_path, SettingsManager
from module.style_manager import StyleManager
from module.modul_order_entry import OrderEntryApp
from module.modul_mail_scraper import MailScraperApp
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
        
        # Falls keiner mitgegeben wird, erzeugen wir zur Sicherheit einen eigenen
        self.settings_manager = settings_manager if settings_manager else SettingsManager()
        
        self.setWindowTitle("Just Business")
        # Das kleine Icon ganz oben links in der Titelleiste
        self.setWindowIcon(QIcon(resource_path("assets/icons/app_icon.png")))
        
        self.resize(1024, 600)  # Nette GrÃ¶ÃŸe, evtl passend zum Intro-Bild AuflÃ¶sungs-Style
        
        # 1. Stylsheets setzen (Das Aussehen steuern)
        self._apply_stylesheets()

        # 2. Den "Center" oder Inhalts-Bereich unseres Fensters bauen
        central_widget = QWidget()
        central_widget.setObjectName("CentralWidget")
        self.setCentralWidget(central_widget)
        
        # Layout des Hauptbereiches (Oben: Toolbar-Leiste, Unten: Programm-Icons)
        self.main_layout = QVBoxLayout(central_widget)
        # Den Rand-Abstand (Padding) auf 0 setzen, damit die Leiste oben bÃ¼ndig klebt.
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        
        # 3. Baue die Kopfleiste (Zahnrad)
        self._build_top_bar()
        
        # 4. Baue den restlichen Inhalt
        self._build_content_area()

    def resizeEvent(self, event):
        """Wird aufgerufen, wenn das Fenster skaliert wird, um die Kacheln dynamisch anzupassen."""
        super().resizeEvent(event)
        
        # Nur anpassen, wenn die Buttons schon existieren
        if hasattr(self, 'btn_scanner'):
            w = self.width()
            
            # Basis-KachelgrÃ¶ÃŸe errechnen (Skaliert mit der Fenster-Breite)
            btn_size = int(w / 6.5)
            # Begrenzung auf einen vernÃ¼nftigen Bereich
            btn_size = max(120, min(250, btn_size))
            icon_size = int(btn_size * 0.55)
            
            # Alle Haupt-App-Buttons skalieren
            apps = [
                self.btn_scanner, self.btn_mail, self.btn_tracker, 
                self.btn_inbound, self.btn_packstation, self.btn_finances,
                self.btn_poms
            ]
            for btn in apps:
                # setFixedSize stellt sicher, dass sie perfekt quadratisch bleiben
                btn.setFixedSize(btn_size, btn_size)
                btn.setIconSize(QSize(icon_size, icon_size))

    def _apply_stylesheets(self):
        """
        Der wichtigste Teil fÃ¼r unser Design!
        Hier holen wir das generierte CSS aus unserem StyleManager, 
        der sich automatisch 9-Slice PNGs zieht, wenn sie vorhanden sind.
        """
        style = StyleManager.get_global_stylesheet()
        self.setStyleSheet(style)

    def _build_top_bar(self):
        """Erzeugt die obere Leiste mit dem Titel und dem Settings-(Zahnrad)-Icon"""
        
        from PyQt6.QtWidgets import QFrame, QSizePolicy
        
        # Ein Container fÃ¼r die Leiste
        top_bar = QFrame()
        top_bar.setObjectName("TopBar") # Damit das CSS von dort oben greift!
        top_bar.setFixedHeight(90) # Die Leiste ist nun 50% hÃ¶her (90 px statt 60)
        # Zwingt die Leiste dazu, sich horizontal komplett auszudehnen
        top_bar.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        
        # Title Layout
        bar_layout = QHBoxLayout(top_bar)
        # AbstÃ¤nde innerhalb der Leiste (links/oben/rechts/unten)
        # Links 35px Abstand zum echten Fensterrand, damit es am Steinrahmen vorbei geht.
        bar_layout.setContentsMargins(35, 0, 35, 0)
        
        # ZurÃ¼ck-Button (Nur Text, retro Style) -> Update zu Modern Icon
        self.btn_back = QPushButton()
        self.btn_back.setObjectName("BackBtn")
        self.btn_back.setStyleSheet("background: transparent; border: none; padding-top: 5px;")
        
        self.btn_back.setIcon(QIcon(resource_path("assets/icons/back_arrow.png")))
        self.btn_back.setIconSize(QSize(45, 45)) 
        self.btn_back.setFixedSize(50, 50) 
        self.btn_back.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_back.hide() # StandardmÃ¤ÃŸig versteckt
        self.btn_back.clicked.connect(self.return_to_main)

        # Titel Text
        self.title_lbl = QLabel("JUST BUSINESS")
        # Inline Style fÃ¼r den Titel. 
        self.title_lbl.setStyleSheet("font-size: 20px; font-weight: bold; color: #DADADA;")
        
        # Zahnrad Button (Nur Icon)
        self.btn_settings = QPushButton()
        self.btn_settings.setObjectName("SettingsBtn")
        # Kein retro-btn Style mehr, da es jetzt ein reines Icon ohne Hintergrund ist
        # padding angepasst auf die kleinere GrÃ¶ÃŸe
        self.btn_settings.setStyleSheet("background: transparent; border: none; padding-bottom: 5px; padding-left: 5px;")
        
        # Icon laden und setzen (Kleinerer 50x50 Button, orientiert am ZurÃ¼ck-Button)
        self.btn_settings.setIcon(QIcon(resource_path("assets/icons/icon_settings.png")))
        self.btn_settings.setIconSize(QSize(40, 40)) # Die eigentliche BildgrÃ¶ÃŸe
        self.btn_settings.setFixedSize(50, 50)     # Die Klick-Bereich-GrÃ¶ÃŸe
        self.btn_settings.setCursor(Qt.CursorShape.PointingHandCursor)
        
        # Klick-Signal mit Funktion verbinden
        self.btn_settings.clicked.connect(self.open_settings)
        
        # Die Bausteine ins Layout der Leiste einfÃ¼gen
        bar_layout.addWidget(self.btn_back)
        bar_layout.addWidget(self.title_lbl)
        bar_layout.addStretch() # Platzhalter in der Mitte! So geht der Titel nach links, Zahnrad nach rechts.
        bar_layout.addWidget(self.btn_settings)

        # Die Leiste in unser gigantisches Hauptfenster einklinken
        self.main_layout.addWidget(top_bar)


    def _build_content_area(self):
        """Erzeugt den Bereich fÃ¼r die Desktop-Icons / Apps"""
        
        from PyQt6.QtWidgets import QStackedWidget
        
        # Der Stack kÃ¼mmert sich um den Wechsel zwischen HauptmenÃ¼ und Apps
        self.stacked_widget = QStackedWidget()
        
        # --- Karte 1: Das HauptmenÃ¼ (Dashboard Body) ---
        self.dashboard_body = QWidget()
        dash_h_layout = QHBoxLayout(self.dashboard_body)
        dash_h_layout.setContentsMargins(0, 0, 0, 0)
        
        apps_container = QWidget()
        content_layout = QVBoxLayout(apps_container)
        # Alignment entfernt, damit das gesamte Grid bei VergÃ¶ÃŸerung den Raum ausnutzt
        content_layout.setContentsMargins(20, 20, 20, 20)
        
        # Wrapper fÃ¼r die gesamte "App" (Icon + Text)
        app_wrapper = QWidget()
        app_layout = QVBoxLayout(app_wrapper)
        app_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        app_layout.setContentsMargins(0, 0, 0, 0)
        app_layout.setSpacing(2) # Reduziert den Abstand zwischen Button und Text auf ein Minimum
        
        # Erstelle den Rechnungs-Scanner-Button (Nur das Icon/der Kasten)
        self.btn_scanner = QPushButton() # Kein Text mehr hier!
        self.btn_scanner.setObjectName("ScannerBtn")      
        self.btn_scanner.setProperty("class", "retro-btn")
        self.btn_scanner.setFixedSize(150, 150) # Etwas grÃ¶ÃŸer (150x150)          
        self.btn_scanner.setIcon(QIcon(resource_path("assets/icons/dash_order_entry.png")))
        self.btn_scanner.setIconSize(QSize(80, 80))
        
        # Erstelle das Text-Label unter dem Button
        lbl_scanner = QLabel("Auftrags\nErfassung")
        lbl_scanner.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_scanner.setStyleSheet("font-size: 12px; font-weight: bold; color: #FFFFFF;")
        
        self.btn_scanner.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_settings.setCursor(Qt.CursorShape.PointingHandCursor)

        # Verbinde den Button mit dem Scanner!
        self.btn_scanner.clicked.connect(self.open_scanner)
        
        # FÃ¼ge Button und Label in den Wrapper
        app_layout.addWidget(self.btn_scanner)
        app_layout.addWidget(lbl_scanner)
        
        # --- App 2: Mail Scraper ---
        app_wrapper2 = QWidget()
        app_layout2 = QVBoxLayout(app_wrapper2)
        app_layout2.setAlignment(Qt.AlignmentFlag.AlignCenter)
        app_layout2.setContentsMargins(0, 0, 0, 0)
        app_layout2.setSpacing(2)
        
        self.btn_mail = QPushButton()
        self.btn_mail.setObjectName("ScannerBtn")      
        self.btn_mail.setProperty("class", "retro-btn")
        self.btn_mail.setFixedSize(150, 150)
        self.btn_mail.setIcon(QIcon(resource_path("assets/icons/dash_mail_scraper.png")))
        self.btn_mail.setIconSize(QSize(80, 80))
        
        lbl_mail = QLabel("Mail\nScraper")
        lbl_mail.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_mail.setStyleSheet("font-size: 12px; font-weight: bold; color: #FFFFFF;")
        
        self.btn_mail.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_mail.clicked.connect(self.open_mail_scraper)
        
        app_layout2.addWidget(self.btn_mail)
        app_layout2.addWidget(lbl_mail)
        
        # --- App 3: Tracker ---
        app_wrapper3 = QWidget()
        app_layout3 = QVBoxLayout(app_wrapper3)
        app_layout3.setAlignment(Qt.AlignmentFlag.AlignCenter)
        app_layout3.setContentsMargins(0, 0, 0, 0)
        app_layout3.setSpacing(2)
        
        self.btn_tracker = QPushButton()
        self.btn_tracker.setObjectName("ScannerBtn")      
        self.btn_tracker.setProperty("class", "retro-btn")
        self.btn_tracker.setFixedSize(150, 150)
        self.btn_tracker.setIcon(QIcon(resource_path("assets/icons/dash_tracker.png")))
        self.btn_tracker.setIconSize(QSize(80, 80))
        
        lbl_tracker = QLabel("Tracking\nRadar")
        lbl_tracker.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_tracker.setStyleSheet("font-size: 12px; font-weight: bold; color: #FFFFFF;")
        
        self.btn_tracker.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_tracker.clicked.connect(self.open_tracker)
        
        app_layout3.addWidget(self.btn_tracker)
        app_layout3.addWidget(lbl_tracker)

        # --- App 4: Wareneingang ---
        app_wrapper4 = QWidget()
        app_layout4 = QVBoxLayout(app_wrapper4)
        app_layout4.setAlignment(Qt.AlignmentFlag.AlignCenter)
        app_layout4.setContentsMargins(0, 0, 0, 0)
        app_layout4.setSpacing(2)
        
        self.btn_inbound = QPushButton()
        self.btn_inbound.setObjectName("ScannerBtn")      
        self.btn_inbound.setProperty("class", "retro-btn")
        self.btn_inbound.setFixedSize(150, 150)
        self.btn_inbound.setIcon(QIcon(resource_path("assets/icons/dash_inbound.png")))
        self.btn_inbound.setIconSize(QSize(80, 80))
        
        lbl_inbound = QLabel("Waren-\neingang")
        lbl_inbound.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_inbound.setStyleSheet("font-size: 12px; font-weight: bold; color: #FFFFFF;")
        
        self.btn_inbound.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_inbound.clicked.connect(self.open_inbound)
        
        app_layout4.addWidget(self.btn_inbound)
        app_layout4.addWidget(lbl_inbound)

        # --- App 5: Packstation ---
        app_wrapper5 = QWidget()
        app_layout5 = QVBoxLayout(app_wrapper5)
        app_layout5.setAlignment(Qt.AlignmentFlag.AlignCenter)
        app_layout5.setContentsMargins(0, 0, 0, 0)
        app_layout5.setSpacing(2)
        
        self.btn_packstation = QPushButton()
        self.btn_packstation.setObjectName("ScannerBtn")      
        self.btn_packstation.setProperty("class", "retro-btn")
        self.btn_packstation.setFixedSize(150, 150)
        self.btn_packstation.setIcon(QIcon(resource_path("assets/icons/dash_packstation.png")))
        self.btn_packstation.setIconSize(QSize(80, 80))
        
        lbl_packstation = QLabel("Packstation\nOutbound")
        lbl_packstation.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_packstation.setStyleSheet("font-size: 12px; font-weight: bold; color: #FFFFFF;")
        
        self.btn_packstation.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_packstation.clicked.connect(self.open_packstation)
        
        app_layout5.addWidget(self.btn_packstation)
        app_layout5.addWidget(lbl_packstation)

        # Packe die App-Wrapper ins Grid Layout (2 Reihen Ã  3 Spalten)
        from PyQt6.QtWidgets import QGridLayout
        apps_layout = QGridLayout()
        # Alignment entfernt, damit sich die Apps gleichmÃ¤ÃŸig in die Breite / HÃ¶he verteilen
        apps_layout.addWidget(app_wrapper, 0, 0)
        apps_layout.addWidget(app_wrapper2, 0, 1)
        apps_layout.addWidget(app_wrapper3, 0, 2)
        apps_layout.addWidget(app_wrapper4, 1, 0)
        apps_layout.addWidget(app_wrapper5, 1, 1)
        
        # --- App 6: Finanzen ---
        app_wrapper6 = QWidget()
        app_layout6 = QVBoxLayout(app_wrapper6)
        app_layout6.setAlignment(Qt.AlignmentFlag.AlignCenter)
        app_layout6.setContentsMargins(0, 0, 0, 0)
        app_layout6.setSpacing(2)
        
        self.btn_finances = QPushButton()
        self.btn_finances.setObjectName("ScannerBtn")      
        self.btn_finances.setProperty("class", "retro-btn")
        self.btn_finances.setFixedSize(150, 150)
        self.btn_finances.setIcon(QIcon(resource_path("assets/icons/dash_finances.png")))
        self.btn_finances.setIconSize(QSize(80, 80))
        
        lbl_finances = QLabel("Finanz-\nÃœbersicht")
        lbl_finances.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_finances.setStyleSheet("font-size: 12px; font-weight: bold; color: #FFFFFF;")
        
        self.btn_finances.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_finances.clicked.connect(self.open_finances)
        
        app_layout6.addWidget(self.btn_finances)
        app_layout6.addWidget(lbl_finances)
        
        apps_layout.addWidget(app_wrapper6, 1, 2)

        # --- App 7: POMS Reborn ---
        app_wrapper7 = QWidget()
        app_layout7 = QVBoxLayout(app_wrapper7)
        app_layout7.setAlignment(Qt.AlignmentFlag.AlignCenter)
        app_layout7.setContentsMargins(0, 0, 0, 0)
        app_layout7.setSpacing(2)
        
        self.btn_poms = QPushButton()
        self.btn_poms.setObjectName("ScannerBtn")      
        self.btn_poms.setProperty("class", "retro-btn")
        self.btn_poms.setFixedSize(150, 150)
        self.btn_poms.setIcon(QIcon(resource_path("assets/icons/dash_poms.png"))) 
        self.btn_poms.setIconSize(QSize(80, 80))
        
        lbl_poms = QLabel("POMS\nÃœbersicht")
        lbl_poms.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_poms.setStyleSheet("font-size: 12px; font-weight: bold; color: #FFFFFF;")
        
        self.btn_poms.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_poms.clicked.connect(self.open_poms)
        
        app_layout7.addWidget(self.btn_poms)
        app_layout7.addWidget(lbl_poms)
        
        apps_layout.addWidget(app_wrapper7, 2, 0)
        
        content_layout.addLayout(apps_layout)
        
        dash_h_layout.addWidget(apps_container, stretch=3)
        
        # To-Do Sidebar erzeugen und NUR auf dem Dashboard (Startseite) anzeigen
        from module.modul_todo import ToDoWidget
        self.todo_widget = ToDoWidget(self.settings_manager)
        self.todo_widget.action_requested.connect(self._handle_todo_action)
        dash_h_layout.addWidget(self.todo_widget, stretch=1)
        
        # MenÃ¼ als Karte 1 hinzufÃ¼gen
        self.stacked_widget.addWidget(self.dashboard_body)
        
        # --- Karte 2: Unser Rechnungs-Scanner ! ---
        self.scanner_app = OrderEntryApp(self.settings_manager)
        self.stacked_widget.addWidget(self.scanner_app)
        
        # --- Karte 3: Mail Scraper ---
        self.mail_app = MailScraperApp(self.settings_manager)
        self.stacked_widget.addWidget(self.mail_app)
        
        # --- Karte 4: Tracker App ---
        self.tracker_app = TrackerApp(self.settings_manager)
        self.stacked_widget.addWidget(self.tracker_app)
        
        # --- Karte 5: Wareneingang App ---
        self.inbound_app = WareneingangApp(self.settings_manager)
        self.stacked_widget.addWidget(self.inbound_app)
        
        # --- Karte 6: Packstation App ---
        self.packstation_app = PackstationApp(self.settings_manager)
        self.stacked_widget.addWidget(self.packstation_app)

        # --- Karte 7: Finanzen App ---
        self.finanzen_app = FinanzenApp(self.settings_manager)
        self.stacked_widget.addWidget(self.finanzen_app)

        # --- Karte 8: POMS App ---
        from module.database_manager import DatabaseManager
        self.poms_app = PomsModule(DatabaseManager(self.settings_manager), self.settings_manager)
        self.stacked_widget.addWidget(self.poms_app)

        # Content in das MainWindow packen 
        # (Toolbar wurde oben hinzugefÃ¼gt, StackedWidget sitzt darunter)
        self.main_layout.addWidget(self.stacked_widget, stretch=1)

    def _handle_todo_action(self, action_str):
        """Wird ausgelÃ¶st, wenn eine Todo-Karte geklickt wird"""
        if action_str == "open_scanner":
            self.open_scanner()
        elif action_str == "open_inbound":
            self.open_inbound()
        elif action_str == "open_packstation":
            self.open_packstation()

    def open_scanner(self):
        """Wechselt zur Rechnungs-Scanner Ansicht"""
        self.stacked_widget.setCurrentWidget(self.scanner_app)
        self.title_lbl.setText("AUFTRAGSERFASSUNG")
        self.btn_back.show()
        
    def open_mail_scraper(self):
        """Wechselt zur Mail Scraper Ansicht"""
        self.stacked_widget.setCurrentWidget(self.mail_app)
        self.title_lbl.setText("MAIL SCRAPER")
        self.btn_back.show()
        
    def open_tracker(self):
        """Wechselt zur Tracking Radar Ansicht"""
        self.stacked_widget.setCurrentWidget(self.tracker_app)
        # Wenn wir den Tracker Ã¶ffnen, auch direkt mal kurz die DB aktualisieren
        self.tracker_app.refresh_data()
        self.title_lbl.setText("TRACKING RADAR")
        self.btn_back.show()

    def open_inbound(self):
        """Wechselt zur Wareneingang Ansicht"""
        self.stacked_widget.setCurrentWidget(self.inbound_app)
        self.inbound_app._load_pending_orders()
        self.title_lbl.setText("WARENEINGANG")
        self.btn_back.show()

    def open_packstation(self):
        """Wechselt zur Packstation Ansicht und resettet den Scan-Flow"""
        self.stacked_widget.setCurrentWidget(self.packstation_app)
        self.packstation_app._reset_workflow()
        self.title_lbl.setText("PACKSTATION OUTBOUND")
        self.btn_back.show()

    def open_finances(self):
        """Wechselt zur Finanzen Ansicht"""
        self.stacked_widget.setCurrentWidget(self.finanzen_app)
        self.finanzen_app.refresh_data()
        self.title_lbl.setText("FINANZ-ÃœBERSICHT")
        self.btn_back.show()

    def open_poms(self):
        """Wechselt zur POMS Ansicht"""
        self.stacked_widget.setCurrentWidget(self.poms_app)
        self.poms_app.refresh_data()
        self.title_lbl.setText("POMS REBORN")
        self.btn_back.show()

    def return_to_main(self):
        """Wechselt zurÃ¼ck zum HauptmenÃ¼ (Dashboard)"""
        self.stacked_widget.setCurrentWidget(self.dashboard_body)
        self.title_lbl.setText("JUST BUSINESS")
        self.btn_back.hide()
        if hasattr(self, 'todo_widget'):
            self.todo_widget.refresh_todos()

    def open_settings(self):
        """
        Diese Funktion Ã¶ffnet den Einstellungs-Dialog, den wir in der
        settings_dialog.py Datei gebaut haben.
        """
        # Wir erzeugen eine Instanz vom Dialog (self als Parent, damit er im Vordergrund dieses Fensters bleibt)
        dialog = SettingsDialog(self)
        # .exec() bedeutet, der Dialog blockiert alles andere, bis er geschlossen wird
        dialog.exec()

