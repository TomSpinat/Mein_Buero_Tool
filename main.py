"""
main.py
Das ist der Einsprungsort (Entry Point) unseres ganzen Programms.
Wenn du die .exe später startest, wird dieser Code hier als Erstes ausgeführt.

Wir zeigen hier zuerst ein komplett schwarzes, transparentes Pixel-Logo (Intro-Bild),
spielen Sound ab, und blenden dann nach der Sound-Länge weich zum Hauptdashboard über.
"""

import sys
from PyQt6.QtWidgets import QApplication, QSplashScreen, QWidget, QMainWindow, QLabel, QVBoxLayout, QMessageBox
from PyQt6.QtGui import QPixmap, QPainter, QFontDatabase, QColor, QFont, QIcon
from PyQt6.QtCore import Qt, QTimer, QUrl, QPropertyAnimation, QObject, pyqtProperty
from PyQt6.QtMultimedia import QSoundEffect

# Wir importieren Funktionen aus unseren eigenen Dateien
from config import resource_path, SettingsManager
from module.database_manager import DatabaseManager
from module.dashboard import DashboardWindow  # Wir bauen das Dashboard im nächsten Schritt!
from module.crash_logger import install_global_exception_hooks, log_exception
from module.secret_store import sanitize_text
from module.test_reset import maybe_wipe_on_start

class FadeOverlay(QWidget):
    """
    Diese kleine Hilfsklasse wird ein schwarzes Viereck zeichen, das über dem
    Intro-Bild liegt. Zu Beginn ist es durchsichtig (0).
    Dann animieren wir das auf (255) schwarz, um den "Fade-Out"-Effekt zu erzeugen.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents)
        self._opacity = 0.0

    # Ein sogenanntes "Property" in PyQt, das wir animieren können
    @pyqtProperty(float)
    def opacity(self):
        return self._opacity

    @opacity.setter
    def opacity(self, val):
        self._opacity = val
        self.update() # Die GUI neu zeichnen (paintEvent wird aufgerufen)

    def paintEvent(self, event):
        """Zeichne ein schwarzes Rechteck in der Größe des Fensters mit der aktuellen Durchsichtigkeit"""
        if self._opacity > 0:
            painter = QPainter(self)
            # R, G, B, Alpha(Durchsichtigkeit)
            color = QColor(0, 0, 0, int(255 * self._opacity))
            painter.fillRect(self.rect(), color)


class IntroSplashScreen(QWidget):
    """
    Unser eigenes Intro-Fenster. Es hat keinen Windows-Rahmen,
    ist pechschwarz im Hintergrund und zeigt das transparente Intro-Bild.
    """
    def __init__(self, settings_manager):
        super().__init__()
        self.settings_manager = settings_manager
        
        # 1. Entferne den typischen Windows-Rahmen (X, Maximieren, Minimieren)
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint)
        
        # 2. Setze den Hintergrund pechschwarz
        self.setStyleSheet("background-color: #000000;")
        
        # 3. Das Layout bauen (Vertikal zentriert)
        layout = QVBoxLayout()
        self.lbl_image = QLabel()
        self.lbl_image.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        # Lade das transparente Intro-Bild über unser resource_path (Wichtig für PyInstaller!)
        pixmap = QPixmap(resource_path("assets/intro_bild.png"))
        self.lbl_image.setPixmap(pixmap)
        
        # Passe die Fenstergröße exakt an das Bild an (Pixelauflösung)
        self.resize(pixmap.width(), pixmap.height())
        
        layout.addWidget(self.lbl_image)
        self.setLayout(layout)

        # 4. Sound Effect vorbereiten
        self.sound = QSoundEffect()
        # Sound-Dateien in PyQt Multimedia brauchen oft lokale File-URLs
        self.sound.setSource(QUrl.fromLocalFile(resource_path("assets/intro_sound.wav")))
        # Wenn die Audio fertig asynchron geladen wurde, hören wir darauf:
        self.sound.statusChanged.connect(self._on_sound_status_changed)
        
        # Für den weichen Fade-Effekt am Ende
        self.fade_overlay = FadeOverlay(self)
        self.fade_overlay.resize(self.size())
        
        # Die Animation, die später unsere opacity(Durchsichtigkeit) von 0 auf 1 ändert
        self.fade_anim = QPropertyAnimation(self.fade_overlay, b"opacity")
        self.fade_anim.setDuration(1200) # FadeOut dauert 1,2 Sekunden
        self.fade_anim.setStartValue(0.0)
        self.fade_anim.setEndValue(1.0)
        # Wenn die Animation zu Ende ist, schließen wir das Fenster endgültig
        self.fade_anim.finished.connect(self.launch_dashboard)
        
        # Jetzt das Fenster mittig auf den Bildschirm setzen
        self._center_on_screen()

    def _center_on_screen(self):
        """Setzt das rahmenlose Fenster exakt in die Bildschirm-Mitte"""
        screen = QApplication.primaryScreen().availableGeometry()
        size = self.geometry()
        self.move(int((screen.width() - size.width()) / 2),
                  int((screen.height() - size.height()) / 2))

    def _on_sound_status_changed(self):
        """Diese Funktion löst aus, wenn die Sound-Datei fertig geladen ist."""
        if self.sound.status() == QSoundEffect.Status.Ready:
            # Jetzt wissen wir, der Sound kann abgespielt werden
            # WICHTIG: QSoundEffect hat in Qt6 kein "duration()" oder "length()" mehr. 
            # Stattdessen spielen wir den Sound einfach ab und bauen einen festen Timer ein.
            # Alternativ nutzt man QMediaPlayer, aber QSoundEffect reicht für kurze Intros.
            # Wir nehmen für das Intro pauschal 3,5 Sekunden an (3500ms).
            sound_length_ms = 3500 
            
            # Starte den Ton
            self.sound.play()
            
            # Da wir weich ausblenden (dauert 1,2sek), beginnen wir die Blende 
            # kurz bevor der Ton ganz aufhört.
            # (Wir subtrahieren 1000ms von der Länge)
            fade_start_ms = max(0, sound_length_ms - 1000)
            
            # QTimer führt eine Funktion nach X Millisekunden aus
            QTimer.singleShot(fade_start_ms, self.start_fade_out)


    def start_fade_out(self):
        """Startet den Effekt, bei dem das Bild schwarz wird"""
        self.fade_anim.start()

    def launch_dashboard(self):
        """Diese Funktion wird von der Animation aufgerufen, wenn sie fertig ist (alles schwarz ist)"""
        # 1. ZUERST das Dashboard öffnen, damit immer ein Fenster für Qt aktiv ist 
        #    (Sonst denkt Qt, die App soll beendet werden, weil kurz 0 Fenster offen sind!)
        self.dashboard = DashboardWindow(self.settings_manager)
        self.dashboard.show()
        self.dashboard.raise_()
        self.dashboard.activateWindow()
        
        # 2. DANN das Intro-Fenster schließen
        self.close()


def main():
    """
    Das Hauptevent: Hier startet Qt.
    """
    install_global_exception_hooks()
    try:
        import ctypes
        # Für Windows: Die Taskleiste dazu zwingen, unser eigenes Icon anzuzeigen (verhindert gruppieren mit python.exe)
        myappid = 'just.business.tools.main.1' 
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
        
        app = QApplication(sys.argv)
        
        # Das Window Icon für alle Fenster der App setzen
        app.setWindowIcon(QIcon(resource_path("assets/app_icon.ico")))
        
        # Globale Stylesheets direkt auf die komplette Applikation anwenden
        from module.style_manager import StyleManager
        app.setStyleSheet(StyleManager.get_global_stylesheet())
        
        # --- NEU: Einstellungen einlesen & Datenbank Schema prüfen/erstellen ---
        settings = SettingsManager()
        secret_warnings = settings.consume_secret_warnings()
        if secret_warnings:
            QMessageBox.warning(None, "Secret-Speicher", "\n\n".join(secret_warnings))
        db_manager = DatabaseManager(settings)
        
        try:
            db_manager.init_database()
            print("[INFO] MySQL Datenbank erfolgreich verbunden und Schema geladen.")
            wipe_result = maybe_wipe_on_start(settings, db_manager=db_manager)
            if wipe_result.get("performed"):
                tables_txt = ", ".join(wipe_result.get("wiped_tables", []))
                print(f"[TEST-WIPE] Daten geloescht: {tables_txt}")
                print(f"[TEST-WIPE] Mapping zurueckgesetzt: {wipe_result.get('mapping_path', '')}")
            elif wipe_result.get("reason") == "error":
                print(f"[TEST-WIPE] Fehler: {wipe_result.get('error', 'unbekannt')}")
        except Exception as e:
            log_exception(__name__, e)
            # Hier geben wir eine freundliche Warnung, lassen das Programm aber weiter hochfahren
            QMessageBox.warning(None, "XAMPP / MySQL nicht erreichbar", 
                                f"Konnte nicht zur lokalen Datenbank verbinden.\n\n"
                                f"Hast du XAMPP und MySQL gestartet?\n\nFehlermeldung:\n{sanitize_text(e)}")
                                
        # 2. Intro Starten
        intro = IntroSplashScreen(settings)
        intro.show()
        
        # Die exec-Schleife hält das Programm am Leben, 
        # andernfalls würde das Skript hier einfach beendet sein.
        sys.exit(app.exec())
    except Exception as e:
        log_exception("main.main", e)
        try:
            QMessageBox.critical(None, "Kritischer Fehler", f"Die App ist abgestuerzt. Details stehen im Crash-Log.\n\nFehler: {sanitize_text(e)}")
        except Exception as e:
            log_exception(__name__, e)
            pass
        raise


if __name__ == "__main__":
    main()


