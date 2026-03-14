import os
import sys
import shutil
from PIL import Image, ImageDraw, ImageFont

# Füge main_app Pfad hinzu
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import SettingsManager
from module.database_manager import DatabaseManager

class MockDataGenerator:
    def __init__(self):
        self.settings = SettingsManager()
        self.db = DatabaseManager(self.settings)
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_daten')
        
        if not os.path.exists(self.test_dir):
            os.makedirs(self.test_dir)

    def clean_database(self):
        """Löscht alle relevanten Tabellen komplett und setzt Mappings zurück."""
        print("[1/3] Leere die Datenbank und lösche Mappings...")
        
        # Mapping.json löschen
        mapping_path = os.path.join(os.path.dirname(self.test_dir), 'mapping.json')
        if os.path.exists(mapping_path):
            try:
                os.remove(mapping_path)
                print("  -> mapping.json erfolgreich gelöscht.")
            except Exception as e:
                print(f"  -> Fehler beim Löschen von mapping.json: {e}")
                
        # Datenbank Tabellen leeren
        conn = self.db._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            cursor.execute("TRUNCATE TABLE waren_positionen;")
            cursor.execute("TRUNCATE TABLE einkauf_bestellungen;")
            cursor.execute("TRUNCATE TABLE verkauf_tickets;")
            cursor.execute("TRUNCATE TABLE ausgangs_pakete;")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            conn.commit()
            print("  -> Datenbank wurde erfolgreich zurückgesetzt.")
        except Exception as e:
            print(f"  -> Fehler beim Leeren: {e}")
        finally:
            cursor.close()
            conn.close()

    def generate_fake_invoice_image(self):
        """Erzeugt ein minimalistisches Bild einer Rechnung für den Gemini Scanner."""
        print("[2/3] Generiere Fake-Dioxyd-Rechnung (Bild)...")
        
        # Ein einfaches A4 hochkant Bild
        img = Image.new('RGB', (800, 1000), color='white')
        d = ImageDraw.Draw(img)
        
        # Simple Fonts (Wir nutzen Default)
        try:
            # Versuch eine Standard-Schrift unter Windows zu laden
            font_title = ImageFont.truetype("arialbd.ttf", 36)
            font_body = ImageFont.truetype("arial.ttf", 20)
            font_small = ImageFont.truetype("arial.ttf", 16)
        except:
            font_title = font_body = font_small = ImageFont.load_default()

        # Briefkopf
        d.text((50, 50), "Rechnung", font=font_title, fill=(0,0,0))
        d.text((50, 100), "Dioxyd Tech GmbH", font=font_body, fill=(50,50,50))
        d.text((50, 125), "Techstraße 1\n10115 Berlin", font=font_body, fill=(50,50,50))
        
        # Metadaten
        d.text((450, 100), "Rechnungsdatum: 05.03.2026", font=font_body, fill=(0,0,0))
        d.text((450, 130), "Bestellnummer: TEST-8991X", font=font_body, fill=(0,0,0))
        d.text((450, 160), "Tracking-Code: DHL-498218310", font=font_body, fill=(0,0,0))

        # Rechnungsposten Header
        y_pos = 300
        d.line([(50, y_pos-10), (750, y_pos-10)], fill=(0,0,0), width=2)
        d.text((50, y_pos), "Menge", font=font_body, fill=(0,0,0))
        d.text((150, y_pos), "Artikel", font=font_body, fill=(0,0,0))
        d.text((550, y_pos), "EAN", font=font_body, fill=(0,0,0))
        d.text((650, y_pos), "Gesamt", font=font_body, fill=(0,0,0))
        d.line([(50, y_pos+30), (750, y_pos+30)], fill=(0,0,0), width=1)

        # Zeile 1
        y_pos += 50
        d.text((50, y_pos), "3x", font=font_body, fill=(0,0,0))
        d.text((150, y_pos), "Sony PlayStation 5 Pro", font=font_body, fill=(0,0,0))
        d.text((550, y_pos), "0711719395201", font=font_small, fill=(50,50,50))
        d.text((650, y_pos), "2.397,00 €", font=font_body, fill=(0,0,0))

        # Zeile 2 (Hier lassen wir absichtlich die EAN weg, um den To-Do Trigger zu testen!)
        y_pos += 40
        d.text((50, y_pos), "1x", font=font_body, fill=(0,0,0))
        d.text((150, y_pos), "Gaming Headset Diox-XY", font=font_body, fill=(0,0,0))
        d.text((550, y_pos), "", font=font_small, fill=(50,50,50)) # LEER
        d.text((650, y_pos), "199,99 €", font=font_body, fill=(0,0,0))

        # Zeile 3
        y_pos += 40
        d.text((50, y_pos), "2x", font=font_body, fill=(0,0,0))
        d.text((150, y_pos), "Ersatzcontroller Weiß", font=font_body, fill=(0,0,0))
        d.text((550, y_pos), "0711719395218", font=font_small, fill=(50,50,50))
        d.text((650, y_pos), "139,98 €", font=font_body, fill=(0,0,0))

        # Summe
        y_pos += 80
        d.line([(450, y_pos-10), (750, y_pos-10)], fill=(0,0,0), width=2)
        d.text((450, y_pos), "Gesamtbetrag:", font=font_title, fill=(0,0,0))
        d.text((450, y_pos+40), "inkl. 19% MwSt.", font=font_small, fill=(50,50,50))
        d.text((650, y_pos), "2.736,97 €", font=font_title, fill=(0,0,0))

        # Footer
        d.text((50, y_pos + 120), "Zahlungsart: Kreditkarte (Visa ****4092)", font=font_body, fill=(50,50,50))

        file_path = os.path.join(self.test_dir, '01_MOCK_Rechnung.png')
        img.save(file_path)
        print(f"  -> Gespeichert unter: {file_path}")

    def generate_fake_discord_bounty(self):
        """Erzeugt einen Screenshot-Mock eines Discord-Deals für den Verkaufsscanner."""
        print("[3/3] Generiere Fake-Discord-Ticket (Bild)...")
        
        img = Image.new('RGB', (600, 400), color='#36393F') # Discord Dark-Grey
        d = ImageDraw.Draw(img)
        
        try:
            font_title = ImageFont.truetype("arialbd.ttf", 24)
            font_body = ImageFont.truetype("arial.ttf", 18)
        except:
            font_title = font_body = ImageFont.load_default()

        # Ticket Header
        d.text((20, 20), "# bounty-ps5-pro-9031", font=font_title, fill='#FFFFFF')
        d.line([(20, 60), (580, 60)], fill='#2F3136', width=2)

        # User Nachricht (Der "Einkäufer" vom Discord)
        d.text((20, 80), "@DioxydBuyer", font=font_title, fill='#5865F2') # Blurple
        d.text((20, 110), "Suche dringend Konsolen für mein Netzwerk asssapppp!!", font=font_body, fill='#DCDDDE')
        
        d.text((20, 150), "Biete:", font=font_title, fill='#FFFFFF')
        d.text((20, 180), "3x Sony PlayStation 5 Pro", font=font_body, fill='#43B581') # Grün
        d.text((20, 210), "Zahle: 900,00 EUR / Stück", font=font_title, fill='#FAA61A') # Orange
        d.text((20, 240), "EAN Pflicht: 0711719395201", font=font_body, fill='#DCDDDE')
        
        d.text((20, 280), "Payout: Instant nach Label-Scan", font=font_body, fill='#DCDDDE')

        file_path = os.path.join(self.test_dir, '02_MOCK_Discord_Ticket.png')
        img.save(file_path)
        print(f"  -> Gespeichert unter: {file_path}")

    def run_all(self):
        print("\n--- TEST UMGEBUNG WIRD AUFGEBAUT ---")
        self.clean_database()
        self.generate_fake_invoice_image()
        self.generate_fake_discord_bounty()
        print("\nFERTIG! Du findest deine Testdateien im Ordner 'test_daten/'.\n")

if __name__ == "__main__":
    app = MockDataGenerator()
    app.run_all()
