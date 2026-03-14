"""Schema- und Wartungslogik fuer DatabaseManager."""

import logging
from mysql.connector import Error
from module.crash_logger import log_exception

class SchemaManagementMixin:
    def init_database(self):
        """
        Erstellt die Datenbank, falls sie nicht existiert, und richtet alle Tabellen ein.
        (Wird direkt vom Dashboard oder beim Start aufgerufen)
        """
        try:
            # 1. Datenbank erstellen (falls neu)
            conn_server = self._get_connection(include_db=False)
            cursor_server = conn_server.cursor()
            db_name = self.settings.get('db_name', 'buchhaltung')
            cursor_server.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            conn_server.commit()
            cursor_server.close()
            conn_server.close()

            # 2. Tabellen in der Datenbank anlegen
            conn_db = self._get_connection()
            cursor_db = conn_db.cursor()

            # Tabelle 1: einkauf_bestellungen (Geldabfluss & Inbound-Logistik)
            cursor_db.execute("""
                CREATE TABLE IF NOT EXISTS einkauf_bestellungen (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    bestellnummer VARCHAR(255) UNIQUE NOT NULL,
                    kaufdatum DATE,
                    shop_name VARCHAR(255),
                    bestell_email VARCHAR(255),
                    tracking_nummer_einkauf VARCHAR(255),
                    paketdienst VARCHAR(255),
                    lieferdatum DATE,
                    sendungsstatus VARCHAR(50) DEFAULT 'Noch nicht los',
                    gesamt_ekp_brutto DECIMAL(10,2),
                    warenwert_brutto DECIMAL(10,2) DEFAULT 0,
                    versandkosten_brutto DECIMAL(10,2) DEFAULT 0,
                    nebenkosten_brutto DECIMAL(10,2) DEFAULT 0,
                    rabatt_brutto DECIMAL(10,2) DEFAULT 0,
                    einstand_gesamt_brutto DECIMAL(10,2) DEFAULT 0,
                    ust_satz DECIMAL(5,2),
                    rechnung_pdf_pfad VARCHAR(500),
                    rechnung_vorhanden BOOLEAN DEFAULT FALSE
                )
            """)

            # NachtrÃ¤gliche Updates fÃ¼r bestehende Datenbanken (Fehler ignorieren, falls Spalte existiert)
            try: cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN paketdienst VARCHAR(255)")
            except Error: pass
            
            try: cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN lieferdatum DATE")
            except Error: pass
            
            # NachtrÃ¤gliches Ã„ndern bestehender DATETIME Spalten zu DATE
            try: cursor_db.execute("ALTER TABLE einkauf_bestellungen MODIFY kaufdatum DATE")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE einkauf_bestellungen MODIFY lieferdatum DATE")
            except Error: pass
            
            try: cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN sendungsstatus VARCHAR(50) DEFAULT 'Noch nicht los'")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN warenwert_brutto DECIMAL(10,2) DEFAULT 0")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN versandkosten_brutto DECIMAL(10,2) DEFAULT 0")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN nebenkosten_brutto DECIMAL(10,2) DEFAULT 0")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN rabatt_brutto DECIMAL(10,2) DEFAULT 0")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN einstand_gesamt_brutto DECIMAL(10,2) DEFAULT 0")
            except Error: pass

            # Tabelle 2: verkauf_tickets (Geldzufluss & Absatz)
            cursor_db.execute("""
                CREATE TABLE IF NOT EXISTS verkauf_tickets (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ticket_name VARCHAR(255) UNIQUE NOT NULL,
                    abnehmer_typ ENUM('Discord', 'Dritter') NOT NULL,
                    erstellungsdatum DATE,
                    zahlungsziel VARCHAR(255),
                    kaeufer VARCHAR(255),
                    pending_payload_json LONGTEXT,
                    matching_status VARCHAR(50) DEFAULT 'MATCHED',
                    rechnung_an_abnehmer_verschickt BOOLEAN DEFAULT FALSE,
                    geld_erhalten BOOLEAN DEFAULT FALSE
                )
            """)
            try: cursor_db.execute("ALTER TABLE verkauf_tickets MODIFY erstellungsdatum DATE")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE verkauf_tickets ADD COLUMN kaeufer VARCHAR(255)")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE verkauf_tickets ADD COLUMN pending_payload_json LONGTEXT")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE verkauf_tickets ADD COLUMN matching_status VARCHAR(50) DEFAULT 'MATCHED'")
            except Error: pass

            # Tabelle 4: ausgangs_pakete (Outbound-Logistik)
            cursor_db.execute("""
                CREATE TABLE IF NOT EXISTS ausgangs_pakete (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    tracking_nummer VARCHAR(255) UNIQUE NOT NULL,
                    versanddatum DATE,
                    paketdienst VARCHAR(255),
                    sendungsstatus VARCHAR(50) DEFAULT 'Noch nicht los'
                )
            """)
            
            # NachtrÃ¤gliche Updates
            try: cursor_db.execute("ALTER TABLE ausgangs_pakete ADD COLUMN paketdienst VARCHAR(255)")
            except Error: pass
            
            try: cursor_db.execute("ALTER TABLE ausgangs_pakete MODIFY versanddatum DATE")
            except Error: pass
            
            try: cursor_db.execute("ALTER TABLE ausgangs_pakete ADD COLUMN sendungsstatus VARCHAR(50) DEFAULT 'Noch nicht los'")
            except Error: pass

            # Tabelle 3: waren_positionen (Das HerzstÃ¼ck / Verbindungsglied)
            cursor_db.execute("""
                CREATE TABLE IF NOT EXISTS waren_positionen (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    einkauf_id INT NOT NULL,
                    verkauf_ticket_id INT NULL,
                    ausgangs_paket_id INT NULL,
                    produkt_name VARCHAR(255) NOT NULL,
                    varianten_info VARCHAR(255),
                    ean VARCHAR(100),
                    menge INT DEFAULT 1,
                    menge_geliefert INT DEFAULT 0,
                    ekp_brutto DECIMAL(10,2),
                    bezugskosten_anteil_brutto DECIMAL(10,2) DEFAULT 0,
                    einstand_brutto DECIMAL(10,2),
                    vk_brutto DECIMAL(10,2),
                    marge_gesamt DECIMAL(10,2),
                    versandart VARCHAR(50), 
                    seriennummern TEXT,
                    status VARCHAR(100) DEFAULT 'WAITING_FOR_ORDER',
                    zahlungsstatus VARCHAR(50) DEFAULT 'Offen',
                    buchhaltungsstatus VARCHAR(50) DEFAULT 'Keine Rechnung',
                    FOREIGN KEY (einkauf_id) REFERENCES einkauf_bestellungen(id) ON DELETE CASCADE,
                    FOREIGN KEY (verkauf_ticket_id) REFERENCES verkauf_tickets(id) ON DELETE SET NULL,
                    FOREIGN KEY (ausgangs_paket_id) REFERENCES ausgangs_pakete(id) ON DELETE SET NULL
                )
            """)
            
            try: cursor_db.execute("ALTER TABLE waren_positionen ADD COLUMN menge_geliefert INT DEFAULT 0")
            except Error: pass
            
            try: cursor_db.execute("ALTER TABLE waren_positionen ADD COLUMN zahlungsstatus VARCHAR(50) DEFAULT 'Offen'")
            except Error: pass
            
            try: cursor_db.execute("ALTER TABLE waren_positionen ADD COLUMN buchhaltungsstatus VARCHAR(50) DEFAULT 'Keine Rechnung'")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE waren_positionen ADD COLUMN bezugskosten_anteil_brutto DECIMAL(10,2) DEFAULT 0")
            except Error: pass
            try: cursor_db.execute("ALTER TABLE waren_positionen ADD COLUMN einstand_brutto DECIMAL(10,2)")
            except Error: pass

            # Tabelle 5: produkt_bilder (FÃ¼r die Zuordnung von Artikeln zu Bildern auf Dateisystem)
            cursor_db.execute("""
                CREATE TABLE IF NOT EXISTS produkt_bilder (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    produkt_name VARCHAR(255) UNIQUE NOT NULL,
                    bild_pfad VARCHAR(500)
                )
            """)

            # Tabelle 6: ean_katalog (zentrale, moduluebergreifende EAN-Wissensbasis)
            cursor_db.execute("""
                CREATE TABLE IF NOT EXISTS ean_katalog (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    produkt_name VARCHAR(255) NOT NULL,
                    varianten_info VARCHAR(255) DEFAULT '',
                    ean VARCHAR(100) NOT NULL,
                    bild_url VARCHAR(500),
                    quelle VARCHAR(100),
                    confidence DECIMAL(5,2) DEFAULT 1.00,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_ean_map (produkt_name, varianten_info, ean),
                    INDEX idx_ean_only (ean),
                    INDEX idx_name (produkt_name)
                )
            """)

            # Tabelle 7: ean_alias_cache (lernt rohe/tatsaechliche Produkttitel fuer spaetere schnelle Wiedererkennung)
            cursor_db.execute("""
                CREATE TABLE IF NOT EXISTS ean_alias_cache (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    raw_name VARCHAR(255) NOT NULL,
                    cleaned_name VARCHAR(255) DEFAULT '',
                    varianten_info VARCHAR(255) DEFAULT '',
                    chosen_query VARCHAR(255) DEFAULT '',
                    matched_title VARCHAR(255) DEFAULT '',
                    ean VARCHAR(100) NOT NULL,
                    brand VARCHAR(100) DEFAULT '',
                    model_code VARCHAR(100) DEFAULT '',
                    category_hint VARCHAR(100) DEFAULT '',
                    confidence DECIMAL(5,2) DEFAULT 0.50,
                    source VARCHAR(100) DEFAULT 'manual',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uniq_alias_map (raw_name, cleaned_name, ean),
                    INDEX idx_alias_raw (raw_name),
                    INDEX idx_alias_cleaned (cleaned_name),
                    INDEX idx_alias_ean (ean)
                )
            """)

            conn_db.commit()
            cursor_db.close()
            conn_db.close()
            
            logging.info("Datenbank und Tabellen erfolgreich initialisiert.")
        except Exception as e:
            log_exception(__name__, e)
            logging.error(f"Fehler bei der Datenbank-Initialisierung: {e}")
            raise Exception(f"Kritischer Fehler beim Anlegen der Tabellen:\n{e}")

    def wipe_all_data_for_testing(self):
        """
        Loescht ALLE Datensaetze aus den Kern-Tabellen fuer reproduzierbare Tests.
        Die Tabellenstruktur bleibt dabei erhalten.
        """
        conn = self._get_connection()
        if not conn.is_connected():
            raise Exception("Keine aktive Verbindung zur Datenbank moeglich.")

        cursor = None
        tables = [
            "waren_positionen",
            "produkt_bilder",
            "ean_katalog",
            "ean_alias_cache",
            "verkauf_tickets",
            "ausgangs_pakete",
            "einkauf_bestellungen",
        ]
        wiped_tables = []

        try:
            cursor = conn.cursor()
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            for table in tables:
                try:
                    cursor.execute(f"TRUNCATE TABLE `{table}`")
                    wiped_tables.append(table)
                except Error:
                    cursor.execute(f"DELETE FROM `{table}`")
                    wiped_tables.append(table)

            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            conn.commit()
            return {"wiped_tables": wiped_tables}
        except Exception as e:
            log_exception(__name__, e)
            try:
                if cursor:
                    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            except Exception:
                pass

            if conn:
                conn.rollback()
            raise Exception(f"Fehler beim Test-Wipe: {e}")
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

