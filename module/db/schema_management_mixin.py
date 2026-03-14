"""Schema- und Wartungslogik fuer DatabaseManager."""

import logging
import textwrap

from mysql.connector import Error

from module.crash_logger import log_exception
from module.status_model import (
    InventoryStatus,
    InvoiceStatus,
    PaymentStatus,
    ShipmentStatus,
    TicketMatchingStatus,
    invoice_db_value,
    payment_db_value,
    shipment_db_value,
)


class SchemaManagementMixin:
    def init_database(self):
        """
        Erstellt die Datenbank, falls sie nicht existiert, und richtet alle Tabellen ein.
        (Wird direkt vom Dashboard oder beim Start aufgerufen)
        """
        try:
            conn_server = self._get_connection(include_db=False)
            cursor_server = conn_server.cursor()
            db_name = self.settings.get("db_name", "buchhaltung")
            cursor_server.execute(
                f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            conn_server.commit()
            cursor_server.close()
            conn_server.close()

            conn_db = self._get_connection()
            cursor_db = conn_db.cursor()

            default_shipment_status = shipment_db_value(ShipmentStatus.NOT_DISPATCHED)
            default_inventory_status = InventoryStatus.WAITING_FOR_ORDER.value
            default_payment_status = payment_db_value(PaymentStatus.OPEN)
            default_invoice_status = invoice_db_value(InvoiceStatus.NO_INVOICE)
            default_matching_status = TicketMatchingStatus.MATCHED.value

            cursor_db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS einkauf_bestellungen (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    bestellnummer VARCHAR(255) UNIQUE NOT NULL,
                    kaufdatum DATE,
                    shop_name VARCHAR(255),
                    bestell_email VARCHAR(255),
                    tracking_nummer_einkauf VARCHAR(255),
                    paketdienst VARCHAR(255),
                    lieferdatum DATE,
                    sendungsstatus VARCHAR(50) DEFAULT '{default_shipment_status}',
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
                """
            )

            try:
                cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN paketdienst VARCHAR(255)")
            except Error:
                pass

            try:
                cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN lieferdatum DATE")
            except Error:
                pass

            try:
                cursor_db.execute("ALTER TABLE einkauf_bestellungen MODIFY kaufdatum DATE")
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE einkauf_bestellungen MODIFY lieferdatum DATE")
            except Error:
                pass

            try:
                cursor_db.execute(
                    f"ALTER TABLE einkauf_bestellungen ADD COLUMN sendungsstatus VARCHAR(50) DEFAULT '{default_shipment_status}'"
                )
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN warenwert_brutto DECIMAL(10,2) DEFAULT 0")
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN versandkosten_brutto DECIMAL(10,2) DEFAULT 0")
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN nebenkosten_brutto DECIMAL(10,2) DEFAULT 0")
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN rabatt_brutto DECIMAL(10,2) DEFAULT 0")
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE einkauf_bestellungen ADD COLUMN einstand_gesamt_brutto DECIMAL(10,2) DEFAULT 0")
            except Error:
                pass

            cursor_db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS verkauf_tickets (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ticket_name VARCHAR(255) UNIQUE NOT NULL,
                    abnehmer_typ ENUM('Discord', 'Dritter') NOT NULL,
                    erstellungsdatum DATE,
                    zahlungsziel VARCHAR(255),
                    kaeufer VARCHAR(255),
                    pending_payload_json LONGTEXT,
                    matching_status VARCHAR(50) DEFAULT '{default_matching_status}',
                    rechnung_an_abnehmer_verschickt BOOLEAN DEFAULT FALSE,
                    geld_erhalten BOOLEAN DEFAULT FALSE
                )
                """
            )
            try:
                cursor_db.execute("ALTER TABLE verkauf_tickets MODIFY erstellungsdatum DATE")
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE verkauf_tickets ADD COLUMN kaeufer VARCHAR(255)")
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE verkauf_tickets ADD COLUMN pending_payload_json LONGTEXT")
            except Error:
                pass
            try:
                cursor_db.execute(
                    f"ALTER TABLE verkauf_tickets ADD COLUMN matching_status VARCHAR(50) DEFAULT '{default_matching_status}'"
                )
            except Error:
                pass

            cursor_db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS ausgangs_pakete (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    tracking_nummer VARCHAR(255) UNIQUE NOT NULL,
                    versanddatum DATE,
                    paketdienst VARCHAR(255),
                    sendungsstatus VARCHAR(50) DEFAULT '{default_shipment_status}'
                )
                """
            )

            try:
                cursor_db.execute("ALTER TABLE ausgangs_pakete ADD COLUMN paketdienst VARCHAR(255)")
            except Error:
                pass

            try:
                cursor_db.execute("ALTER TABLE ausgangs_pakete MODIFY versanddatum DATE")
            except Error:
                pass

            try:
                cursor_db.execute(
                    f"ALTER TABLE ausgangs_pakete ADD COLUMN sendungsstatus VARCHAR(50) DEFAULT '{default_shipment_status}'"
                )
            except Error:
                pass

            cursor_db.execute(
                f"""
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
                    status VARCHAR(100) DEFAULT '{default_inventory_status}',
                    zahlungsstatus VARCHAR(50) DEFAULT '{default_payment_status}',
                    buchhaltungsstatus VARCHAR(50) DEFAULT '{default_invoice_status}',
                    FOREIGN KEY (einkauf_id) REFERENCES einkauf_bestellungen(id) ON DELETE CASCADE,
                    FOREIGN KEY (verkauf_ticket_id) REFERENCES verkauf_tickets(id) ON DELETE SET NULL,
                    FOREIGN KEY (ausgangs_paket_id) REFERENCES ausgangs_pakete(id) ON DELETE SET NULL
                )
                """
            )

            try:
                cursor_db.execute("ALTER TABLE waren_positionen ADD COLUMN menge_geliefert INT DEFAULT 0")
            except Error:
                pass

            try:
                cursor_db.execute(
                    f"ALTER TABLE waren_positionen ADD COLUMN zahlungsstatus VARCHAR(50) DEFAULT '{default_payment_status}'"
                )
            except Error:
                pass

            try:
                cursor_db.execute(
                    f"ALTER TABLE waren_positionen ADD COLUMN buchhaltungsstatus VARCHAR(50) DEFAULT '{default_invoice_status}'"
                )
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE waren_positionen ADD COLUMN bezugskosten_anteil_brutto DECIMAL(10,2) DEFAULT 0")
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE waren_positionen ADD COLUMN einstand_brutto DECIMAL(10,2)")
            except Error:
                pass

            cursor_db.execute(
                """
                CREATE TABLE IF NOT EXISTS produkt_bilder (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    produkt_name VARCHAR(255) UNIQUE NOT NULL,
                    bild_pfad VARCHAR(500)
                )
                """
            )

            cursor_db.execute(
                """
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
                """
            )

            cursor_db.execute(
                """
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
                """
            )

            cursor_db.execute(
                textwrap.dedent(
                    """
                    CREATE TABLE IF NOT EXISTS media_assets (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        media_key VARCHAR(191) NOT NULL,
                        media_type VARCHAR(50) NOT NULL,
                        storage_kind VARCHAR(50) DEFAULT 'local_file',
                        file_path VARCHAR(500) NOT NULL,
                        original_name VARCHAR(255) DEFAULT '',
                        mime_type VARCHAR(120) DEFAULT '',
                        file_ext VARCHAR(20) DEFAULT '',
                        file_size_bytes BIGINT DEFAULT 0,
                        sha256 VARCHAR(64) DEFAULT '',
                        width_px INT DEFAULT NULL,
                        height_px INT DEFAULT NULL,
                        source_module VARCHAR(120) DEFAULT '',
                        source_kind VARCHAR(80) DEFAULT '',
                        source_ref VARCHAR(255) DEFAULT '',
                        source_url VARCHAR(1000) DEFAULT '',
                        metadata_json LONGTEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_media_key (media_key),
                        INDEX idx_media_type (media_type),
                        INDEX idx_media_sha256 (sha256)
                    )
                    """
                )
            )

            cursor_db.execute(
                textwrap.dedent(
                    """
                    CREATE TABLE IF NOT EXISTS shop_logo_links (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        shop_key VARCHAR(191) NOT NULL,
                        shop_name VARCHAR(255) NOT NULL,
                        sender_domain VARCHAR(255) DEFAULT '',
                        media_asset_id INT NOT NULL,
                        is_primary BOOLEAN DEFAULT TRUE,
                        priority INT DEFAULT 100,
                        source_note VARCHAR(255) DEFAULT '',
                        confidence DECIMAL(5,2) DEFAULT 1.00,
                        metadata_json LONGTEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_shop_logo (shop_key, media_asset_id),
                        INDEX idx_shop_logo_shop (shop_key),
                        INDEX idx_shop_logo_domain (sender_domain),
                        INDEX idx_shop_logo_primary (shop_key, is_primary, priority),
                        CONSTRAINT fk_shop_logo_asset
                            FOREIGN KEY (media_asset_id) REFERENCES media_assets(id)
                            ON DELETE CASCADE
                    )
                    """
                )
            )

            try:
                cursor_db.execute("ALTER TABLE shop_logo_links ADD COLUMN sender_domain VARCHAR(255) DEFAULT ''")
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE shop_logo_links ADD COLUMN confidence DECIMAL(5,2) DEFAULT 1.00")
            except Error:
                pass
            try:
                cursor_db.execute("ALTER TABLE shop_logo_links ADD COLUMN last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
            except Error:
                pass
            try:
                cursor_db.execute("CREATE INDEX idx_shop_logo_domain ON shop_logo_links (sender_domain)")
            except Error:
                pass

            cursor_db.execute(
                textwrap.dedent(
                    """
                    CREATE TABLE IF NOT EXISTS product_image_links (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        product_key VARCHAR(191) NOT NULL,
                        product_name VARCHAR(255) NOT NULL,
                        ean VARCHAR(100) DEFAULT '',
                        variant_text VARCHAR(255) DEFAULT '',
                        media_asset_id INT NOT NULL,
                        is_primary BOOLEAN DEFAULT FALSE,
                        priority INT DEFAULT 100,
                        source_note VARCHAR(255) DEFAULT '',
                        metadata_json LONGTEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_product_image (product_key, media_asset_id),
                        INDEX idx_product_key (product_key),
                        INDEX idx_product_ean (ean),
                        INDEX idx_product_primary (product_key, is_primary, priority),
                        CONSTRAINT fk_product_image_asset
                            FOREIGN KEY (media_asset_id) REFERENCES media_assets(id)
                            ON DELETE CASCADE
                    )
                    """
                )
            )

            cursor_db.execute(
                textwrap.dedent(
                    """
                    CREATE TABLE IF NOT EXISTS order_item_image_links (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        order_item_id INT NOT NULL,
                        media_asset_id INT NOT NULL,
                        decision_state VARCHAR(40) DEFAULT 'candidate',
                        is_selected BOOLEAN DEFAULT FALSE,
                        is_rejected BOOLEAN DEFAULT FALSE,
                        selection_mode VARCHAR(20) DEFAULT 'auto',
                        source_type VARCHAR(80) DEFAULT '',
                        source_ref VARCHAR(255) DEFAULT '',
                        metadata_json LONGTEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        decided_at TIMESTAMP NULL DEFAULT NULL,
                        UNIQUE KEY uniq_order_item_image (order_item_id, media_asset_id),
                        INDEX idx_order_item_image_item (order_item_id),
                        INDEX idx_order_item_image_selected (order_item_id, is_selected, selection_mode),
                        INDEX idx_order_item_image_state (order_item_id, decision_state),
                        CONSTRAINT fk_order_item_image_item
                            FOREIGN KEY (order_item_id) REFERENCES waren_positionen(id)
                            ON DELETE CASCADE,
                        CONSTRAINT fk_order_item_image_asset
                            FOREIGN KEY (media_asset_id) REFERENCES media_assets(id)
                            ON DELETE CASCADE
                    )
                    """
                )
            )
            cursor_db.execute(
                textwrap.dedent(
                    """
                    CREATE TABLE IF NOT EXISTS screenshot_regions (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        screenshot_asset_id INT NOT NULL,
                        crop_asset_id INT DEFAULT NULL,
                        region_kind VARCHAR(50) DEFAULT 'product_candidate',
                        label VARCHAR(255) DEFAULT '',
                        x INT NOT NULL,
                        y INT NOT NULL,
                        width INT NOT NULL,
                        height INT NOT NULL,
                        coord_origin VARCHAR(30) DEFAULT 'top_left',
                        coord_units VARCHAR(20) DEFAULT 'px',
                        source_kind VARCHAR(80) DEFAULT '',
                        source_ref VARCHAR(255) DEFAULT '',
                        metadata_json LONGTEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_region_screenshot (screenshot_asset_id),
                        INDEX idx_region_crop (crop_asset_id),
                        CONSTRAINT fk_region_screenshot
                            FOREIGN KEY (screenshot_asset_id) REFERENCES media_assets(id)
                            ON DELETE CASCADE,
                        CONSTRAINT fk_region_crop
                            FOREIGN KEY (crop_asset_id) REFERENCES media_assets(id)
                            ON DELETE SET NULL
                    )
                    """
                )
            )

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
            "order_item_image_links",
            "screenshot_regions",
            "product_image_links",
            "shop_logo_links",
            "media_assets",
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



