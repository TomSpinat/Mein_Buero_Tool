import os
import sys
from datetime import datetime

# Make project root importable when script is executed from dev_tools
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import SettingsManager
from module.database_manager import DatabaseManager


class E2ETicketFolgtPath:
    def __init__(self):
        self.settings = SettingsManager()
        self.db = DatabaseManager(self.settings)
        self.tag = datetime.now().strftime("%Y%m%d_%H%M%S")

    def _log(self, msg):
        print(msg)

    def _assert(self, condition, msg):
        if not condition:
            raise AssertionError(msg)

    def _query_one(self, sql, params=(), dictionary=False):
        conn = self.db._get_connection()
        cur = None
        try:
            cur = conn.cursor(dictionary=dictionary)
            cur.execute(sql, params)
            return cur.fetchone()
        finally:
            if cur:
                cur.close()
            if conn and conn.is_connected():
                conn.close()

    def _query_val(self, sql, params=()):
        row = self._query_one(sql, params=params, dictionary=False)
        return row[0] if row else None

    def reset_database(self):
        self._log("[1/6] Initialisiere Schema und leere E2E-Tabellen...")
        self.db.init_database()

        conn = self.db._get_connection()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            cur.execute("TRUNCATE TABLE waren_positionen")
            cur.execute("TRUNCATE TABLE einkauf_bestellungen")
            cur.execute("TRUNCATE TABLE verkauf_tickets")
            cur.execute("TRUNCATE TABLE ausgangs_pakete")
            cur.execute("SET FOREIGN_KEY_CHECKS = 1")
            conn.commit()
        finally:
            if cur:
                cur.close()
            if conn and conn.is_connected():
                conn.close()

    def create_ticket_folgt_sale(self):
        self._log("[2/6] Lege Discord-Verkauf zuerst an (ohne vorhandenen Einkauf)...")
        self.ticket_name = f"E2E-TICKET-{self.tag}"
        self.sale_data = {
            "ticket_name": self.ticket_name,
            "kaeufer": "E2EBuyer",
            "zahlungsziel": "ticket folgt",
            "waren": [
                {
                    "produkt_name": "Sony PlayStation 5 Pro",
                    "ean": "0711719395201",
                    "menge": "2",
                    "vk_brutto": "899.00",
                    "marge_gesamt": "200.00",
                },
                {
                    "produkt_name": "Gaming Headset Diox-XY",
                    "ean": "",
                    "menge": "1",
                    "vk_brutto": "149.00",
                    "marge_gesamt": "45.00",
                },
            ],
        }

        matched, pending, pending_summary = self.db.preview_verkauf_discord(self.sale_data)
        self._assert(len(matched) == 0, "Es sollten anfangs keine Matches existieren.")
        self._assert(len(pending) == 3, "Es sollten 3 offene Einheiten als 'ticket folgt' verbleiben.")
        self._assert(len(pending_summary) >= 1, "Pending-Zusammenfassung darf nicht leer sein.")

        result = self.db.confirm_verkauf_discord(self.sale_data, matched, pending)
        self._assert(result["pending_count"] == 3, "Ticket muss mit 3 offenen Einheiten gespeichert werden.")

        row = self._query_one(
            "SELECT id, matching_status, pending_payload_json FROM verkauf_tickets WHERE ticket_name = %s",
            (self.ticket_name,),
            dictionary=True,
        )
        self._assert(row is not None, "Verkaufsticket wurde nicht gespeichert.")
        self.ticket_id = int(row["id"])
        self._assert(row["matching_status"] == "TICKET_FOLGT", "Ticketstatus muss TICKET_FOLGT sein.")
        self._assert(row["pending_payload_json"], "pending_payload_json muss befuellt sein.")

    def import_later_purchase_and_auto_match(self):
        self._log("[3/6] Importiere spaeter den Einkauf und pruefe Auto-Nachverknuepfung...")
        einkauf_data = {
            "bestellnummer": f"E2E-ORDER-{self.tag}",
            "kaufdatum": "2026-03-08",
            "shop_name": "Amazon DE",
            "bestell_email": "e2e@example.com",
            "tracking_nummer_einkauf": "E2E-INBOUND-001",
            "paketdienst": "DHL",
            "lieferdatum": "",
            "sendungsstatus": "Noch nicht los",
            "gesamt_ekp_brutto": "1947.00",
            "ust_satz": "19.00",
            "waren": [
                {
                    "produkt_name": "Sony PlayStation 5 Pro",
                    "varianten_info": "",
                    "ean": "0711719395201",
                    "menge": "2",
                    "ekp_brutto": "799.00",
                },
                {
                    "produkt_name": "Gaming Headset Diox-XY",
                    "varianten_info": "",
                    "ean": "",
                    "menge": "1",
                    "ekp_brutto": "99.00",
                },
            ],
        }
        self.db.upsert_einkauf_mit_waren(einkauf_data)

        row = self._query_one(
            "SELECT matching_status, pending_payload_json FROM verkauf_tickets WHERE id = %s",
            (self.ticket_id,),
            dictionary=True,
        )
        self._assert(row is not None, "Ticket nach Einkauf nicht mehr auffindbar.")
        self._assert(row["matching_status"] == "MATCHED", "Ticket sollte nach Einkauf vollstaendig MATCHED sein.")
        self._assert(not row["pending_payload_json"], "pending_payload_json sollte nach Matching leer sein.")

        linked_count = self._query_val(
            "SELECT COUNT(*) FROM waren_positionen WHERE verkauf_ticket_id = %s",
            (self.ticket_id,),
        )
        self._assert(int(linked_count or 0) == 3, "Es muessen 3 Einheiten mit dem Ticket verknuepft sein.")

    def verify_non_discord_sale_path(self):
        self._log("[4/6] Pruefe explizit den Pfad ohne Discord-Ticket...")
        einkauf_data = {
            "bestellnummer": f"E2E-NODISCORD-{self.tag}",
            "kaufdatum": "2026-03-08",
            "shop_name": "Saturn",
            "bestell_email": "e2e@example.com",
            "tracking_nummer_einkauf": "E2E-INBOUND-002",
            "paketdienst": "DHL",
            "lieferdatum": "",
            "sendungsstatus": "Noch nicht los",
            "gesamt_ekp_brutto": "120.00",
            "ust_satz": "19.00",
            "waren": [
                {
                    "produkt_name": "Standalone Artikel",
                    "varianten_info": "",
                    "ean": "9990001112223",
                    "menge": "2",
                    "ekp_brutto": "60.00",
                }
            ],
        }
        self.db.upsert_einkauf_mit_waren(einkauf_data)

        rows_without_ticket = self._query_val(
            """
            SELECT COUNT(*)
            FROM waren_positionen w
            JOIN einkauf_bestellungen e ON w.einkauf_id = e.id
            WHERE e.bestellnummer = %s AND w.verkauf_ticket_id IS NULL
            """,
            (einkauf_data["bestellnummer"],),
        )
        self._assert(int(rows_without_ticket or 0) == 2, "Nicht-Discord-Einkauf muss ohne Ticket verbleiben.")

        conn = self.db._get_connection()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO ausgangs_pakete (tracking_nummer, versanddatum, sendungsstatus) VALUES (%s, CURDATE(), 'Noch nicht los')",
                (f"E2E-OUT-{self.tag}",),
            )
            paket_id = cur.lastrowid

            cur.execute(
                """
                UPDATE waren_positionen w
                JOIN einkauf_bestellungen e ON w.einkauf_id = e.id
                SET w.status = 'SHIPPED', w.ausgangs_paket_id = %s, w.seriennummern = %s
                WHERE e.bestellnummer = %s
                ORDER BY w.id ASC
                LIMIT 1
                """,
                (paket_id, f"E2E-SN-{self.tag}", einkauf_data["bestellnummer"]),
            )
            conn.commit()
        finally:
            if cur:
                cur.close()
            if conn and conn.is_connected():
                conn.close()

        shipped_without_ticket = self._query_val(
            """
            SELECT COUNT(*)
            FROM waren_positionen w
            JOIN einkauf_bestellungen e ON w.einkauf_id = e.id
            WHERE e.bestellnummer = %s
              AND w.status = 'SHIPPED'
              AND w.verkauf_ticket_id IS NULL
            """,
            (einkauf_data["bestellnummer"],),
        )
        self._assert(
            int(shipped_without_ticket or 0) >= 1,
            "Mindestens eine Einheit ohne Discord-Ticket sollte versandbar sein.",
        )

    def print_summary(self):
        self._log("[5/6] Ergebnisse zusammenfassen...")
        todo_count = self._query_val("SELECT COUNT(*) FROM verkauf_tickets WHERE matching_status = 'TICKET_FOLGT'")
        self._log(f"  -> Offene ticket-folgt Tickets: {int(todo_count or 0)}")
        linked_count = self._query_val("SELECT COUNT(*) FROM waren_positionen WHERE verkauf_ticket_id IS NOT NULL")
        self._log(f"  -> Mit Tickets verknuepfte Einheiten: {int(linked_count or 0)}")

    def run(self):
        self._log("=== E2E PATH: TICKET FOLGT + OHNE DISCORD ===")
        self.reset_database()
        self.create_ticket_folgt_sale()
        self.import_later_purchase_and_auto_match()
        self.verify_non_discord_sale_path()
        self.print_summary()
        self._log("[6/6] E2E erfolgreich abgeschlossen.")


if __name__ == "__main__":
    runner = E2ETicketFolgtPath()
    runner.run()