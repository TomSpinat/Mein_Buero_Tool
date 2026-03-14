"""Ausgelagerte Repository-Methoden fuer DatabaseManager."""

from module.crash_logger import log_exception
from module.status_model import InventoryStatus, ShipmentStatus, shipment_db_value


class TodoMixin:
    def get_todo_items(self):
        """
        Ermittelt alle aktuell relevanten ToDos durch Datenbankabfragen.
        Gibt eine Liste von Dicts zurueck, die in modul_todo.py fuer UI-Karten genutzt werden.
        """
        todos = []
        conn = self._get_connection()
        if not conn.is_connected():
            return todos

        try:
            cursor = conn.cursor(dictionary=True)

            # 1. Fehlende EAN (Artikel ohne Barcode)
            cursor.execute(
                """
                SELECT w.produkt_name, e.bestellnummer, e.shop_name, COUNT(w.id) AS anzahl
                FROM waren_positionen w
                JOIN einkauf_bestellungen e ON w.einkauf_id = e.id
                WHERE w.ean IS NULL OR w.ean = ''
                GROUP BY w.produkt_name, e.bestellnummer, e.shop_name
                """
            )
            missing_eans = cursor.fetchall()
            for item in missing_eans:
                count = int(item.get("anzahl", 1) or 1)
                count_text = f" x{count}" if count > 1 else ""
                todos.append(
                    {
                        "title": "EAN fehlt! (Stammdaten)",
                        "desc": f"'{item.get('produkt_name')}'{count_text} (Bestellung: {item.get('bestellnummer')}) benoetigt einen Barcode.",
                        "type": "warning",
                        "action": "open_scanner",
                    }
                )

            # 2. Eingangspruefung ausstehend (Paket geliefert, aber Artikel noch nicht auf Lager)
            cursor.execute(
                """
                SELECT e.id, e.bestellnummer, e.shop_name, COUNT(w.id) as anzahl
                FROM einkauf_bestellungen e
                JOIN waren_positionen w ON w.einkauf_id = e.id
                WHERE e.sendungsstatus = %s
                  AND w.status != %s
                GROUP BY e.id
                """,
                (
                    shipment_db_value(ShipmentStatus.DELIVERED),
                    InventoryStatus.IN_STOCK.value,
                ),
            )
            pending_inbound = cursor.fetchall()
            for item in pending_inbound:
                todos.append(
                    {
                        "title": "Wareneingang pruefen",
                        "desc": f"Das Paket von {item.get('shop_name')} ({item.get('bestellnummer')}) liegt hier. Bitte mit Scanner pruefen!",
                        "type": "info",
                        "action": "open_inbound",
                    }
                )

            # 3. Versandbereit (verkauft, aber noch nicht gepackt)
            cursor.execute(
                """
                SELECT t.ticket_name, COUNT(w.id) as anzahl
                FROM verkauf_tickets t
                JOIN waren_positionen w ON w.verkauf_ticket_id = t.id
                WHERE w.ausgangs_paket_id IS NULL AND w.status = %s
                GROUP BY t.id
                """,
                (InventoryStatus.IN_STOCK.value,),
            )
            ready_to_ship = cursor.fetchall()
            for item in ready_to_ship:
                todos.append(
                    {
                        "title": "Versandbereit",
                        "desc": f"Ticket {item.get('ticket_name')}: {item.get('anzahl')} Artikel warten auf Verpackung.",
                        "type": "success",
                        "action": "open_packstation",
                    }
                )

            cursor.close()
        except Exception as exc:
            log_exception(__name__, exc)
            print(f"Fehler bei get_todo_items: {exc}")
        finally:
            if conn and conn.is_connected():
                conn.close()

        return todos
