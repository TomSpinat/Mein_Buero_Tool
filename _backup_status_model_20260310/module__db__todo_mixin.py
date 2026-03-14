"""Ausgelagerte Repository-Methoden fuer DatabaseManager."""

from module.crash_logger import log_exception

class TodoMixin:
    def get_todo_items(self):
        """
        Ermittelt alle aktuell relevanten ToDos durch Datenbankabfragen.
        Gibt eine Liste von Dicts zurÃ¼ck, die in modul_todo.py fÃ¼r UI-Karten genutzt werden.
        """
        todos = []
        conn = self._get_connection()
        if not conn.is_connected():
            return todos

        try:
            cursor = conn.cursor(dictionary=True)

            # 1. Fehlende EAN (Artikel ohne Barcode)
            cursor.execute("""
                SELECT w.produkt_name, e.bestellnummer, e.shop_name, COUNT(w.id) AS anzahl
                FROM waren_positionen w 
                JOIN einkauf_bestellungen e ON w.einkauf_id = e.id
                WHERE w.ean IS NULL OR w.ean = ''
                GROUP BY w.produkt_name, e.bestellnummer, e.shop_name
            """)
            missing_eans = cursor.fetchall()
            for item in missing_eans:
                count = int(item.get("anzahl", 1) or 1)
                count_text = f" x{count}" if count > 1 else ""
                todos.append({
                    "title": "EAN fehlt! (Stammdaten)",
                    "desc": f"'{item.get('produkt_name')}'{count_text} (Bestellung: {item.get('bestellnummer')}) benÃ¶tigt einen Barcode.",
                    "type": "warning",
                    "action": "open_scanner"
                })

            # 2. EingangsprÃ¼fung ausstehend (Paket geliefert, aber Artikel nicht IN_STOCK)
            # Wir betrachten EinkÃ¤ufe, bei denen der Sendungsstatus 'Geliefert' ist, 
            # aber noch Artikel vorhanden sind, die auf PrÃ¼fung warten
            cursor.execute("""
                SELECT e.id, e.bestellnummer, e.shop_name, COUNT(w.id) as anzahl 
                FROM einkauf_bestellungen e 
                JOIN waren_positionen w ON w.einkauf_id = e.id 
                WHERE LOWER(e.sendungsstatus) = 'geliefert' 
                  AND w.status != 'IN_STOCK'
                GROUP BY e.id
            """)
            pending_inbound = cursor.fetchall()
            for item in pending_inbound:
                todos.append({
                    "title": "Wareneingang prÃ¼fen",
                    "desc": f"Das Paket von {item.get('shop_name')} ({item.get('bestellnummer')}) liegt hier. Bitte mit Scanner prÃ¼fen!",
                    "type": "info",
                    "action": "open_inbound"
                })

            # 3. Versandbereit (Artikel wurden auf Ticket verkauft, aber noch nicht gepackt)
            # Artikel ist auf ein Ticket gebucht und ist theoretisch IN_STOCK, hat aber kein ausgangs_paket
            cursor.execute("""
                SELECT t.ticket_name, COUNT(w.id) as anzahl 
                FROM verkauf_tickets t 
                JOIN waren_positionen w ON w.verkauf_ticket_id = t.id 
                WHERE w.ausgangs_paket_id IS NULL AND w.status = 'IN_STOCK'
                GROUP BY t.id
            """)
            ready_to_ship = cursor.fetchall()
            for item in ready_to_ship:
                # Plural check
                artikel_wort = "Artikel" if item.get('anzahl', 1) == 1 else "Artikel"
                todos.append({
                    "title": "Versandbereit",
                    "desc": f"Ticket {item.get('ticket_name')}: {item.get('anzahl')} {artikel_wort} warten auf Verpackung.",
                    "type": "success",
                    "action": "open_packstation"
                })

            cursor.close()
        except Exception as e:
            log_exception(__name__, e)
            print(f"Fehler bei get_todo_items: {e}")
        finally:
            if conn and conn.is_connected():
                conn.close()

        return todos

