"""Ausgelagerte Repository-Methoden fuer DatabaseManager."""

import logging
from mysql.connector import Error
from module.crash_logger import log_exception

class PomsRepositoryMixin:
    def get_poms_stats(self):
        """
        Berechnet die Statistiken fÃ¼r das POMS Dashboard:
        - Open Orders: Nicht finalisiert (z.B. WAITING_FOR_ORDER oder IN_STOCK aber noch nicht versendet)
        - Sent Orders: Pakete die unterwegs sind
        - Delivered w/o Invoice: Geliefert aber noch keine Rechnung gebucht
        - Turnover & Profit: VK / Gewinn fÃ¼r den aktuellen Monat anhand der verkauf_tickets
        """
        stats = {
            'open_orders': 0,
            'sent_orders': 0,
            'out_for_delivery': 0,
            'delivered_not_invoiced': 0,
            'revenue_current': 0.0,
            'profit_current': 0.0,
            'revenue_last': 0.0,
            'profit_last': 0.0
        }
        
        conn = self._get_connection()
        if not conn.is_connected():
            return stats

        try:
            cursor = conn.cursor(dictionary=True)

            # Open Orders: Alle, die nicht storniert (status) und noch nicht final verschickt sind
            cursor.execute("SELECT COUNT(id) as count FROM waren_positionen WHERE status IN ('WAITING_FOR_ORDER', 'IN_STOCK')")
            if row := cursor.fetchone():
                stats['open_orders'] = int(row['count'])

            # Sent Orders: Ausgangspakete existieren, Status nicht 'Geliefert'
            cursor.execute("SELECT COUNT(w.id) as count FROM waren_positionen w JOIN ausgangs_pakete a ON w.ausgangs_paket_id = a.id WHERE a.sendungsstatus != 'Geliefert'")
            if row := cursor.fetchone():
                stats['sent_orders'] = int(row['count'])
                
            # Out for Delivery: Wie Sent, aber mit spezifischem Status
            cursor.execute("SELECT COUNT(w.id) as count FROM waren_positionen w JOIN ausgangs_pakete a ON w.ausgangs_paket_id = a.id WHERE a.sendungsstatus IN ('Unterwegs', 'In Auslieferung')")
            if row := cursor.fetchone():
                stats['out_for_delivery'] = int(row['count'])

            # Delivered without Invoice status "Gebucht"
            # Da wir buchhaltungsstatus hinzugefÃ¼gt haben, zÃ¤hlen wir hier alle gelieferten Pakete, wo KEINE Rechnung gebucht ist.
            cursor.execute("SELECT COUNT(w.id) as count FROM waren_positionen w JOIN ausgangs_pakete a ON w.ausgangs_paket_id = a.id WHERE a.sendungsstatus = 'Geliefert' AND w.buchhaltungsstatus != 'Gebucht'")
            if row := cursor.fetchone():
                stats['delivered_not_invoiced'] = int(row['count'])

            # Financials Current Month (Nutzt das Erstellungsdatum des Tickets)
            cursor.execute("""
                SELECT SUM(w.vk_brutto) as revenue, SUM(w.marge_gesamt) as profit 
                FROM waren_positionen w 
                JOIN verkauf_tickets t ON w.verkauf_ticket_id = t.id 
                WHERE DATE_FORMAT(t.erstellungsdatum, '%Y-%m') = DATE_FORMAT(NOW(), '%Y-%m')
            """)
            if row := cursor.fetchone():
                stats['revenue_current'] = float(row['revenue'] or 0.0)
                stats['profit_current'] = float(row['profit'] or 0.0)

            # Financials Last Month
            cursor.execute("""
                SELECT SUM(w.vk_brutto) as revenue, SUM(w.marge_gesamt) as profit 
                FROM waren_positionen w 
                JOIN verkauf_tickets t ON w.verkauf_ticket_id = t.id 
                WHERE DATE_FORMAT(t.erstellungsdatum, '%Y-%m') = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 MONTH), '%Y-%m')
            """)
            if row := cursor.fetchone():
                stats['revenue_last'] = float(row['revenue'] or 0.0)
                stats['profit_last'] = float(row['profit'] or 0.0)

            cursor.close()
        except Exception as e:
            log_exception(__name__, e)
            print(f"Fehler bei get_poms_stats: {e}")
        finally:
            conn.close()

        return stats

    def get_poms_orders(self, search="", show_all=False, filter_type=""):
        """
        Zieht sich alle "Positionen" und stellt sie im flachen Layout der POMS JS App dar.
        Eine Zeile = Eine Warenposition mit verknuepftem Einkauf und ggf. Verkauf.
        """
        orders = []
        conn = self._get_connection()
        if not conn.is_connected():
            return orders

        try:
            cursor = conn.cursor(dictionary=True)

            sql = """
                SELECT
                    w.id,
                    e.bestellnummer as ordernumber,
                    e.shop_name as shop,
                    e.kaufdatum as orderdate,
                    e.bestell_email as mail,
                    w.produkt_name as item,
                    w.menge,
                    COALESCE(w.einstand_brutto, w.ekp_brutto) as ek,
                    w.vk_brutto as vk,
                    w.marge_gesamt as win,
                    w.status as orderstate,
                    w.zahlungsstatus as paymentstate,
                    w.buchhaltungsstatus as invoicestate,
                    IFNULL(a.tracking_nummer, e.tracking_nummer_einkauf) as tracking,
                    t.ticket_name as notes
                FROM waren_positionen w
                JOIN einkauf_bestellungen e ON w.einkauf_id = e.id
                LEFT JOIN verkauf_tickets t ON w.verkauf_ticket_id = t.id
                LEFT JOIN ausgangs_pakete a ON w.ausgangs_paket_id = a.id
            """

            where_clauses = []
            params = []

            if search:
                pattern = self._build_contains_like_pattern(search)
                where_clauses.append(
                    "(e.bestellnummer LIKE %s ESCAPE '\\' OR e.shop_name LIKE %s ESCAPE '\\' OR w.produkt_name LIKE %s ESCAPE '\\')"
                )
                params.extend([pattern, pattern, pattern])
            elif filter_type == 'open_orders':
                where_clauses.append("w.status IN ('WAITING_FOR_ORDER', 'IN_STOCK')")
            elif filter_type == 'sent_orders':
                where_clauses.append("a.id IS NOT NULL AND a.sendungsstatus != 'Geliefert'")
            elif filter_type == 'out_for_delivery':
                where_clauses.append("a.id IS NOT NULL AND (a.sendungsstatus = 'Unterwegs' OR a.sendungsstatus = 'In Auslieferung')")
            elif filter_type == 'delivered_not_invoiced':
                where_clauses.append("w.buchhaltungsstatus != 'Gebucht'")
            elif not show_all:
                where_clauses.append("w.status != 'CANCELLED' AND w.buchhaltungsstatus != 'Gebucht'")

            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)

            sql += " ORDER BY w.id DESC LIMIT 500"

            cursor.execute(sql, tuple(params))
            for row in cursor.fetchall():
                orders.append(row)

            cursor.close()
        except Exception as e:
            log_exception(__name__, e)
            print(f"Fehler bei get_poms_orders: {e}")
        finally:
            conn.close()

        return orders

    def update_poms_status_bulk(self, ids, field, value):
        """
        Aktualisiert ein oder mehrere Felder in der POMS Tabelle bulk.
        GÃ¼ltige Felder: orderstate, paymentstate, invoicestate
        """
        if field not in ['orderstate', 'paymentstate', 'invoicestate']:
            return False

        # Mapping von der flachen Ansicht in unsere DB
        db_column = field
        if field == 'orderstate': db_column = 'status'
        elif field == 'paymentstate': db_column = 'zahlungsstatus'
        elif field == 'invoicestate': db_column = 'buchhaltungsstatus'

        if not ids: return False
        
        # Validierung von Statuswerten fÃ¼r Buchhaltung und Zahlung
        valid_value = value
        if field == 'paymentstate':
            status_map = {1: 'Offen', 2: 'Bezahlt', 3: 'Erstattet'}
            valid_value = status_map.get(int(value), 'Offen')
        elif field == 'invoicestate':
            status_map = {1: 'Keine Rechnung', 2: 'Rechnung vorhanden', 3: 'Gebucht'}
            valid_value = status_map.get(int(value), 'Keine Rechnung')
        elif field == 'orderstate':
            # POMS Order States: 1: Ordered, 2: Sent, 3: Delivered, 4: Canceled
            status_map = {1: 'WAITING_FOR_ORDER', 2: 'IN_STOCK', 3: 'DELIVERED', 4: 'CANCELLED'} # grobes Mapping
            valid_value = status_map.get(int(value), 'WAITING_FOR_ORDER')

        conn = self._get_connection()
        if not conn.is_connected():
            return False

        try:
            cursor = conn.cursor()
            format_strings = ','.join(['%s'] * len(ids))
            params = [valid_value] + ids
            cursor.execute(f"UPDATE waren_positionen SET {db_column} = %s WHERE id IN ({format_strings})", tuple(params))
            conn.commit()
            cursor.close()
            return True
        except Exception as e:
            log_exception(__name__, e)
            print(f"Fehler bei update_poms_status_bulk: {e}")
            if conn: conn.rollback()
            return False
        finally:
            conn.close()

