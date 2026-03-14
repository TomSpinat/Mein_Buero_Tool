"""Ausgelagerte Repository-Methoden fuer DatabaseManager."""

import time
from module.crash_logger import log_exception


class OrderLookupMixin:
    def find_order_by_number(self, bestellnummer):
        """Liefert Kopfdaten zur Bestellnummer oder None."""
        number = str(bestellnummer or "").strip()
        if not number:
            return None

        conn = self._get_connection()
        if not conn.is_connected():
            return None

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT id, bestellnummer, kaufdatum, shop_name
                FROM einkauf_bestellungen
                WHERE bestellnummer = %s
                LIMIT 1
                """,
                (number,),
            )
            return cursor.fetchone()
        except Exception as e:
            log_exception(__name__, e)
            return None
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def suggest_new_order_number(self, base_bestellnummer):
        """Erzeugt eine freie Bestellnummer auf Basis einer bestehenden."""
        base = str(base_bestellnummer or "").strip()
        if not base:
            base = f"AUTO_{int(time.time())}"

        conn = self._get_connection()
        if not conn.is_connected():
            return base

        cursor = None
        try:
            cursor = conn.cursor()
            candidate = base
            idx = 2
            while True:
                cursor.execute(
                    "SELECT id FROM einkauf_bestellungen WHERE bestellnummer = %s LIMIT 1",
                    (candidate,),
                )
                if not cursor.fetchone():
                    return candidate
                candidate = f"{base}-DOK{idx}"
                idx += 1
        except Exception as e:
            log_exception(__name__, e)
            return base
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()
