"""
database_manager.py
Kapselt jegliche Logik für die Kommunikation mit dem lokalen/entfernten MySQL Server.
Hier wird beim ersten Start auch das komplette Datenbank-Schema generiert (Tabellen 1-4).
"""

import mysql.connector

from module.crash_logger import log_exception
from module.db.ean_repository_mixin import EanRepositoryMixin
from module.db.media_repository_mixin import MediaRepositoryMixin
from module.db.order_lookup_mixin import OrderLookupMixin
from module.db.poms_repository_mixin import PomsRepositoryMixin
from module.db.todo_mixin import TodoMixin
from module.db.ticket_matching_mixin import TicketMatchingMixin
from module.db.order_processing_mixin import OrderProcessingMixin
from module.db.schema_management_mixin import SchemaManagementMixin

class DatabaseManager(
    EanRepositoryMixin,
    PomsRepositoryMixin,
    MediaRepositoryMixin,
    OrderLookupMixin,
    TicketMatchingMixin,
    OrderProcessingMixin,
    TodoMixin,
    SchemaManagementMixin,
):
    def __init__(self, settings_manager):
        self.settings = settings_manager

    def _to_float(self, val, default=0.0):
        if val in (None, ""):
            return default
        try:
            txt = str(val).strip()
            if not txt:
                return default

            txt = txt.replace("EUR", "").replace("eur", "").replace("€", "").replace(" ", "")

            if "," in txt and "." in txt:
                if txt.rfind(",") > txt.rfind("."):
                    txt = txt.replace(".", "").replace(",", ".")
                else:
                    txt = txt.replace(",", "")
            elif "," in txt:
                txt = txt.replace(".", "").replace(",", ".")

            return float(txt)
        except (TypeError, ValueError):
            return default

    def _to_int(self, val, default=1):
        if val in (None, ""):
            return default
        try:
            return int(float(str(val).replace(",", ".")))
        except (TypeError, ValueError):
            return default

    def _has_value(self, val):
        return str(val).strip() not in ("", "None", "null")

    def _escape_like_value(self, value):
        txt = str(value or "")
        return txt.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def _build_contains_like_pattern(self, value):
        return f"%{self._escape_like_value(value).strip()}%"

    def _round_money(self, value):
        return round(float(value or 0.0), 2)

    def _get_connection(self, include_db=True):
        """Erzeugt eine frische Verbindung zur Datenbank."""
        config = {
            'host': self.settings.get('db_host', '127.0.0.1'),
            'port': int(self.settings.get('db_port', '3306')),
            'user': self.settings.get('db_user', 'root'),
            'password': self.settings.get('db_pass', ''),
            'use_pure': True,        # WICHTIG: Verhindert C-Extension Crash mit PyQt6
            'charset': 'utf8mb4',   # Zentrales Encoding: erzwingt UTF-8 für alle DB-Strings
            'collation': 'utf8mb4_unicode_ci',
        }
        if include_db:
            config['database'] = self.settings.get('db_name', 'buchhaltung')
            
        try:
            return mysql.connector.connect(**config)
        except Exception as e:
            log_exception(__name__, e)
            raise Exception(f"Verbindung zur MySQL Datenbank fehlgeschlagen: {e}")

    def test_connection(self):
        """
        Versucht eine Verbindung zur Datenbank aufzubauen, um die Credentials zu testen.
        Gibt True zurück, wenn es klappt, ansonsten False.
        """
        try:
            conn = self._get_connection(include_db=False)
            if conn.is_connected():
                conn.close()
                return True
            return False
        except Exception as e:
            log_exception(__name__, e)
            return False



