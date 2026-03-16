"""Ausgelagerte Repository-Methoden fuer DatabaseManager."""

from module.crash_logger import log_exception
from module.status_model import (
    InventoryStatus,
    InvoiceStatus,
    PaymentStatus,
    ShipmentStatus,
    inventory_db_value,
    invoice_db_value,
    legacy_invoice_status_from_code,
    legacy_order_status_from_code,
    legacy_payment_status_from_code,
    normalize_inventory_status,
    normalize_invoice_status,
    normalize_payment_status,
    payment_db_value,
    shipment_db_value,
)


class PomsRepositoryMixin:
    def get_ops_dashboard_stats(self):
        """
        Interne Kennzahlen fuer die Uebersicht.
        Die sichtbare POMS-Maske liest diese Werte ueber eine duenne View-Huelle.
        POMS ist dabei kein eigenes Fremdsystem mehr, sondern nur noch die sichtbare Huelle.
        """
        stats = {
            "open_orders": 0,
            "sent_orders": 0,
            "out_for_delivery": 0,
            "delivered_not_invoiced": 0,
            "revenue_current": 0.0,
            "profit_current": 0.0,
            "revenue_last": 0.0,
            "profit_last": 0.0,
        }

        conn = self._get_connection()
        if not conn.is_connected():
            return stats

        try:
            cursor = conn.cursor(dictionary=True)

            open_statuses = (
                InventoryStatus.WAITING_FOR_ORDER.value,
                InventoryStatus.IN_STOCK.value,
            )
            cursor.execute(
                "SELECT COUNT(id) as count FROM waren_positionen WHERE status IN (%s, %s)",
                open_statuses,
            )
            if row := cursor.fetchone():
                stats["open_orders"] = int(row["count"])

            delivered_label = shipment_db_value(ShipmentStatus.DELIVERED)
            cursor.execute(
                """
                SELECT COUNT(w.id) as count
                FROM waren_positionen w
                JOIN ausgangs_pakete a ON w.ausgangs_paket_id = a.id
                WHERE a.sendungsstatus != %s
                """,
                (delivered_label,),
            )
            if row := cursor.fetchone():
                stats["sent_orders"] = int(row["count"])

            transit_labels = (
                shipment_db_value(ShipmentStatus.IN_TRANSIT),
                shipment_db_value(ShipmentStatus.OUT_FOR_DELIVERY),
            )
            cursor.execute(
                """
                SELECT COUNT(w.id) as count
                FROM waren_positionen w
                JOIN ausgangs_pakete a ON w.ausgangs_paket_id = a.id
                WHERE a.sendungsstatus IN (%s, %s)
                """,
                transit_labels,
            )
            if row := cursor.fetchone():
                stats["out_for_delivery"] = int(row["count"])

            invoice_booked = invoice_db_value(InvoiceStatus.BOOKED)
            cursor.execute(
                """
                SELECT COUNT(w.id) as count
                FROM waren_positionen w
                JOIN ausgangs_pakete a ON w.ausgangs_paket_id = a.id
                WHERE a.sendungsstatus = %s
                  AND w.buchhaltungsstatus != %s
                """,
                (delivered_label, invoice_booked),
            )
            if row := cursor.fetchone():
                stats["delivered_not_invoiced"] = int(row["count"])

            cursor.execute(
                """
                SELECT SUM(w.vk_brutto) as revenue, SUM(w.marge_gesamt) as profit
                FROM waren_positionen w
                JOIN verkauf_tickets t ON w.verkauf_ticket_id = t.id
                WHERE DATE_FORMAT(t.erstellungsdatum, '%Y-%m') = DATE_FORMAT(NOW(), '%Y-%m')
                """
            )
            if row := cursor.fetchone():
                stats["revenue_current"] = float(row["revenue"] or 0.0)
                stats["profit_current"] = float(row["profit"] or 0.0)

            cursor.execute(
                """
                SELECT SUM(w.vk_brutto) as revenue, SUM(w.marge_gesamt) as profit
                FROM waren_positionen w
                JOIN verkauf_tickets t ON w.verkauf_ticket_id = t.id
                WHERE DATE_FORMAT(t.erstellungsdatum, '%Y-%m') = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 MONTH), '%Y-%m')
                """
            )
            if row := cursor.fetchone():
                stats["revenue_last"] = float(row["revenue"] or 0.0)
                stats["profit_last"] = float(row["profit"] or 0.0)

            cursor.close()
        except Exception as exc:
            log_exception(__name__, exc)
            print(f"Fehler bei get_ops_dashboard_stats: {exc}")
        finally:
            conn.close()

        return stats

    def get_poms_stats(self):
        """Legacy-Fassade fuer bestehende Aufrufer ausserhalb des aktiven POMS-Hauptpfads."""
        return self.get_ops_dashboard_stats()

    def _to_internal_ops_row(self, row):
        normalized = dict(row)
        normalized["inventory_status"] = normalize_inventory_status(normalized.get("inventory_status")).value
        normalized["payment_status"] = normalize_payment_status(normalized.get("payment_status")).value
        normalized["invoice_status"] = normalize_invoice_status(normalized.get("invoice_status")).value
        return normalized

    def _to_legacy_poms_row(self, row):
        legacy = dict(row)
        legacy["orderstate"] = legacy.get("inventory_status")
        legacy["paymentstate"] = legacy.get("payment_status")
        legacy["invoicestate"] = legacy.get("invoice_status")
        return legacy

    def get_ops_orders(self, search="", show_all=False, filter_type=""):
        """
        Interne Daten fuer die Uebersichtsmaske.
        Rueckgabe enthaelt nur unsere internen Statusfelder.
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
                    w.status as inventory_status,
                    w.zahlungsstatus as payment_status,
                    w.buchhaltungsstatus as invoice_status,
                    IFNULL(a.tracking_nummer, e.tracking_nummer_einkauf) as tracking,
                    t.ticket_name as notes,
                    t.zahlungseingang_datum
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
            elif filter_type == "open_orders":
                where_clauses.append("w.status IN (%s, %s)")
                params.extend(
                    [
                        InventoryStatus.WAITING_FOR_ORDER.value,
                        InventoryStatus.IN_STOCK.value,
                    ]
                )
            elif filter_type == "sent_orders":
                where_clauses.append("a.id IS NOT NULL AND a.sendungsstatus != %s")
                params.append(shipment_db_value(ShipmentStatus.DELIVERED))
            elif filter_type == "out_for_delivery":
                where_clauses.append("a.id IS NOT NULL AND a.sendungsstatus IN (%s, %s)")
                params.extend(
                    [
                        shipment_db_value(ShipmentStatus.IN_TRANSIT),
                        shipment_db_value(ShipmentStatus.OUT_FOR_DELIVERY),
                    ]
                )
            elif filter_type == "delivered_not_invoiced":
                where_clauses.append("w.buchhaltungsstatus != %s")
                params.append(invoice_db_value(InvoiceStatus.BOOKED))
            elif not show_all:
                where_clauses.append("w.status != %s AND w.buchhaltungsstatus != %s")
                params.extend(
                    [
                        InventoryStatus.CANCELLED.value,
                        invoice_db_value(InvoiceStatus.BOOKED),
                    ]
                )

            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)

            sql += " ORDER BY w.id DESC LIMIT 500"

            cursor.execute(sql, tuple(params))
            for row in cursor.fetchall():
                orders.append(self._to_internal_ops_row(row))

            cursor.close()
        except Exception as exc:
            log_exception(__name__, exc)
            print(f"Fehler bei get_ops_orders: {exc}")
        finally:
            conn.close()

        return orders

    def get_poms_orders(self, search="", show_all=False, filter_type=""):
        """Legacy-Fassade fuer bestehende Aufrufer ausserhalb des aktiven POMS-Hauptpfads."""
        return [
            self._to_legacy_poms_row(row)
            for row in self.get_ops_orders(search=search, show_all=show_all, filter_type=filter_type)
        ]

    def update_ops_status_bulk(self, ids, field, value):
        """
        Aktualisiert ein oder mehrere Statusfelder ueber das zentrale Statusmodell.
        Die aktive POMS-Ansicht arbeitet mit internen Feldnamen.
        Legacy-Aliasse bleiben als Kompatibilitaet zulaessig.
        """
        field_aliases = {
            "inventory_status": "inventory_status",
            "payment_status": "payment_status",
            "invoice_status": "invoice_status",
            "orderstate": "inventory_status",
            "paymentstate": "payment_status",
            "invoicestate": "invoice_status",
        }
        normalized_field = field_aliases.get(field)
        if normalized_field is None:
            return False

        if not ids:
            return False

        db_column_map = {
            "inventory_status": "status",
            "payment_status": "zahlungsstatus",
            "invoice_status": "buchhaltungsstatus",
        }
        db_column = db_column_map[normalized_field]

        if normalized_field == "inventory_status":
            if isinstance(value, int) or (isinstance(value, str) and value.strip().isdigit()):
                valid_value = inventory_db_value(legacy_order_status_from_code(value))
            else:
                valid_value = inventory_db_value(normalize_inventory_status(value))
        elif normalized_field == "payment_status":
            if isinstance(value, int) or (isinstance(value, str) and value.strip().isdigit()):
                valid_value = payment_db_value(legacy_payment_status_from_code(value))
            else:
                valid_value = payment_db_value(normalize_payment_status(value))
        else:
            if isinstance(value, int) or (isinstance(value, str) and value.strip().isdigit()):
                valid_value = invoice_db_value(legacy_invoice_status_from_code(value))
            else:
                valid_value = invoice_db_value(normalize_invoice_status(value))

        clean_ids = []
        for item_id in ids:
            try:
                clean_ids.append(int(item_id))
            except (TypeError, ValueError):
                continue
        if not clean_ids:
            return False

        conn = self._get_connection()
        if not conn.is_connected():
            return False

        try:
            cursor = conn.cursor()
            format_strings = ",".join(["%s"] * len(clean_ids))
            params = [valid_value] + clean_ids
            cursor.execute(
                f"UPDATE waren_positionen SET {db_column} = %s WHERE id IN ({format_strings})",
                tuple(params),
            )

            # Phase 8: Bei Zahlung erhalten -> geld_erhalten + zahlungseingang_datum setzen
            if normalized_field == "payment_status" and normalize_payment_status(value) == PaymentStatus.PAID:
                cursor.execute(
                    f"""
                    UPDATE verkauf_tickets
                    SET geld_erhalten = TRUE,
                        zahlungseingang_datum = CASE
                            WHEN zahlungseingang_datum IS NULL THEN CURDATE()
                            ELSE zahlungseingang_datum
                        END
                    WHERE id IN (
                        SELECT DISTINCT verkauf_ticket_id
                        FROM waren_positionen
                        WHERE id IN ({format_strings}) AND verkauf_ticket_id IS NOT NULL
                    )
                    """,
                    tuple(clean_ids),
                )

            conn.commit()
            cursor.close()
            return True
        except Exception as exc:
            log_exception(__name__, exc)
            print(f"Fehler bei update_ops_status_bulk: {exc}")
            if conn:
                conn.rollback()
            return False
        finally:
            conn.close()

    def update_poms_status_bulk(self, ids, field, value):
        """Legacy-Fassade fuer bestehende Aufrufer ausserhalb des aktiven POMS-Hauptpfads."""
        return self.update_ops_status_bulk(ids=ids, field=field, value=value)



