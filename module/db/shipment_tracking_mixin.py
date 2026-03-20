"""Repository-Methoden fuer Tracking-Board und Sendungs-Historie."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from module.crash_logger import log_exception
from module.status_model import ShipmentStatus, normalize_shipment_status, shipment_db_value


class ShipmentTrackingMixin:
    _INBOUND_EXTRA_UPDATE_COLUMNS = {
        "lieferdatum": "lieferdatum",
        "wareneingang_datum": "wareneingang_datum",
    }
    _OUTBOUND_EXTRA_UPDATE_COLUMNS = {}

    def _shipment_status_label(self, value) -> str:
        return shipment_db_value(normalize_shipment_status(value))

    def _write_shipment_status_history_cursor(
        self,
        cursor,
        direction,
        shipment_id,
        old_status,
        new_status,
        source,
        note="",
    ) -> bool:
        old_label = self._shipment_status_label(old_status) if str(old_status or "").strip() else ""
        new_label = self._shipment_status_label(new_status) if str(new_status or "").strip() else ""
        if not new_label or old_label == new_label:
            return False

        cursor.execute(
            """
            INSERT INTO shipment_status_history (
                direction, shipment_id, old_status, new_status, source, note
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                str(direction or "").strip(),
                int(shipment_id),
                old_label or None,
                new_label,
                str(source or "manual").strip() or "manual",
                str(note or "").strip() or None,
            ),
        )
        return True

    def _coerce_db_date_value(self, value):
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if value in (None, ""):
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text[:10]).date()
        except ValueError:
            return text[:10]

    def _set_shipment_status(
        self,
        table_name,
        shipment_id,
        new_status,
        source,
        direction,
        note="",
        extra_updates=None,
        allowed_extra_updates=None,
    ):
        normalized_status = self._shipment_status_label(new_status)
        extra_updates = dict(extra_updates or {})
        allowed_extra_updates = dict(allowed_extra_updates or {})

        conn = self._get_connection()
        if not conn.is_connected():
            raise Exception("Keine aktive Verbindung zur Datenbank moeglich.")

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                f"""
                SELECT id, sendungsstatus
                FROM {table_name}
                WHERE id = %s
                LIMIT 1
                """,
                (int(shipment_id),),
            )
            row = cursor.fetchone()
            if not row:
                raise Exception(f"Sendung {shipment_id} wurde nicht gefunden.")

            old_status = row.get("sendungsstatus")
            set_clauses = ["sendungsstatus = %s"]
            params = [normalized_status]

            for key, value in extra_updates.items():
                column = allowed_extra_updates.get(str(key or "").strip())
                if not column:
                    continue
                set_clauses.append(f"{column} = %s")
                params.append(self._coerce_db_date_value(value))

            params.append(int(shipment_id))
            cursor.execute(
                f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE id = %s",
                tuple(params),
            )

            changed = self._write_shipment_status_history_cursor(
                cursor,
                direction=direction,
                shipment_id=shipment_id,
                old_status=old_status,
                new_status=normalized_status,
                source=source,
                note=note,
            )
            conn.commit()
            return {
                "shipment_id": int(shipment_id),
                "old_status": self._shipment_status_label(old_status) if str(old_status or "").strip() else "",
                "new_status": normalized_status,
                "changed": bool(changed),
            }
        except Exception as exc:
            log_exception(__name__, exc, extra={"shipment_id": shipment_id, "direction": direction})
            if conn:
                conn.rollback()
            raise
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def set_inbound_shipment_status(self, shipment_id, new_status, source="tracker_manual", note="", extra_updates=None):
        return self._set_shipment_status(
            table_name="einkauf_bestellungen",
            shipment_id=shipment_id,
            new_status=new_status,
            source=source,
            direction="inbound",
            note=note,
            extra_updates=extra_updates,
            allowed_extra_updates=self._INBOUND_EXTRA_UPDATE_COLUMNS,
        )

    def set_outbound_shipment_status(self, shipment_id, new_status, source="tracker_manual", note="", extra_updates=None):
        return self._set_shipment_status(
            table_name="ausgangs_pakete",
            shipment_id=shipment_id,
            new_status=new_status,
            source=source,
            direction="outbound",
            note=note,
            extra_updates=extra_updates,
            allowed_extra_updates=self._OUTBOUND_EXTRA_UPDATE_COLUMNS,
        )

    def list_shipment_status_history(self, direction, shipment_id, limit=50):
        conn = self._get_connection()
        if not conn.is_connected():
            return []

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT id, direction, shipment_id, old_status, new_status, source, note, created_at
                FROM shipment_status_history
                WHERE direction = %s
                  AND shipment_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (str(direction or "").strip(), int(shipment_id), int(limit)),
            )
            return cursor.fetchall() or []
        except Exception as exc:
            log_exception(__name__, exc, extra={"shipment_id": shipment_id, "direction": direction})
            return []
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def _load_history_meta_map(self, direction):
        conn = self._get_connection()
        if not conn.is_connected():
            return {}

        delivered_label = shipment_db_value(ShipmentStatus.DELIVERED)
        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT shipment_id,
                       MAX(created_at) AS last_change_at,
                       MAX(CASE WHEN new_status = %s THEN created_at ELSE NULL END) AS delivered_at
                FROM shipment_status_history
                WHERE direction = %s
                GROUP BY shipment_id
                """,
                (delivered_label, str(direction or "").strip()),
            )
            rows = cursor.fetchall() or []
            return {int(row.get("shipment_id", 0) or 0): row for row in rows}
        except Exception as exc:
            log_exception(__name__, exc, extra={"direction": direction})
            return {}
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def _is_recent_enough(self, value, cutoff_date):
        if isinstance(value, datetime):
            value = value.date()
        return isinstance(value, date) and value >= cutoff_date

    def _build_tracker_sort_key(self, row):
        status_code = str(row.get("status_code", ShipmentStatus.NOT_DISPATCHED.value) or ShipmentStatus.NOT_DISPATCHED.value)
        if status_code == ShipmentStatus.NOT_DISPATCHED.value:
            ref = row.get("primary_date") or row.get("last_status_change_at")
            fallback = ref.toordinal() if isinstance(ref, date) else 0
            return (fallback, int(row.get("id", 0) or 0))

        ref = row.get("last_status_change_at") or row.get("primary_date") or row.get("secondary_date")
        if isinstance(ref, datetime):
            timestamp = ref.timestamp()
        elif isinstance(ref, date):
            timestamp = float(ref.toordinal())
        else:
            timestamp = 0.0
        return (-timestamp, -int(row.get("id", 0) or 0))

    def _sort_tracker_rows(self, rows):
        grouped = {}
        for row in rows:
            grouped.setdefault(row.get("status_code"), []).append(row)
        for values in grouped.values():
            values.sort(key=self._build_tracker_sort_key)

        ordered = []
        for status in (
            ShipmentStatus.ISSUE_DELAYED.value,
            ShipmentStatus.OUT_FOR_DELIVERY.value,
            ShipmentStatus.IN_TRANSIT.value,
            ShipmentStatus.NOT_DISPATCHED.value,
            ShipmentStatus.DELIVERED.value,
        ):
            ordered.extend(grouped.get(status, []))
        return ordered

    def list_tracker_inbound_shipments(self, delivered_days=7):
        conn = self._get_connection()
        if not conn.is_connected():
            return []

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT
                    b.id,
                    b.bestellnummer,
                    b.kaufdatum,
                    b.shop_name,
                    b.paketdienst,
                    b.tracking_nummer_einkauf,
                    b.tracking_url,
                    b.tracking_url_source,
                    b.tracking_url_kind,
                    b.amazon_marketplace_domain,
                    b.amazon_order_id,
                    b.amazon_ordering_shipment_id,
                    b.amazon_package_id,
                    b.sendungsstatus,
                    b.lieferdatum,
                    b.wareneingang_datum,
                    COUNT(w.id) AS item_count
                FROM einkauf_bestellungen b
                LEFT JOIN waren_positionen w ON w.einkauf_id = b.id
                WHERE (
                    (b.tracking_nummer_einkauf IS NOT NULL AND b.tracking_nummer_einkauf != '')
                    OR (b.tracking_url IS NOT NULL AND b.tracking_url != '')
                    OR (
                        b.amazon_marketplace_domain IS NOT NULL AND b.amazon_marketplace_domain != ''
                        AND b.amazon_order_id IS NOT NULL AND b.amazon_order_id != ''
                        AND b.amazon_ordering_shipment_id IS NOT NULL AND b.amazon_ordering_shipment_id != ''
                        AND b.amazon_package_id IS NOT NULL AND b.amazon_package_id != ''
                    )
                )
                GROUP BY b.id
                ORDER BY b.id DESC
                """
            )
            rows = cursor.fetchall() or []
        except Exception as exc:
            log_exception(__name__, exc)
            return []
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

        cutoff_date = date.today() - timedelta(days=max(0, int(delivered_days or 0)))
        history_meta = self._load_history_meta_map("inbound")
        result = []
        for row in rows:
            shipment_id = int(row.get("id", 0) or 0)
            status_code = normalize_shipment_status(row.get("sendungsstatus")).value
            meta = history_meta.get(shipment_id, {})
            delivered_at = meta.get("delivered_at")
            last_change_at = meta.get("last_change_at")
            lieferdatum = row.get("lieferdatum")
            wareneingang_datum = row.get("wareneingang_datum")
            show_delivered = True
            if status_code == ShipmentStatus.DELIVERED.value:
                show_delivered = (
                    self._is_recent_enough(delivered_at, cutoff_date)
                    or self._is_recent_enough(wareneingang_datum, cutoff_date)
                    or self._is_recent_enough(lieferdatum, cutoff_date)
                )
            if not show_delivered:
                continue

            primary_date = row.get("kaufdatum")
            if status_code == ShipmentStatus.DELIVERED.value:
                primary_date = wareneingang_datum or lieferdatum or primary_date

            result.append(
                {
                    "direction": "inbound",
                    "id": shipment_id,
                    "bestellnummer": str(row.get("bestellnummer", "") or "").strip(),
                    "shop_name": str(row.get("shop_name", "") or "").strip(),
                    "paketdienst": str(row.get("paketdienst", "") or "").strip(),
                    "tracking_number": str(row.get("tracking_nummer_einkauf", "") or "").strip(),
                    "tracking_url": str(row.get("tracking_url", "") or "").strip(),
                    "tracking_url_source": str(row.get("tracking_url_source", "") or "").strip(),
                    "tracking_url_kind": str(row.get("tracking_url_kind", "") or "").strip(),
                    "amazon_marketplace_domain": str(row.get("amazon_marketplace_domain", "") or "").strip(),
                    "amazon_order_id": str(row.get("amazon_order_id", "") or "").strip(),
                    "amazon_ordering_shipment_id": str(row.get("amazon_ordering_shipment_id", "") or "").strip(),
                    "amazon_package_id": str(row.get("amazon_package_id", "") or "").strip(),
                    "status_code": status_code,
                    "status_label": shipment_db_value(status_code),
                    "kaufdatum": row.get("kaufdatum"),
                    "lieferdatum": lieferdatum,
                    "wareneingang_datum": wareneingang_datum,
                    "item_count": int(row.get("item_count", 0) or 0),
                    "last_status_change_at": last_change_at,
                    "delivered_at": delivered_at,
                    "primary_date": primary_date,
                    "secondary_date": last_change_at,
                }
            )
        return self._sort_tracker_rows(result)

    def list_tracker_outbound_shipments(self, delivered_days=7):
        conn = self._get_connection()
        if not conn.is_connected():
            return []

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT
                    p.id,
                    p.tracking_nummer,
                    p.versanddatum,
                    p.paketdienst,
                    p.tracking_url,
                    p.tracking_url_source,
                    p.tracking_url_kind,
                    p.amazon_marketplace_domain,
                    p.amazon_order_id,
                    p.amazon_ordering_shipment_id,
                    p.amazon_package_id,
                    p.sendungsstatus,
                    COUNT(w.id) AS item_count
                FROM ausgangs_pakete p
                LEFT JOIN waren_positionen w ON w.ausgangs_paket_id = p.id
                GROUP BY p.id
                ORDER BY p.id DESC
                """
            )
            rows = cursor.fetchall() or []
        except Exception as exc:
            log_exception(__name__, exc)
            return []
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

        cutoff_date = date.today() - timedelta(days=max(0, int(delivered_days or 0)))
        history_meta = self._load_history_meta_map("outbound")
        result = []
        for row in rows:
            shipment_id = int(row.get("id", 0) or 0)
            status_code = normalize_shipment_status(row.get("sendungsstatus")).value
            meta = history_meta.get(shipment_id, {})
            delivered_at = meta.get("delivered_at")
            last_change_at = meta.get("last_change_at")
            versanddatum = row.get("versanddatum")
            show_delivered = True
            if status_code == ShipmentStatus.DELIVERED.value:
                show_delivered = self._is_recent_enough(delivered_at, cutoff_date) or self._is_recent_enough(versanddatum, cutoff_date)
            if not show_delivered:
                continue

            primary_date = versanddatum
            if status_code == ShipmentStatus.DELIVERED.value:
                primary_date = delivered_at or versanddatum

            result.append(
                {
                    "direction": "outbound",
                    "id": shipment_id,
                    "title": f"Paket #{shipment_id}",
                    "paketdienst": str(row.get("paketdienst", "") or "").strip(),
                    "tracking_number": str(row.get("tracking_nummer", "") or "").strip(),
                    "tracking_url": str(row.get("tracking_url", "") or "").strip(),
                    "tracking_url_source": str(row.get("tracking_url_source", "") or "").strip(),
                    "tracking_url_kind": str(row.get("tracking_url_kind", "") or "").strip(),
                    "amazon_marketplace_domain": str(row.get("amazon_marketplace_domain", "") or "").strip(),
                    "amazon_order_id": str(row.get("amazon_order_id", "") or "").strip(),
                    "amazon_ordering_shipment_id": str(row.get("amazon_ordering_shipment_id", "") or "").strip(),
                    "amazon_package_id": str(row.get("amazon_package_id", "") or "").strip(),
                    "status_code": status_code,
                    "status_label": shipment_db_value(status_code),
                    "versanddatum": versanddatum,
                    "item_count": int(row.get("item_count", 0) or 0),
                    "last_status_change_at": last_change_at,
                    "delivered_at": delivered_at,
                    "primary_date": primary_date,
                    "secondary_date": last_change_at,
                }
            )
        return self._sort_tracker_rows(result)
