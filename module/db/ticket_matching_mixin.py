"""Ausgelagerte Repository-Methoden fuer DatabaseManager."""

import difflib
import json
from module.crash_logger import log_exception
from module.status_model import InventoryStatus, TicketMatchingStatus, normalize_inventory_status

class TicketMatchingMixin:
    def _build_sale_units(self, waren_liste):
        sale_units = []

        for ware_index, ware in enumerate(waren_liste or []):
            menge = max(1, self._to_int(ware.get("menge", 1), default=1))
            vk_brutto = self._to_float(ware.get("vk_brutto", 0.0))
            marge_total = self._to_float(ware.get("marge_gesamt", 0.0))
            marge_pro_stueck = marge_total / menge if menge else 0.0

            for unit_index in range(menge):
                sale_units.append({
                    "ware_index": ware_index,
                    "unit_index": unit_index,
                    "produkt_name": str(ware.get("produkt_name", "")).strip(),
                    "ean": str(ware.get("ean", "")).strip(),
                    "vk_brutto": vk_brutto,
                    "marge_gesamt": marge_pro_stueck
                })

        return sale_units

    def _summarize_pending_units(self, pending_units):
        counts = {}
        for unit in pending_units:
            name = str(unit.get("produkt_name", "Unbekannt")).strip() or "Unbekannt"
            counts[name] = counts.get(name, 0) + 1

        return [f"{name} x{count}" for name, count in sorted(counts.items())]

    def _score_inventory_match(self, sale_unit, position):
        sale_name = str(sale_unit.get("produkt_name", "")).strip().lower()
        db_name = str(position.get("produkt_name", "")).strip().lower()
        sale_ean = str(sale_unit.get("ean", "")).strip()
        db_ean = str(position.get("ean", "") or "").strip()

        ratio = difflib.SequenceMatcher(None, sale_name, db_name).ratio()
        sale_words = set(sale_name.split())
        db_words = set(db_name.split())
        overlap_score = 0.0
        if sale_words and db_words:
            overlap_score = len(sale_words.intersection(db_words)) / len(sale_words)

        score = max(ratio, overlap_score)

        if sale_ean and db_ean:
            if sale_ean == db_ean:
                score += 1.0
            elif sale_ean in db_ean or db_ean in sale_ean:
                score += 0.4

        status = normalize_inventory_status(position.get("status", ""))
        if status == InventoryStatus.IN_STOCK:
            score += 0.15
        elif status == InventoryStatus.WAITING_FOR_ORDER:
            score += 0.05

        return score

    def _expand_positions_to_units(self, cursor, einkauf_id=None):
        query = """
            SELECT
                id, einkauf_id, produkt_name, varianten_info, ean, menge, menge_geliefert,
                ekp_brutto, bezugskosten_anteil_brutto, einstand_brutto,
                vk_brutto, marge_gesamt, versandart, seriennummern, status,
                zahlungsstatus, buchhaltungsstatus
            FROM waren_positionen
            WHERE COALESCE(menge, 1) > 1
              AND verkauf_ticket_id IS NULL
              AND (ausgangs_paket_id IS NULL OR ausgangs_paket_id = 0)
        """
        params = []
        if einkauf_id is not None:
            query += " AND einkauf_id = %s"
            params.append(einkauf_id)

        cursor.execute(query, tuple(params))
        multi_rows = cursor.fetchall()

        for row in multi_rows:
            menge = max(1, self._to_int(row.get("menge", 1), default=1))
            if menge <= 1:
                continue

            seriennummern = str(row.get("seriennummern", "") or "").strip()
            if seriennummern:
                continue

            geliefert = min(max(self._to_int(row.get("menge_geliefert", 0), default=0), 0), menge)

            cursor.execute(
                "UPDATE waren_positionen SET menge = 1, menge_geliefert = %s WHERE id = %s",
                (1 if geliefert > 0 else 0, row["id"])
            )

            insert_sql = """
                INSERT INTO waren_positionen (
                    einkauf_id, verkauf_ticket_id, ausgangs_paket_id, produkt_name, varianten_info,
                    ean, menge, menge_geliefert, ekp_brutto, bezugskosten_anteil_brutto, einstand_brutto,
                    vk_brutto, marge_gesamt, versandart, seriennummern, status, zahlungsstatus, buchhaltungsstatus
                ) VALUES (
                    %s, NULL, NULL, %s, %s, %s, 1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            for idx in range(1, menge):
                geliefert_unit = 1 if idx < geliefert else 0
                cursor.execute(
                    insert_sql,
                    (
                        row["einkauf_id"],
                        row["produkt_name"],
                        row.get("varianten_info"),
                        row.get("ean"),
                        geliefert_unit,
                        row.get("ekp_brutto"),
                        row.get("bezugskosten_anteil_brutto"),
                        row.get("einstand_brutto"),
                        row.get("vk_brutto"),
                        row.get("marge_gesamt"),
                        row.get("versandart"),
                        row.get("seriennummern"),
                        row.get("status"),
                        row.get("zahlungsstatus"),
                        row.get("buchhaltungsstatus")
                    )
                )

    def ensure_order_positions_are_unitized(self, einkauf_id):
        conn = self._get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            self._expand_positions_to_units(cursor, einkauf_id=einkauf_id)
            conn.commit()
        except Exception as e:
            log_exception(__name__, e)
            if conn:
                conn.rollback()
            raise
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()

    def ensure_open_positions_unitized(self):
        conn = self._get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            self._expand_positions_to_units(cursor)
            conn.commit()
        except Exception as e:
            log_exception(__name__, e)
            if conn:
                conn.rollback()
            raise
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()

    def _find_inventory_matches(self, cursor, sale_units):
        self._expand_positions_to_units(cursor)

        cursor.execute(
            """
            SELECT
                w.id, w.produkt_name, w.ean, w.menge, w.status,
                w.ekp_brutto, w.einstand_brutto,
                e.bestellnummer, e.kaufdatum, e.shop_name
            FROM waren_positionen w
            JOIN einkauf_bestellungen e ON w.einkauf_id = e.id
            WHERE w.verkauf_ticket_id IS NULL
              AND (w.ausgangs_paket_id IS NULL OR w.ausgangs_paket_id = 0)
              AND (w.status IS NULL OR w.status != %s)
            ORDER BY w.id DESC
            """,
            (InventoryStatus.CANCELLED.value,),
        )

        used_ids = set()
        matched_info = []
        pending_units = []

        for sale_unit in sale_units:
            best_match = None
            best_score = 0.0

            for position in positions:
                if position["id"] in used_ids:
                    continue
                if self._to_int(position.get("menge", 1), default=1) != 1:
                    continue

                score = self._score_inventory_match(sale_unit, position)
                if score > best_score:
                    best_score = score
                    best_match = position

            if best_match and best_score >= 0.45:
                used_ids.add(best_match["id"])

                sale_vk = self._to_float(sale_unit.get("vk_brutto", 0.0))
                cost_raw = best_match.get("einstand_brutto")
                if cost_raw in (None, ""):
                    cost_raw = best_match.get("ekp_brutto")
                kostenbasis = self._to_float(cost_raw, 0.0)

                if sale_vk > 0:
                    margin_value = sale_vk - kostenbasis
                else:
                    margin_value = self._to_float(sale_unit.get("marge_gesamt", 0.0))

                matched_info.append({
                    "ware_index": sale_unit["ware_index"],
                    "db_id": best_match["id"],
                    "bestellnummer": best_match.get("bestellnummer"),
                    "kaufdatum": best_match.get("kaufdatum"),
                    "produkt_name": best_match.get("produkt_name"),
                    "ean": best_match.get("ean") or sale_unit.get("ean", ""),
                    "status": best_match.get("status"),
                    "vk_brutto": sale_vk,
                    "marge_gesamt": self._round_money(margin_value),
                    "kostenbasis": self._round_money(kostenbasis),
                    "ticket_produkt": sale_unit.get("produkt_name", "")
                })
            else:
                pending_units.append(dict(sale_unit))

        return matched_info, pending_units

    def _apply_ticket_matches(self, cursor, ticket_id, matched_info):
        for match in matched_info:
            update_sql = """
                UPDATE waren_positionen
                SET verkauf_ticket_id = %s, vk_brutto = %s, marge_gesamt = %s
            """
            params = [
                ticket_id,
                self._to_float(match.get("vk_brutto", 0.0)),
                self._to_float(match.get("marge_gesamt", 0.0))
            ]

            match_ean = str(match.get("ean", "")).strip()
            if match_ean:
                update_sql += ", ean = CASE WHEN ean IS NULL OR ean = '' THEN %s ELSE ean END"
                params.append(match_ean)

            update_sql += " WHERE id = %s"
            params.append(match["db_id"])

            cursor.execute(update_sql, tuple(params))

    def _ticket_matching_status(self, total_units, pending_units):
        if pending_units and len(pending_units) == total_units:
            return TicketMatchingStatus.TICKET_FOLGT.value
        if pending_units:
            return TicketMatchingStatus.PARTIAL.value
        return TicketMatchingStatus.MATCHED.value

    def _upsert_ticket_header(self, cursor, data_dict, total_units, pending_units):
        ticket_name = str(data_dict.get("ticket_name", "")).strip()
        if not ticket_name:
            raise ValueError("Ticket-Name fehlt. Speichern abgebrochen.")

        status = self._ticket_matching_status(total_units, pending_units)
        pending_payload = self._safe_json_dump(pending_units) if pending_units else None

        cursor.execute("""
            INSERT INTO verkauf_tickets (
                ticket_name, abnehmer_typ, erstellungsdatum, zahlungsziel, kaeufer,
                pending_payload_json, matching_status
            ) VALUES (
                %s, 'Discord', CURDATE(), %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                zahlungsziel = VALUES(zahlungsziel),
                kaeufer = VALUES(kaeufer),
                pending_payload_json = VALUES(pending_payload_json),
                matching_status = VALUES(matching_status)
        """, (
            ticket_name,
            str(data_dict.get("zahlungsziel", "")).strip(),
            str(data_dict.get("kaeufer", "")).strip(),
            pending_payload,
            status
        ))

        cursor.execute("SELECT id FROM verkauf_tickets WHERE ticket_name = %s", (ticket_name,))
        result = cursor.fetchone()
        return result["id"]

    def _resolve_pending_ticket_matches(self, cursor, apply_changes=True):
        summary = {
            "matched_units": 0,
            "affected_tickets": []
        }

        cursor.execute("""
            SELECT id, ticket_name, pending_payload_json
            FROM verkauf_tickets
            WHERE pending_payload_json IS NOT NULL
              AND pending_payload_json != ''
              AND pending_payload_json != '[]'
            ORDER BY id DESC
        """)
        pending_tickets = cursor.fetchall()

        for ticket in pending_tickets:
            try:
                pending_units = json.loads(ticket["pending_payload_json"])
            except (TypeError, ValueError, json.JSONDecodeError):
                continue

            matched_info, still_pending = self._find_inventory_matches(cursor, pending_units)
            if matched_info:
                matched_count = len(matched_info)
                summary["matched_units"] += matched_count
                summary["affected_tickets"].append({
                    "ticket_id": ticket["id"],
                    "ticket_name": str(ticket.get("ticket_name", "")).strip(),
                    "matched_units": matched_count,
                    "remaining_units": len(still_pending)
                })

                if apply_changes:
                    self._apply_ticket_matches(cursor, ticket["id"], matched_info)

            if apply_changes:
                cursor.execute(
                    "UPDATE verkauf_tickets SET pending_payload_json = %s, matching_status = %s WHERE id = %s",
                    (
                        self._safe_json_dump(still_pending) if still_pending else None,
                        self._ticket_matching_status(len(pending_units), still_pending),
                        ticket["id"]
                    )
                )

        return summary

    def _build_order_match_preview_positions(self, data_dict):
        positions = []
        for ware_index, ware in enumerate(data_dict.get("waren", []) or []):
            menge = max(1, self._to_int(ware.get("menge", 1), default=1))
            produkt_name = str(ware.get("produkt_name", "") or "").strip()
            ean = str(ware.get("ean", "") or "").strip()
            ekp_brutto = self._to_float(ware.get("ekp_brutto", 0.0))
            for unit_index in range(menge):
                positions.append({
                    "id": f"draft-{ware_index}-{unit_index}",
                    "produkt_name": produkt_name,
                    "ean": ean,
                    "menge": 1,
                    "status": InventoryStatus.WAITING_FOR_ORDER.value,
                    "ekp_brutto": ekp_brutto,
                    "einstand_brutto": ekp_brutto,
                })
        return positions

    def preview_pending_matches_for_order(self, data_dict):
        summary = {"source_scope": "draft_order", "candidate_units": 0, "matched_units": 0, "affected_tickets": []}
        draft_positions = self._build_order_match_preview_positions(data_dict if isinstance(data_dict, dict) else {})
        summary["candidate_units"] = len(draft_positions)
        if not draft_positions:
            return summary
        conn = self._get_connection()
        if not conn.is_connected():
            raise Exception("Keine aktive Verbindung zur Datenbank")
        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT id, ticket_name, pending_payload_json FROM verkauf_tickets WHERE pending_payload_json IS NOT NULL AND pending_payload_json != '' AND pending_payload_json != '[]' ORDER BY id DESC")
            tickets = cursor.fetchall() or []
            available = list(draft_positions)
            for ticket in tickets:
                try:
                    pending_units = json.loads(ticket["pending_payload_json"])
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                used_ids = set()
                matched = 0
                for sale_unit in pending_units:
                    best_match = None
                    best_score = 0.0
                    for position in available:
                        if position.get("id") in used_ids:
                            continue
                        score = self._score_inventory_match(sale_unit, position)
                        if score > best_score:
                            best_match = position
                            best_score = score
                    if best_match and best_score >= 0.45:
                        used_ids.add(best_match.get("id"))
                        matched += 1
                if matched <= 0:
                    continue
                available = [position for position in available if position.get("id") not in used_ids]
                summary["matched_units"] += matched
                summary["affected_tickets"].append({
                    "ticket_id": ticket["id"],
                    "ticket_name": str(ticket.get("ticket_name", "") or "").strip(),
                    "matched_units": matched,
                    "remaining_units": max(0, len(pending_units) - matched),
                })
                if not available:
                    break
            return summary
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def preview_pending_ticket_matches(self):
        """
        Liefert eine Vorschau, wie viele offene ticket-folgt Einheiten aktuell
        mit vorhandenem Bestand gematcht werden koennten.
        Es werden dabei KEINE DB-Aenderungen gespeichert.
        """
        conn = self._get_connection()
        if not conn.is_connected():
            raise Exception("Keine aktive Verbindung zur Datenbank")

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            summary = self._resolve_pending_ticket_matches(cursor, apply_changes=False)
            return summary
        finally:
            if conn and conn.is_connected():
                conn.rollback()
                if cursor:
                    cursor.close()
                conn.close()

    def apply_pending_ticket_matches(self):
        """
        Fuehrt das echte Matching aller offenen ticket-folgt Positionen aus.
        """
        conn = self._get_connection()
        if not conn.is_connected():
            raise Exception("Keine aktive Verbindung zur Datenbank")

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            summary = self._resolve_pending_ticket_matches(cursor, apply_changes=True)
            conn.commit()
            return summary
        except Exception as e:
            log_exception(__name__, e)
            if conn:
                conn.rollback()
            raise
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def preview_verkauf_discord(self, data_dict):
        """
        Sucht nach passenden Waren fÃ¼r ein Discord-Ticket OHNE diese final abzuspeichern.
        RÃ¼ckgabe: (matched_info_list, pending_units_list, pending_summary_list)
        """
        conn = self._get_connection()
        if not conn.is_connected():
            raise Exception("Keine aktive Verbindung zur Datenbank")

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            sale_units = self._build_sale_units(data_dict.get("waren", []))
            matched_info, pending_units = self._find_inventory_matches(cursor, sale_units)
            return matched_info, pending_units, self._summarize_pending_units(pending_units)
        except Exception as e:
            log_exception(__name__, e)
            raise Exception(f"Fehler bei der Preview: {e}")
        finally:
            if conn and conn.is_connected():
                conn.rollback()
                if cursor:
                    cursor.close()
                conn.close()

    def confirm_verkauf_discord(self, data_dict, matched_info, pending_units):
        """
        Speichert das Ticket, verknÃ¼pft sofort gefundene Waren und hÃ¤lt offene Einheiten
        als "ticket folgt" fest, damit spÃ¤tere EinkÃ¤ufe automatisch matchen kÃ¶nnen.
        """
        conn = self._get_connection()
        if not conn.is_connected():
            raise Exception("Keine aktive Verbindung zur Datenbank")

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            sale_units = self._build_sale_units(data_dict.get("waren", []))
            ticket_id = self._upsert_ticket_header(cursor, data_dict, len(sale_units), pending_units)

            for unit in sale_units:
                if str(unit.get("ean", "")).strip():
                    self._upsert_local_ean_mapping_cursor(
                        cursor,
                        unit.get("produkt_name", ""),
                        unit.get("ean", ""),
                        varianten_info="",
                        quelle="verkauf_ticket",
                        confidence=0.80
                    )

            if matched_info:
                self._apply_ticket_matches(cursor, ticket_id, matched_info)

            conn.commit()
            return {
                "ticket_id": ticket_id,
                "matched_count": len(matched_info),
                "pending_count": len(pending_units)
            }
        except Exception as e:
            log_exception(__name__, e)
            if conn:
                conn.rollback()
            raise Exception(f"Fehler: {e}")
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()






