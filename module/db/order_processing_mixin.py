"""Ausgelagerte Repository-Methoden fuer DatabaseManager."""

import logging
import time
from module.crash_logger import log_exception
from module.order_change_review import ChangeProposal, build_source_meta_from_payload, format_change_line, summarize_change_counts
from module.status_model import ShipmentStatus, shipment_db_value
from module.module1_trace_logger import write_module1_trace

class OrderProcessingMixin:
    def _calculate_order_costs(self, data_dict, unit_rows):
        """
        Berechnet Warenwert, Bezugskosten und Einstandskosten.
        Regel:
        - ekp_brutto in der Position = reiner Produkt-Stueckpreis.
        - Versand/Nebenkosten/Rabatt werden zusaetzlich verteilt.
        - Falls keine Zusatzfelder geliefert werden, wird die Differenz zu gesamt_ekp_brutto genutzt.
        """
        total_units = max(1, len(unit_rows))
        warenwert_brutto = self._round_money(sum(row["ekp_brutto"] for row in unit_rows))

        header_total = self._to_float(data_dict.get("gesamt_ekp_brutto", 0.0))
        versand = self._to_float(data_dict.get("versandkosten_brutto", 0.0))
        neben = self._to_float(data_dict.get("nebenkosten_brutto", 0.0))
        rabatt = abs(self._to_float(data_dict.get("rabatt_brutto", 0.0)))

        explicit_components = any(
            self._has_value(data_dict.get(key))
            for key in ("versandkosten_brutto", "nebenkosten_brutto", "rabatt_brutto")
        )

        if explicit_components:
            einstand_gesamt = warenwert_brutto + versand + neben - rabatt
            if header_total > 0 and abs(header_total - einstand_gesamt) > 0.01:
                delta = header_total - einstand_gesamt
                if delta >= 0:
                    neben += delta
                else:
                    rabatt += abs(delta)
                einstand_gesamt = header_total
        else:
            if header_total > 0:
                delta = header_total - warenwert_brutto
                if delta >= 0:
                    neben = delta
                else:
                    rabatt = abs(delta)
                einstand_gesamt = header_total
            else:
                einstand_gesamt = warenwert_brutto

        versand = self._round_money(versand)
        neben = self._round_money(neben)
        rabatt = self._round_money(rabatt)
        einstand_gesamt = self._round_money(einstand_gesamt)

        extras_total = self._round_money(versand + neben - rabatt)

        use_value_weight = warenwert_brutto > 0.0
        distributed = []
        distributed_sum = 0.0

        for row in unit_rows:
            base = self._to_float(row.get("ekp_brutto", 0.0))
            if use_value_weight:
                share_raw = extras_total * (base / warenwert_brutto)
            else:
                share_raw = extras_total / total_units
            share = self._round_money(share_raw)
            distributed.append(share)
            distributed_sum += share

        rounding_delta = self._round_money(extras_total - distributed_sum)
        if distributed and abs(rounding_delta) > 0:
            distributed[-1] = self._round_money(distributed[-1] + rounding_delta)

        for idx, row in enumerate(unit_rows):
            bezugskosten_anteil = distributed[idx] if idx < len(distributed) else 0.0
            row["bezugskosten_anteil_brutto"] = self._round_money(bezugskosten_anteil)
            row["einstand_brutto"] = self._round_money(
                self._to_float(row.get("ekp_brutto", 0.0)) + bezugskosten_anteil
            )

        return {
            "warenwert_brutto": warenwert_brutto,
            "versandkosten_brutto": versand,
            "nebenkosten_brutto": neben,
            "rabatt_brutto": rabatt,
            "einstand_gesamt_brutto": einstand_gesamt
        }

    def _enrich_existing_order_positions(self, cursor, einkauf_id, unit_rows):
        """
        Ergaenzt bestehende Positionen einer Bestellung mit spaeteren Dokumenten,
        ohne bereits verknuepfte Ticket-/Versand-Beziehungen zu loeschen.
        Update-Regel: nur fehlende/0-Werte werden nachgezogen.
        """
        if not unit_rows:
            return 0

        cursor.execute("""
            SELECT id, produkt_name, varianten_info, ean,
                   ekp_brutto, bezugskosten_anteil_brutto, einstand_brutto, status
            FROM waren_positionen
            WHERE einkauf_id = %s
            ORDER BY id ASC
        """, (einkauf_id,))
        existing_rows = cursor.fetchall() or []
        if not existing_rows:
            return 0

        used_input = set()
        updated = 0

        for row in existing_rows:
            best_idx = None
            best_score = 0.0

            for idx, unit in enumerate(unit_rows):
                if idx in used_input:
                    continue

                sale_unit = {
                    "produkt_name": str(unit.get("produkt_name", "")).strip(),
                    "ean": str(unit.get("ean", "")).strip(),
                }
                score = self._score_inventory_match(sale_unit, row)
                if score > best_score:
                    best_score = score
                    best_idx = idx

            if best_idx is None or best_score < 0.40:
                continue

            used_input.add(best_idx)
            unit = unit_rows[best_idx]

            row_name = str(row.get("produkt_name", "") or "").strip()
            row_var = str(row.get("varianten_info", "") or "").strip()
            row_ean = str(row.get("ean", "") or "").strip()
            row_ekp = self._to_float(row.get("ekp_brutto", 0.0))
            row_bezug = self._to_float(row.get("bezugskosten_anteil_brutto", 0.0))
            row_einstand = self._to_float(row.get("einstand_brutto", 0.0))

            unit_name = str(unit.get("produkt_name", "") or "").strip()
            unit_var = str(unit.get("varianten_info", "") or "").strip()
            unit_ean = str(unit.get("ean", "") or "").strip()
            unit_ekp = self._to_float(unit.get("ekp_brutto", 0.0))
            unit_bezug = self._to_float(unit.get("bezugskosten_anteil_brutto", 0.0))
            unit_einstand = self._to_float(unit.get("einstand_brutto", 0.0))

            new_name = row_name
            if (not row_name or row_name.lower() in ("unbekanntes produkt", "xtest-produkt-999")) and unit_name:
                new_name = unit_name

            new_var = row_var if row_var else unit_var
            new_ean = row_ean if row_ean else unit_ean
            new_ekp = row_ekp if row_ekp > 0 else unit_ekp
            new_bezug = row_bezug if abs(row_bezug) > 0 else unit_bezug

            if row_einstand > 0:
                new_einstand = row_einstand
            else:
                if unit_einstand > 0:
                    new_einstand = unit_einstand
                else:
                    new_einstand = new_ekp + new_bezug

            changed = (
                new_name != row_name
                or new_var != row_var
                or new_ean != row_ean
                or abs(new_ekp - row_ekp) > 0.0001
                or abs(new_bezug - row_bezug) > 0.0001
                or abs(new_einstand - row_einstand) > 0.0001
            )

            if changed:
                cursor.execute(
                    """
                    UPDATE waren_positionen
                    SET produkt_name = %s,
                        varianten_info = %s,
                        ean = %s,
                        ekp_brutto = %s,
                        bezugskosten_anteil_brutto = %s,
                        einstand_brutto = %s
                    WHERE id = %s
                    """,
                    (
                        new_name,
                        new_var,
                        new_ean,
                        self._round_money(new_ekp),
                        self._round_money(new_bezug),
                        self._round_money(new_einstand),
                        row["id"],
                    ),
                )
                updated += 1

        return updated

    def _persist_product_media_from_payload(self, data_dict, einkauf_id=None):
        bestellnummer = str((data_dict or {}).get("bestellnummer", "")).strip()
        try:
            from module.media.media_service import MediaService

            media = MediaService(self)
            media.ensure_shop_logo_from_existing_sources(
                shop_name=str((data_dict or {}).get("shop_name", "") or "").strip(),
                sender_domain=str((data_dict or {}).get("_email_sender_domain", (data_dict or {}).get("sender_domain", "")) or "").strip(),
                sender_text=str((data_dict or {}).get("_email_sender", "") or "").strip(),
                payload=data_dict,
                source_module="order_processing",
                source_kind="einkauf_payload_logo",
                priority=75,
                confidence=0.88,
            )
            for ware in (data_dict or {}).get("waren", []) or []:
                if not isinstance(ware, dict):
                    continue
                product_name = str(ware.get("produkt_name", "")).strip()
                ean = str(ware.get("ean", "")).strip()
                variant_text = str(ware.get("varianten_info", "")).strip()
                bild_url = str(ware.get("bild_url", "") or ware.get("image_url", "")).strip()
                local_path = str(ware.get("bild_pfad", "") or ware.get("image_path", "")).strip()
                if not bild_url and not local_path:
                    continue
                media.ensure_product_image_from_existing_sources(
                    product_name=product_name,
                    ean=ean,
                    variant_text=variant_text,
                    bild_url=bild_url,
                    local_path=local_path,
                    source_module="order_processing",
                    source_kind="einkauf_payload",
                    is_primary=True,
                    priority=70,
                    metadata={
                        "bestellnummer": bestellnummer,
                        "source_scope": "einkauf_payload",
                    },
                    payload=data_dict,
                    item=ware,
                )

            origin_module = str((data_dict or {}).get("_origin_module", "") or "").strip()
            if origin_module == "modul_order_entry":
                detection_result = {
                    "processed": False,
                    "reason": "module1_ai_cropping_disabled",
                    "created": [],
                    "rejected": [],
                    "created_count": 0,
                    "rejected_count": 0,
                }
                logging.info(
                    "module1_screenshot_detection_persist_skipped: bestellnummer=%s, einkauf_id=%s, reason=%s",
                    bestellnummer,
                    einkauf_id,
                    "phase_a_disable_module1_ai_cropping",
                )
                write_module1_trace(
                    "module1_screenshot_detection_persist_skipped",
                    order_id=int(einkauf_id or 0) if einkauf_id not in (None, "") else 0,
                    reason="phase_a_disable_module1_ai_cropping",
                    bestellnummer=bestellnummer,
                )
            else:
                detection_result = media.register_payload_screenshot_detections(
                    data_dict,
                    source_module="order_processing",
                    source_kind="einkauf_payload_detection",
                )
                if detection_result.get("processed"):
                    logging.info(
                        "Screenshot-Crop-Pipeline im Save-Flow ausgefuehrt: bestellnummer=%s, erstellt=%s, verworfen=%s",
                        bestellnummer,
                        detection_result.get("created_count", 0),
                        detection_result.get("rejected_count", 0),
                    )
                elif detection_result.get("reason") not in ("no_detections", "invalid_payload"):
                    logging.info(
                        "Screenshot-Crop-Pipeline uebersprungen: bestellnummer=%s, reason=%s",
                        bestellnummer,
                        detection_result.get("reason", "unknown"),
                    )

            if einkauf_id not in (None, "") and origin_module != "modul_order_entry":
                order_item_images = media.register_order_item_candidates_from_payload(
                    einkauf_id=einkauf_id,
                    payload=data_dict,
                    detection_result=detection_result,
                    source_module="order_processing",
                    source_kind="einkauf_payload",
                )
                if order_item_images.get("processed"):
                    logging.info(
                        "Bildentscheidungen fuer Bestellpositionen vorbereitet: bestellnummer=%s, kandidaten=%s, ausgewaehlt=%s",
                        bestellnummer,
                        order_item_images.get("candidate_count", 0),
                        order_item_images.get("selected_count", 0),
                    )
        except Exception as exc:
            log_exception(__name__, exc, extra={"bestellnummer": bestellnummer})
            logging.warning("Produktbilder oder Screenshot-Detektionen aus dem Payload konnten nicht in die Medienstruktur uebernommen werden: %s", exc)

    def upsert_einkauf_mit_waren(self, data_dict, apply_pending_match=True):
        """
        Nimmt das normierte Gemini JSON-Dictionary entgegen und speichert
        1. Die Kopfdaten in `einkauf_bestellungen`
        2. Die zugehoerigen Artikel in `waren_positionen`
        3. Offene "ticket folgt" Verkaeufe werden direkt nachverknuepft
        """
        bestellnummer = str(data_dict.get("bestellnummer", "")).strip()
        if not bestellnummer:
            bestellnummer = f"AUTO_{int(time.time())}"
            data_dict["bestellnummer"] = bestellnummer

        waren_liste = data_dict.get("waren", [])
        unit_rows = []
        if isinstance(waren_liste, list):
            for ware in waren_liste:
                menge = max(1, self._to_int(ware.get("menge", 1), default=1))
                unit_template = {
                    "produkt_name": str(ware.get("produkt_name", "Unbekanntes Produkt")).strip(),
                    "varianten_info": str(ware.get("varianten_info", "")).strip(),
                    "ean": str(ware.get("ean", "")).strip(),
                    "ekp_brutto": self._to_float(ware.get("ekp_brutto", 0.0))
                }
                for _ in range(menge):
                    unit_rows.append(dict(unit_template))

        if unit_rows:
            kosten_meta = self._calculate_order_costs(data_dict, unit_rows)
        else:
            total_input = self._to_float(data_dict.get("gesamt_ekp_brutto", 0.0))
            kosten_meta = {
                "warenwert_brutto": 0.0,
                "versandkosten_brutto": self._to_float(data_dict.get("versandkosten_brutto", 0.0)),
                "nebenkosten_brutto": self._to_float(data_dict.get("nebenkosten_brutto", 0.0)),
                "rabatt_brutto": abs(self._to_float(data_dict.get("rabatt_brutto", 0.0))),
                "einstand_gesamt_brutto": self._round_money(total_input)
            }

        gesamt_ekp_input = self._to_float(data_dict.get("gesamt_ekp_brutto", 0.0))
        if gesamt_ekp_input <= 0:
            gesamt_ekp_input = self._to_float(kosten_meta.get("einstand_gesamt_brutto", 0.0))

        conn = self._get_connection()
        if not conn.is_connected():
            raise Exception("Keine aktive Verbindung zur Datenbank moeglich.")

        cursor = None
        match_summary = {
            "matched_units": 0,
            "affected_tickets": []
        }
        try:
            cursor = conn.cursor(dictionary=True)

            default_shipment_status = shipment_db_value(ShipmentStatus.NOT_DISPATCHED)
            sql_kopf = f"""
            INSERT INTO einkauf_bestellungen (
                bestellnummer, kaufdatum, shop_name, bestell_email,
                tracking_nummer_einkauf, paketdienst, lieferdatum, sendungsstatus,
                gesamt_ekp_brutto, warenwert_brutto, versandkosten_brutto,
                nebenkosten_brutto, rabatt_brutto, einstand_gesamt_brutto, ust_satz
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                kaufdatum = IF(VALUES(kaufdatum) IS NOT NULL, VALUES(kaufdatum), kaufdatum),
                shop_name = IF(VALUES(shop_name) != '', VALUES(shop_name), shop_name),
                bestell_email = IF(VALUES(bestell_email) != '', VALUES(bestell_email), bestell_email),
                tracking_nummer_einkauf = IF(VALUES(tracking_nummer_einkauf) != '', VALUES(tracking_nummer_einkauf), tracking_nummer_einkauf),
                paketdienst = IF(VALUES(paketdienst) != '', VALUES(paketdienst), paketdienst),
                lieferdatum = IF(VALUES(lieferdatum) IS NOT NULL, VALUES(lieferdatum), lieferdatum),
                sendungsstatus = IF(VALUES(sendungsstatus) != '{default_shipment_status}', VALUES(sendungsstatus), sendungsstatus),
                gesamt_ekp_brutto = IF(VALUES(gesamt_ekp_brutto) > 0, VALUES(gesamt_ekp_brutto), gesamt_ekp_brutto),
                warenwert_brutto = IF(VALUES(warenwert_brutto) > 0, VALUES(warenwert_brutto), warenwert_brutto),
                versandkosten_brutto = IF(VALUES(versandkosten_brutto) > 0, VALUES(versandkosten_brutto), versandkosten_brutto),
                nebenkosten_brutto = IF(VALUES(nebenkosten_brutto) > 0, VALUES(nebenkosten_brutto), nebenkosten_brutto),
                rabatt_brutto = IF(VALUES(rabatt_brutto) > 0, VALUES(rabatt_brutto), rabatt_brutto),
                einstand_gesamt_brutto = IF(VALUES(einstand_gesamt_brutto) > 0, VALUES(einstand_gesamt_brutto), einstand_gesamt_brutto),
                ust_satz = IF(VALUES(ust_satz) > 0, VALUES(ust_satz), ust_satz)
            """

            kaufdatum = data_dict.get("kaufdatum") or None
            if kaufdatum:
                kaufdatum = str(kaufdatum)[:10]

            lieferdatum = data_dict.get("lieferdatum") or None
            if lieferdatum:
                lieferdatum = str(lieferdatum)[:10]

            sendungsstatus = shipment_db_value(data_dict.get("sendungsstatus", ""))

            cursor.execute(sql_kopf, (
                bestellnummer,
                kaufdatum,
                str(data_dict.get("shop_name", "")).strip(),
                str(data_dict.get("bestell_email", "")).strip(),
                str(data_dict.get("tracking_nummer_einkauf", "")).strip(),
                str(data_dict.get("paketdienst", "")).strip(),
                lieferdatum,
                sendungsstatus,
                self._round_money(gesamt_ekp_input),
                self._round_money(kosten_meta.get("warenwert_brutto", 0.0)),
                self._round_money(kosten_meta.get("versandkosten_brutto", 0.0)),
                self._round_money(kosten_meta.get("nebenkosten_brutto", 0.0)),
                self._round_money(kosten_meta.get("rabatt_brutto", 0.0)),
                self._round_money(kosten_meta.get("einstand_gesamt_brutto", 0.0)),
                self._to_float(data_dict.get("ust_satz", 0.0))
            ))

            cursor.execute("SELECT id FROM einkauf_bestellungen WHERE bestellnummer = %s", (bestellnummer,))
            result = cursor.fetchone()
            if not result:
                raise Exception("Fehler beim Abrufen der Einkaufs-ID nach dem Speichern!")
            einkauf_id = result["id"]

            if unit_rows:
                cursor.execute("""
                    SELECT COUNT(*) AS protected_count
                    FROM waren_positionen
                    WHERE einkauf_id = %s
                      AND (
                          verkauf_ticket_id IS NOT NULL
                          OR (ausgangs_paket_id IS NOT NULL AND ausgangs_paket_id != 0)
                          OR (seriennummern IS NOT NULL AND seriennummern != '')
                      )
                """, (einkauf_id,))
                protected_row = cursor.fetchone() or {}
                protected_count = int(protected_row.get("protected_count", 0))

                if protected_count == 0:
                    cursor.execute("DELETE FROM waren_positionen WHERE einkauf_id = %s", (einkauf_id,))

                    insert_sql = """
                        INSERT INTO waren_positionen (
                            einkauf_id, produkt_name, varianten_info, ean, menge,
                            ekp_brutto, bezugskosten_anteil_brutto, einstand_brutto
                        ) VALUES (
                            %s, %s, %s, %s, 1, %s, %s, %s
                        )
                    """

                    for row in unit_rows:
                        cursor.execute(insert_sql, (
                            einkauf_id,
                            row["produkt_name"],
                            row["varianten_info"],
                            row["ean"],
                            self._round_money(row.get("ekp_brutto", 0.0)),
                            self._round_money(row.get("bezugskosten_anteil_brutto", 0.0)),
                            self._round_money(row.get("einstand_brutto", 0.0))
                        ))
                else:
                    # Bereits verknuepfte Positionen bleiben erhalten; neue Dokumente ergaenzen nur fehlende Werte.
                    self._enrich_existing_order_positions(cursor, einkauf_id, unit_rows)

            for row in unit_rows:
                if str(row.get("ean", "")).strip():
                    self._upsert_local_ean_mapping_cursor(
                        cursor,
                        row.get("produkt_name", ""),
                        row.get("ean", ""),
                        varianten_info=row.get("varianten_info", ""),
                        quelle="einkauf_beleg",
                        confidence=0.95
                    )

            if apply_pending_match:
                match_summary = self._resolve_pending_ticket_matches(cursor, apply_changes=True)
            else:
                match_summary = {
                    "matched_units": 0,
                    "affected_tickets": []
                }

            conn.commit()
            result = {
                "bestellnummer": bestellnummer,
                "einkauf_id": einkauf_id,
                "matched_units": int(match_summary.get("matched_units", 0) or 0),
                "affected_tickets": match_summary.get("affected_tickets", [])
            }
            self._persist_product_media_from_payload(data_dict, einkauf_id=einkauf_id)
            return result

        except Exception as e:
            log_exception(__name__, e)
            if conn:
                conn.rollback()
            raise Exception(f"Fehler bei UPSERT_EINKAUF: {e}")
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()


    def preview_order_enrichment(self, data_dict, max_lines=12):
        bestellnummer = str(data_dict.get("bestellnummer", "")).strip()
        source_meta = build_source_meta_from_payload(data_dict)
        empty = {
            "order_exists": False,
            "bestellnummer": bestellnummer,
            "source": source_meta,
            "changes": [],
            "head_changes": [],
            "item_changes": [],
            "summary": summarize_change_counts([]),
            "head_new": [],
            "head_new_count": 0,
            "position_new": [],
            "position_new_count": 0,
            "head_overwrite": [],
            "head_overwrite_count": 0,
            "position_overwrite": [],
            "position_overwrite_count": 0,
            "head_apply_mode": "overwrite_non_empty_on_confirm",
            "position_apply_mode": "enrich_missing_only",
            "protected_positions": 0,
            "conservative_note": "Gelb markierte Abweichungen werden jetzt sichtbar. Die bestehende Speicherlogik bleibt unveraendert.",
            "item_rows": [],
            "item_summary": {"new": 0, "changed": 0, "same": 0, "unclear": 0, "total": 0},
        }
        if not bestellnummer:
            return empty
        conn = self._get_connection()
        if not conn.is_connected():
            return empty
        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM einkauf_bestellungen WHERE bestellnummer = %s LIMIT 1", (bestellnummer,))
            existing = cursor.fetchone()
            if not existing:
                return empty

            input_rows = []
            unit_rows = []
            for source_index, ware in enumerate(data_dict.get("waren", []) or []):
                if not isinstance(ware, dict):
                    continue
                menge = max(1, self._to_int(ware.get("menge", 1), default=1))
                tpl = {
                    "produkt_name": str(ware.get("produkt_name", "Unbekanntes Produkt")).strip(),
                    "varianten_info": str(ware.get("varianten_info", "")).strip(),
                    "ean": str(ware.get("ean", "")).strip(),
                    "menge": menge,
                    "ekp_brutto": self._to_float(ware.get("ekp_brutto", 0.0)),
                    "bezugskosten_anteil_brutto": self._to_float(ware.get("bezugskosten_anteil_brutto", 0.0)),
                    "einstand_brutto": self._to_float(ware.get("einstand_brutto", 0.0)),
                    "source_row_index": source_index,
                }
                input_rows.append(dict(tpl))
                for _ in range(menge):
                    unit_rows.append(dict(tpl))

            if unit_rows:
                kosten_meta = self._calculate_order_costs(data_dict, unit_rows)
            else:
                kosten_meta = {
                    "warenwert_brutto": 0.0,
                    "versandkosten_brutto": self._to_float(data_dict.get("versandkosten_brutto", 0.0)),
                    "nebenkosten_brutto": self._to_float(data_dict.get("nebenkosten_brutto", 0.0)),
                    "rabatt_brutto": abs(self._to_float(data_dict.get("rabatt_brutto", 0.0))),
                    "einstand_gesamt_brutto": self._round_money(self._to_float(data_dict.get("gesamt_ekp_brutto", 0.0))),
                }

            gesamt = self._to_float(data_dict.get("gesamt_ekp_brutto", 0.0)) or self._to_float(kosten_meta.get("einstand_gesamt_brutto", 0.0))
            proposed = {
                "kaufdatum": str(data_dict.get("kaufdatum") or "")[:10],
                "shop_name": str(data_dict.get("shop_name", "")).strip(),
                "bestell_email": str(data_dict.get("bestell_email", "")).strip(),
                "tracking_nummer_einkauf": str(data_dict.get("tracking_nummer_einkauf", "")).strip(),
                "paketdienst": str(data_dict.get("paketdienst", "")).strip(),
                "lieferdatum": str(data_dict.get("lieferdatum") or "")[:10],
                "sendungsstatus": shipment_db_value(data_dict.get("sendungsstatus", "")),
                "gesamt_ekp_brutto": self._round_money(gesamt),
                "warenwert_brutto": self._round_money(kosten_meta.get("warenwert_brutto", 0.0)),
                "versandkosten_brutto": self._round_money(kosten_meta.get("versandkosten_brutto", 0.0)),
                "nebenkosten_brutto": self._round_money(kosten_meta.get("nebenkosten_brutto", 0.0)),
                "rabatt_brutto": self._round_money(kosten_meta.get("rabatt_brutto", 0.0)),
                "einstand_gesamt_brutto": self._round_money(kosten_meta.get("einstand_gesamt_brutto", 0.0)),
                "ust_satz": self._to_float(data_dict.get("ust_satz", 0.0)),
            }
            default_ship = shipment_db_value(ShipmentStatus.NOT_DISPATCHED)
            changes, head_changes, item_changes = [], [], []

            def _date(value):
                return value.strftime("%Y-%m-%d") if hasattr(value, "strftime") else str(value or "").strip()

            def _text_kind(old, new, missing=None):
                missing = {str(x).strip().lower() for x in (missing or []) if str(x).strip()}
                old_t = str(old or "").strip()
                new_t = str(new or "").strip()
                old_t = "" if old_t.lower() in missing else old_t
                new_t = "" if new_t.lower() in missing else new_t
                if not new_t:
                    return None, old_t, new_t
                if not old_t:
                    return "add", old_t, new_t
                return ("unchanged" if old_t == new_t else "overwrite"), old_t, new_t

            def _num_kind(old, new):
                old_n = self._to_float(old, 0.0)
                new_n = self._to_float(new, 0.0)
                if abs(new_n) < 0.0001:
                    return None, old_n, new_n
                if abs(old_n) < 0.0001:
                    return "add", old_n, new_n
                return ("unchanged" if abs(old_n - new_n) < 0.0001 else "overwrite"), old_n, new_n

            def _date_kind(old, new):
                old_t = _date(old)
                new_t = _date(new)
                if not new_t:
                    return None, old_t, new_t
                if not old_t:
                    return "add", old_t, new_t
                return ("unchanged" if old_t == new_t else "overwrite"), old_t, new_t

            def _ship_kind(old, new):
                old_t = str(old or "").strip()
                new_t = str(new or "").strip()
                if not new_t or new_t == default_ship:
                    return ("unchanged", old_t, new_t) if new_t and old_t == new_t else (None, old_t, new_t)
                if old_t in ("", default_ship):
                    return "add", old_t, new_t
                return ("unchanged" if old_t == new_t else "overwrite"), old_t, new_t

            def _item_kind(kind):
                return {"add": "item_add", "overwrite": "item_update"}.get(kind, kind)

            def _push(target, etype, ident, key, label, old, new, kind, meta=None):
                row = ChangeProposal(
                    entity_type=etype,
                    entity_identifier=str(ident),
                    field_key=key,
                    field_label=label,
                    old_value=old,
                    new_value=new,
                    change_kind=kind,
                    source_kind=source_meta.get("source_kind", "unknown"),
                    source_reference=source_meta.get("source_reference", ""),
                    document_context=dict(source_meta.get("document_context", {}) or {}),
                    raw_context=dict(source_meta.get("raw_context", {}) or {}),
                    metadata=dict(meta or {}),
                ).to_dict()
                target.append(row)
                changes.append(row)

            head_specs = {
                "kaufdatum": ("Kaufdatum", _date_kind),
                "shop_name": ("Shop", _text_kind),
                "bestell_email": ("Bestell-Email", _text_kind),
                "tracking_nummer_einkauf": ("Tracking", _text_kind),
                "paketdienst": ("Paketdienst", _text_kind),
                "lieferdatum": ("Lieferdatum", _date_kind),
                "sendungsstatus": ("Sendungsstatus", _ship_kind),
                "gesamt_ekp_brutto": ("Gesamtpreis", _num_kind),
                "warenwert_brutto": ("Warenwert", _num_kind),
                "versandkosten_brutto": ("Versandkosten", _num_kind),
                "nebenkosten_brutto": ("Nebenkosten", _num_kind),
                "rabatt_brutto": ("Rabatt/Skonto", _num_kind),
                "einstand_gesamt_brutto": ("Einstand gesamt", _num_kind),
                "ust_satz": ("USt.-Satz", _num_kind),
            }
            for key, (label, fn) in head_specs.items():
                kind, old_v, new_v = fn(existing.get(key), proposed.get(key))
                if kind:
                    _push(head_changes, "order_head", bestellnummer, key, label, old_v if old_v != "" else existing.get(key), new_v if new_v != "" else proposed.get(key), kind, {"apply_mode": "overwrite_non_empty_on_confirm"})

            cursor.execute("SELECT COUNT(*) AS protected_count FROM waren_positionen WHERE einkauf_id = %s AND (verkauf_ticket_id IS NOT NULL OR (ausgangs_paket_id IS NOT NULL AND ausgangs_paket_id != 0) OR (seriennummern IS NOT NULL AND seriennummern != ''))", (existing["id"],))
            protected_count = int((cursor.fetchone() or {}).get("protected_count", 0) or 0)
            pos_mode = "replace_all" if protected_count == 0 else "enrich_missing_only"
            item_rows = []
            item_summary = {"new": 0, "changed": 0, "same": 0, "unclear": 0, "total": 0}

            if input_rows:
                cursor.execute("SELECT id, produkt_name, varianten_info, ean, ekp_brutto, bezugskosten_anteil_brutto, einstand_brutto, status FROM waren_positionen WHERE einkauf_id = %s ORDER BY id ASC", (existing["id"],))
                rows = cursor.fetchall() or []
                used_row_ids = set()
                item_specs = (
                    ("produkt_name", "Produktname", _text_kind, ["Unbekanntes Produkt", "xtest-produkt-999"]),
                    ("varianten_info", "Variante", _text_kind, []),
                    ("ean", "EAN", _text_kind, []),
                    ("ekp_brutto", "EKP", _num_kind, []),
                    ("bezugskosten_anteil_brutto", "Bezugskostenanteil", _num_kind, []),
                    ("einstand_brutto", "Einstand", _num_kind, []),
                )

                def _match_candidates(unit):
                    ranked = []
                    for row in rows:
                        if row.get("id") in used_row_ids:
                            continue
                        score = self._score_inventory_match(
                            {
                                "produkt_name": str(unit.get("produkt_name", "")).strip(),
                                "ean": str(unit.get("ean", "")).strip(),
                            },
                            row,
                        )
                        ranked.append({"row": row, "score": score})
                    ranked.sort(key=lambda entry: entry.get("score", 0.0), reverse=True)
                    return ranked

                def _build_new_item_fields(unit):
                    detail_specs = (
                        ("produkt_name", "Produktname"),
                        ("varianten_info", "Variante"),
                        ("ean", "EAN"),
                        ("menge", "Menge"),
                        ("ekp_brutto", "EKP"),
                        ("bezugskosten_anteil_brutto", "Bezugskostenanteil"),
                        ("einstand_brutto", "Einstand"),
                    )
                    fields = []
                    for key, label in detail_specs:
                        value = unit.get(key)
                        if key == "menge":
                            if self._to_int(value, default=1) <= 0:
                                continue
                        elif key in ("ekp_brutto", "bezugskosten_anteil_brutto", "einstand_brutto"):
                            if abs(self._to_float(value, 0.0)) < 0.0001:
                                continue
                        elif not str(value or "").strip():
                            continue
                        fields.append({
                            "field_key": key,
                            "field_label": label,
                            "old_value": None,
                            "new_value": value,
                            "change_kind": "item_add",
                            "selected_side": "new",
                            "editable": key != "menge",
                        })
                    return fields

                for idx, unit in enumerate(input_rows):
                    candidates = _match_candidates(unit)
                    best = candidates[0] if candidates else None
                    second = candidates[1] if len(candidates) > 1 else None
                    best_score = float(best.get("score", 0.0) if best else 0.0)
                    second_score = float(second.get("score", 0.0) if second else 0.0)
                    strong_candidates = [entry for entry in candidates if float(entry.get("score", 0.0)) >= 0.40]
                    is_unclear = best is not None and best_score >= 0.40 and len(strong_candidates) > 1 and (best_score - second_score) < 0.12

                    row_review = {
                        "source_row_index": int(unit.get("source_row_index", idx) or idx),
                        "input_label": f"Artikel {idx + 1}",
                        "produkt_name": str(unit.get("produkt_name", "") or "").strip(),
                        "varianten_info": str(unit.get("varianten_info", "") or "").strip(),
                        "ean": str(unit.get("ean", "") or "").strip(),
                        "menge": self._to_int(unit.get("menge", 1), default=1),
                        "ekp_brutto": self._round_money(self._to_float(unit.get("ekp_brutto", 0.0))),
                        "fields": [],
                        "change_count": 0,
                        "status": "new",
                        "status_label": "Neu",
                        "match_kind": "new",
                        "match_label": "Neue Position",
                        "match_position_id": None,
                        "match_candidates": [],
                        "hint": "",
                        "apply_mode": pos_mode,
                    }

                    if best is None or best_score < 0.40:
                        row_review["fields"] = _build_new_item_fields(unit)
                        row_review["change_count"] = len(row_review["fields"])
                        row_review["hint"] = "Zu dieser erkannten Zeile wurde keine passende bestehende Position gefunden."
                        item_summary["new"] += 1
                        item_summary["total"] += 1

                        desc = [str(unit.get("produkt_name", "") or "").strip() or "Unbekannt"]
                        if str(unit.get("ean", "") or "").strip():
                            desc.append(f"EAN {str(unit.get('ean', '')).strip()}")
                        if self._to_float(unit.get("ekp_brutto", 0.0)) > 0:
                            desc.append(f"EKP {self._round_money(self._to_float(unit.get('ekp_brutto', 0.0))):.2f}")
                        _push(item_changes, "order_item", f"Neu {idx + 1}", "new_item", f"Neue Position {idx + 1}", None, " | ".join(desc), "item_add", {"apply_mode": pos_mode, "preview_only_if_protected": protected_count > 0, "source_row_index": row_review["source_row_index"]})
                        item_rows.append(row_review)
                        continue

                    matched_row = best.get("row", {}) or {}
                    row_review["match_position_id"] = matched_row.get("id")
                    row_review["match_candidates"] = [
                        {"position_id": entry.get("row", {}).get("id"), "score": round(float(entry.get("score", 0.0) or 0.0), 3)}
                        for entry in strong_candidates[:3]
                        if entry.get("row")
                    ]

                    if is_unclear:
                        candidate_labels = [f"Pos {entry.get('row', {}).get('id')}" for entry in strong_candidates[:3] if entry.get("row", {}).get("id")]
                        row_review["status"] = "unclear"
                        row_review["status_label"] = "Unklar"
                        row_review["match_kind"] = "unclear"
                        row_review["match_label"] = " / ".join(candidate_labels) if candidate_labels else "Match unklar"
                        row_review["hint"] = "Mehrere bestehende Positionen sehen aehnlich aus. Bitte Details aufklappen und die Werte pruefen."
                    else:
                        row_review["match_kind"] = "matched"
                        row_review["match_label"] = f"Pos {matched_row.get('id')}"
                        row_review["hint"] = "Diese erkannte Zeile wurde einer bestehenden Position zugeordnet."
                        used_row_ids.add(matched_row.get("id"))

                    relevant_count = 0
                    for key, label, fn, missing in item_specs:
                        if fn is _text_kind:
                            kind, old_v, new_v = fn(matched_row.get(key), unit.get(key), missing=missing)
                        else:
                            kind, old_v, new_v = fn(matched_row.get(key), unit.get(key))
                        if not kind:
                            continue
                        detail_kind = _item_kind(kind)
                        row_review["fields"].append({
                            "field_key": key,
                            "field_label": label,
                            "old_value": old_v if old_v != "" else matched_row.get(key),
                            "new_value": new_v if new_v != "" else unit.get(key),
                            "change_kind": detail_kind,
                            "selected_side": "new",
                            "editable": True,
                        })
                        if detail_kind in ("item_add", "item_update"):
                            relevant_count += 1
                        _push(item_changes, "order_item", f"Pos {matched_row.get('id')}", key, label, old_v if old_v != "" else matched_row.get(key), new_v if new_v != "" else unit.get(key), detail_kind, {"position_id": matched_row.get("id"), "apply_mode": pos_mode, "match_score": round(best_score, 3), "source_row_index": row_review["source_row_index"], "match_kind": row_review["match_kind"]})

                    row_review["change_count"] = relevant_count
                    if row_review["status"] != "unclear":
                        if relevant_count > 0:
                            row_review["status"] = "changed"
                            row_review["status_label"] = "Geaendert"
                            item_summary["changed"] += 1
                        else:
                            row_review["status"] = "same"
                            row_review["status_label"] = "Gleich"
                            row_review["hint"] = "Zur gematchten Position wurden aktuell keine relevanten Unterschiede erkannt."
                            item_summary["same"] += 1
                    else:
                        item_summary["unclear"] += 1
                    item_summary["total"] += 1
                    item_rows.append(row_review)

            summary = summarize_change_counts(changes)
            head_new = [format_change_line(x) for x in head_changes if x.get("change_kind") == "add"]
            head_over = [format_change_line(x) for x in head_changes if x.get("change_kind") == "overwrite"]
            pos_new = [format_change_line(x) for x in item_changes if x.get("change_kind") == "item_add"]
            pos_over = [format_change_line(x) for x in item_changes if x.get("change_kind") == "item_update"]
            return {
                "order_exists": True,
                "order_id": existing["id"],
                "bestellnummer": bestellnummer,
                "source": source_meta,
                "changes": changes[: max_lines * 4],
                "head_changes": head_changes[: max_lines * 2],
                "item_changes": item_changes[: max_lines * 4],
                "summary": summary,
                "head_new": head_new[:max_lines],
                "head_new_count": len(head_new),
                "position_new": pos_new[:max_lines],
                "position_new_count": len(pos_new),
                "head_overwrite": head_over[:max_lines],
                "head_overwrite_count": len(head_over),
                "position_overwrite": pos_over[:max_lines],
                "position_overwrite_count": len(pos_over),
                "head_apply_mode": "overwrite_non_empty_on_confirm",
                "position_apply_mode": pos_mode,
                "protected_positions": protected_count,
                "conservative_note": "Gelb markierte Kopfwerte koennen nach deiner Bestaetigung aktualisiert werden. Bei geschuetzten Positionen zieht die aktuelle Speicherlogik weiterhin nur fehlende Positionswerte nach.",
                "item_rows": item_rows,
                "item_summary": item_summary,
            }
        except Exception as e:
            log_exception(__name__, e)
            return empty | {"order_exists": True}
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()







