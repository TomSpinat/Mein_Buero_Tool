"""Ausgelagerte Repository-Methoden fuer DatabaseManager."""

import time
from module.crash_logger import log_exception

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

            sql_kopf = """
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
                sendungsstatus = IF(VALUES(sendungsstatus) != 'Noch nicht los', VALUES(sendungsstatus), sendungsstatus),
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

            sendungsstatus = str(data_dict.get("sendungsstatus", "")).strip()
            if sendungsstatus not in ["Noch nicht los", "Unterwegs", "In Auslieferung", "Geliefert"]:
                sendungsstatus = "Noch nicht los"

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
            return {
                "bestellnummer": bestellnummer,
                "einkauf_id": einkauf_id,
                "matched_units": int(match_summary.get("matched_units", 0) or 0),
                "affected_tickets": match_summary.get("affected_tickets", [])
            }

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
        """
        Vorschau fuer bestehende Bestellungen: welche Informationen waeren neu
        (Ergaenzung) und welche Positionsfelder koennten nachgezogen werden.
        """
        bestellnummer = str(data_dict.get("bestellnummer", "")).strip()
        if not bestellnummer:
            return {"order_exists": False, "head_new": [], "position_new": []}

        conn = self._get_connection()
        if not conn.is_connected():
            return {"order_exists": False, "head_new": [], "position_new": []}

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT *
                FROM einkauf_bestellungen
                WHERE bestellnummer = %s
                LIMIT 1
                """,
                (bestellnummer,),
            )
            existing = cursor.fetchone()
            if not existing:
                return {"order_exists": False, "head_new": [], "position_new": []}

            waren_liste = data_dict.get("waren", [])
            unit_rows = []
            if isinstance(waren_liste, list):
                for ware in waren_liste:
                    menge = max(1, self._to_int(ware.get("menge", 1), default=1))
                    unit_template = {
                        "produkt_name": str(ware.get("produkt_name", "Unbekanntes Produkt")).strip(),
                        "varianten_info": str(ware.get("varianten_info", "")).strip(),
                        "ean": str(ware.get("ean", "")).strip(),
                        "ekp_brutto": self._to_float(ware.get("ekp_brutto", 0.0)),
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
                    "einstand_gesamt_brutto": self._round_money(total_input),
                }

            gesamt_ekp_input = self._to_float(data_dict.get("gesamt_ekp_brutto", 0.0))
            if gesamt_ekp_input <= 0:
                gesamt_ekp_input = self._to_float(kosten_meta.get("einstand_gesamt_brutto", 0.0))

            kaufdatum = data_dict.get("kaufdatum") or None
            if kaufdatum:
                kaufdatum = str(kaufdatum)[:10]

            lieferdatum = data_dict.get("lieferdatum") or None
            if lieferdatum:
                lieferdatum = str(lieferdatum)[:10]

            sendungsstatus = str(data_dict.get("sendungsstatus", "")).strip()
            if sendungsstatus not in ["Noch nicht los", "Unterwegs", "In Auslieferung", "Geliefert"]:
                sendungsstatus = "Noch nicht los"

            proposed = {
                "kaufdatum": kaufdatum,
                "shop_name": str(data_dict.get("shop_name", "")).strip(),
                "bestell_email": str(data_dict.get("bestell_email", "")).strip(),
                "tracking_nummer_einkauf": str(data_dict.get("tracking_nummer_einkauf", "")).strip(),
                "paketdienst": str(data_dict.get("paketdienst", "")).strip(),
                "lieferdatum": lieferdatum,
                "sendungsstatus": sendungsstatus,
                "gesamt_ekp_brutto": self._round_money(gesamt_ekp_input),
                "warenwert_brutto": self._round_money(kosten_meta.get("warenwert_brutto", 0.0)),
                "versandkosten_brutto": self._round_money(kosten_meta.get("versandkosten_brutto", 0.0)),
                "nebenkosten_brutto": self._round_money(kosten_meta.get("nebenkosten_brutto", 0.0)),
                "rabatt_brutto": self._round_money(kosten_meta.get("rabatt_brutto", 0.0)),
                "einstand_gesamt_brutto": self._round_money(kosten_meta.get("einstand_gesamt_brutto", 0.0)),
                "ust_satz": self._to_float(data_dict.get("ust_satz", 0.0)),
            }

            def _date_txt(v):
                if hasattr(v, "strftime"):
                    return v.strftime("%Y-%m-%d")
                return str(v or "").strip()

            def _is_missing_text(v):
                return str(v or "").strip() == ""

            def _is_missing_num(v):
                return abs(self._to_float(v, 0.0)) < 0.0001

            label_map = {
                "kaufdatum": "Kaufdatum",
                "shop_name": "Shop",
                "bestell_email": "Bestell-Email",
                "tracking_nummer_einkauf": "Tracking",
                "paketdienst": "Paketdienst",
                "lieferdatum": "Lieferdatum",
                "sendungsstatus": "Sendungsstatus",
                "gesamt_ekp_brutto": "Gesamtpreis",
                "warenwert_brutto": "Warenwert",
                "versandkosten_brutto": "Versandkosten",
                "nebenkosten_brutto": "Nebenkosten",
                "rabatt_brutto": "Rabatt/Skonto",
                "einstand_gesamt_brutto": "Einstand gesamt",
                "ust_satz": "USt.-Satz",
            }

            head_new = []
            for key, label in label_map.items():
                new_val = proposed.get(key)
                old_val = existing.get(key)

                if key in ("kaufdatum", "lieferdatum"):
                    new_txt = _date_txt(new_val)
                    old_txt = _date_txt(old_val)
                    if new_txt and not old_txt:
                        head_new.append(f"{label}: {new_txt}")
                    continue

                if key == "sendungsstatus":
                    old_txt = str(old_val or "").strip()
                    new_txt = str(new_val or "").strip()
                    if new_txt and new_txt != "Noch nicht los" and (not old_txt or old_txt == "Noch nicht los"):
                        head_new.append(f"{label}: {new_txt}")
                    continue

                if isinstance(new_val, (int, float)):
                    if not _is_missing_num(new_val) and _is_missing_num(old_val):
                        head_new.append(f"{label}: {self._round_money(new_val)}")
                else:
                    new_txt = str(new_val or "").strip()
                    if new_txt and _is_missing_text(old_val):
                        head_new.append(f"{label}: {new_txt}")

            position_new = []
            if unit_rows:
                cursor.execute(
                    """
                    SELECT id, produkt_name, varianten_info, ean,
                           ekp_brutto, bezugskosten_anteil_brutto, einstand_brutto
                    FROM waren_positionen
                    WHERE einkauf_id = %s
                    ORDER BY id ASC
                    """,
                    (existing["id"],),
                )
                existing_rows = cursor.fetchall() or []
                used_input = set()

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
                    row_id = row.get("id")

                    row_name = str(row.get("produkt_name", "") or "").strip()
                    unit_name = str(unit.get("produkt_name", "") or "").strip()
                    if (not row_name or row_name.lower() in ("unbekanntes produkt", "xtest-produkt-999")) and unit_name:
                        position_new.append(f"Pos {row_id}: Produktname -> {unit_name}")

                    row_var = str(row.get("varianten_info", "") or "").strip()
                    unit_var = str(unit.get("varianten_info", "") or "").strip()
                    if not row_var and unit_var:
                        position_new.append(f"Pos {row_id}: Variante -> {unit_var}")

                    row_ean = str(row.get("ean", "") or "").strip()
                    unit_ean = str(unit.get("ean", "") or "").strip()
                    if not row_ean and unit_ean:
                        position_new.append(f"Pos {row_id}: EAN -> {unit_ean}")

                    row_ekp = self._to_float(row.get("ekp_brutto", 0.0))
                    unit_ekp = self._to_float(unit.get("ekp_brutto", 0.0))
                    if row_ekp <= 0 and unit_ekp > 0:
                        position_new.append(f"Pos {row_id}: EKP -> {self._round_money(unit_ekp)}")

                    row_bezug = self._to_float(row.get("bezugskosten_anteil_brutto", 0.0))
                    unit_bezug = self._to_float(unit.get("bezugskosten_anteil_brutto", 0.0))
                    if abs(row_bezug) < 0.0001 and abs(unit_bezug) > 0:
                        position_new.append(f"Pos {row_id}: Bezugskostenanteil -> {self._round_money(unit_bezug)}")

                    row_einstand = self._to_float(row.get("einstand_brutto", 0.0))
                    unit_einstand = self._to_float(unit.get("einstand_brutto", 0.0))
                    if row_einstand <= 0 and unit_einstand > 0:
                        position_new.append(f"Pos {row_id}: Einstand -> {self._round_money(unit_einstand)}")

            return {
                "order_exists": True,
                "order_id": existing["id"],
                "head_new": head_new[:max_lines],
                "head_new_count": len(head_new),
                "position_new": position_new[:max_lines],
                "position_new_count": len(position_new),
            }
        except Exception as e:
            log_exception(__name__, e)
            return {"order_exists": True, "head_new": [], "position_new": []}
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

