"""
Zentrale Pipeline fuer Einkaufsbelege.

Diese Datei enthaelt bewusst den gemeinsamen Ablauf fuer:
- Nachbearbeitung von KI-Daten (Normalisierung + Amazon-Land)
- Speichern mit Rueckfragen (ergaenzen / neu / verwerfen)
- Ticket-Matching mit expliziter Bestaetigung

Damit laufen Modul 1 und Mail-Scraper ueber denselben Kern.
"""

from PyQt6.QtWidgets import QMessageBox, QDialog

from module.custom_msgbox import CustomMsgBox
from module.database_manager import DatabaseManager
from module.normalization_dialog import normalize_value
from module.amazon_country_dialog import AmazonCountryDialog


from module.crash_logger import log_exception
class EinkaufPipeline:
    @staticmethod
    def normalize_einkauf_result(parent, result_dict):
        """
        Normalisiert Shop und Zahlungsart fuer Einkaufsdaten.
        Amazon wird bei Bedarf per Dialog nach Land aufgeloest.
        """
        if not isinstance(result_dict, dict):
            return result_dict

        raw_shop = str(result_dict.get("shop_name", ""))
        raw_payment = str(result_dict.get("zahlungsart", ""))

        norm_shop = ""
        if "amazon" in raw_shop.lower():
            amazon_dialog = AmazonCountryDialog(parent)
            if amazon_dialog.exec() == QDialog.DialogCode.Accepted:
                norm_shop = amazon_dialog.selected_country
            else:
                norm_shop = normalize_value("shops", raw_shop, parent) if raw_shop else ""
        else:
            norm_shop = normalize_value("shops", raw_shop, parent) if raw_shop else ""

        norm_payment = normalize_value("zahlungsarten", raw_payment, parent) if raw_payment else ""

        result_dict["shop_name"] = norm_shop
        result_dict["zahlungsart"] = norm_payment
        return result_dict

    @staticmethod
    def sanitize_einkauf_payload(item):
        """
        Entfernt interne Felder (mit _-Prefix) aus Kopf und Warenliste.
        """
        if not isinstance(item, dict):
            return {}

        clean_item = {k: v for k, v in item.items() if not str(k).startswith("_")}
        waren = clean_item.get("waren", [])
        if isinstance(waren, list):
            clean_waren = []
            for ware in waren:
                if isinstance(ware, dict):
                    clean_waren.append({k: v for k, v in ware.items() if not str(k).startswith("_")})
                else:
                    clean_waren.append(ware)
            clean_item["waren"] = clean_waren
        return clean_item

    @staticmethod
    def confirm_and_save_single(parent, settings_manager, data_dict, on_order_number_changed=None, show_new_number_info=True, db=None):
        """
        Fuehrt den gemeinsamen Speichern-Dialog aus und speichert optional.

        Rueckgabe:
        {
          "status": "saved" | "discarded",
          "bestellnummer": str,
          "renamed": bool,
          "db": DatabaseManager
        }
        """
        clean_item = EinkaufPipeline.sanitize_einkauf_payload(data_dict)
        bestellnummer = str(clean_item.get("bestellnummer", "")).strip()

        if not bestellnummer:
            CustomMsgBox.warning(parent, "Fehler", "Bestellnummer fehlt!")
            return {"status": "discarded", "bestellnummer": "", "renamed": False, "db": db}

        if db is None:
            db = DatabaseManager(settings_manager)

        existing = db.find_order_by_number(bestellnummer)
        renamed = False

        if existing:
            msg = EinkaufPipeline._build_existing_order_message(db, clean_item, existing, bestellnummer)
            decision = CustomMsgBox.question(
                parent,
                "Bestellung vorhanden",
                msg,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Yes,
            )

            if decision == QMessageBox.StandardButton.Cancel:
                return {"status": "discarded", "bestellnummer": bestellnummer, "renamed": False, "db": db}

            if decision == QMessageBox.StandardButton.No:
                new_no = db.suggest_new_order_number(bestellnummer)
                clean_item["bestellnummer"] = new_no
                renamed = True
                if callable(on_order_number_changed):
                    on_order_number_changed(new_no)
                if show_new_number_info:
                    CustomMsgBox.information(parent, "Neue Bestellung", f"Neue Bestellnummer gesetzt: {new_no}")
                bestellnummer = new_no
        else:
            save_reply = CustomMsgBox.question(
                parent,
                "Speichern bestaetigen",
                f"Neue Bestellung {bestellnummer} wirklich speichern?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.Yes,
            )
            if save_reply != QMessageBox.StandardButton.Yes:
                return {"status": "discarded", "bestellnummer": bestellnummer, "renamed": False, "db": db}

        db.upsert_einkauf_mit_waren(clean_item, apply_pending_match=False)
        return {"status": "saved", "bestellnummer": bestellnummer, "renamed": renamed, "db": db}

    @staticmethod
    def confirm_and_apply_pending_matches(parent, settings_manager, db=None):
        """
        Zeigt die Matching-Vorschau und fragt vor dem Anwenden nach.
        """
        if db is None:
            db = DatabaseManager(settings_manager)

        preview = db.preview_pending_ticket_matches() or {}
        preview_units = int(preview.get("matched_units", 0) or 0)
        preview_tickets = preview.get("affected_tickets", []) or []

        result = {
            "preview_units": preview_units,
            "matched_units": 0,
            "matched_ticket_names": [],
            "applied": False,
            "db": db,
        }

        if preview_units <= 0:
            return result

        ticket_lines = []
        for item in preview_tickets[:8]:
            tname = str(item.get("ticket_name", "")).strip() or f"Ticket#{item.get('ticket_id', '?')}"
            mcount = int(item.get("matched_units", 0) or 0)
            rem = int(item.get("remaining_units", 0) or 0)
            ticket_lines.append(f"- {tname}: {mcount} Match(es), {rem} offen")

        msg = [
            f"Bestellung gespeichert. {preview_units} Einheit(en) koennen jetzt mit offenen Tickets verknuepft werden.",
            "",
            "Vorschau:",
            *ticket_lines,
            "",
            "Jetzt Matching anwenden?",
            "Yes = Matching jetzt anwenden",
            "No = ohne Matching speichern (spaeter moeglich)",
        ]

        reply = CustomMsgBox.question(
            parent,
            "Matching bestaetigen (Bestellung -> Ticket)",
            "\n".join(msg),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes,
        )

        if reply != QMessageBox.StandardButton.Yes:
            return result

        apply_summary = db.apply_pending_ticket_matches() or {}
        result["matched_units"] = int(apply_summary.get("matched_units", 0) or 0)

        matched_ticket_names = []
        for item in apply_summary.get("affected_tickets", []) or []:
            name = str(item.get("ticket_name", "")).strip()
            if name and name not in matched_ticket_names:
                matched_ticket_names.append(name)
        result["matched_ticket_names"] = matched_ticket_names
        result["applied"] = True
        return result

    @staticmethod
    def build_match_result_message(match_result):
        preview_units = int(match_result.get("preview_units", 0) or 0)
        matched_units = int(match_result.get("matched_units", 0) or 0)
        matched_ticket_names = match_result.get("matched_ticket_names", []) or []

        if matched_units > 0:
            ticket_preview = ", ".join(matched_ticket_names[:3]) if matched_ticket_names else "ticket folgt"
            if len(matched_ticket_names) > 3:
                ticket_preview += f" (+{len(matched_ticket_names) - 3})"
            return "Perfekt", f"Erfolgreich uebertragen!\nAuto-Match: {matched_units} Einheit(en) -> {ticket_preview}"

        if preview_units > 0:
            return "Gespeichert", f"Bestellung gespeichert ohne Matching. {preview_units} Einheit(en) bleiben offen."

        return "Perfekt", "Erfolgreich uebertragen!"

    @staticmethod
    def save_batch_with_confirmations(parent, settings_manager, items, log_callback=None):
        """
        Gemeinsamer Batch-Flow fuer den Mail-Scraper:
        - jedes Dokument normalisieren
        - fuer jedes Dokument aktiv bestaetigen
        - dann einmaliges Matching-Dialogfenster
        """
        db = DatabaseManager(settings_manager)
        saved = 0
        discarded = 0
        renamed = 0

        for idx, raw_item in enumerate(items, start=1):
            item = EinkaufPipeline.normalize_einkauf_result(parent, raw_item)

            res = EinkaufPipeline.confirm_and_save_single(
                parent,
                settings_manager,
                item,
                on_order_number_changed=None,
                show_new_number_info=False,
                db=db,
            )

            if res.get("status") == "saved":
                saved += 1
                if res.get("renamed"):
                    renamed += 1
                if callable(log_callback):
                    log_callback(f"[Dokument {idx}] gespeichert: {res.get('bestellnummer', '-')}")
            else:
                discarded += 1
                if callable(log_callback):
                    log_callback(f"[Dokument {idx}] verworfen")

        match_result = {"preview_units": 0, "matched_units": 0, "matched_ticket_names": [], "applied": False}
        if saved > 0:
            match_result = EinkaufPipeline.confirm_and_apply_pending_matches(parent, settings_manager, db=db)

        return {
            "saved": saved,
            "discarded": discarded,
            "renamed": renamed,
            "match": match_result,
        }

    @staticmethod
    def _build_existing_order_message(db, data_dict, existing, bestellnummer):
        existing_date = existing.get("kaufdatum")
        if hasattr(existing_date, "strftime"):
            existing_date_text = existing_date.strftime("%d.%m.%Y")
        else:
            existing_date_text = str(existing_date or "-")
        existing_shop = str(existing.get("shop_name", "") or "-")

        preview = db.preview_order_enrichment(data_dict) or {}
        head_new = preview.get("head_new", []) or []
        pos_new = preview.get("position_new", []) or []
        head_new_count = int(preview.get("head_new_count", len(head_new)) or len(head_new))
        pos_new_count = int(preview.get("position_new_count", len(pos_new)) or len(pos_new))

        msg = [
            f"Es gibt bereits eine Bestellung mit Nummer {bestellnummer}.",
            f"Bestehender Datensatz: Datum {existing_date_text}, Shop {existing_shop}",
            "",
            "Neu ergaenzt wuerde:",
        ]

        if not head_new and not pos_new:
            msg.append("- Keine neuen Felder erkannt.")
        else:
            if head_new:
                msg.append("- Kopfdaten:")
                shown = head_new[:10]
                for line in shown:
                    msg.append(f"  - {line}")
                if head_new_count > len(shown):
                    msg.append(f"  - ... und {head_new_count - len(shown)} weitere")

            if pos_new:
                msg.append("- Positionen:")
                shown = pos_new[:10]
                for line in shown:
                    msg.append(f"  - {line}")
                if pos_new_count > len(shown):
                    msg.append(f"  - ... und {pos_new_count - len(shown)} weitere")

        msg.extend(
            [
                "",
                "Wie willst du fortfahren?",
                "Yes = bestehenden Datensatz ergaenzen",
                "No = neuen Datensatz anlegen",
                "Cancel = verwerfen",
            ]
        )
        return "\n".join(msg)
