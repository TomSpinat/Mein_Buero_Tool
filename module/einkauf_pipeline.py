"""
Zentrale Pipeline fuer Einkaufsbelege.

Diese Datei enthaelt bewusst den gemeinsamen Ablauf fuer:
- Nachbearbeitung von KI-Daten (Normalisierung + Amazon-Land)
- Speichern mit Rueckfragen (ergaenzen / neu / verwerfen)
- Ticket-Matching mit expliziter Bestaetigung

Damit laufen Modul 1 und Mail-Scraper ueber denselben Kern.
"""

from PyQt6.QtWidgets import QDialog, QMessageBox

from module.amazon_country_dialog import AmazonCountryDialog, is_generic_amazon_shop, normalize_amazon_shop_value
from module.crash_logger import log_exception
from module.custom_msgbox import CustomMsgBox
from module.database_manager import DatabaseManager
from module.einkauf_ui import OrderExistingReviewDialog
from module.normalization_dialog import normalize_value, resolve_known_mapping
from module.order_change_review import build_source_meta_from_payload, format_change_line


class EinkaufPipeline:
    @staticmethod
    def prepare_mapping_workflow(result_dict):
        """
        Ermittelt die fachlich noetigen Mapping-Schritte.

        Rueckgabe:
        {
          "payload": dict,   # bereits mit bekannten Mappings vorbelegt
          "tasks": [
             {
               "task_type": "normalization" | "amazon_country",
               "field": "shop_name" | "zahlungsart",
               "category": "shops" | "zahlungsarten",
               "label": str,
               "raw_value": str,
             }
          ]
        }
        """
        payload = dict(result_dict or {})
        tasks = []

        raw_shop = str(payload.get("shop_name", "") or "").strip()
        if raw_shop:
            known_shop = resolve_known_mapping("shops", raw_shop)
            if known_shop is not None:
                payload["shop_name"] = known_shop
            else:
                normalized_amazon = normalize_amazon_shop_value(raw_shop)
                if normalized_amazon != raw_shop and normalized_amazon.lower().startswith("amazon "):
                    payload["shop_name"] = normalized_amazon
                elif is_generic_amazon_shop(raw_shop):
                    tasks.append(
                        {
                            "task_type": "amazon_country",
                            "field": "shop_name",
                            "category": "shops",
                            "label": "Amazon-Land",
                            "raw_value": raw_shop,
                        }
                    )
                else:
                    tasks.append(
                        {
                            "task_type": "normalization",
                            "field": "shop_name",
                            "category": "shops",
                            "label": "Shop-Name",
                            "raw_value": raw_shop,
                        }
                    )
        else:
            payload["shop_name"] = ""

        raw_payment = str(payload.get("zahlungsart", "") or "").strip()
        if raw_payment:
            known_payment = resolve_known_mapping("zahlungsarten", raw_payment)
            if known_payment is not None:
                payload["zahlungsart"] = known_payment
            else:
                tasks.append(
                    {
                        "task_type": "normalization",
                        "field": "zahlungsart",
                        "category": "zahlungsarten",
                        "label": "Zahlungsart",
                        "raw_value": raw_payment,
                    }
                )
        else:
            payload["zahlungsart"] = ""

        return {"payload": payload, "tasks": tasks}

    @staticmethod
    def apply_mapping_decision(payload, task, selected_value):
        updated = dict(payload or {})
        if not isinstance(task, dict):
            return updated
        field = str(task.get("field", "") or "").strip()
        if not field:
            return updated
        updated[field] = str(selected_value or "").strip()
        return updated

    @staticmethod
    def run_mapping_workflow_with_dialogs(parent, workflow):
        payload = dict((workflow or {}).get("payload", {}) or {})
        tasks = list((workflow or {}).get("tasks", []) or [])

        for task in tasks:
            task_type = str(task.get("task_type", "") or "").strip()
            raw_value = str(task.get("raw_value", "") or "").strip()
            category = str(task.get("category", "") or "").strip()

            resolved_value = raw_value
            if task_type == "amazon_country":
                amazon_dialog = AmazonCountryDialog(parent, raw_value=raw_value or "Amazon")
                if amazon_dialog.exec() == QDialog.DialogCode.Accepted:
                    resolved_value = amazon_dialog.selected_country
                else:
                    resolved_value = normalize_value(category or "shops", raw_value, parent) if raw_value else ""
            elif task_type == "normalization":
                resolved_value = normalize_value(category, raw_value, parent) if raw_value else ""

            payload = EinkaufPipeline.apply_mapping_decision(payload, task, resolved_value)
        return payload

    @staticmethod
    def normalize_einkauf_result(parent, result_dict):
        """
        Popup-kompatibler Wrapper fuer Modul 1 und andere bestehende Aufrufer.
        """
        if not isinstance(result_dict, dict):
            return result_dict

        workflow = EinkaufPipeline.prepare_mapping_workflow(result_dict)
        payload = dict(workflow.get("payload", {}) or {})
        tasks = list(workflow.get("tasks", []) or [])
        if not tasks:
            return payload
        return EinkaufPipeline.run_mapping_workflow_with_dialogs(parent, workflow)

    @staticmethod
    def sanitize_einkauf_payload(item):
        """
        Entfernt interne Felder (mit _-Prefix) aus Kopf und Warenliste.
        Medienrelevante Workflow-Referenzen bleiben erhalten, damit der Save-Kern
        Screenshots und Detektionen weiterverarbeiten kann.
        """
        if not isinstance(item, dict):
            return {}

        passthrough_internal = {
            "_scan_sources",
            "_primary_scan_media_asset_id",
            "_primary_scan_file_path",
            "_primary_scan_source_type",
            "_screenshot_media_asset_id",
            "_provider_meta",
            "_email_sender",
            "_email_sender_domain",
            "_email_date",
            "_origin_module",
            "_screenshot_detections",
        }
        clean_item = {
            k: v
            for k, v in item.items()
            if not str(k).startswith("_") or str(k) in passthrough_internal
        }
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
    def build_order_review_bundle(settings_manager, data_dict, db=None):
        review_item = dict(data_dict or {}) if isinstance(data_dict, dict) else {}
        if db is None:
            db = DatabaseManager(settings_manager)
        order_preview = db.preview_order_enrichment(review_item) or {}
        matching_preview = db.preview_pending_matches_for_order(review_item) or {}
        source_meta = order_preview.get("source") or build_source_meta_from_payload(review_item)
        return {"bestellnummer": str(review_item.get("bestellnummer", "") or "").strip(), "source": source_meta, "order_preview": order_preview, "matching_preview": matching_preview, "db": db}

    @staticmethod
    def _build_pre_save_match_lines(match_preview, limit=5):
        lines = []
        for item in (match_preview or {}).get("affected_tickets", [])[:limit]:
            ticket_name = str(item.get("ticket_name", "") or "").strip() or f"Ticket#{item.get('ticket_id', '?')}"
            lines.append(f"- {ticket_name}: {int(item.get('matched_units', 0) or 0)} moegliche(s) Match(es), {int(item.get('remaining_units', 0) or 0)} danach noch offen")
        return lines

    @staticmethod
    def confirm_and_save_single(parent, settings_manager, data_dict, on_order_number_changed=None, show_new_number_info=True, db=None, review_bundle=None, skip_existing_review_dialog=False):
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
            if review_bundle is None:
                review_bundle = EinkaufPipeline.build_order_review_bundle(settings_manager, data_dict, db=db)
            else:
                review_bundle["db"] = db

            if skip_existing_review_dialog:
                decision = QMessageBox.StandardButton.Yes
            else:
                try:
                    decision = OrderExistingReviewDialog.choose(parent, bestellnummer, existing, review_bundle)
                except Exception as dialog_error:
                    log_exception(__name__, dialog_error)
                    msg = EinkaufPipeline._build_existing_order_message(db, data_dict, existing, bestellnummer, review_bundle=review_bundle)
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

        save_info = db.upsert_einkauf_mit_waren(clean_item, apply_pending_match=False)
        einkauf_id = save_info.get("einkauf_id") if isinstance(save_info, dict) else None
        return {
            "status": "saved",
            "bestellnummer": bestellnummer,
            "renamed": renamed,
            "db": db,
            "einkauf_id": einkauf_id,
            "save_info": save_info if isinstance(save_info, dict) else {},
        }

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
    def _build_existing_order_message(db, data_dict, existing, bestellnummer, review_bundle=None):
        existing_date = existing.get("kaufdatum")
        existing_date_text = existing_date.strftime("%d.%m.%Y") if hasattr(existing_date, "strftime") else str(existing_date or "-")
        existing_shop = str(existing.get("shop_name", "") or "-")
        bundle = review_bundle or {"order_preview": db.preview_order_enrichment(data_dict) or {}, "matching_preview": db.preview_pending_matches_for_order(data_dict) or {}}
        preview = bundle.get("order_preview", {}) or {}
        matching_preview = bundle.get("matching_preview", {}) or {}
        changes = list(preview.get("changes", []) or [])
        add_lines = [format_change_line(change) for change in changes if change.get("change_kind") in ("add", "item_add")]
        overwrite_lines = [format_change_line(change) for change in changes if change.get("change_kind") in ("overwrite", "item_update")]
        unchanged_lines = [format_change_line(change) for change in changes if change.get("change_kind") == "unchanged"]
        msg = [f"Es gibt bereits eine Bestellung mit Nummer {bestellnummer}.", f"Bestehender Datensatz: Datum {existing_date_text}, Shop {existing_shop}", "", "Pruefvorschau:"]
        if add_lines:
            msg.append("Neue oder fuellende Werte:")
            msg.extend(add_lines[:6])
        if overwrite_lines:
            if add_lines:
                msg.append("")
            msg.append("Abweichende Werte:")
            msg.extend(overwrite_lines[:6])
        if unchanged_lines and not add_lines and not overwrite_lines:
            msg.append("Erkannt, aber unveraendert:")
            msg.extend(unchanged_lines[:4])
        if not add_lines and not overwrite_lines and not unchanged_lines:
            msg.append("- Keine relevanten Aenderungen erkannt.")
        note = str(preview.get("conservative_note", "") or "").strip()
        if note:
            msg.extend(["", note])
        preview_units = int(matching_preview.get("matched_units", 0) or 0)
        if preview_units > 0:
            msg.extend(["", f"Separater Hinweis: Nach dem Speichern koennten voraussichtlich {preview_units} Ticket-Einheit(en) automatisch weitergematcht werden."])
            msg.extend(EinkaufPipeline._build_pre_save_match_lines(matching_preview, limit=4))
        msg.extend(["", "Wie willst du fortfahren?", "Yes = bestehenden Datensatz pruefen und aktualisieren", "No = neuen Datensatz anlegen", "Cancel = verwerfen"])
        return "\n".join(msg)



