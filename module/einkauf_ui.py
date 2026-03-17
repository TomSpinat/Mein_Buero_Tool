import json
import logging
import os

from PyQt6.QtCore import QSize, Qt, QTimer, pyqtSignal
from PyQt6.QtGui import QColor, QIcon, QPixmap
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QAbstractScrollArea,
    QCheckBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from module.ui_media_pixmap import create_placeholder_pixmap, render_preview_pixmap
from module.module1_trace_logger import write_module1_trace

from module.order_change_review import (
    change_kind_label,
    format_review_value,
    source_label,
    summarize_change_counts,
)
from module.scan_output_contract import EINKAUF_FIELDS
from module.order_visual_state import OrderVisualState

EINKAUF_FIELD_SECTIONS = (
    (
        "Bestellung",
        (
            ("bestellnummer", "Bestellnummer"),
            ("kaufdatum", "Kaufdatum"),
            ("shop_name", "Shop-Name (normiert)"),
            ("bestell_email", "Bestell-E-Mail"),
        ),
    ),
    (
        "Lieferung",
        (
            ("sendungsstatus", "Sendungsstatus"),
            ("tracking_nummer_einkauf", "Tracking Code"),
            ("paketdienst", "Paketdienst"),
            ("lieferdatum", "Lieferdatum"),
            ("wareneingang_datum", "Wareneingang"),
        ),
    ),
    (
        "Rechnung & Status",
        (
            ("rechnung_pdf_pfad", "Rechnungsdatei (Pfad)"),
            ("storno_status", "Storno-Status"),
        ),
    ),
    (
        "Kosten und Zahlung",
        (
            ("gesamt_ekp_brutto", "Gesamtpreis (brutto)"),
            ("versandkosten_brutto", "Versandkosten (brutto)"),
            ("nebenkosten_brutto", "Nebenkosten (brutto)"),
            ("rabatt_brutto", "Rabatt/Gutschrift (brutto)"),
            ("ust_satz", "USt.-Satz"),
            ("zahlungsart", "Zahlungsart (normiert)"),
        ),
    ),
)
EINKAUF_VISIBLE_FIELD_KEYS = tuple(key for _section, fields in EINKAUF_FIELD_SECTIONS for key, _label in fields)
EINKAUF_ITEM_MAIN_COLUMNS = (
    ("bild", "Bild"),
    ("status", "Status"),
    ("produkt_name", "Produkt"),
    ("varianten_info", "Variante"),
    ("ean", "EAN"),
    ("menge", "Menge"),
    ("ekp_brutto", "Stueckpreis"),
    ("match_label", "Zuordnung"),
    ("change_count", "Aenderungen"),
    ("details", "Details"),
)
EINKAUF_ITEM_EDIT_KEYS = ("produkt_name", "varianten_info", "ean", "menge", "ekp_brutto")
EINKAUF_ITEM_REVIEW_KEYS = EINKAUF_ITEM_EDIT_KEYS + (
    "bezugskosten_anteil_brutto",
    "einstand_brutto",
    "menge_geliefert",
    "seriennummern",
    "zahlungsstatus",
)
EINKAUF_VISIBLE_FIELD_SET = set(EINKAUF_VISIBLE_FIELD_KEYS) | {"reverse_charge", "rechnung_vorhanden"}
EINKAUF_TOP_LEVEL_ALLOWED = set(EINKAUF_FIELDS)
NUMERIC_ITEM_KEYS = {"ekp_brutto", "bezugskosten_anteil_brutto", "einstand_brutto", "menge_geliefert"}

CHANGE_COLORS = {
    "add": {"bg": "#203225", "fg": "#9ece6a"},
    "item_add": {"bg": "#203225", "fg": "#9ece6a"},
    "overwrite": {"bg": "#3a3117", "fg": "#f7c66f"},
    "item_update": {"bg": "#3a3117", "fg": "#f7c66f"},
    "mapped": {"bg": "#2e2a1a", "fg": "#f7c66f"},
    "unchanged": {"bg": "#24283b", "fg": "#c0caf5"},
}
ITEM_STATUS_STYLES = {
    "new": {"bg": "#1f3340", "fg": "#7dcfff"},
    "changed": {"bg": "#3a3117", "fg": "#f7c66f"},
    "same": {"bg": "#24283b", "fg": "#a9b1d6"},
    "unclear": {"bg": "#3c2418", "fg": "#ff9e64"},
    "pending": {"bg": "#24283b", "fg": "#c0caf5"},
}
FIELD_KIND_STYLES = {
    "item_add": {"bg": "#203225", "fg": "#9ece6a", "accent": "#9ece6a"},
    "item_update": {"bg": "#3a3117", "fg": "#f7c66f", "accent": "#f7a34b"},
    "unchanged": {"bg": "#24283b", "fg": "#a9b1d6", "accent": "#7aa2f7"},
}
IMAGE_DECISION_STYLES = {
    "selected_manual": {"bg": "#3a3117", "fg": "#f7c66f", "border": "#f7a34b"},
    "selected_auto": {"bg": "#203225", "fg": "#9ece6a", "border": "#9ece6a"},
    "candidate": {"bg": "#202233", "fg": "#c0caf5", "border": "#414868"},
    "unmapped": {"bg": "#1f3340", "fg": "#7dcfff", "border": "#4ea1d3"},
    "rejected": {"bg": "#2b2332", "fg": "#c0caf5", "border": "#6b7280"},
}

CHECK_MARK = chr(10003)

# --- FieldState-Styling importieren (zentrale Farbdefinition) ---
from module.lookup_results import FieldState, FIELD_STATE_STYLES  # noqa: E402


def set_field_state(widget, state: FieldState):
    """Setzt die Hintergrund-/Text-/Border-Farbe eines QLineEdit nach FieldState.

    Funktioniert fuer jedes QWidget mit setStyleSheet. Wird von
    FieldLookupBinding und direkt aus Modulen heraus verwendet.
    """
    style = FIELD_STATE_STYLES.get(state, FIELD_STATE_STYLES[FieldState.EMPTY])
    bg = style["bg"]
    fg = style["fg"]
    border = style["border"]
    badge = style.get("badge", "")

    widget.setStyleSheet(
        f"QLineEdit {{ background-color: {bg}; color: {fg}; "
        f"border: 2px solid {border}; border-radius: 4px; "
        f"padding: 4px 8px; font-size: 14px; }}"
        f"QLineEdit:focus {{ border: 2px solid #a561ff; }}"
    )
    # Badge als ToolTip setzen wenn vorhanden
    if badge:
        current_tip = str(widget.toolTip() or "").strip()
        prefix = f"[{badge}] "
        if not current_tip.startswith("["):
            widget.setToolTip(prefix + current_tip)
    else:
        current_tip = str(widget.toolTip() or "").strip()
        if current_tip.startswith("["):
            # Alten Badge-Prefix entfernen
            idx = current_tip.find("] ")
            if idx >= 0:
                widget.setToolTip(current_tip[idx + 2:])


def _format_extra_value(value):
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def _coerce_quantity_text(value):
    try:
        return str(max(1, int(float(str(value or "0").replace(",", ".")))))
    except Exception:
        text = str(value or "").strip()
        return text or "1"


def _coerce_item_value(key, value):
    if value is None:
        return ""
    if key == "menge":
        return _coerce_quantity_text(value)
    if key in NUMERIC_ITEM_KEYS:
        if str(value or "").strip() == "":
            return ""
        try:
            return f"{float(str(value).replace(',', '.')):.2f}"
        except Exception:
            return str(value).strip()
    return str(value).strip()


def _display_item_value(key, value):
    if value in (None, ""):
        return ""
    if key == "menge":
        return _coerce_quantity_text(value)
    if key in NUMERIC_ITEM_KEYS:
        try:
            return f"{float(str(value).replace(',', '.')):.2f}"
        except Exception:
            return str(value)
    return str(value)


def _normalized_compare_value(key, value):
    if value in (None, ""):
        return ""
    if key == "menge":
        return _coerce_quantity_text(value)
    if key in NUMERIC_ITEM_KEYS:
        try:
            return round(float(str(value).replace(',', '.')), 4)
        except Exception:
            return str(value).strip()
    return str(value).strip()


def _has_meaningful_item_data(item):
    if not isinstance(item, dict):
        return False
    for key in ("produkt_name", "varianten_info", "ean", "ekp_brutto", "bezugskosten_anteil_brutto", "einstand_brutto"):
        if str(item.get(key, "") or "").strip():
            return True
    return False


def collect_einkauf_extra_fields(payload):
    payload = payload if isinstance(payload, dict) else {}
    rows = []

    for key, value in payload.items():
        if str(key).startswith("_") or key in EINKAUF_VISIBLE_FIELD_SET or key == "waren":
            continue
        if value in ("", None, [], {}):
            continue
        rows.append((str(key), _format_extra_value(value)))

    waren = payload.get("waren", [])
    if isinstance(waren, list):
        for row_index, item in enumerate(waren, start=1):
            if not isinstance(item, dict):
                continue
            for key, value in item.items():
                if str(key).startswith("_") or key in EINKAUF_ITEM_REVIEW_KEYS:
                    continue
                if value in ("", None, [], {}):
                    continue
                rows.append((f"Artikel {row_index} - {key}", _format_extra_value(value)))

    rows.sort(key=lambda pair: pair[0].lower())
    return rows


class _UiMediaPreviewResolver:
    def __init__(self, settings_manager=None):
        self._settings_manager = settings_manager
        self._media_service = None
        self._service_failed = False
        self._pixmap_cache = {}
        self._icon_cache = {}
        self._placeholder_cache = {}
        self._seen_revision = OrderVisualState.current_revision()

    def _ensure_revision_current(self):
        current_revision = OrderVisualState.current_revision()
        if self._seen_revision == current_revision:
            return
        self._pixmap_cache = {}
        self._icon_cache = {}
        self._seen_revision = current_revision
        logging.debug("Tabellen-Mediencache lokal invalidiert: revision=%s", current_revision)

    def get_media_service(self):
        if self._service_failed:
            return None
        if self._media_service is not None:
            return self._media_service
        if self._settings_manager is None:
            return None
        try:
            from module.database_manager import DatabaseManager
            from module.media.media_service import MediaService

            self._media_service = MediaService(DatabaseManager(self._settings_manager))
            return self._media_service
        except Exception as exc:
            self._service_failed = True
            logging.warning("Medienvorschau konnte nicht initialisiert werden: %s", exc)
            return None

    def _placeholder_pixmap(self, label, size, background="#24283b", foreground="#7aa2f7"):
        cache_key = (str(label or "").strip().upper()[:2], int(size), background, foreground)
        if cache_key in self._placeholder_cache:
            return self._placeholder_cache[cache_key]

        pixmap = create_placeholder_pixmap(label, size, background=background, foreground=foreground, radius=8)
        self._placeholder_cache[cache_key] = pixmap
        return pixmap

    def _load_pixmap(self, path_value, size, fallback_label, background="#24283b", foreground="#7aa2f7"):
        self._ensure_revision_current()
        cache_key = (str(path_value or ""), int(size), str(fallback_label or ""), background, foreground)
        if cache_key in self._pixmap_cache:
            return self._pixmap_cache[cache_key]

        source = QPixmap(str(path_value or ""))
        if source.isNull():
            if str(path_value or "").strip():
                logging.debug("Bildvorschau faellt auf Platzhalter zurueck: path=%s", path_value)
            pixmap = self._placeholder_pixmap(fallback_label, size, background=background, foreground=foreground)
        else:
            pixmap = render_preview_pixmap(source, size, background="#ffffff", radius=8, inset=2)
        self._pixmap_cache[cache_key] = pixmap
        return pixmap
    def resolve_shop_preview(self, payload, size=46):
        payload = payload if isinstance(payload, dict) else {}
        shop_name = str(payload.get("shop_name", "") or "").strip()
        sender_domain = str(payload.get("_email_sender_domain", payload.get("sender_domain", "")) or "").strip()
        label_text = shop_name or sender_domain or "Shop noch offen"
        lookup_shop_name = "" if shop_name.strip().lower() == "shop noch offen" else shop_name
        payload_for_lookup = dict(payload)
        payload_for_lookup["shop_name"] = lookup_shop_name
        if str(payload.get("_origin_module", "") or "") == "modul_order_entry":
            payload_for_lookup = {
                "shop_name": lookup_shop_name,
                "sender_domain": sender_domain,
                "_email_sender_domain": sender_domain,
            }
        service = self.get_media_service()
        logo_path = ""
        resolved_logo = {}
        if service is not None:
            try:
                resolved_logo = service.resolve_shop_logo(shop_name=lookup_shop_name, sender_domain=sender_domain, payload=payload_for_lookup) or {}
                logo_path = str(resolved_logo.get("path", "") or "")
                asset = dict(resolved_logo.get("asset") or {}) if isinstance(resolved_logo.get("asset"), dict) else {}
                if logo_path:
                    source_kind = str(asset.get("source_kind", "") or "").strip().lower()
                    if str(payload.get("_origin_module", "") or "") == "modul_order_entry" and "screenshot_logo_guess" in source_kind:
                        logging.info("Modul-1-Shopvorschau ignoriert screenshot-basiertes Fallbacklogo: source_kind=%s, asset_id=%s", source_kind, asset.get("id"))
                        write_module1_trace(
                            "shop_logo_preview_screenshot_fallback_suppressed",
                            shop_name=shop_name,
                            sender_domain=sender_domain,
                            source_kind=source_kind,
                            asset_id=asset.get("id"),
                        )
                        logo_path = ""
                    else:
                        logging.info("shop_logo_preview_resolved: shop_name=%s, sender_domain=%s, source_kind=%s, asset_id=%s", shop_name, sender_domain, source_kind, asset.get("id"))
                        write_module1_trace(
                            "shop_logo_preview_resolved",
                            shop_name=shop_name,
                            sender_domain=sender_domain,
                            source_kind=source_kind,
                            asset_id=asset.get("id"),
                        )
            except Exception as exc:
                logging.warning("Shoplogo konnte nicht aufgeloest werden: %s", exc)
                logo_path = ""
        if not logo_path:
            logging.debug("Platzhalterlogo fuer Shop-Vorschau aktiv: shop=%s, domain=%s", shop_name or "", sender_domain or "")
        pixmap = self._load_pixmap(logo_path, size, "?", background="#f3f4f6", foreground="#4b5563")
        hint_text = "Shoplogo gefunden" if logo_path else (
            "Platzhalterlogo aktiv, bis ein echtes Shoplogo gefunden wird"
            if (shop_name or sender_domain)
            else "Shop noch offen - Platzhalterlogo bleibt sichtbar"
        )
        return {
            "pixmap": pixmap,
            "has_media": bool(logo_path),
            "is_placeholder": not bool(logo_path),
            "label_text": label_text,
            "hint": hint_text,
        }

    def resolve_product_icon(self, item, size=36):
        self._ensure_revision_current()
        item = item if isinstance(item, dict) else {}
        product_name = str(item.get("produkt_name", "") or "").strip()
        ean = str(item.get("ean", "") or "").strip()
        variant_text = str(item.get("varianten_info", item.get("variant_text", "")) or "").strip()
        cache_key = (product_name, ean, variant_text, int(size))
        if cache_key in self._icon_cache:
            return self._icon_cache[cache_key]

        service = self.get_media_service()
        image_path = ""
        if service is not None:
            try:
                image_path = str(
                    service.get_product_image_path(
                        product_name=product_name,
                        ean=ean,
                        variant_text=variant_text,
                        item=item,
                    ) or ""
                )
            except Exception as exc:
                logging.warning("Produktbild konnte nicht aufgeloest werden: %s", exc)
                image_path = ""

        label = (product_name[:1] or ean[:2] or "P").upper()
        pixmap = self._load_pixmap(image_path, size, label, background="#202233", foreground="#a9b1d6")
        result = {
            "icon": QIcon(pixmap),
            "has_media": bool(image_path),
            "tooltip": product_name or ean or "Produktbild",
        }
        self._icon_cache[cache_key] = result
        return result

    def resolve_product_table_preview(self, item, review_row=None, payload=None, source_row_index=-1, draft_state=None, size=42):
        item = item if isinstance(item, dict) else {}
        review_row = review_row if isinstance(review_row, dict) else {}
        payload = payload if isinstance(payload, dict) else {}
        draft_state = draft_state if isinstance(draft_state, dict) else {}
        module1_ai_cropping_disabled = str(payload.get("_origin_module", "") or "") == "modul_order_entry"
        product_name = str(item.get("produkt_name", "") or "").strip()
        ean = str(item.get("ean", "") or "").strip()
        variant_text = str(item.get("varianten_info", item.get("variant_text", "")) or "").strip()
        label = (product_name[:1] or ean[:2] or "P").upper()
        path_value = ""
        status_text = "Fallback"
        source_text = "Kein Produktbild hinterlegt"
        has_media = False
        service = self.get_media_service()

        try:
            if service is not None and int(source_row_index) >= 0 and payload.get("waren"):
                bundle = service.get_payload_item_image_candidates(
                    payload=payload,
                    source_row_index=int(source_row_index),
                    review_row=review_row,
                    fallback_to_product=True,
                    source_module="product_table_column",
                )
                selected_asset_id = int(draft_state.get("selected_asset_id", 0) or 0)
                if selected_asset_id > 0:
                    for candidate in list(bundle.get("candidates", []) or []):
                        if module1_ai_cropping_disabled and str(candidate.get("source_type", "") or "").strip() == "screenshot_detection_crop":
                            continue
                        if int(candidate.get("media_asset_id", 0) or 0) == selected_asset_id and not bool(candidate.get("is_rejected")):
                            path_value = str(candidate.get("path", "") or "")
                            has_media = bool(path_value)
                            status_text = str(candidate.get("status_label", "Manuell gesetzt") or "Manuell gesetzt")
                            source_text = str(candidate.get("source_label", "Produktbild") or "Produktbild")
                            break
                if not path_value:
                    selected_candidate = bundle.get("selected") if isinstance(bundle.get("selected"), dict) else None
                    fallback_candidate = bundle.get("fallback_global") if isinstance(bundle.get("fallback_global"), dict) else None
                    if module1_ai_cropping_disabled and isinstance(fallback_candidate, dict) and str(fallback_candidate.get("source_type", "") or "").strip() == "screenshot_detection_crop":
                        fallback_candidate = None
                    if selected_candidate and not str(selected_candidate.get("path", "") or "").strip():
                        selected_candidate = None
                    candidate_rows = [
                        dict(candidate)
                        for candidate in list(bundle.get("candidates", []) or [])
                        if isinstance(candidate, dict)
                        and not bool(candidate.get("is_rejected"))
                        and str(candidate.get("path", "") or "").strip()
                        and (not module1_ai_cropping_disabled or str(candidate.get("source_type", "") or "").strip() != "screenshot_detection_crop")
                    ]
                    candidate_rows.sort(
                        key=lambda candidate: (
                            int(candidate.get("preview_priority", 99) or 99),
                            0 if str(candidate.get("status_key", "") or "") == "selected_manual" else 1,
                            0 if str(candidate.get("status_key", "") or "") == "selected_auto" else 1,
                        )
                    )
                    screenshot_image_candidate = next(
                        (
                            candidate
                            for candidate in candidate_rows
                            if str(candidate.get("source_type", "") or "").strip() == "screenshot_detection_crop"
                            and str(candidate.get("detection_role", "") or "").strip() == "product_image"
                        ),
                        None,
                    )
                    screenshot_candidate = next(
                        (
                            candidate
                            for candidate in candidate_rows
                            if str(candidate.get("source_type", "") or "").strip() == "screenshot_detection_crop"
                        ),
                        None,
                    )
                    if module1_ai_cropping_disabled:
                        screenshot_image_candidate = None
                        screenshot_candidate = None
                        if selected_candidate and str(selected_candidate.get("source_type", "") or "").strip() == "screenshot_detection_crop":
                            selected_candidate = None
                    elif selected_candidate and str(selected_candidate.get("source_type", "") or "").strip() == "screenshot_detection_crop":
                        selected_role = str(selected_candidate.get("detection_role", "") or "").strip()
                        if selected_role and selected_role != "product_image" and screenshot_image_candidate:
                            logging.info("Bildvorschau bevorzugt product_image-Crop statt generischem Detection-Crop: role=%s", selected_role)
                            selected_candidate = None
                    first_candidate = candidate_rows[0] if candidate_rows else None
                    chosen = screenshot_image_candidate or selected_candidate or screenshot_candidate or first_candidate or fallback_candidate
                    if chosen:
                        path_value = str(chosen.get("path", "") or "")
                        has_media = bool(path_value)
                        default_status = "Kandidat" if chosen in (screenshot_image_candidate, screenshot_candidate, first_candidate) else "Ausgewaehlt"
                        status_text = str(chosen.get("status_label", default_status) or default_status)
                        source_text = str(chosen.get("source_label", "Produktbild") or "Produktbild")
            if not path_value and service is not None:
                order_item_id = int(review_row.get("match_position_id", 0) or 0)
                if order_item_id > 0:
                    resolved = service.resolve_order_item_selected_image(order_item_id, fallback_to_product=True)
                    resolved_asset = resolved.get("asset") if isinstance(resolved.get("asset"), dict) else {}
                    if module1_ai_cropping_disabled and str((resolved_asset or {}).get("source_kind", "") or "").strip() == "screenshot_detection_crop":
                        resolved = {}
                    path_value = str(resolved.get("path", "") or "")
                    has_media = bool(path_value)
                    status_text = "Ausgewaehlt" if str(resolved.get("source", "") or "") == "order_item_selection" else "Fallback"
                    source_text = "Bestellpositionsbild" if str(resolved.get("source", "") or "") == "order_item_selection" else "Globales Produktbild"
            if not path_value and service is not None:
                resolved = service.resolve_product_image(
                    product_name=product_name,
                    ean=ean,
                    variant_text=variant_text,
                    item=item,
                    payload=payload or item,
                )
                resolved_asset = resolved.get("asset") if isinstance(resolved.get("asset"), dict) else {}
                if module1_ai_cropping_disabled and str((resolved_asset or {}).get("source_kind", "") or "").strip() == "screenshot_detection_crop":
                    resolved = {}
                path_value = str((resolved or {}).get("path", "") or "")
                has_media = bool(path_value)
                if has_media:
                    status_text = "Fallback"
                    source_text = "Globales Produktbild"
        except Exception as exc:
            logging.warning("Tabellenbild konnte nicht aufgeloest werden: %s", exc)
            path_value = ""
            has_media = False
            status_text = "Fallback"
            source_text = "Kein Produktbild hinterlegt"

        badge_text = "?"
        badge_bg = "#2f3545"
        badge_fg = "#c0caf5"
        badge_border = "#6b7280"
        summary_text = f"{status_text} {source_text}".strip().lower()
        if has_media:
            if "manuell" in summary_text:
                badge_text = "M"
                badge_bg = "#3a3117"
                badge_fg = "#f7c66f"
                badge_border = "#f7a34b"
            elif "screenshot" in summary_text:
                badge_text = "SC"
                badge_bg = "#1f3340"
                badge_fg = "#7dcfff"
                badge_border = "#4ea1d3"
            elif "fallback" in summary_text or "global" in summary_text:
                badge_text = "FB"
                badge_bg = "#24283b"
                badge_fg = "#a9b1d6"
                badge_border = "#6b7280"
            elif "ausgewaehlt" in summary_text or "bestellpositionsbild" in summary_text:
                badge_text = "OK"
                badge_bg = "#203225"
                badge_fg = "#9ece6a"
                badge_border = "#9ece6a"
            else:
                badge_text = "IMG"
                badge_bg = "#202233"
                badge_fg = "#c0caf5"
                badge_border = "#414868"

        pixmap = self._load_pixmap(path_value, size, label, background="#202233", foreground="#a9b1d6")
        tooltip = product_name or ean or "Produktbild"
        tooltip_parts = [part for part in (tooltip, status_text, source_text) if str(part or "").strip()]
        if str(payload.get("_origin_module", "") or "") == "modul_order_entry":
            write_module1_trace(
                "wizard_product_preview_resolved",
                source_row_index=int(source_row_index),
                has_media=bool(has_media),
                path_value=str(path_value or ""),
                status_text=status_text,
                source_text=source_text,
                product_name=product_name,
                ean=ean,
            )
        return {
            "pixmap": pixmap,
            "has_media": has_media,
            "tooltip": "\n".join(tooltip_parts),
            "status_text": status_text,
            "source_text": source_text,
            "badge_text": badge_text,
            "badge_bg": badge_bg,
            "badge_fg": badge_fg,
            "badge_border": badge_border,
        }

    def resolve_candidate_preview(self, candidate, size=80):
        candidate = candidate if isinstance(candidate, dict) else {}
        path_value = str(candidate.get("path", "") or "")
        fallback_label = str(candidate.get("source_label", "") or candidate.get("source_type", "") or "Bild")[:2] or "BI"
        pixmap = self._load_pixmap(path_value, size, fallback_label, background="#202233", foreground="#a9b1d6")
        return {
            "pixmap": pixmap,
            "has_media": bool(path_value),
            "tooltip": str(candidate.get("source_label", "") or candidate.get("source_ref", "") or "Bildkandidat"),
        }

class SummenBannerWidget(QFrame):
    """Wiederverwendbares Banner: Berechneter Warenwert vs. KI-Gesamtpreis mit Abweichungswarnung."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet(
            "QFrame { background-color: #202233; border: 1px solid #414868; border-radius: 6px; }"
        )
        layout = QHBoxLayout(self)
        layout.setContentsMargins(12, 6, 12, 6)
        layout.setSpacing(16)

        self.lbl_berechnet = QLabel("Berechnet: --")
        self.lbl_berechnet.setStyleSheet("color: #c0caf5; font-size: 12px; border: none;")
        layout.addWidget(self.lbl_berechnet)

        self.lbl_ki = QLabel("KI-Gesamtpreis: --")
        self.lbl_ki.setStyleSheet("color: #a9b1d6; font-size: 12px; border: none;")
        layout.addWidget(self.lbl_ki)

        self.lbl_warnung = QLabel("")
        self.lbl_warnung.setStyleSheet("color: #f7c66f; font-size: 12px; font-weight: bold; border: none;")
        layout.addWidget(self.lbl_warnung)

        layout.addStretch()
        self.setVisible(False)

    def update_from_items(self, items, gesamt_ekp_brutto=None):
        """Berechnet Warenwert aus Artikelliste und vergleicht mit KI-Gesamtpreis.

        items: list of dicts with 'menge' and 'ekp_brutto' keys
        gesamt_ekp_brutto: KI-erkannter Gesamtpreis (float or str)
        """
        if not items:
            self.setVisible(False)
            return

        warenwert = 0.0
        for item in items:
            try:
                menge = float(str(item.get("menge", 1) or 1).replace(",", "."))
                preis = float(str(item.get("ekp_brutto", 0) or 0).replace(",", "."))
                warenwert += menge * preis
            except (ValueError, TypeError):
                pass

        self.lbl_berechnet.setText(f"Berechnet: {warenwert:.2f} EUR")

        ki_gesamt = 0.0
        try:
            ki_gesamt = float(str(gesamt_ekp_brutto or 0).replace(",", "."))
        except (ValueError, TypeError):
            pass

        if ki_gesamt > 0:
            self.lbl_ki.setText(f"KI-Gesamtpreis: {ki_gesamt:.2f} EUR")
            self.lbl_ki.setVisible(True)
            delta = abs(warenwert - ki_gesamt)
            if delta > 0.02:
                self.lbl_warnung.setText(f"Abweichung: {delta:.2f} EUR")
                self.lbl_warnung.setVisible(True)
            else:
                self.lbl_warnung.setVisible(False)
        else:
            self.lbl_ki.setVisible(False)
            self.lbl_warnung.setVisible(False)

        self.setVisible(True)


class InlineChangeFieldRow(QWidget):
    # Proxy signal so FieldLookupBinding can connect to returnPressed
    # exactly like it would on a plain QLineEdit.
    returnPressed = pyqtSignal()

    def __init__(self, field_key, label_text, parent=None):
        super().__init__(parent)
        self.field_key = str(field_key or "")
        self.label_text = str(label_text or self.field_key or "Wert")
        self._display_mode = "normal"
        self._change_kind = "unchanged"
        self._old_value = ""
        self._new_value = ""
        self._selected_side = "new"
        self._build_ui()
        self._show_normal_field("")

    def setPlaceholderText(self, text: str):
        """Proxy so callers can treat this like a QLineEdit."""
        self.normal_input.setPlaceholderText(text)

    def _build_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        self.lbl_name = QLabel(self.label_text + ":")
        self.lbl_name.setMinimumWidth(220)
        self.lbl_name.setStyleSheet("font-size: 12px; color: #c0caf5;")
        layout.addWidget(self.lbl_name, 0, Qt.AlignmentFlag.AlignTop)

        self.value_host = QVBoxLayout()
        self.value_host.setContentsMargins(0, 0, 0, 0)
        self.value_host.setSpacing(4)
        layout.addLayout(self.value_host, 1)

        self.normal_input = QLineEdit()
        # Forward returnPressed so FieldLookupBinding works transparently
        self.normal_input.returnPressed.connect(self.returnPressed)
        self.value_host.addWidget(self.normal_input)

        self.compare_widget = QWidget(self)
        compare_layout = QVBoxLayout(self.compare_widget)
        compare_layout.setContentsMargins(0, 0, 0, 0)
        compare_layout.setSpacing(4)

        self.compare_value_row = QHBoxLayout()
        self.compare_value_row.setContentsMargins(0, 0, 0, 0)
        self.compare_value_row.setSpacing(8)

        self.btn_old = QPushButton()
        self.btn_old.setMinimumHeight(36)
        self.btn_old.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_old.clicked.connect(self._select_old)
        self.compare_value_row.addWidget(self.btn_old, 1)

        self.lbl_old_indicator = QLabel(CHECK_MARK)
        self.lbl_old_indicator.setMinimumWidth(18)
        self.lbl_old_indicator.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.compare_value_row.addWidget(self.lbl_old_indicator)

        self.lbl_arrow = QLabel("->")
        self.lbl_arrow.setStyleSheet("font-size: 13px; font-weight: bold; color: #7aa2f7;")
        self.lbl_arrow.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.compare_value_row.addWidget(self.lbl_arrow)

        self.edit_new = ReviewSelectableLineEdit(editable=True, parent=self)
        self.edit_new.setMinimumHeight(36)
        self.edit_new.clicked.connect(self._select_new)
        self.edit_new.valueCommitted.connect(self._commit_new_value)
        self.compare_value_row.addWidget(self.edit_new, 1)

        self.lbl_new_indicator = QLabel(CHECK_MARK)
        self.lbl_new_indicator.setMinimumWidth(18)
        self.lbl_new_indicator.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.compare_value_row.addWidget(self.lbl_new_indicator)

        compare_layout.addLayout(self.compare_value_row)

        self.lbl_compare_hint = QLabel("")
        self.lbl_compare_hint.setWordWrap(True)
        self.lbl_compare_hint.setStyleSheet("font-size: 11px; color: #a9b1d6;")
        compare_layout.addWidget(self.lbl_compare_hint)

        self.compare_widget.setVisible(False)
        self.value_host.addWidget(self.compare_widget)

    def _apply_normal_style(self, change_kind=None):
        if change_kind == "add":
            border = "2px solid #9ece6a"
            bg = "#203225"
            fg = "#9ece6a"
        else:
            border = "1px solid #414868"
            bg = "#171824"
            fg = "#c0caf5"
        self.normal_input.setStyleSheet(
            "QLineEdit {"
            f"background-color: {bg}; color: {fg}; border: {border}; border-radius: 4px; padding: 6px;"
            "}"
        )

    def _show_normal_field(self, text, change_kind=None):
        self._display_mode = "fill" if change_kind == "add" else "normal"
        self.compare_widget.setVisible(False)
        self.normal_input.setVisible(True)
        self.normal_input.setText(str(text or "").strip())
        self._apply_normal_style(change_kind)

    def _kind_palette(self):
        if self._change_kind == "mapped":
            return {
                "accent": "#f7c66f", "bg": "#2e2a1a", "text": "#f7c66f",
                "hint": "Automatisch gemappt (mapping.json). Wert kann angepasst werden.",
                "old_bg": "#1a1b26", "old_fg": "#6b7280", "old_border": "1px solid #30374d",
            }
        return {"accent": "#f7a34b", "bg": "#3a3117", "text": "#f7c66f", "hint": "Neuer Wert wuerde den vorhandenen DB-Wert aendern."}

    def _apply_compare_styles(self):
        palette = self._kind_palette()
        is_mapped = self._change_kind == "mapped"

        if is_mapped:
            # Mapped mode: left side is always gray/disabled, right side is always active yellow
            self.btn_old.setEnabled(False)
            self.btn_old.setStyleSheet(
                "QPushButton {"
                f"background-color: {palette['old_bg']}; color: {palette['old_fg']}; border: {palette['old_border']};"
                " border-radius: 6px; padding: 6px 10px; text-align: left;"
                "}"
                f"QPushButton:disabled {{ color: {palette['old_fg']}; border-color: #30374d; background-color: {palette['old_bg']}; }}"
            )
            self.edit_new.setStyleSheet(
                "QLineEdit {"
                f"background-color: {palette['bg']}; color: {palette['text']}; border: 2px solid {palette['accent']};"
                " border-radius: 6px; padding: 6px 10px;"
                "}"
            )
            self.lbl_old_indicator.setVisible(False)
            self.lbl_new_indicator.setVisible(True)
            self.lbl_new_indicator.setStyleSheet(f"font-size: 16px; font-weight: bold; color: {palette['accent']};")
        else:
            self.btn_old.setEnabled(True)
            inactive_border = "1px solid #414868"
            active_border = f"2px solid {palette['accent']}"
            old_border = active_border if self._selected_side == "old" else inactive_border
            new_border = active_border if self._selected_side == "new" else inactive_border
            old_bg = palette["bg"] if self._selected_side == "old" else "#171824"
            new_bg = palette["bg"] if self._selected_side == "new" else "#171824"
            old_fg = palette["text"] if self._selected_side == "old" else "#c0caf5"
            new_fg = palette["text"] if self._selected_side == "new" else "#c0caf5"

            self.btn_old.setStyleSheet(
                "QPushButton {"
                f"background-color: {old_bg}; color: {old_fg}; border: {old_border}; border-radius: 6px; padding: 6px 10px; text-align: left;"
                "}"
                "QPushButton:disabled { color: #6b7280; border-color: #30374d; }"
            )
            self.edit_new.setStyleSheet(
                "QLineEdit {"
                f"background-color: {new_bg}; color: {new_fg}; border: {new_border}; border-radius: 6px; padding: 6px 10px;"
                "}"
            )
            self.lbl_old_indicator.setVisible(self._selected_side == "old")
            self.lbl_new_indicator.setVisible(self._selected_side == "new")
            self.lbl_old_indicator.setStyleSheet(f"font-size: 16px; font-weight: bold; color: {palette['accent']};")
            self.lbl_new_indicator.setStyleSheet(f"font-size: 16px; font-weight: bold; color: {palette['accent']};")

        self.lbl_compare_hint.setText(palette["hint"])

    def _select_old(self):
        self._selected_side = "old"
        self._apply_compare_styles()

    def _select_new(self):
        self._selected_side = "new"
        self._apply_compare_styles()

    def _commit_new_value(self, text):
        self._new_value = str(text or "").strip()
        self._selected_side = "new"
        self._apply_compare_styles()

    def setText(self, value):
        text = str(value or "").strip()
        if self._display_mode == "compare":
            self._new_value = text
            self.edit_new.set_committed_text(self._new_value)
            self._apply_compare_styles()
        else:
            self.normal_input.setText(text)

    def text(self):
        if self._display_mode == "compare":
            if self._change_kind == "mapped":
                return str(self.edit_new.text() or "")
            if self._selected_side == "old":
                return str(self._old_value or "")
            return str(self.edit_new.text() or "")
        return self.normal_input.text()

    def clear(self):
        self._change_kind = "unchanged"
        self._old_value = ""
        self._new_value = ""
        self._selected_side = "new"
        self._show_normal_field("")

    def set_review_change(self, old_value, new_value, change_kind):
        self._change_kind = str(change_kind or "overwrite")
        self._old_value = "" if old_value is None else str(old_value)
        self._new_value = "" if new_value is None else str(new_value)
        self._selected_side = "new"

        if self._change_kind == "add":
            fill_text = str(self.normal_input.text() or "").strip()
            if not fill_text:
                fill_text = format_review_value(self._new_value)
            self._show_normal_field(fill_text, change_kind="add")
            return

        if self._change_kind != "overwrite":
            keep_text = str(self.normal_input.text() or "").strip()
            if not keep_text:
                keep_text = format_review_value(self._new_value)
            self.clear_review_change(keep_text)
            return

        self._display_mode = "compare"
        self.normal_input.setVisible(False)
        self.compare_widget.setVisible(True)
        self.btn_old.setText(format_review_value(self._old_value))
        self.edit_new.set_committed_text(str(self.normal_input.text() or "").strip() or format_review_value(self._new_value))
        self._apply_compare_styles()

    # ── Auto-Mapping-Anzeige ─────────────────────────────────────────────

    def set_mapping_change(self, raw_value, resolved_value):
        """Zeigt eine Auto-Mapping-Transformation: grauer Rohwert -> gelber gemappter Wert.

        Der Rohwert (links) ist nur informativ und nicht klickbar.
        Der gemappte Wert (rechts) ist editierbar und wird beim Speichern uebernommen.
        """
        self._change_kind = "mapped"
        self._old_value = "" if raw_value is None else str(raw_value)
        self._new_value = "" if resolved_value is None else str(resolved_value)
        self._selected_side = "new"

        self._display_mode = "compare"
        self.normal_input.setVisible(False)
        self.compare_widget.setVisible(True)
        self.btn_old.setText(format_review_value(self._old_value))
        self.edit_new.set_committed_text(format_review_value(self._new_value))
        self._apply_compare_styles()

    # ── Inline-Suggestion-Dropdown ──────────────────────────────────────

    def set_suggestion_dropdown(self, suggestions: list):
        """Zeigt ein gelbes Feld mit Dropdown-Pfeil und Vorschlaegen.

        Der User kann einen Vorschlag waehlen oder den Wert manuell eingeben.
        """
        self._suggestions = list(suggestions or [])
        if not self._suggestions:
            return

        # Sicherstellen, dass Normal-Mode aktiv ist
        if self._display_mode == "compare":
            self.clear_review_change(self.text())

        # Dropdown-Button rechts im Feld
        if not hasattr(self, "_btn_suggest"):
            self._btn_suggest = QPushButton("▼")
            self._btn_suggest.setFixedSize(28, 28)
            self._btn_suggest.setCursor(Qt.CursorShape.PointingHandCursor)
            self._btn_suggest.setStyleSheet(
                "QPushButton { background-color: #3a3117; color: #f7c66f; border: 1px solid #f7c66f;"
                " border-radius: 4px; font-size: 14px; font-weight: bold; }"
                "QPushButton:hover { background-color: #4a4127; }"
            )
            self._btn_suggest.clicked.connect(self._show_suggestion_menu)
            # In das Layout neben dem Input einfuegen
            parent_layout = self.layout()
            if parent_layout:
                parent_layout.addWidget(self._btn_suggest)

        self._btn_suggest.setVisible(True)

        # Feld gelb markieren
        self.normal_input.setStyleSheet(
            "QLineEdit { background-color: #2e2a1a; color: #f7c66f;"
            " border: 2px solid #f7c66f; border-radius: 4px; padding: 6px; }"
        )

    def _show_suggestion_menu(self):
        """Zeigt das Dropdown-Menu mit den Vorschlaegen."""
        if not hasattr(self, "_suggestions") or not self._suggestions:
            return
        menu = QMenu(self)
        menu.setStyleSheet(
            "QMenu { background-color: #24283b; color: #c0caf5; border: 1px solid #414868; }"
            "QMenu::item:selected { background-color: #3a4160; }"
            "QMenu::item { padding: 6px 16px; }"
        )
        for suggestion in self._suggestions:
            action = menu.addAction(str(suggestion))
            action.triggered.connect(lambda checked, s=suggestion: self._apply_suggestion(s))

        menu.addSeparator()
        edit_action = menu.addAction("Eigenen Wert eingeben...")
        edit_action.triggered.connect(self._enable_manual_input)

        # Menu unter dem Button oeffnen
        btn = self._btn_suggest
        menu.exec(btn.mapToGlobal(btn.rect().bottomLeft()))

    def _apply_suggestion(self, value: str):
        """Wendet einen Suggestion-Wert an und entfernt den Dropdown."""
        self.normal_input.setText(str(value or ""))
        self._clear_suggestion_ui()
        # returnPressed emittieren damit FieldLookupBinding den Lookup auslöst (z.B. Logo-Suche)
        self.normal_input.returnPressed.emit()

    def _enable_manual_input(self):
        """Entfernt den Dropdown und macht das Feld editierbar."""
        self._clear_suggestion_ui()
        self.normal_input.setFocus()
        self.normal_input.selectAll()

    def _clear_suggestion_ui(self):
        """Entfernt die gelbe Markierung und den Dropdown-Button."""
        if hasattr(self, "_btn_suggest"):
            self._btn_suggest.setVisible(False)
        self._apply_normal_style()

    def clear_suggestions(self):
        """Oeffentliche Methode um den Suggestion-Modus komplett zu entfernen."""
        self._suggestions = []
        self._clear_suggestion_ui()

    def clear_review_change(self, value=None):
        current_value = self.text() if value is None else value
        self._change_kind = "unchanged"
        self._old_value = ""
        self._new_value = ""
        self._selected_side = "new"
        self._show_normal_field(str(current_value or "").strip())

class EinkaufHeadFormWidget(QWidget):
    logoSearchRequested = pyqtSignal(object)

    def __init__(self, parent=None, logo_search_mode="context"):
        super().__init__(parent)
        # "direct": Logo-Frame immer sichtbar, Suche liest shop_name direkt aus Eingabe
        # "context": Logo-Frame nur bei vorhandenem Kontext, Suche braucht shop_name ODER sender_domain
        self._logo_search_mode = logo_search_mode
        self.inputs = {}
        self.field_rows = {}
        self._extra_rows = []
        self._media_preview = _UiMediaPreviewResolver(getattr(parent, "settings_manager", None))
        self._payload_context = {}
        self._review_bundle = {}
        self._draft_image_states = {}
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        self.shop_preview_frame = QFrame()
        self.shop_preview_frame.setStyleSheet("QFrame { background-color: #202233; border: 1px solid #414868; border-radius: 6px; }")
        shop_preview_layout = QHBoxLayout(self.shop_preview_frame)
        shop_preview_layout.setContentsMargins(12, 10, 12, 10)
        shop_preview_layout.setSpacing(10)

        self.lbl_shop_logo = QLabel()
        self.lbl_shop_logo.setFixedSize(52, 52)
        self.lbl_shop_logo.setAlignment(Qt.AlignmentFlag.AlignCenter)
        shop_preview_layout.addWidget(self.lbl_shop_logo, 0, Qt.AlignmentFlag.AlignTop)

        shop_text_layout = QVBoxLayout()
        shop_text_layout.setContentsMargins(0, 0, 0, 0)
        shop_text_layout.setSpacing(2)
        self.lbl_shop_title = QLabel("Shop")
        self.lbl_shop_title.setStyleSheet("font-size: 13px; font-weight: bold; color: #c0caf5;")
        shop_text_layout.addWidget(self.lbl_shop_title)
        self.lbl_shop_media_hint = QLabel("")
        self.lbl_shop_media_hint.setWordWrap(True)
        self.lbl_shop_media_hint.setStyleSheet("font-size: 11px; color: #a9b1d6;")
        shop_text_layout.addWidget(self.lbl_shop_media_hint)
        shop_preview_layout.addLayout(shop_text_layout, 1)

        self.btn_logo_search = QPushButton("Logo suchen")
        self.btn_logo_search.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_logo_search.clicked.connect(self._request_logo_search)
        shop_preview_layout.addWidget(self.btn_logo_search, 0, Qt.AlignmentFlag.AlignTop)

        if self._logo_search_mode == "direct":
            self._show_shop_placeholder()
        else:
            self.shop_preview_frame.setVisible(False)
        layout.addWidget(self.shop_preview_frame)

        for section_title, fields in EINKAUF_FIELD_SECTIONS:
            frame = QFrame()
            frame.setStyleSheet("QFrame { background-color: #24283b; border: 1px solid #414868; border-radius: 6px; }")
            frame_layout = QVBoxLayout(frame)
            frame_layout.setContentsMargins(12, 12, 12, 12)
            frame_layout.setSpacing(8)

            title = QLabel(section_title)
            title.setStyleSheet("font-size: 13px; font-weight: bold; color: #7aa2f7;")
            frame_layout.addWidget(title)

            for key, label_text in fields:
                field_row = InlineChangeFieldRow(key, label_text, self)
                self.inputs[key] = field_row
                self.field_rows[key] = field_row
                frame_layout.addWidget(field_row)

            # Rechnung vorhanden Checkbox direkt im Rechnung & Status Block
            if section_title == "Rechnung & Status":
                chk_row = QHBoxLayout()
                chk_row.setContentsMargins(0, 4, 0, 0)
                chk_row.setSpacing(8)
                lbl_rg_icon = QLabel("\U0001f9fe")
                lbl_rg_icon.setStyleSheet("font-size: 15px;")
                lbl_rg_icon.setToolTip("Gibt an ob eine Rechnung fuer diese Bestellung vorliegt.")
                chk_row.addWidget(lbl_rg_icon)
                self.chk_rechnung_vorhanden = QCheckBox("Rechnung vorhanden")
                self.chk_rechnung_vorhanden.setStyleSheet("color: #c0caf5; font-size: 12px;")
                self.chk_rechnung_vorhanden.setToolTip("Haken setzen sobald die Rechnung vorliegt.")
                chk_row.addWidget(self.chk_rechnung_vorhanden)
                chk_row.addStretch()
                frame_layout.addLayout(chk_row)

            layout.addWidget(frame)

        # --- Reverse Charge Checkbox ---
        rc_frame = QFrame()
        rc_frame.setStyleSheet("QFrame { background-color: #24283b; border: 1px solid #414868; border-radius: 6px; }")
        rc_frame_layout = QHBoxLayout(rc_frame)
        rc_frame_layout.setContentsMargins(12, 8, 12, 8)
        rc_frame_layout.setSpacing(8)

        lbl_rc_icon = QLabel("\u26a0")
        lbl_rc_icon.setStyleSheet("font-size: 16px; color: #e0af68;")
        lbl_rc_icon.setToolTip("§13b UStG: Steuerschuldner ist der Leistungsempfaenger. Netto = Brutto.")
        rc_frame_layout.addWidget(lbl_rc_icon)

        self.chk_reverse_charge = QCheckBox("Reverse Charge (§13b UStG)")
        self.chk_reverse_charge.setStyleSheet("color: #c0caf5; font-size: 12px;")
        self.chk_reverse_charge.setToolTip(
            "§13b UStG: Steuerschuldner ist der Leistungsempfaenger.\n"
            "Wird automatisch angehakt wenn die KI einen entsprechenden Hinweis erkennt.\n"
            "Kann manuell uebersteuert werden."
        )
        rc_frame_layout.addWidget(self.chk_reverse_charge)
        rc_frame_layout.addStretch()
        layout.addWidget(rc_frame)

        self.extra_frame = QFrame()
        self.extra_frame.setStyleSheet("QFrame { background-color: #202233; border: 1px solid #414868; border-radius: 6px; }")
        extra_layout = QVBoxLayout(self.extra_frame)
        extra_layout.setContentsMargins(12, 12, 12, 12)
        extra_layout.setSpacing(8)

        self.extra_title = QLabel("Zusatzwerte aus dem Payload")
        self.extra_title.setStyleSheet("font-size: 13px; font-weight: bold; color: #7aa2f7;")
        extra_layout.addWidget(self.extra_title)

        self.extra_hint = QLabel("Hier erscheinen erkannte Werte, die noch keinen festen Platz in der Hauptmaske haben.")
        self.extra_hint.setWordWrap(True)
        self.extra_hint.setStyleSheet("font-size: 12px; color: #a9b1d6;")
        extra_layout.addWidget(self.extra_hint)

        self.extra_form_layout = QFormLayout()
        self.extra_form_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        self.extra_form_layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        self.extra_form_layout.setVerticalSpacing(6)
        extra_layout.addLayout(self.extra_form_layout)
        self.extra_frame.setVisible(False)
        layout.addWidget(self.extra_frame)
        layout.addStretch()

    def _show_shop_placeholder(self):
        """Zeigt den Logo-Frame mit Platzhalter-Inhalt (wird im 'direct'-Modus genutzt)."""
        px = create_placeholder_pixmap("?", 48, background="#202233", foreground="#414868", radius=8)
        self.lbl_shop_logo.setPixmap(px)
        self.lbl_shop_title.setText("Shop noch offen")
        self.lbl_shop_media_hint.setText("Platzhalterlogo aktiv, bis ein echtes Shoplogo gefunden wird")
        self.btn_logo_search.setText("Logo suchen")
        self.shop_preview_frame.setVisible(True)

    def _update_shop_preview(self, payload):
        payload = dict(payload or {}) if isinstance(payload, dict) else {}
        current_shop_name = str(self.inputs.get("shop_name").text() or "").strip() if "shop_name" in self.inputs else ""
        if current_shop_name and not str(payload.get("shop_name", "") or "").strip():
            payload["shop_name"] = current_shop_name

        label_text = str(payload.get("shop_name", "") or payload.get("_email_sender_domain", payload.get("sender_domain", "")) or "").strip()
        has_partial_context = any(
            str(payload.get(key, "") or "").strip()
            for key in ("shop_name", "_email_sender_domain", "sender_domain", "bestellnummer", "bestell_email")
        )
        if not label_text and has_partial_context:
            payload["shop_name"] = "Shop noch offen"
            logging.debug(
                "Shop-Vorschau bleibt trotz Teilkontext sichtbar: bestellnummer=%s",
                str(payload.get("bestellnummer", "") or "").strip(),
            )
        elif not label_text:
            if self._logo_search_mode == "direct":
                self._show_shop_placeholder()
            else:
                self.shop_preview_frame.setVisible(False)
            return

        preview = self._media_preview.resolve_shop_preview(payload)
        title_text = str(preview.get("label_text", payload.get("shop_name", "Shop noch offen")) or payload.get("shop_name", "Shop noch offen"))
        hint_text = str(preview.get("hint", "") or "")
        self.lbl_shop_logo.setPixmap(preview.get("pixmap"))
        self.lbl_shop_title.setText(title_text)
        self.lbl_shop_media_hint.setText(hint_text)
        tooltip_parts = [part for part in (title_text, hint_text) if str(part or "").strip()]
        self.shop_preview_frame.setToolTip("\n".join(tooltip_parts))
        self.btn_logo_search.setText("Logo aendern" if bool(preview.get("has_media")) else "Logo suchen")
        self.shop_preview_frame.setVisible(True)

    def set_shop_logo_path(self, file_path: str):
        """Setzt das Shop-Logo direkt aus einem lokalen Dateipfad.

        Wird vom LookupService aufgerufen wenn ein Logo in der DB gefunden wurde.
        Vermeidet damit einen API-Call.
        """
        import os
        path = str(file_path or "").strip()
        if not path or not os.path.exists(path):
            return
        try:
            px = QPixmap(path)
            if px.isNull():
                return
            preview_px = render_preview_pixmap(px, 48, background="#ffffff", radius=8, inset=2)
            self.lbl_shop_logo.setPixmap(preview_px)
            # Titel auf aktuellen Shop-Namen aktualisieren (falls im Eingabefeld vorhanden)
            current_shop_name = str(self.inputs["shop_name"].text() or "").strip() if "shop_name" in self.inputs else ""
            if current_shop_name and current_shop_name.lower() not in ("shop noch offen", ""):
                self.lbl_shop_title.setText(current_shop_name)
            self.lbl_shop_media_hint.setText("Logo aus DB geladen")
            self.btn_logo_search.setText("Logo aendern")
            self.shop_preview_frame.setVisible(True)
        except Exception:
            pass

    def _build_shop_search_context(self):
        payload = dict(self._payload_context or {}) if isinstance(self._payload_context, dict) else {}
        payload["shop_name"] = str(self.inputs.get("shop_name").text() or payload.get("shop_name", "") or "").strip() if "shop_name" in self.inputs else str(payload.get("shop_name", "") or "").strip()
        payload["bestell_email"] = str(self.inputs.get("bestell_email").text() or payload.get("bestell_email", "") or "").strip() if "bestell_email" in self.inputs else str(payload.get("bestell_email", "") or "").strip()
        sender_domain = str(payload.get("_email_sender_domain", payload.get("sender_domain", "")) or "").strip()
        canonical_shop_name = str(payload.get("shop_name", "") or "").strip()
        if canonical_shop_name.lower() == "shop noch offen":
            canonical_shop_name = ""
        return {
            "canonical_shop_name": canonical_shop_name,
            "shop_name": canonical_shop_name,
            "sender_domain": sender_domain,
            "bestell_email": str(payload.get("bestell_email", "") or "").strip(),
            "bestellnummer": str(payload.get("bestellnummer", "") or "").strip(),
            "raw_shop_name": str((self._payload_context or {}).get("shop_name", "") or "").strip() if isinstance(self._payload_context, dict) else "",
            "payload": payload,
            "source_module": "modul_order_entry",
        }

    def _request_logo_search(self):
        context = self._build_shop_search_context()
        if self._logo_search_mode == "direct":
            # Modul 1: reicht wenn shop_name im Eingabefeld steht
            if not str(context.get("canonical_shop_name", "") or "").strip():
                QMessageBox.information(self, "Logo-Suche", "Bitte zuerst einen Shop-Namen eintragen.")
                return
        else:
            # Modul 2 / Mail Scraper: braucht shop_name ODER sender_domain aus E-Mail-Kontext
            if not str(context.get("canonical_shop_name", "") or "").strip() and not str(context.get("sender_domain", "") or "").strip():
                QMessageBox.information(self, "Logo-Suche", "Es gibt noch nicht genug Shop-Kontext fuer eine sinnvolle Logosuche.")
                return
        self.logoSearchRequested.emit(context)

    def _clear_extra_rows(self):
        while self.extra_form_layout.count():
            item = self.extra_form_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self._extra_rows = []

    def set_payload(self, payload):
        payload = payload if isinstance(payload, dict) else {}
        self._payload_context = dict(payload)
        for key in EINKAUF_VISIBLE_FIELD_KEYS:
            self.inputs[key].setText(str(payload.get(key, "") or ""))
        self.chk_reverse_charge.setChecked(bool(payload.get("reverse_charge", False)))
        self.chk_rechnung_vorhanden.setChecked(bool(payload.get("rechnung_vorhanden", False)))

        auto_mapped = payload.get("_auto_mapped_fields") or {}
        for key, mapping_info in auto_mapped.items():
            if key in self.inputs and isinstance(mapping_info, dict):
                raw = str(mapping_info.get("raw", "") or "")
                resolved = str(mapping_info.get("resolved", "") or "")
                if raw and resolved:
                    self.inputs[key].set_mapping_change(raw, resolved)

        self._update_shop_preview(payload)
        self.set_extra_values(payload)

    def set_review_data(self, review_bundle):
        self._review_bundle = dict(review_bundle or {}) if isinstance(review_bundle, dict) else {}
        order_preview = review_bundle.get("order_preview", {}) if isinstance(review_bundle, dict) else {}
        if not order_preview.get("order_exists"):
            self.clear_review_data()
            return
        head_changes = {}
        for change in order_preview.get("head_changes", []) or []:
            key = str(change.get("field_key", "") or "")
            if key:
                head_changes[key] = dict(change)
        for key in EINKAUF_VISIBLE_FIELD_KEYS:
            change = head_changes.get(key)
            kind = str((change or {}).get("change_kind", "") or "").lower()
            if kind in ("add", "overwrite"):
                self.inputs[key].set_review_change(change.get("old_value"), change.get("new_value"), kind)
            else:
                self.inputs[key].clear_review_change()

    def clear_review_data(self):
        for key in EINKAUF_VISIBLE_FIELD_KEYS:
            self.inputs[key].clear_review_change()

    def set_extra_values(self, payload):
        self._clear_extra_rows()
        extra_rows = collect_einkauf_extra_fields(payload)
        if not extra_rows:
            self.extra_frame.setVisible(False)
            return

        for label_text, value_text in extra_rows:
            line_edit = QLineEdit()
            line_edit.setReadOnly(True)
            line_edit.setText(value_text)
            line_edit.setStyleSheet("QLineEdit { background-color: #171824; border: 1px solid #414868; border-radius: 4px; padding: 6px; color: #a9b1d6; }")
            self.extra_form_layout.addRow(QLabel(label_text + ":"), line_edit)
            self._extra_rows.append((label_text, line_edit))
        self.extra_frame.setVisible(True)

    def apply_to_payload(self, payload):
        updated = dict(payload or {})
        for key in EINKAUF_VISIBLE_FIELD_KEYS:
            updated[key] = self.inputs[key].text().strip()
        updated["reverse_charge"] = self.chk_reverse_charge.isChecked()
        updated["rechnung_vorhanden"] = self.chk_rechnung_vorhanden.isChecked()
        return updated

    def clear_values(self):
        self._payload_context = {}
        for key in EINKAUF_VISIBLE_FIELD_KEYS:
            self.inputs[key].clear()
        self.chk_reverse_charge.setChecked(False)
        self.chk_rechnung_vorhanden.setChecked(False)
        self._clear_extra_rows()
        self.extra_frame.setVisible(False)
        if self._logo_search_mode == "direct":
            self._show_shop_placeholder()
        else:
            self.shop_preview_frame.setVisible(False)

class ReviewSelectableLineEdit(QLineEdit):
    clicked = pyqtSignal()
    valueCommitted = pyqtSignal(str)

    def __init__(self, editable=True, parent=None):
        super().__init__(parent)
        self._editable_value = bool(editable)
        self._last_committed = ""
        self.setReadOnly(True)

    def set_committed_text(self, text):
        self._last_committed = str(text or "")
        self.setText(self._last_committed)
        self.setReadOnly(True)

    def mousePressEvent(self, event):
        self.clicked.emit()
        super().mousePressEvent(event)

    def mouseDoubleClickEvent(self, event):
        self.clicked.emit()
        if self._editable_value:
            self.setReadOnly(False)
            self.setFocus(Qt.FocusReason.MouseFocusReason)
            self.selectAll()
        super().mouseDoubleClickEvent(event)

    def keyPressEvent(self, event):
        if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            self._finish_edit(commit=True)
            event.accept()
            return
        if event.key() == Qt.Key.Key_Escape:
            self.setText(self._last_committed)
            self.setReadOnly(True)
            event.accept()
            return
        super().keyPressEvent(event)

    def focusOutEvent(self, event):
        self._finish_edit(commit=True)
        super().focusOutEvent(event)

    def _finish_edit(self, commit=True):
        if self.isReadOnly():
            return
        text = str(self.text() or "").strip()
        self.setReadOnly(True)
        if commit:
            self._last_committed = text
            self.setText(self._last_committed)
            self.valueCommitted.emit(self._last_committed)
        else:
            self.setText(self._last_committed)


class ItemFieldChoiceWidget(QFrame):
    selectionChanged = pyqtSignal(object)

    def __init__(self, field_data, parent=None):
        super().__init__(parent)
        self._field_data = dict(field_data or {})
        self._selected_side = str(self._field_data.get("selected_side", "new") or "new")
        self._build_ui()
        self._apply_styles()

    def _build_ui(self):
        self.setFrameShape(QFrame.Shape.NoFrame)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        header = QHBoxLayout()
        self.lbl_field = QLabel(str(self._field_data.get("field_label", self._field_data.get("field_key", "Wert")) or "Wert"))
        self.lbl_field.setStyleSheet("font-size: 12px; font-weight: bold; color: #c0caf5;")
        header.addWidget(self.lbl_field)
        self.lbl_kind = QLabel(change_kind_label(self._field_data.get("change_kind", "unchanged")))
        self.lbl_kind.setStyleSheet("font-size: 11px; color: #a9b1d6;")
        header.addStretch(1)
        header.addWidget(self.lbl_kind)
        layout.addLayout(header)

        value_row = QHBoxLayout()
        value_row.setSpacing(8)

        self.btn_old = QPushButton(format_review_value(self._field_data.get("old_value")))
        self.btn_old.setMinimumHeight(34)
        self.btn_old.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_old.clicked.connect(self._select_old)
        self.btn_old.setEnabled(True)
        self.btn_old.setToolTip("Klick: alten Wert behalten")
        value_row.addWidget(self.btn_old, 1)

        arrow = QLabel("->")
        arrow.setStyleSheet("font-size: 13px; color: #7aa2f7;")
        arrow.setAlignment(Qt.AlignmentFlag.AlignCenter)
        value_row.addWidget(arrow)

        self.edit_new = ReviewSelectableLineEdit(editable=bool(self._field_data.get("editable", True)), parent=self)
        self.edit_new.set_committed_text(format_review_value(self._field_data.get("new_value")))
        self.edit_new.setMinimumHeight(34)
        self.edit_new.setPlaceholderText("Neuer Wert")
        self.edit_new.clicked.connect(self._select_new)
        self.edit_new.valueCommitted.connect(self._commit_new_value)
        self.edit_new.setToolTip("Klick: neuen Wert uebernehmen | Doppelklick: neuen Wert direkt bearbeiten")
        value_row.addWidget(self.edit_new, 1)

        layout.addLayout(value_row)

    def _select_old(self):
        if not self.btn_old.isEnabled():
            return
        self._selected_side = "old"
        self._emit_change()

    def _select_new(self):
        self._selected_side = "new"
        self._emit_change()

    def _commit_new_value(self, text):
        self._field_data["new_value"] = text
        self._selected_side = "new"
        self._emit_change()

    def _emit_change(self):
        self._apply_styles()
        payload = dict(self._field_data)
        payload["selected_side"] = self._selected_side
        payload["new_value"] = str(self.edit_new.text() or "").strip()
    def _apply_styles(self):
        kind = str(self._field_data.get("change_kind", "unchanged") or "unchanged")
        info = FIELD_KIND_STYLES.get(kind, FIELD_KIND_STYLES["unchanged"])
        accent = info.get("accent", info.get("fg", "#7aa2f7"))
        old_border = f"2px solid {accent}" if self._selected_side == "old" else "1px solid #414868"
        new_border = f"2px solid {accent}" if self._selected_side == "new" else "1px solid #414868"
        old_bg = info["bg"] if self._selected_side == "old" else "#171824"
        old_fg = info["fg"] if self._selected_side == "old" else "#c0caf5"
        self.btn_old.setStyleSheet(
            "QPushButton {"
            f"background-color: {old_bg}; color: {old_fg}; border: {old_border}; border-radius: 6px; padding: 6px; text-align: left;"
            "}"
            "QPushButton:disabled { color: #6b7280; border-color: #30374d; }"
        )
        self.edit_new.setStyleSheet(
            "QLineEdit {"
            f"background-color: {info['bg']}; color: {info['fg']}; border: {new_border}; border-radius: 6px; padding: 6px;"
            "}"
        )



class OrderItemImageCandidateWidget(QFrame):
    actionTriggered = pyqtSignal(object)

    def __init__(self, candidate, resolver, parent=None):
        super().__init__(parent)
        self._candidate = dict(candidate or {})
        self._resolver = resolver
        self._build_ui()

    def _build_ui(self):
        status_key = str(self._candidate.get("status_key", "candidate") or "candidate")
        info = IMAGE_DECISION_STYLES.get(status_key, IMAGE_DECISION_STYLES["candidate"])
        self.setStyleSheet(
            "QFrame {"
            f"background-color: {info['bg']}; border: 1px solid {info['border']}; border-radius: 8px;"
            "}"
        )
        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        preview = self._resolver.resolve_candidate_preview(self._candidate, size=72)
        lbl_preview = QLabel(self)
        lbl_preview.setPixmap(preview.get("pixmap"))
        lbl_preview.setFixedSize(72, 72)
        lbl_preview.setToolTip(preview.get("tooltip", ""))
        layout.addWidget(lbl_preview)

        text_col = QVBoxLayout()
        text_col.setSpacing(4)
        lbl_status = QLabel(str(self._candidate.get("status_label", "Kandidat") or "Kandidat"))
        lbl_status.setStyleSheet(f"font-size: 12px; font-weight: bold; color: {info['fg']};")
        text_col.addWidget(lbl_status)

        source_line = str(self._candidate.get("source_label", "Bildquelle") or "Bildquelle")
        if str(self._candidate.get("source_ref", "") or "").strip():
            source_line += f" | {str(self._candidate.get('source_ref', '') or '').strip()}"
        lbl_source = QLabel(source_line)
        lbl_source.setWordWrap(True)
        lbl_source.setStyleSheet("font-size: 12px; color: #c0caf5;")
        text_col.addWidget(lbl_source)

        detail_parts = []
        if str(self._candidate.get("storage_kind", "") or "").strip() == "remote_url" and str(self._candidate.get("source_url", "") or "").strip():
            detail_parts.append("Remote-Referenz")
        elif str(self._candidate.get("path", "") or "").strip():
            detail_parts.append(os.path.basename(str(self._candidate.get("path", "") or "")))
        if self._candidate.get("attached_to_order_item"):
            detail_parts.append("Position zugeordnet")
        lbl_detail = QLabel(" | ".join(part for part in detail_parts if part) or "Kein weiterer Hinweis")
        lbl_detail.setWordWrap(True)
        lbl_detail.setStyleSheet("font-size: 11px; color: #9aa4c5;")
        text_col.addWidget(lbl_detail)
        layout.addLayout(text_col, 1)

        actions = QVBoxLayout()
        actions.setSpacing(6)
        media_asset_id = int(self._candidate.get("media_asset_id", 0) or 0)
        is_selected = bool(self._candidate.get("is_selected")) and not bool(self._candidate.get("is_rejected"))
        is_rejected = bool(self._candidate.get("is_rejected"))
        is_attached = bool(self._candidate.get("attached_to_order_item"))

        if media_asset_id > 0 and not is_attached:
            btn_map = QPushButton("Mappen")
            btn_map.clicked.connect(lambda _checked=False: self._emit_action("map"))
            actions.addWidget(btn_map)

        if media_asset_id > 0 and not is_selected and not is_rejected:
            btn_select = QPushButton("Als primaer setzen")
            btn_select.clicked.connect(lambda _checked=False: self._emit_action("select"))
            actions.addWidget(btn_select)
        elif is_selected:
            btn_active = QPushButton("Aktiv")
            btn_active.setEnabled(False)
            actions.addWidget(btn_active)

        if media_asset_id > 0 and not is_rejected:
            btn_reject = QPushButton("Verwerfen")
            btn_reject.clicked.connect(lambda _checked=False: self._emit_action("reject"))
            actions.addWidget(btn_reject)

        actions.addStretch(1)
        layout.addLayout(actions)

    def _emit_action(self, action):
        self.actionTriggered.emit({"action": str(action or "").strip(), "candidate": dict(self._candidate)})


class OrderItemImageManagerWidget(QFrame):
    stateChanged = pyqtSignal(object)
    searchRequested = pyqtSignal(object)

    def __init__(self, resolver, item, review_row=None, payload=None, draft_state=None, parent=None):
        super().__init__(parent)
        self._resolver = resolver
        self._media_service = resolver.get_media_service() if resolver is not None else None
        self._item = dict(item or {})
        self._review_row = dict(review_row or {})
        self._payload = dict(payload or {}) if isinstance(payload, dict) else {}
        self._source_row_index = int(self._review_row.get("source_row_index", -1) or -1)
        self._order_item_id = int(self._review_row.get("match_position_id", 0) or 0)
        self._persist_immediately = bool(self._order_item_id and str(self._review_row.get("match_kind", "") or "") == "matched")
        self._draft_state = self._normalize_state(draft_state)
        self._base_result = {}
        self._build_ui()
        self._reload_candidates()

    def _normalize_state(self, state):
        state = dict(state or {}) if isinstance(state, dict) else {}
        return {
            "candidate_asset_ids": [int(value) for value in list(state.get("candidate_asset_ids", []) or []) if str(value).strip().isdigit()],
            "rejected_asset_ids": [int(value) for value in list(state.get("rejected_asset_ids", []) or []) if str(value).strip().isdigit()],
            "selected_asset_id": int(state.get("selected_asset_id", 0) or 0),
            "selected_mode": str(state.get("selected_mode", "manual") or "manual"),
            "local_candidates": [dict(row or {}) for row in list(state.get("local_candidates", []) or []) if isinstance(row, dict)],
        }

    def export_state(self):
        return {
            "candidate_asset_ids": sorted({int(value) for value in self._draft_state.get("candidate_asset_ids", []) if int(value or 0) > 0}),
            "rejected_asset_ids": sorted({int(value) for value in self._draft_state.get("rejected_asset_ids", []) if int(value or 0) > 0}),
            "selected_asset_id": int(self._draft_state.get("selected_asset_id", 0) or 0),
            "selected_mode": str(self._draft_state.get("selected_mode", "manual") or "manual"),
            "local_candidates": [dict(row or {}) for row in self._draft_state.get("local_candidates", []) if isinstance(row, dict)],
        }

    def _emit_state(self):
        self.stateChanged.emit({"source_row_index": self._source_row_index, "state": self.export_state()})

    def _build_ui(self):
        self.setStyleSheet("QFrame { background-color: #171824; border: 1px solid #30374d; border-radius: 8px; }")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        head_row = QHBoxLayout()
        lbl_title = QLabel("Bildpflege fuer diese Position")
        lbl_title.setStyleSheet("font-size: 13px; font-weight: bold; color: #7aa2f7;")
        head_row.addWidget(lbl_title)
        head_row.addStretch(1)
        self.btn_add_image = QPushButton("Bild hinzufuegen")
        self.btn_add_image.clicked.connect(self._add_manual_image)
        head_row.addWidget(self.btn_add_image)
        self.btn_web_search = QPushButton("Im Web suchen")
        self.btn_web_search.clicked.connect(self._request_web_search)
        head_row.addWidget(self.btn_web_search)
        layout.addLayout(head_row)

        self.lbl_mode = QLabel("")
        self.lbl_mode.setWordWrap(True)
        self.lbl_mode.setStyleSheet("font-size: 11px; color: #9aa4c5;")
        layout.addWidget(self.lbl_mode)

        selected_row = QHBoxLayout()
        selected_row.setSpacing(10)
        self.lbl_selected_preview = QLabel(self)
        self.lbl_selected_preview.setFixedSize(78, 78)
        selected_row.addWidget(self.lbl_selected_preview)

        selected_text = QVBoxLayout()
        selected_text.setSpacing(4)
        self.lbl_selected_title = QLabel("Noch kein Bild ausgewaehlt")
        self.lbl_selected_title.setStyleSheet("font-size: 12px; font-weight: bold; color: #c0caf5;")
        selected_text.addWidget(self.lbl_selected_title)
        self.lbl_selected_meta = QLabel("Wenn ein Kandidat vorhanden ist, kannst du ihn hier direkt uebernehmen oder verwerfen.")
        self.lbl_selected_meta.setWordWrap(True)
        self.lbl_selected_meta.setStyleSheet("font-size: 11px; color: #9aa4c5;")
        selected_text.addWidget(self.lbl_selected_meta)
        selected_row.addLayout(selected_text, 1)
        layout.addLayout(selected_row)

        self.candidates_host = QVBoxLayout()
        self.candidates_host.setContentsMargins(0, 0, 0, 0)
        self.candidates_host.setSpacing(8)
        layout.addLayout(self.candidates_host)

    def _clear_candidate_widgets(self):
        while self.candidates_host.count():
            item = self.candidates_host.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()

    def _merge_candidates(self):
        merged = []
        seen = set()
        module1_ai_cropping_disabled = str((self._payload or {}).get("_origin_module", "") or "") == "modul_order_entry"
        for candidate in list((self._base_result or {}).get("candidates", []) or []) + list(self._draft_state.get("local_candidates", []) or []):
            if not isinstance(candidate, dict):
                continue
            if module1_ai_cropping_disabled and str(candidate.get("source_type", "") or "").strip() == "screenshot_detection_crop":
                continue
            key = (
                int(candidate.get("media_asset_id", 0) or 0),
                str(candidate.get("path", "") or "").strip(),
                str(candidate.get("source_url", "") or "").strip(),
            )
            if key in seen:
                continue
            seen.add(key)
            prepared = dict(candidate)
            asset_id = int(prepared.get("media_asset_id", 0) or 0)
            if asset_id and asset_id in self._draft_state.get("rejected_asset_ids", []):
                prepared["is_rejected"] = True
                prepared["status_key"] = "rejected"
                prepared["status_label"] = "Verworfen"
            elif asset_id and asset_id == int(self._draft_state.get("selected_asset_id", 0) or 0):
                prepared["is_selected"] = True
                prepared["is_rejected"] = False
                prepared["selection_mode"] = str(self._draft_state.get("selected_mode", "manual") or "manual")
                prepared["status_key"] = "selected_manual"
                prepared["status_label"] = "Manuell gesetzt"
            merged.append(prepared)
        return merged

    def _selected_candidate_from(self, candidates):
        module1_ai_cropping_disabled = str((self._payload or {}).get("_origin_module", "") or "") == "modul_order_entry"
        selected_asset_id = int(self._draft_state.get("selected_asset_id", 0) or 0)
        if selected_asset_id > 0:
            for candidate in candidates:
                if int(candidate.get("media_asset_id", 0) or 0) == selected_asset_id and not bool(candidate.get("is_rejected")):
                    return candidate
        selected = (self._base_result or {}).get("selected")
        if isinstance(selected, dict):
            if not module1_ai_cropping_disabled or str(selected.get("source_type", "") or "").strip() != "screenshot_detection_crop":
                return dict(selected)
        fallback = (self._base_result or {}).get("fallback_global")
        if isinstance(fallback, dict):
            if not module1_ai_cropping_disabled or str(fallback.get("source_type", "") or "").strip() != "screenshot_detection_crop":
                return dict(fallback)
        return candidates[0] if candidates else None

    def _set_selected_preview(self, candidate):
        preview = self._resolver.resolve_candidate_preview(candidate or {}, size=78)
        self.lbl_selected_preview.setPixmap(preview.get("pixmap"))
        if isinstance(candidate, dict):
            self.lbl_selected_title.setText(str(candidate.get("status_label", "Kandidat") or "Kandidat"))
            self.lbl_selected_meta.setText(
                (str(candidate.get("source_label", "") or "Bildquelle") + (f" | {str(candidate.get('source_ref', '') or '').strip()}" if str(candidate.get("source_ref", "") or "").strip() else ""))
            )
        else:
            self.lbl_selected_title.setText("Noch kein Bild ausgewaehlt")
            self.lbl_selected_meta.setText("Fuer diese Position ist aktuell nur der neutrale Fallback sichtbar.")

    def _reload_candidates(self):
        if self._media_service is None or self._source_row_index < 0:
            self._base_result = {"candidates": [], "selected": None, "fallback_global": None}
        else:
            try:
                self._base_result = self._media_service.get_payload_item_image_candidates(
                    payload=self._payload,
                    source_row_index=self._source_row_index,
                    review_row=self._review_row,
                    fallback_to_product=True,
                    source_module="wizard_image_review",
                )
            except Exception as exc:
                logging.warning("Bildkandidaten fuer Wizard-Zeile konnten nicht geladen werden: %s", exc)
                self._base_result = {"candidates": [], "selected": None, "fallback_global": None}
        self._render_candidates()

    def _render_candidates(self):
        self._clear_candidate_widgets()
        mode_text = "Bildentscheidungen werden sofort auf die bestehende Position geschrieben." if self._persist_immediately else "Bildentscheidungen werden hier vorgemerkt und nach dem Speichern an die echte Position gehangt."
        self.lbl_mode.setText(mode_text)
        merged = self._merge_candidates()
        selected = self._selected_candidate_from(merged)
        self._set_selected_preview(selected)

        if not merged:
            lbl_empty = QLabel("Noch kein Bildkandidat vorhanden. Du kannst ein lokales Bild hinzufuegen oder spaeter mit globalem Produktbild weiterarbeiten.")
            lbl_empty.setWordWrap(True)
            lbl_empty.setStyleSheet("font-size: 11px; color: #9aa4c5;")
            self.candidates_host.addWidget(lbl_empty)
            return

        for candidate in merged:
            card = OrderItemImageCandidateWidget(candidate, self._resolver, self)
            card.actionTriggered.connect(self._handle_candidate_action)
            self.candidates_host.addWidget(card)

    def _remember_local_candidate(self, candidate):
        if not isinstance(candidate, dict):
            return
        asset_id = int(candidate.get("media_asset_id", 0) or 0)
        existing = []
        seen = False
        for row in list(self._draft_state.get("local_candidates", []) or []):
            if not isinstance(row, dict):
                continue
            if asset_id > 0 and int(row.get("media_asset_id", 0) or 0) == asset_id:
                existing.append(dict(candidate))
                seen = True
            else:
                existing.append(dict(row))
        if not seen:
            existing.append(dict(candidate))
        self._draft_state["local_candidates"] = existing
        if asset_id > 0 and asset_id not in self._draft_state["candidate_asset_ids"]:
            self._draft_state["candidate_asset_ids"].append(asset_id)

    def _handle_candidate_action(self, payload):
        action = str((payload or {}).get("action", "") or "").strip()
        candidate = dict((payload or {}).get("candidate") or {})
        asset_id = int(candidate.get("media_asset_id", 0) or 0)
        if asset_id <= 0:
            return

        try:
            if action == "map":
                if self._persist_immediately and self._order_item_id > 0 and self._media_service is not None:
                    self._media_service.register_order_item_image_candidate(
                        order_item_id=self._order_item_id,
                        media_asset_id=asset_id,
                        source_type="wizard_map",
                        source_ref=f"row:{self._source_row_index}",
                        metadata={"source_row_index": self._source_row_index},
                        auto_select=False,
                        replace_existing_auto=False,
                    )
                else:
                    self._remember_local_candidate(candidate)
                    if asset_id in self._draft_state["rejected_asset_ids"]:
                        self._draft_state["rejected_asset_ids"] = [value for value in self._draft_state["rejected_asset_ids"] if value != asset_id]
                    self._emit_state()
                self._reload_candidates()
                return

            if action == "select":
                if self._persist_immediately and self._order_item_id > 0 and self._media_service is not None:
                    if not bool(candidate.get("attached_to_order_item")):
                        self._media_service.register_order_item_image_candidate(
                            order_item_id=self._order_item_id,
                            media_asset_id=asset_id,
                            source_type="wizard_map",
                            source_ref=f"row:{self._source_row_index}",
                            metadata={"source_row_index": self._source_row_index},
                            auto_select=False,
                            replace_existing_auto=False,
                        )
                    self._media_service.set_manual_order_item_image(
                        order_item_id=self._order_item_id,
                        media_asset_id=asset_id,
                        source_type="wizard_manual_selection",
                        source_ref=f"row:{self._source_row_index}",
                        metadata={"source_row_index": self._source_row_index},
                    )
                else:
                    self._remember_local_candidate(candidate)
                    self._draft_state["selected_asset_id"] = asset_id
                    self._draft_state["selected_mode"] = "manual"
                    self._draft_state["rejected_asset_ids"] = [value for value in self._draft_state["rejected_asset_ids"] if value != asset_id]
                    self._emit_state()
                self._reload_candidates()
                return

            if action == "reject":
                if self._persist_immediately and self._order_item_id > 0 and self._media_service is not None:
                    self._media_service.reject_order_item_image(
                        order_item_id=self._order_item_id,
                        media_asset_id=asset_id,
                        selection_mode="manual",
                        source_type="wizard_reject",
                        source_ref=f"row:{self._source_row_index}",
                        metadata={"source_row_index": self._source_row_index},
                    )
                else:
                    self._remember_local_candidate(candidate)
                    if asset_id not in self._draft_state["rejected_asset_ids"]:
                        self._draft_state["rejected_asset_ids"].append(asset_id)
                    if int(self._draft_state.get("selected_asset_id", 0) or 0) == asset_id:
                        self._draft_state["selected_asset_id"] = 0
                    self._emit_state()
                self._reload_candidates()
        except Exception as exc:
            logging.warning("Bildaktion im Wizard fehlgeschlagen: %s", exc)
            QMessageBox.warning(self, "Bildpflege", f"Die Bildaktion konnte gerade nicht ausgefuehrt werden:\n{exc}")

    def _build_search_context(self):
        return {
            "source_row_index": int(self._source_row_index),
            "row": int(self._source_row_index),
            "produkt_name": str(self._item.get("produkt_name", "") or "").strip(),
            "varianten_info": str(self._item.get("varianten_info", self._item.get("variant_text", "")) or "").strip(),
            "ean": str(self._item.get("ean", "") or "").strip(),
            "source_module": "wizard_image_review",
        }

    def _request_web_search(self):
        context = self._build_search_context()
        if not str(context.get("produkt_name", "") or "").strip():
            QMessageBox.information(self, "Bildpflege", "Fuer diese Zeile ist noch kein Produktname vorhanden.")
            return
        self.searchRequested.emit(context)

    def _add_manual_image(self):
        if self._media_service is None:
            QMessageBox.information(self, "Bildpflege", "Die Medienverwaltung ist in diesem Moment nicht verfuegbar.")
            return
        start_dir = self._settings_manager.get_last_dir("upload_bild") if self._settings_manager else os.path.expanduser("~")
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Lokales Bild fuer diese Position auswaehlen",
            start_dir,
            "Bilder (*.png *.jpg *.jpeg *.bmp *.webp *.gif);;Alle Dateien (*.*)"
        )
        if not file_path:
            return
        if self._settings_manager:
            self._settings_manager.set_last_dir("upload_bild", file_path)
        try:
            candidate = self._media_service.register_manual_item_candidate_from_file(
                product_name=str(self._item.get("produkt_name", "") or "").strip(),
                file_path=file_path,
                ean=str(self._item.get("ean", "") or "").strip(),
                variant_text=str(self._item.get("varianten_info", "") or "").strip(),
                order_item_id=self._order_item_id if self._persist_immediately else None,
                source_module="wizard_image_review",
                source_ref=file_path,
                metadata={"source_row_index": self._source_row_index},
            )
        except Exception as exc:
            logging.warning("Manuelles Bild konnte im Wizard nicht registriert werden: %s", exc)
            QMessageBox.warning(self, "Bildpflege", f"Das Bild konnte nicht uebernommen werden:\n{exc}")
            return

        if isinstance(candidate, dict):
            self._remember_local_candidate(candidate)
            asset_id = int(candidate.get("media_asset_id", 0) or 0)
            if not self._persist_immediately and asset_id > 0:
                self._draft_state["selected_asset_id"] = asset_id
                self._draft_state["selected_mode"] = "manual"
                self._draft_state["rejected_asset_ids"] = [value for value in self._draft_state["rejected_asset_ids"] if value != asset_id]
                self._emit_state()
        self._reload_candidates()

class ItemReviewDetailWidget(QFrame):
    fieldDecisionChanged = pyqtSignal(object)

    def __init__(self, row_review, fields, image_widget=None, parent=None):
        super().__init__(parent)
        self._row_review = dict(row_review or {})
        self._fields = [dict(field or {}) for field in (fields or [])]
        self._image_widget = image_widget
        self._build_ui()

    def _build_ui(self):
        self.setStyleSheet("QFrame { background-color: #202233; border: 1px solid #414868; border-radius: 8px; }")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        hint = str(self._row_review.get("hint", "") or "").strip()
        if hint:
            lbl_hint = QLabel(hint)
            lbl_hint.setWordWrap(True)
            lbl_hint.setStyleSheet("font-size: 12px; color: #a9b1d6;")
            layout.addWidget(lbl_hint)

        candidates = self._row_review.get("match_candidates", []) or []
        if len(candidates) > 1:
            pieces = []
            for candidate in candidates[:3]:
                position_id = candidate.get("position_id")
                if not position_id:
                    continue
                pieces.append(f"Pos {position_id}")
            if pieces:
                lbl_candidates = QLabel("Moegliche Zuordnung: " + ", ".join(pieces))
                lbl_candidates.setWordWrap(True)
                lbl_candidates.setStyleSheet("font-size: 12px; color: #ff9e64;")
                layout.addWidget(lbl_candidates)

        if not self._fields:
            lbl_empty = QLabel("Hier gibt es aktuell keine relevanten Feldunterschiede. Die Zeile ist nur zur Einordnung sichtbar.")
            lbl_empty.setWordWrap(True)
            lbl_empty.setStyleSheet("font-size: 12px; color: #a9b1d6;")
            layout.addWidget(lbl_empty)
        else:
            for field in self._fields:
                widget = ItemFieldChoiceWidget(field, self)
                widget.selectionChanged.connect(self._forward_change)
                layout.addWidget(widget)

        if self._image_widget is not None:
            layout.addWidget(self._image_widget)

    def _forward_change(self, payload):
        result = dict(payload or {})
        result["source_row_index"] = int(self._row_review.get("source_row_index", -1) or -1)
        self.fieldDecisionChanged.emit(result)

class EinkaufItemsTableWidget(QWidget):
    imageSearchRequested = pyqtSignal(object)
    eanLookupRequested = pyqtSignal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._items = []
        self._review_rows = {}
        self._expanded_rows = {}
        self._visual_rows = []
        self._source_main_rows = {}
        self._ignore_table_changes = False
        self._media_preview = _UiMediaPreviewResolver(getattr(parent, "settings_manager", None))
        self._payload_context = {}
        self._review_bundle = {}
        self._draft_image_states = {}
        self._pending_visual_refresh = False
        self._visual_refresh_timer = QTimer(self)
        self._visual_refresh_timer.setSingleShot(True)
        self._visual_refresh_timer.timeout.connect(self._perform_visual_refresh)
        OrderVisualState.bus().visualsInvalidated.connect(self._on_visuals_invalidated)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self.lbl_hint = QLabel("Die Haupttabelle bleibt kompakt. Bei Aenderungen kannst du pro Zeile Details aufklappen.")
        self.lbl_hint.setWordWrap(True)
        self.lbl_hint.setStyleSheet("font-size: 12px; color: #a9b1d6;")
        layout.addWidget(self.lbl_hint)

        self.table = QTableWidget()
        self.table.setColumnCount(len(EINKAUF_ITEM_MAIN_COLUMNS))
        self.table.setHorizontalHeaderLabels([label for _key, label in EINKAUF_ITEM_MAIN_COLUMNS])
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(
            QAbstractItemView.EditTrigger.DoubleClicked
            | QAbstractItemView.EditTrigger.SelectedClicked
            | QAbstractItemView.EditTrigger.EditKeyPressed
        )
        self.table.verticalHeader().setVisible(False)
        self.table.verticalHeader().setDefaultSectionSize(52)
        self.table.setStyleSheet(
            "QTableWidget { background-color: #171824; border: 1px solid #414868; border-radius: 6px; gridline-color: #414868; }"
            "QHeaderView::section { background-color: #1f2335; color: #7aa2f7; font-weight: bold; padding: 5px; border: 1px solid #414868; }"
        )
        self.table.setIconSize(QSize(36, 36))
        self.table.setSizeAdjustPolicy(QAbstractScrollArea.SizeAdjustPolicy.AdjustIgnored)
        self.table.itemChanged.connect(self._on_table_item_changed)
        layout.addWidget(self.table)

        header = self.table.horizontalHeader()
        header.setMinimumSectionSize(48)
        header.setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self.table.setWordWrap(False)
        self.table.setTextElideMode(Qt.TextElideMode.ElideRight)
        self.table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.table.setColumnWidth(0, 64)
        self.table.setColumnWidth(1, 96)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.table.setColumnWidth(3, 150)
        self.table.setColumnWidth(4, 92)
        self.table.setColumnWidth(5, 76)
        self.table.setColumnWidth(6, 84)
        self.table.setColumnWidth(7, 110)
        self.table.setColumnWidth(8, 112)
        self.table.setColumnWidth(9, 104)

        btn_row = QHBoxLayout()
        self.btn_ean_lookup = QPushButton("EAN suchen (markierte Zeile)")
        self.btn_ean_lookup.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_ean_lookup.setStyleSheet("font-size: 12px; padding: 4px 10px;")
        self.btn_ean_lookup.clicked.connect(self._on_ean_lookup_clicked)
        btn_row.addWidget(self.btn_ean_lookup)
        btn_row.addStretch()
        layout.addLayout(btn_row)

    def _on_ean_lookup_clicked(self):
        context = self.get_selected_context()
        if context:
            self.eanLookupRequested.emit(context)

    def add_empty_row(self):
        """Fuegt eine leere Artikelzeile hinzu (fuer manuelle Eingabe in Modul 1)."""
        self._items.append({
            "produkt_name": "",
            "varianten_info": "",
            "ean": "",
            "menge": 1,
            "ekp_brutto": 0.0,
        })
        self._rebuild_table(select_source_index=len(self._items) - 1)

    def delete_selected_rows(self):
        """Loescht die aktuell markierte Zeile aus der Artikelliste."""
        visual_row = self.table.currentRow()
        info = self._visual_row_info(visual_row)
        if not info:
            return
        source_index = info.get("source_row_index", -1)
        if 0 <= source_index < len(self._items):
            del self._items[source_index]
            self._review_rows.pop(source_index, None)
            self._expanded_rows.pop(source_index, None)
            self._draft_image_states.pop(source_index, None)
            # Reindex remaining review/expanded rows
            self._review_rows = {
                (k if k < source_index else k - 1): v
                for k, v in self._review_rows.items()
            }
            self._expanded_rows = {
                (k if k < source_index else k - 1): v
                for k, v in self._expanded_rows.items()
            }
            self._draft_image_states = {
                (k if k < source_index else k - 1): v
                for k, v in self._draft_image_states.items()
            }
            self._rebuild_table()

    def clear_items(self):
        self._items = []
        self._review_rows = {}
        self._expanded_rows = {}
        self._payload_context = {}
        self._review_bundle = {}
        self._draft_image_states = {}
        self._rebuild_table()

    def clear_review_data(self):
        self._review_rows = {}
        self._expanded_rows = {}
        self._review_bundle = {}
        self.lbl_hint.setText("Die Haupttabelle bleibt kompakt. Bei Aenderungen kannst du pro Zeile Details aufklappen.")
        self._rebuild_table()

    def set_payload_context(self, payload):
        self._payload_context = dict(payload or {}) if isinstance(payload, dict) else {}
        self._payload_context["waren"] = self.get_items()

    def set_items(self, items, ean_fill_callback=None, payload=None):
        self._items = []
        self._review_rows = {}
        self._expanded_rows = {}
        self._draft_image_states = {}
        self._payload_context = dict(payload or {}) if isinstance(payload, dict) else {}

        for raw_item in (items if isinstance(items, list) else []):
            item = dict(raw_item or {})
            produkt_name = str(item.get("produkt_name", "") or "").strip()
            varianten_info = str(item.get("varianten_info", "") or "").strip()
            ean = str(item.get("ean", "") or "").strip()
            if not ean and callable(ean_fill_callback) and produkt_name:
                ean = str(ean_fill_callback(produkt_name, varianten_info) or "").strip()
                if ean:
                    item["ean"] = ean

            normalized = dict(item)
            for key in EINKAUF_ITEM_REVIEW_KEYS:
                normalized[key] = _coerce_item_value(key, item.get(key, ""))
            normalized["menge"] = normalized.get("menge") or "1"
            self._items.append(normalized)

        self._payload_context["waren"] = self.get_items()
        self._rebuild_table()

    def set_review_data(self, review_bundle):
        self._review_bundle = dict(review_bundle or {}) if isinstance(review_bundle, dict) else {}
        order_preview = review_bundle.get("order_preview", {}) if isinstance(review_bundle, dict) else {}
        if not order_preview.get("order_exists"):
            self.clear_review_data()
            return

        self._review_rows = {}
        for row_review in order_preview.get("item_rows", []) or []:
            if not isinstance(row_review, dict):
                continue
            source_index = int(row_review.get("source_row_index", -1) or -1)
            if source_index < 0:
                continue
            prepared = dict(row_review)
            prepared["fields"] = [dict(field or {}) for field in row_review.get("fields", []) or []]
            for field in prepared["fields"]:
                field["selected_side"] = str(field.get("selected_side", "new") or "new")
                field["new_value"] = _coerce_item_value(field.get("field_key", ""), field.get("new_value", ""))
            self._recalculate_review_row(prepared)
            self._review_rows[source_index] = prepared

        summary = order_preview.get("item_summary", {}) or {}
        self.lbl_hint.setText(
            f"Artikel direkt hier pruefen: {int(summary.get('changed', 0) or 0)} geaendert, "
            f"{int(summary.get('new', 0) or 0)} neu, "
            f"{int(summary.get('unclear', 0) or 0)} unklar, "
            f"{int(summary.get('same', 0) or 0)} gleich."
        )
        self._rebuild_table()

    def get_items(self):
        rows = []
        for item in self._items:
            if not _has_meaningful_item_data(item):
                continue
            row = dict(item)
            row["menge"] = _coerce_quantity_text(row.get("menge", "1"))
            rows.append(row)
        return rows

    def _current_payload_snapshot(self):
        payload = dict(self._payload_context or {}) if isinstance(self._payload_context, dict) else {}
        payload["waren"] = self.get_items()
        return payload

    def _draft_state_for_row(self, source_index):
        return dict(self._draft_image_states.get(int(source_index), {}) or {})

    def _store_draft_state_for_row(self, source_index, state):
        source_index = int(source_index)
        state = dict(state or {}) if isinstance(state, dict) else {}
        has_values = bool(state.get("selected_asset_id")) or bool(state.get("candidate_asset_ids")) or bool(state.get("rejected_asset_ids")) or bool(state.get("local_candidates"))
        if has_values:
            self._draft_image_states[source_index] = state
        else:
            self._draft_image_states.pop(source_index, None)

    def _on_image_state_changed(self, payload):
        source_index = int((payload or {}).get("source_row_index", -1) or -1)
        if source_index < 0:
            return
        self._store_draft_state_for_row(source_index, (payload or {}).get("state", {}))
        if self._expanded_rows.get(source_index):
            self._rebuild_table(select_source_index=source_index)
        else:
            self._refresh_main_row_visual(source_index)

    def _on_visuals_invalidated(self, payload):
        if not self._items:
            return
        self._pending_visual_refresh = True
        if not self.isVisible():
            return
        logging.debug("Produkttabellen-Refresh eingeplant: %s", (payload or {}).get("reason", "unknown"))
        self._visual_refresh_timer.start(80)

    def _perform_visual_refresh(self):
        if not self._items:
            return
        self._pending_visual_refresh = False
        selected_context = self.get_selected_context() or {}
        selected_row = selected_context.get("row")
        if any(bool(value) for value in (self._expanded_rows or {}).values()):
            self._rebuild_table(select_source_index=selected_row)
            return
        for source_index in range(len(self._items)):
            self._refresh_main_row_visual(source_index)

    def showEvent(self, event):
        super().showEvent(event)
        if self._pending_visual_refresh:
            self._visual_refresh_timer.start(0)

    def get_pending_image_decisions(self):
        result = {}
        for source_index, state in (self._draft_image_states or {}).items():
            if not isinstance(state, dict):
                continue
            prepared = {
                "candidate_asset_ids": [int(value) for value in list(state.get("candidate_asset_ids", []) or []) if int(value or 0) > 0],
                "rejected_asset_ids": [int(value) for value in list(state.get("rejected_asset_ids", []) or []) if int(value or 0) > 0],
                "selected_asset_id": int(state.get("selected_asset_id", 0) or 0),
                "selected_mode": str(state.get("selected_mode", "manual") or "manual"),
            }
            if prepared["selected_asset_id"] or prepared["candidate_asset_ids"] or prepared["rejected_asset_ids"]:
                result[int(source_index)] = prepared
        return result

    def apply_saved_image_decisions(self, db, einkauf_id):
        decisions = self.get_pending_image_decisions()
        if not decisions or db is None or einkauf_id in (None, ""):
            return {"processed": False, "reason": "no_pending_decisions"}
        try:
            from module.media.media_service import MediaService

            payload = self._current_payload_snapshot()
            media = MediaService(db)
            result = media.apply_payload_image_decisions(
                einkauf_id=einkauf_id,
                payload=payload,
                decisions=decisions,
                source_module="wizard_image_review",
                source_ref=str(payload.get("bestellnummer", "") or f"order:{einkauf_id}"),
            )
            if result.get("processed"):
                self._draft_image_states = {}
            return result
        except Exception as exc:
            logging.warning("Gemerkte Bildentscheidungen konnten nach dem Speichern nicht angewendet werden: %s", exc)
            return {"processed": False, "reason": "error", "message": str(exc)}

    def get_selected_context(self):
        visual_row = self.table.currentRow()
        info = self._visual_row_info(visual_row)
        if not info:
            return None
        source_index = info.get("source_row_index", -1)
        if source_index < 0 or source_index >= len(self._items):
            return None
        item = self._items[source_index]
        return {
            "row": source_index,
            "produkt_name": str(item.get("produkt_name", "") or "").strip(),
            "varianten_info": str(item.get("varianten_info", "") or "").strip(),
            "ean": str(item.get("ean", "") or "").strip(),
            "ean_col": 4,
        }

    def set_ean_for_row(self, row, ean):
        if row < 0 or row >= len(self._items):
            return
        self._items[row]["ean"] = str(ean or "").strip()
        review_row = self._review_rows.get(row)
        if review_row:
            for field in review_row.get("fields", []) or []:
                if field.get("field_key") == "ean":
                    field["new_value"] = str(ean or "").strip()
                    field["selected_side"] = "new"
                    break
            self._recalculate_review_row(review_row)
        self._payload_context["waren"] = self.get_items()
        self._rebuild_table(select_source_index=row)

    def _visual_row_info(self, visual_row):
        if visual_row < 0 or visual_row >= len(self._visual_rows):
            return None
        return self._visual_rows[visual_row]

    def _detail_fields_for_row(self, review_row):
        fields = []
        for field in review_row.get("fields", []) or []:
            kind = str(field.get("change_kind", "") or "")
            if kind in ("item_add", "item_update"):
                fields.append(dict(field))
        return fields

    def _row_has_detail(self, review_row, source_index=None):
        if source_index is not None and 0 <= int(source_index) < len(self._items):
            return True
        if not isinstance(review_row, dict):
            return False
        if self._detail_fields_for_row(review_row):
            return True
        if str(review_row.get("hint", "") or "").strip() and str(review_row.get("status", "") or "") in ("new", "unclear"):
            return True
        return False

    def _status_widget(self, status_key, status_label):
        info = ITEM_STATUS_STYLES.get(status_key, ITEM_STATUS_STYLES["pending"])
        label = QLabel(status_label)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setStyleSheet(
            "QLabel {"
            f"background-color: {info['bg']}; color: {info['fg']}; font-size: 12px; font-weight: bold; padding: 4px 8px; border-radius: 10px;"
            "}"
        )
        return label

    def _image_cell_widget(self, preview):
        preview = preview if isinstance(preview, dict) else {}
        container = QFrame(self)
        container.setFixedSize(48, 48)
        container.setStyleSheet(
            "QFrame {"
            f"background-color: #202233; border: 1px solid {str(preview.get('badge_border', '#30374d') or '#30374d')}; border-radius: 8px;"
            "}"
        )

        label = QLabel(container)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setGeometry(0, 0, 48, 48)
        label.setPixmap(preview.get("pixmap"))
        label.setStyleSheet("QLabel { background-color: transparent; border: none; padding: 2px; }")

        badge_text = str(preview.get("badge_text", "") or "").strip()
        if badge_text:
            badge = QLabel(badge_text, container)
            badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
            badge.setStyleSheet(
                "QLabel {"
                f"background-color: {str(preview.get('badge_bg', '#2f3545') or '#2f3545')}; color: {str(preview.get('badge_fg', '#c0caf5') or '#c0caf5')};"
                f"border: 1px solid {str(preview.get('badge_border', '#6b7280') or '#6b7280')}; border-radius: 7px;"
                "font-size: 8px; font-weight: bold; padding: 0px 4px;"
                "}"
            )
            badge_width = 22 if len(badge_text) > 1 else 16
            badge.setGeometry(48 - badge_width - 2, 48 - 16 - 2, badge_width, 16)

        tooltip = str(preview.get("tooltip", "") or "").strip()
        if tooltip:
            container.setToolTip(tooltip)
            label.setToolTip(tooltip)
        return container

    def _on_image_search_requested(self, payload):
        self.imageSearchRequested.emit(dict(payload or {}))

    def _rebuild_table(self, select_source_index=None):
        self._ignore_table_changes = True
        self.table.blockSignals(True)
        self.table.clearContents()
        self.table.clearSpans()
        self._visual_rows = []
        self._source_main_rows = {}

        row_count = 0
        for source_index, _item in enumerate(self._items):
            review_row = self._review_rows.get(source_index)
            row_count += 1
            if self._expanded_rows.get(source_index) and self._row_has_detail(review_row, source_index=source_index):
                row_count += 1
        self.table.setRowCount(row_count)

        visual_row = 0
        for source_index, item in enumerate(self._items):
            review_row = self._review_rows.get(source_index)
            self._visual_rows.append({"type": "main", "source_row_index": source_index})
            self._source_main_rows[source_index] = visual_row
            self._build_main_row(visual_row, source_index, item, review_row)
            visual_row += 1

            if self._expanded_rows.get(source_index) and self._row_has_detail(review_row, source_index=source_index):
                self._visual_rows.append({"type": "detail", "source_row_index": source_index})
                detail_fields = self._detail_fields_for_row(review_row or {})
                image_panel = OrderItemImageManagerWidget(
                    self._media_preview,
                    item,
                    review_row=review_row or {"source_row_index": source_index},
                    payload=self._current_payload_snapshot(),
                    draft_state=self._draft_state_for_row(source_index),
                    parent=self,
                )
                image_panel.stateChanged.connect(self._on_image_state_changed)
                image_panel.searchRequested.connect(self._on_image_search_requested)
                detail_panel = ItemReviewDetailWidget(
                    review_row or {"source_row_index": source_index},
                    detail_fields,
                    image_widget=image_panel,
                    parent=self,
                )
                detail_panel.fieldDecisionChanged.connect(self._on_detail_field_decision)
                self.table.setSpan(visual_row, 0, 1, self.table.columnCount())
                holder = QTableWidgetItem("")
                holder.setFlags(Qt.ItemFlag.NoItemFlags)
                self.table.setItem(visual_row, 0, holder)
                self.table.setCellWidget(visual_row, 0, detail_panel)
                self.table.setRowHeight(visual_row, max(180, detail_panel.sizeHint().height() + 16))
                visual_row += 1

        self.table.blockSignals(False)
        self._ignore_table_changes = False
        self.table.resizeRowsToContents()

        if select_source_index is not None and select_source_index in self._source_main_rows:
            self.table.setCurrentCell(self._source_main_rows[select_source_index], 2)

    def _build_main_row(self, row_index, source_index, item, review_row):
        status_key = "pending"
        status_label = "Offen"
        match_label = "Noch offen"
        change_count = ""
        if review_row:
            status_key = str(review_row.get("status", "pending") or "pending")
            status_label = str(review_row.get("status_label", "Offen") or "Offen")
            match_label = str(review_row.get("match_label", "") or "").strip()
            if not match_label:
                match_kind = str(review_row.get("match_kind", "") or "")
                match_position_id = int(review_row.get("match_position_id", 0) or 0)
                if match_kind == "matched":
                    match_label = f"Pos {match_position_id}" if match_position_id > 0 else "Zugeordnet"
                elif match_kind == "unclear":
                    match_label = "Pruefen"
                elif match_kind == "new":
                    match_label = "Neue Position"
                else:
                    match_label = "Noch offen"
            count = int(review_row.get("change_count", 0) or 0)
            if count > 0:
                change_count = str(count)
            elif status_key == "same":
                change_count = "0"

        preview = self._media_preview.resolve_product_table_preview(
            item,
            review_row=review_row,
            payload=self._current_payload_snapshot(),
            source_row_index=source_index,
            draft_state=self._draft_state_for_row(source_index),
            size=42,
        )
        self.table.setCellWidget(row_index, 0, self._image_cell_widget(preview))
        self.table.setCellWidget(row_index, 1, self._status_widget(status_key, status_label))
        self.table.setRowHeight(row_index, 52)

        column_values = {
            2: _display_item_value("produkt_name", item.get("produkt_name", "")),
            3: _display_item_value("varianten_info", item.get("varianten_info", "")),
            4: _display_item_value("ean", item.get("ean", "")),
            5: _display_item_value("menge", item.get("menge", "1")),
            6: _display_item_value("ekp_brutto", item.get("ekp_brutto", "")),
            7: match_label,
            8: change_count,
        }
        editable_columns = {2: "produkt_name", 3: "varianten_info", 4: "ean", 5: "menge", 6: "ekp_brutto"}
        status_info = ITEM_STATUS_STYLES.get(status_key, ITEM_STATUS_STYLES["pending"])

        for column_index, value in column_values.items():
            cell = QTableWidgetItem(str(value or ""))
            if column_index in editable_columns:
                cell.setFlags(cell.flags() | Qt.ItemFlag.ItemIsEditable)
            else:
                cell.setFlags(cell.flags() & ~Qt.ItemFlag.ItemIsEditable)
            if column_index in (7, 8) and review_row:
                cell.setBackground(QColor(status_info["bg"]))
                cell.setForeground(QColor(status_info["fg"]))
            self.table.setItem(row_index, column_index, cell)

        product_item = self.table.item(row_index, 2)
        if product_item is not None and str(preview.get("tooltip", "") or "").strip():
            product_item.setToolTip(str(preview.get("tooltip", "") or "").strip())

        btn_details = QPushButton("Zuklappen" if self._expanded_rows.get(source_index) else "Aufklappen")
        btn_details.setEnabled(self._row_has_detail(review_row, source_index=source_index))
        btn_details.setCursor(Qt.CursorShape.PointingHandCursor if btn_details.isEnabled() else Qt.CursorShape.ArrowCursor)
        btn_details.setStyleSheet("QPushButton { background-color: #1f2335; border: 1px solid #414868; border-radius: 6px; padding: 4px 8px; } QPushButton:disabled { color: #6b7280; }")
        btn_details.clicked.connect(lambda _checked=False, idx=source_index: self._toggle_detail_row(idx))
        self.table.setCellWidget(row_index, 9, btn_details)

        if review_row and review_row.get("match_candidates"):
            tooltip = []
            for candidate in review_row.get("match_candidates", [])[:3]:
                position_id = candidate.get("position_id")
                if not position_id:
                    continue
                tooltip.append(f"Pos {position_id}")
            if tooltip and self.table.item(row_index, 7) is not None:
                self.table.item(row_index, 7).setToolTip("Moegliche Zuordnung: " + ", ".join(tooltip))

    def _refresh_main_row_visual(self, source_index):
        row_index = self._source_main_rows.get(source_index)
        if row_index is None or source_index < 0 or source_index >= len(self._items):
            return
        self._ignore_table_changes = True
        self.table.blockSignals(True)
        self._build_main_row(row_index, source_index, self._items[source_index], self._review_rows.get(source_index))
        self.table.blockSignals(False)
        self._ignore_table_changes = False

    def _toggle_detail_row(self, source_index):
        self._expanded_rows[source_index] = not self._expanded_rows.get(source_index, False)
        self._rebuild_table(select_source_index=source_index)

    def _on_table_item_changed(self, table_item):
        if self._ignore_table_changes or table_item is None:
            return
        info = self._visual_row_info(table_item.row())
        if not info or info.get("type") != "main":
            return
        source_index = info.get("source_row_index", -1)
        if source_index < 0 or source_index >= len(self._items):
            return

        column_key_map = {2: "produkt_name", 3: "varianten_info", 4: "ean", 5: "menge", 6: "ekp_brutto"}
        field_key = column_key_map.get(table_item.column())
        if not field_key:
            return

        new_value = _coerce_item_value(field_key, table_item.text())
        self._items[source_index][field_key] = new_value
        if str(table_item.text() or "") != new_value:
            self._ignore_table_changes = True
            table_item.setText(new_value)
            self._ignore_table_changes = False

        review_row = self._review_rows.get(source_index)
        if review_row:
            for field in review_row.get("fields", []) or []:
                if field.get("field_key") == field_key:
                    field["new_value"] = new_value
                    field["selected_side"] = "new"
            self._recalculate_review_row(review_row)
        self._payload_context["waren"] = self.get_items()
        if self._expanded_rows.get(source_index):
            self._rebuild_table(select_source_index=source_index)
        else:
            self._refresh_main_row_visual(source_index)

    def _on_detail_field_decision(self, payload):
        source_index = int((payload or {}).get("source_row_index", -1) or -1)
        field_key = str((payload or {}).get("field_key", "") or "")
        selected_side = str((payload or {}).get("selected_side", "new") or "new")
        if source_index < 0 or source_index >= len(self._items) or not field_key:
            return

        review_row = self._review_rows.get(source_index)
        if not review_row:
            return

        for field in review_row.get("fields", []) or []:
            if field.get("field_key") != field_key:
                continue
            field["selected_side"] = selected_side
            field["new_value"] = _coerce_item_value(field_key, (payload or {}).get("new_value", field.get("new_value", "")))
            chosen_value = field.get("old_value") if selected_side == "old" else field.get("new_value")
            self._items[source_index][field_key] = _coerce_item_value(field_key, chosen_value)
            break

        self._recalculate_review_row(review_row)
        self._payload_context["waren"] = self.get_items()
        if self._expanded_rows.get(source_index):
            self._rebuild_table(select_source_index=source_index)
        else:
            self._refresh_main_row_visual(source_index)

    def _recalculate_review_row(self, review_row):
        if not isinstance(review_row, dict):
            return
        status_key = str(review_row.get("match_kind", "") or "")
        active_changes = 0
        for field in review_row.get("fields", []) or []:
            field_key = str(field.get("field_key", "") or "")
            selected_side = str(field.get("selected_side", "new") or "new")
            chosen_value = field.get("old_value") if selected_side == "old" else field.get("new_value")
            kind = str(field.get("change_kind", "unchanged") or "unchanged")
            if kind == "item_add":
                if _normalized_compare_value(field_key, chosen_value) not in ("", 0.0):
                    active_changes += 1
            elif kind == "item_update":
                if _normalized_compare_value(field_key, chosen_value) != _normalized_compare_value(field_key, field.get("old_value")):
                    active_changes += 1
        review_row["change_count"] = active_changes

        if status_key == "new":
            review_row["status"] = "new"
            review_row["status_label"] = "Neu"
        elif status_key == "unclear":
            review_row["status"] = "unclear"
            review_row["status_label"] = "Unklar"
        elif active_changes > 0:
            review_row["status"] = "changed"
            review_row["status_label"] = "Geaendert"
        else:
            review_row["status"] = "same"
            review_row["status_label"] = "Gleich"


class OrderReviewPanelWidget(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setStyleSheet("QFrame { background-color: #1f2335; border: 1px solid #414868; border-radius: 8px; }")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        self.lbl_intro = QLabel("")
        self.lbl_intro.setWordWrap(True)
        self.lbl_intro.setStyleSheet("font-size: 12px; color: #c0caf5; border: none;")
        layout.addWidget(self.lbl_intro)

        badge_row = QHBoxLayout()
        self.badge_add = QLabel()
        self.badge_overwrite = QLabel()
        self.badge_unchanged = QLabel()
        for badge in (self.badge_add, self.badge_overwrite, self.badge_unchanged):
            badge_row.addWidget(badge)
        badge_row.addStretch(1)
        layout.addLayout(badge_row)

        self.clear_review()

    def _set_badge(self, label, title, count, kind):
        info = CHANGE_COLORS.get(kind, CHANGE_COLORS["unchanged"])
        label.setText(f"{title}: {count}")
        label.setStyleSheet(f"QLabel {{ background-color: {info['bg']}; color: {info['fg']}; font-size: 12px; font-weight: bold; padding: 4px 8px; border-radius: 10px; }}")

    def clear_review(self, message="Sobald eine bestehende Bestellung erkannt wird, erscheint hier nur noch die kompakte Kurzuebersicht."):
        self._set_badge(self.badge_add, "Fuellt leer", 0, "add")
        self._set_badge(self.badge_overwrite, "Abweichung", 0, "overwrite")
        self._set_badge(self.badge_unchanged, "Schon gleich", 0, "unchanged")
        self.lbl_intro.setText(str(message or ""))

    def set_review_data(self, review_bundle):
        if not isinstance(review_bundle, dict):
            self.clear_review()
            return
        order_preview = review_bundle.get("order_preview", {}) or {}
        all_changes = list(order_preview.get("changes", []) or [])
        summary = order_preview.get("summary") or summarize_change_counts(all_changes)
        bestellnummer = str(order_preview.get("bestellnummer", "") or review_bundle.get("bestellnummer", "") or "").strip()

        self._set_badge(self.badge_add, "Fuellt leer", int(summary.get("add", 0) or 0) + int(summary.get("item_add", 0) or 0), "add")
        self._set_badge(self.badge_overwrite, "Abweichung", int(summary.get("overwrite", 0) or 0) + int(summary.get("item_update", 0) or 0), "overwrite")
        self._set_badge(self.badge_unchanged, "Schon gleich", int(summary.get("unchanged", 0) or 0), "unchanged")

        if order_preview.get("order_exists"):
            self.lbl_intro.setText("Diese Bestellnummer existiert bereits. Die eigentliche Entscheidung triffst du direkt oben in der Maske und in der Artikeltabelle.")
        elif bestellnummer:
            self.lbl_intro.setText("Zu dieser Bestellnummer wurde noch kein bestehender Datensatz gefunden.")
        else:
            self.lbl_intro.setText("Sobald eine Bestellnummer vorhanden ist, erscheint hier nur noch die kompakte Kurzuebersicht.")


class OrderExistingReviewDialog(QDialog):
    def __init__(self, bestellnummer, existing, review_bundle, parent=None):
        super().__init__(parent)
        self._decision = QMessageBox.StandardButton.Cancel
        self.setWindowTitle("Bestehende Bestellung pruefen")
        self.resize(1080, 720)
        self.setMinimumSize(940, 640)
        self.setStyleSheet("background-color: #1a1b26; color: #DADADA;")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(12)

        kaufdatum = existing.get("kaufdatum")
        kaufdatum_text = kaufdatum.strftime("%d.%m.%Y") if hasattr(kaufdatum, "strftime") else str(kaufdatum or "-")
        shop_text = str(existing.get("shop_name", "") or "-")

        title = QLabel(f"Bestellung {bestellnummer} ist bereits vorhanden")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #7aa2f7;")
        layout.addWidget(title)

        intro = QLabel(
            "Hier bleibt die Bestaetigung kompakt. Die Kopffeld-Pruefung siehst du unten zusammengefasst. "
            "Die eigentlichen Positionsentscheidungen triffst du direkt in der Artikeltabelle im Hauptfenster.\n"
            + f"Bestehender Datensatz: Datum {kaufdatum_text}, Shop {shop_text}."
        )
        intro.setWordWrap(True)
        intro.setStyleSheet("font-size: 12px; color: #c0caf5;")
        layout.addWidget(intro)

        self.review_panel = OrderReviewPanelWidget(self)
        self.review_panel.set_review_data(review_bundle)
        layout.addWidget(self.review_panel, 1)

        footer = QLabel("Yes aktualisiert den bestehenden Datensatz im bisherigen Rahmen. No legt stattdessen eine neue Bestellnummer an. Cancel bricht ab.")
        footer.setWordWrap(True)
        footer.setStyleSheet("font-size: 12px; color: #a9b1d6;")
        layout.addWidget(footer)

        row = QHBoxLayout()
        row.addStretch(1)
        btn_yes = QPushButton("Bestehende Bestellung aktualisieren")
        btn_yes.clicked.connect(self._accept_yes)
        row.addWidget(btn_yes)
        btn_no = QPushButton("Als neue Bestellung speichern")
        btn_no.clicked.connect(self._accept_no)
        row.addWidget(btn_no)
        btn_cancel = QPushButton("Abbrechen")
        btn_cancel.clicked.connect(self.reject)
        row.addWidget(btn_cancel)
        layout.addLayout(row)

    def _accept_yes(self):
        self._decision = QMessageBox.StandardButton.Yes
        self.accept()

    def _accept_no(self):
        self._decision = QMessageBox.StandardButton.No
        self.accept()

    def reject(self):
        self._decision = QMessageBox.StandardButton.Cancel
        super().reject()

    @classmethod
    def choose(cls, parent, bestellnummer, existing, review_bundle):
        dialog = cls(bestellnummer, existing, review_bundle, parent=parent)
        dialog.exec()
        return dialog._decision






























































