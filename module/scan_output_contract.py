"""
Interner, provider-unabhaengiger Output-Vertrag fuer KI-Scans.

Ziel:
- Die App arbeitet nach der KI-Antwort nur noch mit validierten Strukturen.
- Provider-spezifische Details bleiben ausserhalb dieses Vertrags.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any, Dict, List, Sequence, Union


VALID_SCAN_MODES = {"einkauf", "verkauf"}

EINKAUF_FIELDS: Sequence[str] = (
    "bestellnummer",
    "kaufdatum",
    "shop_name",
    "bestell_email",
    "tracking_nummer_einkauf",
    "tracking_url",
    "paketdienst",
    "lieferdatum",
    "sendungsstatus",
    "amazon_marketplace_domain",
    "amazon_order_id",
    "amazon_ordering_shipment_id",
    "amazon_package_id",
    "gesamt_ekp_brutto",
    "versandkosten_brutto",
    "nebenkosten_brutto",
    "rabatt_brutto",
    "ust_satz",
    "reverse_charge",
    "zahlungsart",
    "waren",
    "screenshot_detections",
)
EINKAUF_REQUIRED_FIELDS: Sequence[str] = ("waren",)

VERKAUF_FIELDS: Sequence[str] = (
    "ticket_name",
    "kaeufer",
    "zahlungsziel",
    "waren",
)
VERKAUF_REQUIRED_FIELDS: Sequence[str] = ("waren",)


class StructuredOutputValidationError(ValueError):
    """Validierungsfehler fuer den internen Output-Vertrag."""

    def __init__(
        self,
        error_kind: str,
        user_message: str,
        technical_message: str = "",
        scan_mode: str = "",
        field_name: str = "",
    ):
        super().__init__(user_message)
        self.error_kind = str(error_kind or "schema_violation")
        self.user_message = str(user_message or "Strukturierte Modellantwort ist ungueltig.")
        self.technical_message = str(technical_message or "")
        self.scan_mode = str(scan_mode or "")
        self.field_name = str(field_name or "")


def _to_text(value: Any, field_name: str, scan_mode: str) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value).strip()
    raise StructuredOutputValidationError(
        error_kind="schema_violation",
        user_message=f"Antwortformat ungueltig: Feld '{field_name}' hat einen unerwarteten Datentyp.",
        technical_message=f"field '{field_name}' type: {type(value).__name__}",
        scan_mode=scan_mode,
        field_name=field_name,
    )


def _normalize_quantity(value: Any, field_name: str, scan_mode: str) -> str:
    txt = _to_text(value, field_name, scan_mode)
    if not txt:
        return "1"
    try:
        if "." in txt or "," in txt:
            number = int(float(txt.replace(",", ".")))
        else:
            number = int(txt)
        return str(max(1, number))
    except Exception:
        digits = "".join(ch for ch in txt if ch.isdigit())
        if digits:
            try:
                return str(max(1, int(digits)))
            except Exception:
                return "1"
        return "1"


def _normalize_date(value: Any, field_name: str, scan_mode: str) -> str:
    txt = _to_text(value, field_name, scan_mode)
    if len(txt) >= 10 and txt[4:5] == "-" and txt[7:8] == "-":
        return txt[:10]
    return txt


def _normalize_money_text(value: Any, field_name: str, scan_mode: str) -> str:
    txt = _to_text(value, field_name, scan_mode)
    if not txt:
        return ""
    cleaned = (
        txt.replace("EUR", "")
        .replace("eur", "")
        .replace("EUR.", "")
        .replace("€", "")
        .replace(" ", "")
        .replace(",", ".")
        .strip()
    )
    return cleaned or txt


def _validate_payload_dict(payload: Any, scan_mode: str) -> Dict[str, Any]:
    if payload is None:
        raise StructuredOutputValidationError(
            error_kind="empty_response",
            user_message="Modellantwort ist leer.",
            technical_message="payload is None",
            scan_mode=scan_mode,
        )
    if not isinstance(payload, dict):
        raise StructuredOutputValidationError(
            error_kind="schema_violation",
            user_message="Antwortformat ungueltig: erwartet wurde ein JSON-Objekt.",
            technical_message=f"payload type: {type(payload).__name__}",
            scan_mode=scan_mode,
        )
    return payload


def _collect_unknown_fields(payload: Dict[str, Any], allowed_fields: Sequence[str]) -> List[str]:
    allowed = set(str(x) for x in allowed_fields)
    unknown = []
    for key in payload.keys():
        if str(key).startswith("_"):
            continue
        if str(key) not in allowed:
            unknown.append(str(key))
    return sorted(unknown)


def _ensure_required_fields(payload: Dict[str, Any], required_fields: Sequence[str], scan_mode: str):
    missing = [str(field) for field in required_fields if str(field) not in payload]
    if missing:
        raise StructuredOutputValidationError(
            error_kind="incomplete_response",
            user_message="Modellantwort ist unvollstaendig.",
            technical_message=f"missing required fields: {', '.join(missing)}",
            scan_mode=scan_mode,
            field_name=missing[0],
        )


def _parse_waren_list(raw_waren: Any, scan_mode: str, verkauf: bool) -> List[Dict[str, str]]:
    if raw_waren is None:
        return []
    if not isinstance(raw_waren, list):
        raise StructuredOutputValidationError(
            error_kind="schema_violation",
            user_message="Antwortformat ungueltig: 'waren' muss eine Liste sein.",
            technical_message=f"waren type: {type(raw_waren).__name__}",
            scan_mode=scan_mode,
            field_name="waren",
        )

    parsed_rows: List[Dict[str, str]] = []
    for idx, item in enumerate(raw_waren):
        if not isinstance(item, dict):
            raise StructuredOutputValidationError(
                error_kind="schema_violation",
                user_message="Antwortformat ungueltig: Artikelzeilen haben ein falsches Format.",
                technical_message=f"waren[{idx}] type: {type(item).__name__}",
                scan_mode=scan_mode,
                field_name=f"waren[{idx}]",
            )

        if verkauf:
            row = {
                "produkt_name": _to_text(item.get("produkt_name", ""), f"waren[{idx}].produkt_name", scan_mode),
                "ean": _to_text(item.get("ean", ""), f"waren[{idx}].ean", scan_mode),
                "menge": _normalize_quantity(item.get("menge", "1"), f"waren[{idx}].menge", scan_mode),
                "vk_brutto": _normalize_money_text(item.get("vk_brutto", ""), f"waren[{idx}].vk_brutto", scan_mode),
                "marge_gesamt": _normalize_money_text(item.get("marge_gesamt", ""), f"waren[{idx}].marge_gesamt", scan_mode),
            }
        else:
            row = {
                "produkt_name": _to_text(item.get("produkt_name", ""), f"waren[{idx}].produkt_name", scan_mode),
                "varianten_info": _to_text(item.get("varianten_info", ""), f"waren[{idx}].varianten_info", scan_mode),
                "ean": _to_text(item.get("ean", ""), f"waren[{idx}].ean", scan_mode),
                "menge": _normalize_quantity(item.get("menge", "1"), f"waren[{idx}].menge", scan_mode),
                "ekp_brutto": _normalize_money_text(item.get("ekp_brutto", ""), f"waren[{idx}].ekp_brutto", scan_mode),
                "bild_url": _to_text(item.get("bild_url", ""), f"waren[{idx}].bild_url", scan_mode),
            }
        parsed_rows.append(row)

    return parsed_rows


def _parse_screenshot_detections(raw_detections: Any, scan_mode: str) -> List[Dict[str, Any]]:
    if raw_detections in (None, ""):
        return []
    if not isinstance(raw_detections, list):
        raise StructuredOutputValidationError(
            error_kind="schema_violation",
            user_message="Antwortformat ungueltig: 'screenshot_detections' muss eine Liste sein.",
            technical_message=f"screenshot_detections type: {type(raw_detections).__name__}",
            scan_mode=scan_mode,
            field_name="screenshot_detections",
        )

    parsed_rows: List[Dict[str, Any]] = []
    for idx, item in enumerate(raw_detections):
        if not isinstance(item, dict):
            raise StructuredOutputValidationError(
                error_kind="schema_violation",
                user_message="Antwortformat ungueltig: Screenshot-Detektionen haben ein falsches Format.",
                technical_message=f"screenshot_detections[{idx}] type: {type(item).__name__}",
                scan_mode=scan_mode,
                field_name=f"screenshot_detections[{idx}]",
            )

        row: Dict[str, Any] = {
            "produkt_name_hint": _to_text(
                item.get("produkt_name_hint", item.get("product_name_hint", "")),
                f"screenshot_detections[{idx}].produkt_name_hint",
                scan_mode,
            ),
            "product_key": _to_text(item.get("product_key", ""), f"screenshot_detections[{idx}].product_key", scan_mode),
            "ean": _to_text(item.get("ean", ""), f"screenshot_detections[{idx}].ean", scan_mode),
            "variant_text": _to_text(
                item.get("variant_text", item.get("varianten_info", "")),
                f"screenshot_detections[{idx}].variant_text",
                scan_mode,
            ),
            "x": _to_text(item.get("x", ""), f"screenshot_detections[{idx}].x", scan_mode),
            "y": _to_text(item.get("y", ""), f"screenshot_detections[{idx}].y", scan_mode),
            "width": _to_text(item.get("width", ""), f"screenshot_detections[{idx}].width", scan_mode),
            "height": _to_text(item.get("height", ""), f"screenshot_detections[{idx}].height", scan_mode),
            "confidence": _to_text(item.get("confidence", ""), f"screenshot_detections[{idx}].confidence", scan_mode),
        }
        coord_units = _to_text(
            item.get("coord_units", item.get("units", "")),
            f"screenshot_detections[{idx}].coord_units",
            scan_mode,
        ).lower()
        if coord_units:
            row["coord_units"] = coord_units
        coord_origin = _to_text(
            item.get("coord_origin", item.get("origin", "")),
            f"screenshot_detections[{idx}].coord_origin",
            scan_mode,
        ).lower()
        if coord_origin:
            row["coord_origin"] = coord_origin

        source_width = _to_text(
            item.get("source_image_width", item.get("detection_image_width", item.get("image_width", item.get("render_width", item.get("viewport_width", ""))))),
            f"screenshot_detections[{idx}].source_image_width",
            scan_mode,
        )
        if source_width:
            row["source_image_width"] = source_width
        source_height = _to_text(
            item.get("source_image_height", item.get("detection_image_height", item.get("image_height", item.get("render_height", item.get("viewport_height", ""))))),
            f"screenshot_detections[{idx}].source_image_height",
            scan_mode,
        )
        if source_height:
            row["source_image_height"] = source_height

        ware_index_value = item.get("ware_index", item.get("waren_index", ""))
        ware_index_text = _to_text(ware_index_value, f"screenshot_detections[{idx}].ware_index", scan_mode)
        if ware_index_text:
            row["ware_index"] = ware_index_text
        parsed_rows.append(row)

    return parsed_rows


@dataclass
class ValidatedEinkaufOutput:
    bestellnummer: str = ""
    kaufdatum: str = ""
    shop_name: str = ""
    bestell_email: str = ""
    tracking_nummer_einkauf: str = ""
    tracking_url: str = ""
    paketdienst: str = ""
    lieferdatum: str = ""
    sendungsstatus: str = ""
    amazon_marketplace_domain: str = ""
    amazon_order_id: str = ""
    amazon_ordering_shipment_id: str = ""
    amazon_package_id: str = ""
    gesamt_ekp_brutto: str = ""
    versandkosten_brutto: str = ""
    nebenkosten_brutto: str = ""
    rabatt_brutto: str = ""
    ust_satz: str = ""
    reverse_charge: bool = False
    zahlungsart: str = ""
    waren: List[Dict[str, str]] = field(default_factory=list)
    screenshot_detections: List[Dict[str, Any]] = field(default_factory=list)
    unknown_fields: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "bestellnummer": self.bestellnummer,
            "kaufdatum": self.kaufdatum,
            "shop_name": self.shop_name,
            "bestell_email": self.bestell_email,
            "tracking_nummer_einkauf": self.tracking_nummer_einkauf,
            "tracking_url": self.tracking_url,
            "paketdienst": self.paketdienst,
            "lieferdatum": self.lieferdatum,
            "sendungsstatus": self.sendungsstatus,
            "amazon_marketplace_domain": self.amazon_marketplace_domain,
            "amazon_order_id": self.amazon_order_id,
            "amazon_ordering_shipment_id": self.amazon_ordering_shipment_id,
            "amazon_package_id": self.amazon_package_id,
            "gesamt_ekp_brutto": self.gesamt_ekp_brutto,
            "versandkosten_brutto": self.versandkosten_brutto,
            "nebenkosten_brutto": self.nebenkosten_brutto,
            "rabatt_brutto": self.rabatt_brutto,
            "ust_satz": self.ust_satz,
            "reverse_charge": self.reverse_charge,
            "zahlungsart": self.zahlungsart,
            "waren": list(self.waren),
            "screenshot_detections": list(self.screenshot_detections),
        }
        if self.unknown_fields:
            data["_schema_unknown_fields"] = list(self.unknown_fields)
        return data


@dataclass
class ValidatedVerkaufOutput:
    ticket_name: str = ""
    kaeufer: str = ""
    zahlungsziel: str = ""
    waren: List[Dict[str, str]] = field(default_factory=list)
    unknown_fields: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "ticket_name": self.ticket_name,
            "kaeufer": self.kaeufer,
            "zahlungsziel": self.zahlungsziel,
            "waren": list(self.waren),
        }
        if self.unknown_fields:
            data["_schema_unknown_fields"] = list(self.unknown_fields)
        return data


ValidatedOutput = Union[ValidatedEinkaufOutput, ValidatedVerkaufOutput]


def get_scan_output_schema(scan_mode: str) -> Dict[str, Any]:
    mode = str(scan_mode or "").strip().lower()
    if mode == "verkauf":
        return {
            "type": "object",
            "required": list(VERKAUF_REQUIRED_FIELDS),
            "properties": {
                "ticket_name": {"type": "string"},
                "kaeufer": {"type": "string"},
                "zahlungsziel": {"type": "string"},
                "waren": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "produkt_name": {"type": "string"},
                            "ean": {"type": "string"},
                            "menge": {"type": ["string", "number", "integer"]},
                            "vk_brutto": {"type": ["string", "number"]},
                            "marge_gesamt": {"type": ["string", "number"]},
                        },
                    },
                },
            },
        }

    return {
        "type": "object",
        "required": list(EINKAUF_REQUIRED_FIELDS),
        "properties": {
            "bestellnummer": {"type": "string"},
            "kaufdatum": {"type": "string"},
            "shop_name": {"type": "string"},
            "bestell_email": {"type": "string"},
            "tracking_nummer_einkauf": {"type": "string"},
            "tracking_url": {"type": "string"},
            "paketdienst": {"type": "string"},
            "lieferdatum": {"type": "string"},
            "sendungsstatus": {"type": "string"},
            "amazon_marketplace_domain": {"type": "string"},
            "amazon_order_id": {"type": "string"},
            "amazon_ordering_shipment_id": {"type": "string"},
            "amazon_package_id": {"type": "string"},
            "gesamt_ekp_brutto": {"type": ["string", "number"]},
            "versandkosten_brutto": {"type": ["string", "number"]},
            "nebenkosten_brutto": {"type": ["string", "number"]},
            "rabatt_brutto": {"type": ["string", "number"]},
            "ust_satz": {"type": ["string", "number"]},
            "reverse_charge": {"type": "boolean"},
            "zahlungsart": {"type": "string"},
            "screenshot_detections": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "produkt_name_hint": {"type": "string"},
                        "product_name_hint": {"type": "string"},
                        "product_key": {"type": "string"},
                        "ean": {"type": "string"},
                        "variant_text": {"type": "string"},
                        "varianten_info": {"type": "string"},
                        "ware_index": {"type": ["string", "number", "integer"]},
                        "waren_index": {"type": ["string", "number", "integer"]},
                        "x": {"type": ["string", "number", "integer"]},
                        "y": {"type": ["string", "number", "integer"]},
                        "width": {"type": ["string", "number", "integer"]},
                        "height": {"type": ["string", "number", "integer"]},
                        "confidence": {"type": ["string", "number", "integer"]},
                        "coord_units": {"type": "string"},
                        "units": {"type": "string"},
                        "coord_origin": {"type": "string"},
                        "origin": {"type": "string"},
                        "source_image_width": {"type": ["string", "number", "integer"]},
                        "source_image_height": {"type": ["string", "number", "integer"]},
                        "detection_image_width": {"type": ["string", "number", "integer"]},
                        "detection_image_height": {"type": ["string", "number", "integer"]},
                        "image_width": {"type": ["string", "number", "integer"]},
                        "image_height": {"type": ["string", "number", "integer"]},
                        "render_width": {"type": ["string", "number", "integer"]},
                        "render_height": {"type": ["string", "number", "integer"]},
                        "viewport_width": {"type": ["string", "number", "integer"]},
                        "viewport_height": {"type": ["string", "number", "integer"]},
                    },
                },
            },
            "waren": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "produkt_name": {"type": "string"},
                        "varianten_info": {"type": "string"},
                        "ean": {"type": "string"},
                        "menge": {"type": ["string", "number", "integer"]},
                        "ekp_brutto": {"type": ["string", "number"]},
                        "bild_url": {"type": "string"},
                    },
                },
            },
        },
    }


def get_scan_output_schema_json(scan_mode: str) -> str:
    return json.dumps(get_scan_output_schema(scan_mode), ensure_ascii=False, indent=2)


def validate_and_normalize_output(scan_mode: str, payload: Any) -> ValidatedOutput:
    mode = str(scan_mode or "").strip().lower()
    if mode not in VALID_SCAN_MODES:
        raise StructuredOutputValidationError(
            error_kind="schema_violation",
            user_message=f"Unbekannter Scan-Modus: {scan_mode}",
            technical_message=f"unsupported scan_mode: {scan_mode}",
            scan_mode=mode,
            field_name="scan_mode",
        )

    source = _validate_payload_dict(payload, mode)

    if mode == "verkauf":
        _ensure_required_fields(source, VERKAUF_REQUIRED_FIELDS, mode)
        unknown_fields = _collect_unknown_fields(source, VERKAUF_FIELDS)
        return ValidatedVerkaufOutput(
            ticket_name=_to_text(source.get("ticket_name", ""), "ticket_name", mode),
            kaeufer=_to_text(source.get("kaeufer", ""), "kaeufer", mode),
            zahlungsziel=_to_text(source.get("zahlungsziel", ""), "zahlungsziel", mode),
            waren=_parse_waren_list(source.get("waren", []), mode, verkauf=True),
            unknown_fields=unknown_fields,
        )

    _ensure_required_fields(source, EINKAUF_REQUIRED_FIELDS, mode)
    unknown_fields = _collect_unknown_fields(source, EINKAUF_FIELDS)
    return ValidatedEinkaufOutput(
        bestellnummer=_to_text(source.get("bestellnummer", ""), "bestellnummer", mode),
        kaufdatum=_normalize_date(source.get("kaufdatum", ""), "kaufdatum", mode),
        shop_name=_to_text(source.get("shop_name", ""), "shop_name", mode),
        bestell_email=_to_text(source.get("bestell_email", ""), "bestell_email", mode),
        tracking_nummer_einkauf=_to_text(source.get("tracking_nummer_einkauf", ""), "tracking_nummer_einkauf", mode),
        tracking_url=_to_text(source.get("tracking_url", ""), "tracking_url", mode),
        paketdienst=_to_text(source.get("paketdienst", ""), "paketdienst", mode),
        lieferdatum=_normalize_date(source.get("lieferdatum", ""), "lieferdatum", mode),
        sendungsstatus=_to_text(source.get("sendungsstatus", ""), "sendungsstatus", mode),
        amazon_marketplace_domain=_to_text(source.get("amazon_marketplace_domain", ""), "amazon_marketplace_domain", mode),
        amazon_order_id=_to_text(source.get("amazon_order_id", ""), "amazon_order_id", mode),
        amazon_ordering_shipment_id=_to_text(source.get("amazon_ordering_shipment_id", ""), "amazon_ordering_shipment_id", mode),
        amazon_package_id=_to_text(source.get("amazon_package_id", ""), "amazon_package_id", mode),
        gesamt_ekp_brutto=_normalize_money_text(source.get("gesamt_ekp_brutto", ""), "gesamt_ekp_brutto", mode),
        versandkosten_brutto=_normalize_money_text(source.get("versandkosten_brutto", ""), "versandkosten_brutto", mode),
        nebenkosten_brutto=_normalize_money_text(source.get("nebenkosten_brutto", ""), "nebenkosten_brutto", mode),
        rabatt_brutto=_normalize_money_text(source.get("rabatt_brutto", ""), "rabatt_brutto", mode),
        ust_satz=_normalize_money_text(source.get("ust_satz", ""), "ust_satz", mode),
        reverse_charge=bool(source.get("reverse_charge", False)),
        zahlungsart=_to_text(source.get("zahlungsart", ""), "zahlungsart", mode),
        waren=_parse_waren_list(source.get("waren", []), mode, verkauf=False),
        screenshot_detections=_parse_screenshot_detections(source.get("screenshot_detections", []), mode),
        unknown_fields=unknown_fields,
    )
