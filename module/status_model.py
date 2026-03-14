"""Zentrales internes Status- und Trackingmodell.

Die UI darf weiterhin vertraute Labels zeigen.
Die interne Logik arbeitet jedoch mit stabilen, eigenen Status-Domaenen.
"""

from __future__ import annotations

from enum import Enum


def _norm_text(value) -> str:
    txt = str(value or "").strip().lower()
    txt = (
        txt.replace("ae", "ae")
        .replace("oe", "oe")
        .replace("ue", "ue")
        .replace("ss", "ss")
        .replace("\u00e4", "ae")
        .replace("\u00f6", "oe")
        .replace("\u00fc", "ue")
        .replace("\u00df", "ss")
    )
    txt = txt.replace("-", " ").replace("_", " ").replace("/", " ")
    return " ".join(txt.split())


class InventoryStatus(str, Enum):
    WAITING_FOR_ORDER = "WAITING_FOR_ORDER"
    IN_STOCK = "IN_STOCK"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"


INVENTORY_DEFAULT = InventoryStatus.WAITING_FOR_ORDER

_INVENTORY_ALIAS = {
    "waiting for order": InventoryStatus.WAITING_FOR_ORDER,
    "waiting": InventoryStatus.WAITING_FOR_ORDER,
    "ordered": InventoryStatus.WAITING_FOR_ORDER,
    "offen": InventoryStatus.WAITING_FOR_ORDER,
    "in stock": InventoryStatus.IN_STOCK,
    "auf lager": InventoryStatus.IN_STOCK,
    "lager": InventoryStatus.IN_STOCK,
    "shipped": InventoryStatus.SHIPPED,
    "versandt": InventoryStatus.SHIPPED,
    "delivered": InventoryStatus.DELIVERED,
    "geliefert": InventoryStatus.DELIVERED,
    "cancelled": InventoryStatus.CANCELLED,
    "canceled": InventoryStatus.CANCELLED,
    "storniert": InventoryStatus.CANCELLED,
}


def normalize_inventory_status(value, default: InventoryStatus = INVENTORY_DEFAULT) -> InventoryStatus:
    if isinstance(value, InventoryStatus):
        return value
    raw = str(value or "").strip()
    if not raw:
        return default
    if raw in InventoryStatus._value2member_map_:
        return InventoryStatus(raw)
    return _INVENTORY_ALIAS.get(_norm_text(raw), default)


def inventory_db_value(value, default: InventoryStatus = INVENTORY_DEFAULT) -> str:
    return normalize_inventory_status(value, default=default).value


def inventory_is_in_stock(value) -> bool:
    return normalize_inventory_status(value) == InventoryStatus.IN_STOCK


def inventory_is_open(value) -> bool:
    status = normalize_inventory_status(value)
    return status in {InventoryStatus.WAITING_FOR_ORDER, InventoryStatus.IN_STOCK}


def inventory_is_cancelled(value) -> bool:
    return normalize_inventory_status(value) == InventoryStatus.CANCELLED


class ShipmentStatus(str, Enum):
    NOT_DISPATCHED = "NOT_DISPATCHED"
    IN_TRANSIT = "IN_TRANSIT"
    OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    DELIVERED = "DELIVERED"
    ISSUE_DELAYED = "ISSUE_DELAYED"


SHIPMENT_DEFAULT = ShipmentStatus.NOT_DISPATCHED

SHIPMENT_DB_LABELS = {
    ShipmentStatus.NOT_DISPATCHED: "Noch nicht los",
    ShipmentStatus.IN_TRANSIT: "Unterwegs",
    ShipmentStatus.OUT_FOR_DELIVERY: "In Auslieferung",
    ShipmentStatus.DELIVERED: "Geliefert",
    ShipmentStatus.ISSUE_DELAYED: "Problem/Verzoegert",
}

_SHIPMENT_ALIAS = {
    "noch nicht los": ShipmentStatus.NOT_DISPATCHED,
    "not dispatched": ShipmentStatus.NOT_DISPATCHED,
    "offen": ShipmentStatus.NOT_DISPATCHED,
    "unterwegs": ShipmentStatus.IN_TRANSIT,
    "in transit": ShipmentStatus.IN_TRANSIT,
    "in auslieferung": ShipmentStatus.OUT_FOR_DELIVERY,
    "out for delivery": ShipmentStatus.OUT_FOR_DELIVERY,
    "geliefert": ShipmentStatus.DELIVERED,
    "delivered": ShipmentStatus.DELIVERED,
    "problem verzoegert": ShipmentStatus.ISSUE_DELAYED,
    "problem verzoegert!": ShipmentStatus.ISSUE_DELAYED,
    "problem": ShipmentStatus.ISSUE_DELAYED,
    "delay": ShipmentStatus.ISSUE_DELAYED,
}


def normalize_shipment_status(value, default: ShipmentStatus = SHIPMENT_DEFAULT) -> ShipmentStatus:
    if isinstance(value, ShipmentStatus):
        return value
    raw = str(value or "").strip()
    if not raw:
        return default
    if raw in ShipmentStatus._value2member_map_:
        return ShipmentStatus(raw)

    for status, label in SHIPMENT_DB_LABELS.items():
        if _norm_text(label) == _norm_text(raw):
            return status

    return _SHIPMENT_ALIAS.get(_norm_text(raw), default)


def shipment_db_value(value, default: ShipmentStatus = SHIPMENT_DEFAULT) -> str:
    status = normalize_shipment_status(value, default=default)
    return SHIPMENT_DB_LABELS.get(status, SHIPMENT_DB_LABELS[default])


def shipment_is_delivered(value) -> bool:
    return normalize_shipment_status(value) == ShipmentStatus.DELIVERED


def shipment_is_in_transit(value) -> bool:
    status = normalize_shipment_status(value)
    return status in {ShipmentStatus.IN_TRANSIT, ShipmentStatus.OUT_FOR_DELIVERY}


def shipment_is_open(value) -> bool:
    return not shipment_is_delivered(value)


class PaymentStatus(str, Enum):
    OPEN = "OPEN"
    PAID = "PAID"
    REFUNDED = "REFUNDED"


PAYMENT_DEFAULT = PaymentStatus.OPEN

PAYMENT_DB_LABELS = {
    PaymentStatus.OPEN: "Offen",
    PaymentStatus.PAID: "Bezahlt",
    PaymentStatus.REFUNDED: "Erstattet",
}

_PAYMENT_ALIAS = {
    "offen": PaymentStatus.OPEN,
    "open": PaymentStatus.OPEN,
    "bezahlt": PaymentStatus.PAID,
    "paid": PaymentStatus.PAID,
    "erstattet": PaymentStatus.REFUNDED,
    "refunded": PaymentStatus.REFUNDED,
}


def normalize_payment_status(value, default: PaymentStatus = PAYMENT_DEFAULT) -> PaymentStatus:
    if isinstance(value, PaymentStatus):
        return value
    raw = str(value or "").strip()
    if not raw:
        return default
    if raw in PaymentStatus._value2member_map_:
        return PaymentStatus(raw)

    for status, label in PAYMENT_DB_LABELS.items():
        if _norm_text(label) == _norm_text(raw):
            return status

    return _PAYMENT_ALIAS.get(_norm_text(raw), default)


def payment_db_value(value, default: PaymentStatus = PAYMENT_DEFAULT) -> str:
    status = normalize_payment_status(value, default=default)
    return PAYMENT_DB_LABELS.get(status, PAYMENT_DB_LABELS[default])


class InvoiceStatus(str, Enum):
    NO_INVOICE = "NO_INVOICE"
    INVOICE_PRESENT = "INVOICE_PRESENT"
    BOOKED = "BOOKED"


INVOICE_DEFAULT = InvoiceStatus.NO_INVOICE

INVOICE_DB_LABELS = {
    InvoiceStatus.NO_INVOICE: "Keine Rechnung",
    InvoiceStatus.INVOICE_PRESENT: "Rechnung vorhanden",
    InvoiceStatus.BOOKED: "Gebucht",
}

_INVOICE_ALIAS = {
    "keine rechnung": InvoiceStatus.NO_INVOICE,
    "none": InvoiceStatus.NO_INVOICE,
    "rechnung vorhanden": InvoiceStatus.INVOICE_PRESENT,
    "invoice present": InvoiceStatus.INVOICE_PRESENT,
    "gebucht": InvoiceStatus.BOOKED,
    "booked": InvoiceStatus.BOOKED,
}


def normalize_invoice_status(value, default: InvoiceStatus = INVOICE_DEFAULT) -> InvoiceStatus:
    if isinstance(value, InvoiceStatus):
        return value
    raw = str(value or "").strip()
    if not raw:
        return default
    if raw in InvoiceStatus._value2member_map_:
        return InvoiceStatus(raw)

    for status, label in INVOICE_DB_LABELS.items():
        if _norm_text(label) == _norm_text(raw):
            return status

    return _INVOICE_ALIAS.get(_norm_text(raw), default)


def invoice_db_value(value, default: InvoiceStatus = INVOICE_DEFAULT) -> str:
    status = normalize_invoice_status(value, default=default)
    return INVOICE_DB_LABELS.get(status, INVOICE_DB_LABELS[default])


class TicketMatchingStatus(str, Enum):
    MATCHED = "MATCHED"
    PARTIAL = "PARTIAL"
    TICKET_FOLGT = "TICKET_FOLGT"


TICKET_MATCHING_DEFAULT = TicketMatchingStatus.MATCHED


def normalize_ticket_matching_status(value, default: TicketMatchingStatus = TICKET_MATCHING_DEFAULT) -> TicketMatchingStatus:
    if isinstance(value, TicketMatchingStatus):
        return value
    raw = str(value or "").strip().upper()
    if raw in TicketMatchingStatus._value2member_map_:
        return TicketMatchingStatus(raw)
    if _norm_text(value) in {"ticket folgt", "ticketfolgt"}:
        return TicketMatchingStatus.TICKET_FOLGT
    return default


def ticket_matching_db_value(value, default: TicketMatchingStatus = TICKET_MATCHING_DEFAULT) -> str:
    return normalize_ticket_matching_status(value, default=default).value


# POMS bleibt als sichtbare Modul-Huelle erhalten.
# Die Optionen hier sind bewusst View-Labels fuer die POMS-Maske,
# waehrend intern weiterhin nur unsere eigene DB, unsere Status-Domaenen
# und unsere Fachlogik gelten.
POMS_ORDER_OPTIONS = [
    ("Ordered (Waiting)", InventoryStatus.WAITING_FOR_ORDER),
    ("Ready (In Stock)", InventoryStatus.IN_STOCK),
    ("Delivered", InventoryStatus.DELIVERED),
    ("Canceled", InventoryStatus.CANCELLED),
]

POMS_PAYMENT_OPTIONS = [
    (PAYMENT_DB_LABELS[PaymentStatus.OPEN], PaymentStatus.OPEN),
    (PAYMENT_DB_LABELS[PaymentStatus.PAID], PaymentStatus.PAID),
    (PAYMENT_DB_LABELS[PaymentStatus.REFUNDED], PaymentStatus.REFUNDED),
]

POMS_INVOICE_OPTIONS = [
    (INVOICE_DB_LABELS[InvoiceStatus.NO_INVOICE], InvoiceStatus.NO_INVOICE),
    (INVOICE_DB_LABELS[InvoiceStatus.INVOICE_PRESENT], InvoiceStatus.INVOICE_PRESENT),
    (INVOICE_DB_LABELS[InvoiceStatus.BOOKED], InvoiceStatus.BOOKED),
]

# Bewusste Kompatibilitaets-Aliasse fuer alte Aufrufer.
LEGACY_ORDER_OPTIONS = POMS_ORDER_OPTIONS
LEGACY_PAYMENT_OPTIONS = POMS_PAYMENT_OPTIONS
LEGACY_INVOICE_OPTIONS = POMS_INVOICE_OPTIONS


def legacy_order_status_from_code(value) -> InventoryStatus:
    try:
        idx = int(value) - 1
    except (TypeError, ValueError):
        idx = 0
    if 0 <= idx < len(LEGACY_ORDER_OPTIONS):
        return LEGACY_ORDER_OPTIONS[idx][1]
    return INVENTORY_DEFAULT


def legacy_payment_status_from_code(value) -> PaymentStatus:
    try:
        idx = int(value) - 1
    except (TypeError, ValueError):
        idx = 0
    if 0 <= idx < len(LEGACY_PAYMENT_OPTIONS):
        return LEGACY_PAYMENT_OPTIONS[idx][1]
    return PAYMENT_DEFAULT


def legacy_invoice_status_from_code(value) -> InvoiceStatus:
    try:
        idx = int(value) - 1
    except (TypeError, ValueError):
        idx = 0
    if 0 <= idx < len(LEGACY_INVOICE_OPTIONS):
        return LEGACY_INVOICE_OPTIONS[idx][1]
    return INVOICE_DEFAULT


def legacy_order_code_for_status(value) -> int:
    status = normalize_inventory_status(value)
    for idx, (_, item_status) in enumerate(LEGACY_ORDER_OPTIONS, start=1):
        if item_status == status:
            return idx
    return 1


def legacy_payment_code_for_status(value) -> int:
    status = normalize_payment_status(value)
    for idx, (_, item_status) in enumerate(LEGACY_PAYMENT_OPTIONS, start=1):
        if item_status == status:
            return idx
    return 1


def legacy_invoice_code_for_status(value) -> int:
    status = normalize_invoice_status(value)
    for idx, (_, item_status) in enumerate(LEGACY_INVOICE_OPTIONS, start=1):
        if item_status == status:
            return idx
    return 1



