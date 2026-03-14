from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import os
from typing import Any


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _basename(path_or_name: Any) -> str:
    text = _safe_text(path_or_name)
    if not text:
        return ""
    return os.path.basename(text)


def _source_kind_from_values(source_type: Any, mime_type: Any = "", file_path: Any = "") -> str:
    source_type_text = _safe_text(source_type).lower()
    mime_text = _safe_text(mime_type).lower()
    file_text = _safe_text(file_path).lower()

    if source_type_text in ("email_message",):
        return "email"
    if source_type_text in ("mail_render_screenshot",):
        return "screenshot"
    if source_type_text in ("manual_text", "text_note"):
        return "manual"
    if "pdf" in mime_text or file_text.endswith(".pdf") or source_type_text == "mail_attachment":
        return "pdf"
    if source_type_text in ("document_file",):
        return "document"
    return "unknown"


@dataclass
class ChangeSourceMeta:
    source_kind: str = "unknown"
    source_reference: str = ""
    document_context: dict[str, Any] = field(default_factory=dict)
    raw_context: dict[str, Any] = field(default_factory=dict)
    observed_at: str = field(default_factory=_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ChangeProposal:
    entity_type: str
    entity_identifier: str
    field_key: str
    field_label: str = ""
    old_value: Any = None
    new_value: Any = None
    change_kind: str = "unchanged"
    source_kind: str = "unknown"
    source_reference: str = ""
    document_context: dict[str, Any] = field(default_factory=dict)
    raw_context: dict[str, Any] = field(default_factory=dict)
    user_decision: str | None = None
    created_at: str = field(default_factory=_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_source_meta_from_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    source_rows = list(payload.get("_scan_sources", []) or [])
    primary_source_type = _safe_text(payload.get("_primary_scan_source_type", ""))
    primary_file_path = _safe_text(payload.get("_primary_scan_file_path", ""))

    primary_row = {}
    if source_rows and primary_source_type:
        for row in source_rows:
            if _safe_text(row.get("source_type", "")).lower() == primary_source_type.lower():
                primary_row = dict(row)
                break
    if not primary_row and source_rows:
        primary_row = dict(source_rows[0])

    source_type = primary_source_type or primary_row.get("source_type", "")
    mime_type = primary_row.get("mime_type", "")
    source_kind = _source_kind_from_values(source_type, mime_type=mime_type, file_path=primary_file_path)

    reference = (
        _basename(primary_file_path)
        or _safe_text(primary_row.get("original_name", ""))
        or _safe_text(payload.get("_email_sender", ""))
        or _safe_text(payload.get("bestellnummer", ""))
    )

    document_context = {
        "origin_module": _safe_text(payload.get("_origin_module", "")),
        "primary_source_type": _safe_text(source_type),
        "primary_file_path": primary_file_path,
        "bestellnummer": _safe_text(payload.get("bestellnummer", "")),
        "shop_name": _safe_text(payload.get("shop_name", "")),
        "email_sender": _safe_text(payload.get("_email_sender", "")),
        "email_sender_domain": _safe_text(payload.get("_email_sender_domain", "")),
    }
    document_context = {key: value for key, value in document_context.items() if value}

    raw_context = {}
    if primary_row:
        raw_context["primary_source"] = primary_row
    if source_rows:
        raw_context["scan_sources"] = source_rows[:4]

    metadata = {}
    if payload.get("_email_date"):
        metadata["email_date"] = _safe_text(payload.get("_email_date"))
    if payload.get("_email_sender_domain"):
        metadata["email_sender_domain"] = _safe_text(payload.get("_email_sender_domain"))
    if payload.get("_mail_tracking_links"):
        metadata["tracking_link_count"] = len(payload.get("_mail_tracking_links", []) or [])

    return ChangeSourceMeta(
        source_kind=source_kind,
        source_reference=reference,
        document_context=document_context,
        raw_context=raw_context,
        metadata=metadata,
    ).to_dict()


def summarize_change_counts(changes: list[dict[str, Any]] | None) -> dict[str, int]:
    summary = {
        "add": 0,
        "overwrite": 0,
        "unchanged": 0,
        "item_add": 0,
        "item_update": 0,
    }
    for change in changes or []:
        kind = _safe_text((change or {}).get("change_kind", ""))
        if kind in summary:
            summary[kind] += 1
    summary["changed_total"] = summary["add"] + summary["overwrite"] + summary["item_add"] + summary["item_update"]
    summary["total"] = summary["changed_total"] + summary["unchanged"]
    return summary


def format_review_value(value: Any) -> str:
    if value in (None, ""):
        return "-"
    if hasattr(value, "strftime"):
        try:
            return value.strftime("%Y-%m-%d")
        except Exception:
            pass
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def change_kind_label(change_kind: Any) -> str:
    kind = _safe_text(change_kind).lower()
    mapping = {
        "add": "Neu fuellt leer",
        "overwrite": "Aendert Wert",
        "unchanged": "Schon gleich",
        "item_add": "Neue Positionsinfo",
        "item_update": "Aendert Positionswert",
    }
    return mapping.get(kind, kind or "Unbekannt")


def entity_type_label(entity_type: Any) -> str:
    entity = _safe_text(entity_type).lower()
    mapping = {
        "order_head": "Bestellung",
        "order_item": "Position",
    }
    return mapping.get(entity, entity or "Unbekannt")


def source_label(source_kind: Any, source_reference: Any = "") -> str:
    kind = _safe_text(source_kind).lower()
    label_map = {
        "email": "E-Mail",
        "pdf": "PDF",
        "screenshot": "Screenshot",
        "manual": "Manuell",
        "document": "Dokument",
        "unknown": "Unbekannt",
    }
    base = label_map.get(kind, kind or "Unbekannt")
    ref = _safe_text(source_reference)
    if ref:
        return f"{base}: {ref}"
    return base


def format_change_line(change: dict[str, Any] | None) -> str:
    change = change if isinstance(change, dict) else {}
    label = _safe_text(change.get("field_label", "")) or _safe_text(change.get("field_key", "")) or "Wert"
    old_text = format_review_value(change.get("old_value"))
    new_text = format_review_value(change.get("new_value"))
    kind = _safe_text(change.get("change_kind", ""))
    if kind in ("add", "item_add"):
        return f"{label}: {new_text}"
    if kind in ("overwrite", "item_update"):
        return f"{label}: {old_text} -> {new_text}"
    return f"{label}: unveraendert {new_text}"
