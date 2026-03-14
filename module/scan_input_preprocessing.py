from __future__ import annotations

from dataclasses import dataclass, field
from email.utils import parseaddr
from html.parser import HTMLParser
import logging
import mimetypes
import os
import re
from typing import Any

from PyQt6.QtGui import QImage

from module.media.media_keys import extract_sender_domain
from module.scan_planner import plan_scan_from_context
from module.scan_profile_catalog import ScanDecision

try:
    from pypdf import PdfReader as _PdfReader
except Exception:
    try:
        from PyPDF2 import PdfReader as _PdfReader
    except Exception:
        _PdfReader = None


TRACKING_KEYWORDS = (
    "track",
    "tracking",
    "sendung",
    "verfolgen",
    "shipment",
    "liefer",
    "dispatch",
    "dhl",
    "ups",
    "hermes",
    "gls",
    "dpd",
    "fedex",
)

PDF_POSITIVE_KEYWORDS = (
    "rechnung",
    "invoice",
    "rechnungsnummer",
    "bestellung",
    "bestellbestaetigung",
    "bestell",
    "order",
    "order confirmation",
    "order number",
    "receipt",
    "beleg",
    "zahlung",
    "payment",
    "kauf",
    "purchase",
    "auftragsbestaetigung",
    "zahlungsbestaetigung",
    "artikelliste",
    "betrag",
    "steuer",
    "vat",
    "versand",
    "lieferadresse",
    "rechnungsadresse",
)

PDF_NEGATIVE_KEYWORDS = (
    "agb",
    "terms",
    "conditions",
    "privacy",
    "datenschutz",
    "widerruf",
    "impressum",
    "informationen",
    "legal",
    "return policy",
    "cancellation",
    "bedingungen",
    "return label",
    "retoure",
    "return",
    "newsletter",
)

MAIL_POSITIVE_KEYWORDS = (
    "bestellung",
    "bestellbestaetigung",
    "order confirmation",
    "order number",
    "bestellnummer",
    "invoice",
    "rechnung",
    "receipt",
    "versand",
    "lieferadresse",
    "rechnungsadresse",
    "betrag",
    "summe",
    "total",
    "subtotal",
    "mwst",
    "vat",
)

AMOUNT_BLOCK_KEYWORDS = (
    "gesamt",
    "summe",
    "subtotal",
    "total",
    "betrag",
    "mwst",
    "steuer",
    "vat",
)

ADDRESS_BLOCK_KEYWORDS = (
    "lieferadresse",
    "rechnungsadresse",
    "shipping address",
    "billing address",
    "street",
    "plz",
    "zip",
    "city",
)

ITEM_TABLE_KEYWORDS = (
    "artikel",
    "produkt",
    "menge",
    "anzahl",
    "einzelpreis",
    "gesamtpreis",
    "sku",
)

ORDER_ENTRY_OVERVIEW_KEYWORDS = (
    "bestell",
    "order",
    "orders",
    "uebersicht",
    "overview",
    "konto",
    "account",
    "amazon",
    "shop",
)

ORDER_ENTRY_DETAIL_KEYWORDS = (
    "order detail",
    "order details",
    "bestelldetail",
    "bestelldetails",
    "order summary",
    "dispatch to",
    "payment method",
    "track package",
    "tracking",
    "invoice",
    "order no",
    "order number",
    "placed by",
    "lieferung",
    "lieferstatus",
    "versand",
)

URL_RE = re.compile(r"https?://[^\s<>'\"]+", re.IGNORECASE)
ORDER_NUMBER_RE = re.compile(r"\b[a-z]{1,4}-?\d{3,}[a-z0-9-]*\b", re.IGNORECASE)
AMOUNT_RE = re.compile(r"\b\d{1,4}(?:[.,]\d{2})\s?(?:eur|euro)\b", re.IGNORECASE)


@dataclass
class ScanSource:
    source_type: str
    origin_module: str
    file_path: str = ""
    original_name: str = ""
    mime_type: str = ""
    scan_mode: str = "einkauf"
    metadata: dict[str, Any] = field(default_factory=dict)
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderEntryScanContext:
    source_channel: str = "order_entry"
    user_mode: str = "einkauf"
    file_path: str = ""
    file_name: str = ""
    mime_type: str = ""
    input_kind: str = "other"
    custom_text: str = ""
    primary_candidate: dict[str, Any] = field(default_factory=dict)
    visible_context_hints: list[str] = field(default_factory=list)
    document_guess: str = ""
    suggested_profile_name: str = ""
    source_reasoning_summary: str = ""
    context_flags: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_channel": str(self.source_channel or "order_entry"),
            "user_mode": str(self.user_mode or "einkauf"),
            "file_path": str(self.file_path or ""),
            "file_name": str(self.file_name or ""),
            "mime_type": str(self.mime_type or ""),
            "input_kind": str(self.input_kind or "other"),
            "custom_text": str(self.custom_text or ""),
            "primary_candidate": dict(self.primary_candidate or {}),
            "visible_context_hints": list(self.visible_context_hints or []),
            "document_guess": str(self.document_guess or ""),
            "suggested_profile_name": str(self.suggested_profile_name or ""),
            "source_reasoning_summary": str(self.source_reasoning_summary or ""),
            "context_flags": dict(self.context_flags or {}),
        }


@dataclass
class MailScanContext:
    source_channel: str = "mail"
    user_mode: str = "einkauf"
    mail_id: str = ""
    sender_domain: str = ""
    sender_name: str = ""
    sender_email: str = ""
    subject: str = ""
    mail_date: str = ""
    body_text_hint: str = ""
    html_summary: str = ""
    screenshot_path: str = ""
    screenshot_asset_id: int | None = None
    tracking_links: list[dict[str, Any]] = field(default_factory=list)
    image_hints: list[dict[str, Any]] = field(default_factory=list)
    logo_hints: list[dict[str, Any]] = field(default_factory=list)
    attachment_candidates: list[dict[str, Any]] = field(default_factory=list)
    pdf_candidates: list[dict[str, Any]] = field(default_factory=list)
    primary_candidates: list[dict[str, Any]] = field(default_factory=list)
    secondary_candidates: list[dict[str, Any]] = field(default_factory=list)
    visible_context_hints: list[str] = field(default_factory=list)
    source_plan: dict[str, Any] = field(default_factory=dict)
    selected_profile_name: str = ""
    source_reasoning_summary: str = ""
    should_allow_second_pass: bool = False
    context_flags: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_channel": str(self.source_channel or "mail"),
            "user_mode": str(self.user_mode or "einkauf"),
            "mail_id": str(self.mail_id or ""),
            "sender_domain": str(self.sender_domain or ""),
            "sender_name": str(self.sender_name or ""),
            "sender_email": str(self.sender_email or ""),
            "subject": str(self.subject or ""),
            "mail_date": str(self.mail_date or ""),
            "body_text_hint": str(self.body_text_hint or ""),
            "html_summary": str(self.html_summary or ""),
            "screenshot_path": str(self.screenshot_path or ""),
            "screenshot_asset_id": self.screenshot_asset_id,
            "tracking_links": [dict(row or {}) for row in list(self.tracking_links or [])],
            "image_hints": [dict(row or {}) for row in list(self.image_hints or [])],
            "logo_hints": [dict(row or {}) for row in list(self.logo_hints or [])],
            "attachment_candidates": [dict(row or {}) for row in list(self.attachment_candidates or [])],
            "pdf_candidates": [dict(row or {}) for row in list(self.pdf_candidates or [])],
            "primary_candidates": [dict(row or {}) for row in list(self.primary_candidates or [])],
            "secondary_candidates": [dict(row or {}) for row in list(self.secondary_candidates or [])],
            "visible_context_hints": list(self.visible_context_hints or []),
            "source_plan": dict(self.source_plan or {}),
            "selected_profile_name": str(self.selected_profile_name or ""),
            "source_reasoning_summary": str(self.source_reasoning_summary or ""),
            "should_allow_second_pass": bool(self.should_allow_second_pass),
            "context_flags": dict(self.context_flags or {}),
        }


@dataclass
class PreparedScanInput:
    origin_module: str
    scan_mode: str
    sources: list[ScanSource] = field(default_factory=list)
    primary_source: ScanSource | None = None
    secondary_source: ScanSource | None = None
    gemini_image_path: str | None = None
    gemini_custom_text: str = ""
    source_plan: dict[str, Any] = field(default_factory=dict)
    prompt_plan: dict[str, Any] = field(default_factory=dict)
    scan_decision: ScanDecision | None = None
    scan_context: dict[str, Any] = field(default_factory=dict)
    planner_info: dict[str, Any] = field(default_factory=dict)

    def iter_temporary_paths(self, cleanup_stage=None):
        for source in self.sources:
            if not (source.file_path and source.metadata.get("temp_file")):
                continue
            source_stage = str(source.metadata.get("cleanup_stage", "") or "").strip()
            if cleanup_stage:
                if not source_stage or source_stage != cleanup_stage:
                    continue
            yield source.file_path


class _HtmlLinkExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self._current_href = ""
        self._current_text = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        self._current_href = ""
        self._current_text = []
        for key, value in attrs:
            if str(key).lower() == "href":
                self._current_href = str(value or "").strip()
                break

    def handle_data(self, data):
        if self._current_href:
            self._current_text.append(str(data or ""))

    def handle_endtag(self, tag):
        if tag.lower() != "a" or not self._current_href:
            return
        text = _clean_whitespace(" ".join(self._current_text))
        self.links.append({"text": text, "href": self._current_href})
        self._current_href = ""
        self._current_text = []


class _HtmlImageExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.images = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "img":
            return
        row = {"src": "", "alt": "", "title": "", "width": "", "height": ""}
        for key, value in attrs:
            key_text = str(key or "").lower()
            if key_text in row:
                row[key_text] = _clean_whitespace(value)
        if row["src"]:
            self.images.append(row)


def _clean_whitespace(value) -> str:
    return " ".join(str(value or "").split())


def _truncate(value, max_chars=240) -> str:
    text = _clean_whitespace(value)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def _guess_mime_type(file_path, provided="") -> str:
    provided = str(provided or "").strip()
    if provided:
        return provided
    guessed, _ = mimetypes.guess_type(str(file_path or ""))
    return guessed or ""


def _basename(path_value, fallback="") -> str:
    if path_value:
        return os.path.basename(str(path_value))
    return str(fallback or "").strip()


def _read_image_dimensions(file_path) -> tuple[int, int] | None:
    path_text = str(file_path or "").strip()
    if not path_text or not os.path.exists(path_text):
        return None
    image = QImage(path_text)
    if image.isNull():
        return None
    width = int(image.width())
    height = int(image.height())
    if width <= 0 or height <= 0:
        return None
    return width, height



def _split_sender_identity(sender_value):
    name, address = parseaddr(str(sender_value or ""))
    sender_name = _clean_whitespace(name)
    sender_email = _clean_whitespace(address)
    sender_domain = extract_sender_domain(sender_email or str(sender_value or ""))
    return {
        "name": sender_name,
        "email": sender_email,
        "domain": sender_domain,
    }

def _guess_input_kind(file_path="", mime_type="") -> str:
    mime_lower = str(mime_type or "").strip().lower()
    extension = os.path.splitext(str(file_path or ""))[1].lower()
    if mime_lower == "application/pdf" or extension == ".pdf":
        return "pdf"
    if mime_lower.startswith("image/") or extension in {".png", ".jpg", ".jpeg", ".bmp", ".webp", ".gif", ".tif", ".tiff"}:
        return "image"
    if file_path or mime_lower:
        return "other"
    return "text"

def _collect_keyword_hits(text, keywords):
    combined = str(text or "").lower()
    hits = []
    for keyword in tuple(keywords or ()):
        key = str(keyword or "").strip().lower()
        if not key or key not in combined or key in hits:
            continue
        hits.append(key)
    return hits

def _looks_like_tracking_link(text, href) -> bool:
    combined = f"{text or ''} {href or ''}".lower()
    return any(keyword in combined for keyword in TRACKING_KEYWORDS)


def _extract_links_from_html(body_html):
    html_text = str(body_html or "").strip()
    if not html_text:
        return []
    parser = _HtmlLinkExtractor()
    try:
        parser.feed(html_text)
    except Exception:
        return []
    return parser.links


def _extract_images_from_html(body_html):
    html_text = str(body_html or "").strip()
    if not html_text:
        return []
    parser = _HtmlImageExtractor()
    try:
        parser.feed(html_text)
    except Exception:
        return []
    return parser.images


def _extract_links_from_text(body_text):
    text = str(body_text or "")
    links = []
    for match in URL_RE.findall(text):
        links.append({"text": "", "href": match.strip()})
    return links


def _dedupe_links(link_rows, max_items=5):
    seen = set()
    compact = []
    for row in link_rows:
        href = _clean_whitespace(row.get("href", ""))
        text = _truncate(row.get("text", ""), max_chars=120)
        if not href:
            continue
        key = href.lower()
        if key in seen:
            continue
        seen.add(key)
        compact.append(
            {
                "text": text,
                "href": href,
                "is_tracking": _looks_like_tracking_link(text, href),
            }
        )
        if len(compact) >= max_items:
            break
    return compact


def _tracking_links_for_mail(raw_email):
    html_links = _extract_links_from_html(raw_email.get("body_html", ""))
    text_links = _extract_links_from_text(raw_email.get("body_text", ""))
    all_links = _dedupe_links(html_links + text_links, max_items=6)
    tracking = [row for row in all_links if row.get("is_tracking")]
    return tracking[:3], all_links


def _collect_mail_image_hints(raw_email, max_items=4):
    hints = []
    seen = set()
    for row in _extract_images_from_html(raw_email.get("body_html", "")):
        src = _clean_whitespace(row.get("src", ""))
        alt = _truncate(row.get("alt", ""), max_chars=80)
        title = _truncate(row.get("title", ""), max_chars=80)
        combined = f"{src} {alt} {title}".lower()
        if not src or src.lower().startswith("data:"):
            continue
        if any(keyword in combined for keyword in ("logo", "icon", "banner", "pixel", "tracking", "spacer", "header", "footer", "facebook", "instagram", "linkedin", "twitter", "youtube")):
            continue
        width = _clean_whitespace(row.get("width", ""))
        height = _clean_whitespace(row.get("height", ""))
        digits = [int(value) for value in (width, height) if str(value or "").isdigit()]
        if digits and max(digits) <= 48:
            continue
        key = src.lower()
        if key in seen:
            continue
        seen.add(key)
        hints.append(
            {
                "src": src,
                "alt": alt,
                "title": title,
                "likely_product": bool(alt or title or "product" in combined or "artikel" in combined or "produkt" in combined or "item" in combined),
            }
        )
        if len(hints) >= max_items:
            break
    return hints



def _collect_mail_logo_hints(raw_email, max_items=4):
    hints = []
    seen = set()
    for row in _extract_images_from_html(raw_email.get("body_html", "")):
        src = _clean_whitespace(row.get("src", ""))
        alt = _truncate(row.get("alt", ""), max_chars=80)
        title = _truncate(row.get("title", ""), max_chars=80)
        combined = f"{src} {alt} {title}".lower()
        if not src or src.lower().startswith("data:"):
            continue
        if not any(keyword in combined for keyword in ("logo", "brand", "shop", "store", "header")):
            continue
        if any(keyword in combined for keyword in ("pixel", "tracking", "spacer", "facebook", "instagram", "linkedin", "twitter", "youtube")):
            continue
        width = _clean_whitespace(row.get("width", ""))
        height = _clean_whitespace(row.get("height", ""))
        digits = [int(value) for value in (width, height) if str(value or "").isdigit()]
        if digits and max(digits) <= 24:
            continue
        key = src.lower()
        if key in seen:
            continue
        seen.add(key)
        hints.append(
            {
                "src": src,
                "alt": alt,
                "title": title,
            }
        )
        if len(hints) >= max_items:
            break
    return hints
def _strip_html_quick(html_text):
    text = re.sub(r"<[^>]+>", " ", str(html_text or ""))
    return _clean_whitespace(text)


def _extract_pdf_text_snippet(file_path, max_pages=2, max_chars=900):
    if _PdfReader is None:
        return ""
    try:
        reader = _PdfReader(str(file_path))
        parts = []
        page_count = min(len(reader.pages), max_pages)
        for index in range(page_count):
            text = reader.pages[index].extract_text() or ""
            text = _clean_whitespace(text)
            if text:
                parts.append(text)
            if len(" ".join(parts)) >= max_chars:
                break
        return _truncate(" ".join(parts), max_chars=max_chars)
    except Exception:
        return ""


def _inspect_pdf_attachment(file_path):
    result = {
        "text_hint": "",
        "page_count": 0,
        "title": "",
        "subject": "",
    }
    if _PdfReader is None:
        return result
    try:
        reader = _PdfReader(str(file_path))
        result["page_count"] = int(len(reader.pages) or 0)
        metadata = getattr(reader, "metadata", None) or {}
        result["title"] = _clean_whitespace(getattr(metadata, "title", "") or metadata.get("/Title", ""))
        result["subject"] = _clean_whitespace(getattr(metadata, "subject", "") or metadata.get("/Subject", ""))
        snippet = _extract_pdf_text_snippet(file_path, max_pages=2, max_chars=900)
        result["text_hint"] = snippet
    except Exception:
        return result
    return result


def _keyword_hits(text, keywords):
    lower_text = str(text or "").lower()
    return [keyword for keyword in keywords if keyword in lower_text]


def _has_any_keyword(text, keywords):
    return bool(_keyword_hits(text, keywords))


def _score_mail_content(raw_email, has_screenshot=False):
    subject = _clean_whitespace(raw_email.get("subject", ""))
    body_text = _clean_whitespace(raw_email.get("body_text", "")) or _strip_html_quick(raw_email.get("body_html", ""))
    snippet = _truncate(body_text, max_chars=2200)
    combined = " ".join([subject, snippet]).lower()
    score = 0
    reasons = []

    positive_hits = _keyword_hits(combined, MAIL_POSITIVE_KEYWORDS)
    if positive_hits:
        score += min(6, len(set(positive_hits)) + 1)
        reasons.append("Mail enthaelt typische Bestellbegriffe")
    if ORDER_NUMBER_RE.search(combined):
        score += 3
        reasons.append("Mail enthaelt moegliche Bestellnummer")
    if _has_any_keyword(combined, AMOUNT_BLOCK_KEYWORDS) or AMOUNT_RE.search(combined):
        score += 2
        reasons.append("Mail enthaelt Summenblock")
    if _has_any_keyword(combined, ADDRESS_BLOCK_KEYWORDS):
        score += 2
        reasons.append("Mail enthaelt Adresshinweise")
    if _has_any_keyword(combined, ITEM_TABLE_KEYWORDS):
        score += 2
        reasons.append("Mail enthaelt Artikelhinweise")
    if len(snippet) >= 220:
        score += 1
        reasons.append("Mailtext ist ausreichend umfangreich")
    if has_screenshot and snippet:
        score += 1
        reasons.append("Screenshot der Mail verfuegbar")

    return {
        "score": int(score),
        "is_useful": bool(score >= 4 or (has_screenshot and score >= 3)),
        "reason": ", ".join(dict.fromkeys(reasons)) if reasons else "Mail enthaelt wenig klare Bestellhinweise",
        "text_hint": snippet,
    }


def _score_pdf_attachment(raw_email, attachment):
    file_name = _clean_whitespace(attachment.get("original_name", "") or attachment.get("file_path", ""))
    subject = _clean_whitespace(raw_email.get("subject", ""))
    sender = _clean_whitespace(raw_email.get("sender", ""))
    inspection = _inspect_pdf_attachment(attachment.get("file_path", ""))
    snippet = inspection.get("text_hint", "")
    combined = " ".join([file_name, subject, sender, inspection.get("title", ""), inspection.get("subject", ""), snippet]).lower()
    score = 0
    reasons = []

    if str(file_name).lower().endswith(".pdf"):
        score += 1
        reasons.append("PDF-Datei")

    positive_hits = [kw for kw in PDF_POSITIVE_KEYWORDS if kw in combined]
    negative_hits = [kw for kw in PDF_NEGATIVE_KEYWORDS if kw in combined]
    structure_hits = []

    if positive_hits:
        score += min(8, len(set(positive_hits)) * 2)
        reasons.append("typische Belegbegriffe")
    if negative_hits:
        score -= min(10, len(set(negative_hits)) * 3)
        reasons.append("wirkt eher wie Neben-Dokument")
    if ORDER_NUMBER_RE.search(combined):
        score += 3
        reasons.append("enthaelt moegliche Bestellnummer")
    if any(keyword in str(file_name).lower() for keyword in ("invoice", "rechnung", "receipt", "order", "bestell", "beleg")):
        score += 3
        reasons.append("Dateiname wirkt nach Beleg")
    if _has_any_keyword(combined, AMOUNT_BLOCK_KEYWORDS) or AMOUNT_RE.search(combined):
        score += 2
        structure_hits.append("Summenblock")
    if _has_any_keyword(combined, ADDRESS_BLOCK_KEYWORDS):
        score += 2
        structure_hits.append("Adressblock")
    if _has_any_keyword(combined, ITEM_TABLE_KEYWORDS):
        score += 2
        structure_hits.append("Artikelblock")
    if inspection.get("page_count", 0) > 0:
        reasons.append(f"{inspection.get('page_count')} PDF-Seiten")
    if structure_hits:
        reasons.append("enthaelt Belegstruktur")

    classification = "weak_pdf_attachment"
    strong_negative = bool(negative_hits) and not (positive_hits or structure_hits or ORDER_NUMBER_RE.search(combined))
    if strong_negative:
        score -= 4
        classification = "irrelevant_pdf_attachment"
    elif score >= 8:
        classification = "pdf_dominant_candidate"
    elif score >= 4:
        classification = "hybrid_pdf_candidate"

    is_relevant = score >= 3
    reason_text = ", ".join(dict.fromkeys(reasons)) if reasons else "kein klarer Hinweis"
    return {
        "score": score,
        "is_relevant": is_relevant,
        "reason": reason_text,
        "text_hint": snippet,
        "classification": classification,
        "page_count": int(inspection.get("page_count", 0) or 0),
        "metadata_title": str(inspection.get("title", "") or ""),
        "metadata_subject": str(inspection.get("subject", "") or ""),
        "positive_hits": positive_hits,
        "negative_hits": negative_hits,
        "structure_hits": structure_hits,
    }


def _describe_source(source):
    if source is None:
        return None
    return {
        "source_type": str(source.source_type or ""),
        "original_name": str(source.original_name or ""),
        "file_path": str(source.file_path or ""),
        "mime_type": str(source.mime_type or ""),
    }


def _describe_source_candidate(source):
    row = dict(_describe_source(source) or {})
    if not row:
        return {}
    metadata = getattr(source, "metadata", {}) if source is not None else {}
    extras = getattr(source, "extras", {}) if source is not None else {}
    row["temp_file"] = bool(metadata.get("temp_file")) if isinstance(metadata, dict) else False
    row["media_asset_id"] = metadata.get("media_asset_id") if isinstance(metadata, dict) else None
    row["media_key"] = str(metadata.get("media_key", "") or "") if isinstance(metadata, dict) else ""
    if isinstance(extras, dict):
        row["pdf_relevance_score"] = int(extras.get("pdf_relevance_score", 0) or 0)
        row["pdf_is_relevant"] = bool(extras.get("pdf_is_relevant", False))
        row["pdf_classification"] = str(extras.get("pdf_classification", "") or "")
        row["pdf_relevance_reason"] = str(extras.get("pdf_relevance_reason", "") or "")
        row["pdf_text_hint"] = str(extras.get("pdf_text_hint", "") or "")
        row["pdf_page_count"] = int(extras.get("pdf_page_count", 0) or 0)
        row["size_bytes"] = int(extras.get("size_bytes", 0) or 0)
    return row

def _dedupe_candidate_rows(rows, limit=4):
    seen = set()
    result = []
    for row in list(rows or []):
        row_dict = dict(row or {})
        key = (
            str(row_dict.get("source_type", "") or ""),
            str(row_dict.get("file_path", "") or ""),
            str(row_dict.get("original_name", "") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(row_dict)
        if len(result) >= int(limit or 4):
            break
    return result

def _find_source_by_descriptor(sources, descriptor, fallback=None):
    descriptor = descriptor if isinstance(descriptor, dict) else {}
    source_type = str(descriptor.get("source_type", "") or "")
    file_path = str(descriptor.get("file_path", "") or "")
    for source in list(sources or []):
        if str(source.source_type or "") != source_type:
            continue
        if file_path and str(source.file_path or "") != file_path:
            continue
        return source
    return fallback if fallback is not None and source_type == str(getattr(fallback, "source_type", "") or "") else None



def build_order_entry_scan_context(scan_mode, file_path=None, original_name="", mime_type="", custom_text=""):
    mode = str(scan_mode or "einkauf").strip().lower() or "einkauf"
    resolved_path = str(file_path or "")
    resolved_name = _basename(original_name or file_path, "")
    resolved_mime_type = _guess_mime_type(file_path, mime_type)
    input_kind = _guess_input_kind(file_path=file_path, mime_type=resolved_mime_type)
    custom_hint = _clean_whitespace(custom_text)
    image_dimensions = _read_image_dimensions(file_path) if str(resolved_mime_type or "").lower().startswith("image/") else None
    if image_dimensions and mode == "einkauf":
        width_px, height_px = image_dimensions
        logging.info(
            "module1_screenshot_detection_prompt_skipped: mode=%s, file=%s, dimensions=%sx%s, reason=%s",
            mode,
            resolved_name or resolved_path,
            width_px,
            height_px,
            "phase_a_disable_order_entry_detection_hint",
        )
    combined_text = " ".join([
        resolved_name.lower(),
        os.path.splitext(resolved_name)[0].lower(),
        custom_hint.lower(),
    ]).strip()
    overview_hits = _collect_keyword_hits(combined_text, ORDER_ENTRY_OVERVIEW_KEYWORDS)
    detail_hits = _collect_keyword_hits(combined_text, ORDER_ENTRY_DETAIL_KEYWORDS)
    primary_candidate = {
        "source_type": "document_file" if resolved_path else "manual_text",
        "file_path": resolved_path,
        "original_name": resolved_name,
        "mime_type": resolved_mime_type,
    }

    if mode == "verkauf":
        document_guess = "discord_ticket_chat"
        suggested_profile = "discord_ticket_sales"
        reasoning = "Verkaufsmodus erkannt; Discord-/Ticketprofil bleibt die fachliche Hauptwahl."
    elif input_kind == "pdf":
        document_guess = "purchase_document_pdf"
        suggested_profile = "purchase_document_pdf"
        reasoning = "PDF oder Dokument als Hauptquelle erkannt; dokumentenfokussierter Einkaufsfall wird vorbereitet."
    elif input_kind == "image" and detail_hits:
        document_guess = "order_detail_screenshot"
        suggested_profile = "ecommerce_order_detail_visual"
        reasoning = (
            "Bilddatei mit Bestelldetail-Hinweisen erkannt; das Order-Detail-Profil passt fachlich besser "
            "als ein generischer Einkaufs-Screenshot."
        )
    elif input_kind == "image" and overview_hits:
        document_guess = "order_overview_screenshot"
        suggested_profile = "order_overview_visual"
        reasoning = "Screenshot mit Bestell- oder Shop-Uebersicht erkannt; das Uebersichtsprofil wird bevorzugt."
    elif input_kind == "image":
        document_guess = "generic_purchase_visual"
        suggested_profile = "purchase_visual_generic"
        reasoning = "Bilddatei ohne starke Shop- oder PDF-Hinweise erkannt; der neutrale Visual-Fallback bleibt aktiv."
    else:
        document_guess = "generic_order_entry_input"
        suggested_profile = "purchase_visual_generic"
        reasoning = "Kein klarer Spezialfall erkannt; der bestehende generische Einkaufs-Fallback bleibt aktiv."

    visible_context_hints = list(dict.fromkeys((detail_hits[:4] + overview_hits[:4]) if detail_hits or overview_hits else ([input_kind] if input_kind else [])))
    return OrderEntryScanContext(
        source_channel="order_entry",
        user_mode=mode,
        file_path=resolved_path,
        file_name=resolved_name,
        mime_type=resolved_mime_type,
        input_kind=input_kind,
        custom_text=custom_hint,
        primary_candidate=primary_candidate,
        visible_context_hints=visible_context_hints,
        document_guess=document_guess,
        suggested_profile_name=suggested_profile,
        source_reasoning_summary=reasoning,
        context_flags={
            "has_file": bool(resolved_path),
            "has_custom_text": bool(custom_hint),
            "looks_like_order_detail": bool(detail_hits),
            "looks_like_order_overview": bool(overview_hits),
            "detail_keyword_hits": detail_hits,
            "overview_keyword_hits": overview_hits,
            "source_image_width": int(image_dimensions[0]) if image_dimensions else 0,
            "source_image_height": int(image_dimensions[1]) if image_dimensions else 0,
        },
    )



def _build_order_entry_source_plan(scan_context, primary_source=None):
    context = scan_context if isinstance(scan_context, OrderEntryScanContext) else OrderEntryScanContext(**dict(scan_context or {}))
    input_kind = str(context.input_kind or "other")
    source_scan_mode = "text_primary"
    source_classification = "single_text_source"
    if input_kind == "pdf":
        source_scan_mode = "pdf_primary"
        source_classification = "single_document_source"
    elif input_kind == "image":
        source_scan_mode = "visual_primary"
        source_classification = "single_visual_source"
    elif input_kind == "other":
        source_scan_mode = "file_primary"
        source_classification = "single_file_source"
    primary_descriptor = _describe_source(primary_source) if primary_source is not None else dict(context.primary_candidate or {})
    return {
        "scan_mode": source_scan_mode,
        "source_classification": source_classification,
        "primary_visual_source": primary_descriptor,
        "secondary_context_source": {},
        "source_reasoning_summary": str(context.source_reasoning_summary or ""),
    }



def _plan_mail_sources(raw_email, mail_source, screenshot_source, pdf_sources):
    mail_eval = _score_mail_content(raw_email, has_screenshot=bool(screenshot_source and screenshot_source.file_path))
    scored_pdfs = [source for source in list(pdf_sources or []) if isinstance(source, ScanSource)]
    scored_pdfs.sort(key=lambda src: int(src.extras.get("pdf_relevance_score", 0) or 0), reverse=True)
    best_pdf = next(
        (
            source
            for source in scored_pdfs
            if str(source.extras.get("pdf_classification", "") or "") != "irrelevant_pdf_attachment"
            and int(source.extras.get("pdf_relevance_score", 0) or 0) > 0
        ),
        None,
    )
    irrelevant_pdf = next(
        (source for source in scored_pdfs if str(source.extras.get("pdf_classification", "") or "") == "irrelevant_pdf_attachment"),
        None,
    )
    mail_visual_source = screenshot_source or mail_source
    mail_score = int(mail_eval.get("score", 0) or 0)
    pdf_score = int((best_pdf.extras.get("pdf_relevance_score", 0) if best_pdf else 0) or 0)

    source_classification = "mail_dominant"
    source_scan_mode = "mail_primary"
    primary_source = mail_visual_source
    secondary_source = None
    reasoning = []

    if irrelevant_pdf and not best_pdf:
        source_classification = "irrelevant_pdf_attachment"
        reasoning.append(f"PDF '{irrelevant_pdf.original_name}' wurde als Neben-Dokument abgewertet")

    if best_pdf and pdf_score >= max(8, mail_score + 3) and not mail_eval.get("is_useful"):
        source_classification = "pdf_dominant"
        source_scan_mode = "pdf_primary"
        primary_source = best_pdf
        secondary_source = mail_source if mail_eval.get("text_hint") else None
        reasoning.append(f"PDF '{best_pdf.original_name}' enthaelt deutlich staerkere Bestellhinweise als die Mail")
    elif best_pdf and mail_eval.get("is_useful"):
        source_classification = "hybrid"
        source_scan_mode = "hybrid_mail_plus_pdf"
        if pdf_score > mail_score + 1:
            primary_source = best_pdf
            secondary_source = mail_visual_source
            reasoning.append(f"PDF '{best_pdf.original_name}' ist staerker, Mail bleibt Zusatzkontext")
        else:
            primary_source = mail_visual_source
            secondary_source = best_pdf
            reasoning.append("Mail enthaelt sichtbare Bestellinfos und bleibt primaere Quelle")
            reasoning.append(f"PDF '{best_pdf.original_name}' wird als Zusatzkontext mitgenommen")
    elif best_pdf and not mail_eval.get("is_useful"):
        source_classification = "pdf_dominant"
        source_scan_mode = "pdf_primary"
        primary_source = best_pdf
        reasoning.append(f"Mail wirkt inhaltlich schwach, PDF '{best_pdf.original_name}' wird primaer")
    else:
        reasoning.append("Mail bleibt primaer, weil kein staerkeres PDF gefunden wurde")

    reasoning.append(str(mail_eval.get("reason", "") or ""))
    if best_pdf:
        reasoning.append(str(best_pdf.extras.get("pdf_relevance_reason", "") or ""))

    return {
        "source_classification": source_classification,
        "scan_mode": source_scan_mode,
        "primary_visual_source": _describe_source(primary_source),
        "secondary_context_source": _describe_source(secondary_source),
        "source_reasoning_summary": " | ".join([part for part in reasoning if str(part or "").strip()]),
        "mail_score": mail_score,
        "pdf_score": pdf_score,
        "mail_reason": str(mail_eval.get("reason", "") or ""),
        "mail_text_hint": str(mail_eval.get("text_hint", "") or ""),
        "mail_evaluation": dict(mail_eval or {}),
        "has_irrelevant_pdf_attachment": bool(irrelevant_pdf),
    }


def build_mail_scan_context(raw_email, scan_mode="einkauf", mail_source=None, screenshot_source=None, attachment_sources=None, pdf_sources=None, source_plan=None, primary_source=None, secondary_source=None, prompt_plan=None):
    mode = str(scan_mode or "einkauf").strip().lower() or "einkauf"
    raw_email = dict(raw_email or {})
    source_plan = dict(source_plan or {}) if isinstance(source_plan, dict) else {}
    prompt_plan = dict(prompt_plan or {}) if isinstance(prompt_plan, dict) else {}
    sender_identity = _split_sender_identity(raw_email.get("sender", ""))
    body_text = _clean_whitespace(raw_email.get("body_text", ""))
    html_summary = _truncate(_strip_html_quick(raw_email.get("body_html", "")), max_chars=1400)
    mail_text_hint = _truncate(source_plan.get("mail_text_hint", "") or body_text or html_summary, max_chars=1800)
    attachment_sources = [source for source in list(attachment_sources or []) if isinstance(source, ScanSource)]
    pdf_sources = [source for source in list(pdf_sources or []) if isinstance(source, ScanSource)]

    mail_extras = getattr(mail_source, "extras", {}) if isinstance(getattr(mail_source, "extras", {}), dict) else {}
    tracking_links = list(mail_extras.get("tracking_links", []) or [])
    image_hints = list(mail_extras.get("image_hints", []) or [])
    logo_hints = list(mail_extras.get("logo_hints", []) or [])
    screenshot_asset_id = None
    if screenshot_source is not None and isinstance(getattr(screenshot_source, "metadata", {}), dict):
        screenshot_asset_id = screenshot_source.metadata.get("media_asset_id")

    primary_rows = []
    for source in [primary_source, screenshot_source, mail_source, *pdf_sources[:2]]:
        row = _describe_source_candidate(source)
        if row:
            primary_rows.append(row)
    secondary_rows = []
    for source in [secondary_source, mail_source, screenshot_source, *pdf_sources[:3]]:
        row = _describe_source_candidate(source)
        if row:
            secondary_rows.append(row)

    visible_context_hints = []
    for hint in [
        str(source_plan.get("scan_mode", "") or ""),
        str(source_plan.get("source_classification", "") or ""),
        "mail_screenshot" if screenshot_source and screenshot_source.file_path else "",
        "tracking_links" if tracking_links else "",
        "image_hints" if image_hints else "",
        "logo_hints" if logo_hints else "",
        "pdf_candidates" if pdf_sources else "",
        "relevant_pdf" if any(bool(getattr(source, "extras", {}).get("pdf_is_relevant", False)) for source in pdf_sources) else "",
        "mail_text_useful" if bool((source_plan.get("mail_evaluation") or {}).get("is_useful", False)) else "",
    ]:
        if hint and hint not in visible_context_hints:
            visible_context_hints.append(hint)

    return MailScanContext(
        source_channel="mail",
        user_mode=mode,
        mail_id=str(raw_email.get("_pipeline_card_key", "") or raw_email.get("message_id", "") or raw_email.get("mail_id", "") or ""),
        sender_domain=str(sender_identity.get("domain", "") or ""),
        sender_name=str(sender_identity.get("name", "") or ""),
        sender_email=str(sender_identity.get("email", "") or ""),
        subject=_truncate(raw_email.get("subject", ""), max_chars=160),
        mail_date=_truncate(raw_email.get("date", ""), max_chars=120),
        body_text_hint=mail_text_hint,
        html_summary=html_summary,
        screenshot_path=str(getattr(screenshot_source, "file_path", "") or ""),
        screenshot_asset_id=screenshot_asset_id,
        tracking_links=[dict(row or {}) for row in tracking_links[:4]],
        image_hints=[dict(row or {}) for row in image_hints[:4]],
        logo_hints=[dict(row or {}) for row in logo_hints[:4]],
        attachment_candidates=[_describe_source_candidate(source) for source in attachment_sources],
        pdf_candidates=[_describe_source_candidate(source) for source in pdf_sources],
        primary_candidates=_dedupe_candidate_rows(primary_rows, limit=4),
        secondary_candidates=_dedupe_candidate_rows(secondary_rows, limit=4),
        visible_context_hints=visible_context_hints,
        source_plan=source_plan,
        selected_profile_name=str(prompt_plan.get("prompt_class", "") or ""),
        source_reasoning_summary=str(source_plan.get("source_reasoning_summary", "") or ""),
        should_allow_second_pass=bool(secondary_source is not None),
        context_flags={
            "attachment_count": len(attachment_sources),
            "pdf_candidate_count": len(pdf_sources),
            "relevant_pdf_count": sum(1 for source in pdf_sources if bool(getattr(source, "extras", {}).get("pdf_is_relevant", False))),
            "tracking_link_count": len(tracking_links),
            "image_hint_count": len(image_hints),
            "logo_hint_count": len(logo_hints),
            "has_screenshot": bool(screenshot_source and screenshot_source.file_path),
            "has_secondary_source": bool(secondary_source is not None),
            "has_irrelevant_pdf_attachment": bool(source_plan.get("has_irrelevant_pdf_attachment", False)),
            "mail_score": int(source_plan.get("mail_score", 0) or 0),
            "pdf_score": int(source_plan.get("pdf_score", 0) or 0),
        },
    )

def _build_mail_hint(raw_email, tracking_links, source_plan=None, image_hints=None):
    parts = []
    image_hints = list(image_hints or [])
    email_date = _clean_whitespace(raw_email.get("date", ""))
    if email_date:
        parts.append(f"Empfangsdatum: {email_date}.")

    subject = _truncate(raw_email.get("subject", ""), max_chars=120)
    if subject:
        parts.append(f"Betreff: {subject}.")

    if isinstance(source_plan, dict) and source_plan:
        parts.append(f"Quellmodus: {source_plan.get('scan_mode', 'mail_primary')}.")
        reasoning = _clean_whitespace(source_plan.get("source_reasoning_summary", ""))
        if reasoning:
            parts.append(f"Quellentscheidung: {reasoning}.")
        primary_row = source_plan.get("primary_visual_source", {}) if isinstance(source_plan.get("primary_visual_source"), dict) else {}
        secondary_row = source_plan.get("secondary_context_source", {}) if isinstance(source_plan.get("secondary_context_source"), dict) else {}
        if primary_row:
            parts.append(f"Primaerquelle: {_clean_whitespace(primary_row.get('original_name') or primary_row.get('source_type') or 'Quelle')}.")
        if secondary_row:
            parts.append(f"Zusatzquelle: {_clean_whitespace(secondary_row.get('original_name') or secondary_row.get('source_type') or 'Quelle')}.")
        if str(primary_row.get("source_type", "") or "") == "mail_attachment":
            parts.append("Die PDF ist die Hauptquelle fuer den Scan.")
        elif str(source_plan.get("scan_mode", "") or "") == "hybrid_mail_plus_pdf":
            parts.append("Die Mail ist Hauptbild, die PDF liefert Zusatzkontext.")

    if tracking_links:
        parts.append("Moegliche Tracking-Links:")
        for link in tracking_links:
            link_text = _truncate(link.get("text") or "Link", max_chars=60)
            href = _truncate(link.get("href"), max_chars=180)
            parts.append(f"- {link_text} -> {href}")

    if image_hints:
        parts.append("Moegliche Produktbild-Hinweise aus der Mail:")
        for hint in image_hints[:4]:
            label = _truncate(hint.get("alt") or hint.get("title") or "Bild", max_chars=70)
            src = _truncate(hint.get("src"), max_chars=220)
            parts.append(f"- {label} -> {src}")

    return "\n".join(parts).strip()


def _build_mail_text_fallback(raw_email, hint_text):
    body_text = _clean_whitespace(raw_email.get("body_text", ""))
    if not body_text:
        body_text = _strip_html_quick(raw_email.get("body_html", ""))
    body_text = _truncate(body_text, max_chars=2600)
    if hint_text and body_text:
        return hint_text + "\n\nTextauszug:\n" + body_text
    return hint_text or body_text


def prepare_order_entry_scan(scan_mode, file_path=None, original_name="", mime_type="", custom_text=""):
    mode = str(scan_mode or "einkauf").strip().lower() or "einkauf"
    resolved_mime_type = _guess_mime_type(file_path, mime_type)
    source_type = "text_note"
    if file_path:
        source_type = "document_file"

    sources = []
    if file_path:
        sources.append(
            ScanSource(
                source_type=source_type,
                origin_module="modul_order_entry",
                file_path=str(file_path),
                original_name=_basename(original_name or file_path),
                mime_type=resolved_mime_type,
                scan_mode=mode,
                metadata={"temp_file": True, "cleanup_stage": "after_gemini"},
                extras={},
            )
        )

    custom_hint = _clean_whitespace(custom_text)
    if custom_hint:
        sources.append(
            ScanSource(
                source_type="manual_text",
                origin_module="modul_order_entry",
                file_path="",
                original_name="Freitext",
                mime_type="text/plain",
                scan_mode=mode,
                metadata={"temp_file": False},
                extras={"preview": _truncate(custom_hint, max_chars=180)},
            )
        )

    primary_source = sources[0] if sources and sources[0].file_path else None
    scan_context = build_order_entry_scan_context(
        scan_mode=mode,
        file_path=file_path,
        original_name=original_name,
        mime_type=resolved_mime_type,
        custom_text=custom_hint,
    )
    custom_hint = str(scan_context.custom_text or custom_hint or "").strip()
    for source in sources:
        if source.source_type == "manual_text":
            source.extras["preview"] = _truncate(custom_hint, max_chars=180)
            break
    source_plan = _build_order_entry_source_plan(scan_context, primary_source=primary_source)
    planner_result = plan_scan_from_context(
        scan_context.to_dict(),
        scan_mode=mode,
        module_hint="modul_order_entry",
        fallback_source_plan=source_plan,
        fallback_primary_visual_source=dict(source_plan.get("primary_visual_source") or {}),
        fallback_secondary_context_source={},
        fallback_should_allow_second_pass=False,
    )
    prompt_plan = dict(planner_result.prompt_plan or {})
    scan_decision = planner_result.decision
    scan_context.suggested_profile_name = str((prompt_plan or {}).get("prompt_class", "") or scan_context.suggested_profile_name)
    scan_context.context_flags["planner_used_fallback"] = bool(planner_result.used_fallback)
    scan_context.context_flags["planner_rule"] = str(planner_result.planner_rule or "")

    logging.info(
        "Order-Entry-Context: mode=%s, input_kind=%s, guess=%s, profile=%s, hints=%s",
        mode,
        scan_context.input_kind,
        scan_context.document_guess,
        str((prompt_plan or {}).get("prompt_class", "") or ""),
        ", ".join(list(scan_context.visible_context_hints or [])[:4]),
    )

    logging.info(
        "Order-Entry-Planner: rule=%s, fallback=%s, profile=%s",
        str(planner_result.planner_rule or ""),
        bool(planner_result.used_fallback),
        str((prompt_plan or {}).get("prompt_class", "") or ""),
    )

    return PreparedScanInput(
        origin_module="modul_order_entry",
        scan_mode=mode,
        sources=sources,
        primary_source=primary_source,
        secondary_source=None,
        gemini_image_path=str(file_path) if file_path else None,
        gemini_custom_text=custom_hint,
        source_plan=source_plan,
        prompt_plan=prompt_plan,
        scan_decision=scan_decision,
        scan_context=scan_context.to_dict(),
        planner_info=planner_result.to_dict(),
    )

def prepare_mail_scan(raw_email, screenshot_path=None, scan_mode="einkauf"):
    mode = str(scan_mode or "einkauf").strip().lower() or "einkauf"
    raw_email = dict(raw_email or {})
    tracking_links, all_links = _tracking_links_for_mail(raw_email)
    sender_domain = extract_sender_domain(raw_email.get("sender", ""))
    mail_source = ScanSource(
        source_type="email_message",
        origin_module="modul_mail_scraper",
        file_path="",
        original_name=_truncate(raw_email.get("subject", "E-Mail"), max_chars=120),
        mime_type="message/rfc822",
        scan_mode=mode,
        metadata={
            "temp_file": False,
            "sender": _truncate(raw_email.get("sender", ""), max_chars=120),
            "subject": _truncate(raw_email.get("subject", ""), max_chars=120),
            "date": _truncate(raw_email.get("date", ""), max_chars=120),
            "sender_domain": sender_domain,
        },
        extras={
            "tracking_links": tracking_links,
            "link_count": len(all_links),
            "sender_domain": sender_domain,
            "image_hints": _collect_mail_image_hints(raw_email),
            "logo_hints": _collect_mail_logo_hints(raw_email),
        },
    )

    sources = [mail_source]
    screenshot_source = None

    if screenshot_path:
        screenshot_asset_id = raw_email.get("_registered_screenshot_asset_id")
        screenshot_media_key = str(raw_email.get("_registered_screenshot_media_key", "") or "")
        screenshot_is_temp = bool(raw_email.get("_registered_screenshot_temp_file", False) or not screenshot_asset_id)
        screenshot_source = ScanSource(
            source_type="mail_render_screenshot",
            origin_module="modul_mail_scraper",
            file_path=str(screenshot_path),
            original_name=_basename(screenshot_path, "mail_render.png"),
            mime_type=_guess_mime_type(screenshot_path, "image/png"),
            scan_mode=mode,
            metadata={
                "temp_file": screenshot_is_temp,
                "cleanup_stage": "after_gemini" if screenshot_is_temp else "",
                "media_asset_id": screenshot_asset_id,
                "media_key": screenshot_media_key,
            },
            extras={
                "media_asset_id": screenshot_asset_id,
                "media_key": screenshot_media_key,
            },
        )
        sources.append(screenshot_source)

    attachment_sources = []
    any_pdf_sources = []
    for attachment in list(raw_email.get("attachments", []) or []):
        file_path = str(attachment.get("file_path", "") or "").strip()
        if not file_path:
            continue
        mime_type = _guess_mime_type(file_path, attachment.get("mime_type", ""))
        score = {
            "score": 0,
            "is_relevant": False,
            "reason": "kein klarer Hinweis",
            "text_hint": "",
            "classification": "weak_pdf_attachment",
            "page_count": 0,
            "metadata_title": "",
            "metadata_subject": "",
        }
        if str(mime_type).lower() == "application/pdf" or str(file_path).lower().endswith(".pdf"):
            score = _score_pdf_attachment(raw_email, attachment)
        source = ScanSource(
            source_type="mail_attachment",
            origin_module="modul_mail_scraper",
            file_path=file_path,
            original_name=_basename(attachment.get("original_name") or file_path),
            mime_type=mime_type,
            scan_mode=mode,
            metadata={"temp_file": bool(attachment.get("temp_file", False)), "cleanup_stage": "after_review" if attachment.get("temp_file", False) else ""},
            extras={
                "size_bytes": int(attachment.get("size_bytes", 0) or 0),
                "pdf_relevance_score": int(score.get("score", 0) or 0),
                "pdf_is_relevant": bool(score.get("is_relevant", False)),
                "pdf_relevance_reason": str(score.get("reason", "") or ""),
                "pdf_text_hint": str(score.get("text_hint", "") or ""),
                "pdf_classification": str(score.get("classification", "") or ""),
                "pdf_page_count": int(score.get("page_count", 0) or 0),
                "pdf_metadata_title": str(score.get("metadata_title", "") or ""),
                "pdf_metadata_subject": str(score.get("metadata_subject", "") or ""),
            },
        )
        sources.append(source)
        attachment_sources.append(source)
        if str(mime_type).lower() == "application/pdf" or str(file_path).lower().endswith(".pdf"):
            any_pdf_sources.append(source)
            if str(source.extras.get("pdf_classification", "") or "") == "irrelevant_pdf_attachment":
                logging.info(
                    "PDF-Anhang abgewertet: file=%s, reason=%s",
                    source.original_name,
                    str(source.extras.get("pdf_relevance_reason", "") or "kein Grund"),
                )

    any_pdf_sources.sort(key=lambda src: int(src.extras.get("pdf_relevance_score", 0) or 0), reverse=True)

    source_plan = _plan_mail_sources(
        raw_email=raw_email,
        mail_source=mail_source,
        screenshot_source=screenshot_source,
        pdf_sources=any_pdf_sources,
    )
    primary_source = _find_source_by_descriptor(
        sources,
        source_plan.get("primary_visual_source"),
        fallback=mail_source,
    )
    secondary_source = _find_source_by_descriptor(
        sources,
        source_plan.get("secondary_context_source"),
        fallback=mail_source,
    )
    if secondary_source is primary_source:
        secondary_source = None

    hint_text = _build_mail_hint(raw_email, tracking_links, source_plan=source_plan, image_hints=mail_source.extras.get("image_hints", []))
    secondary_pdf_hint = ""
    if secondary_source and secondary_source.source_type == "mail_attachment":
        secondary_pdf_hint = _truncate(secondary_source.extras.get("pdf_text_hint", ""), max_chars=500)
    elif primary_source and primary_source.source_type == "mail_attachment":
        secondary_pdf_hint = _truncate(primary_source.extras.get("pdf_text_hint", ""), max_chars=500)
    if secondary_pdf_hint:
        hint_text = (hint_text + "\n\nPDF-Kurzinhalt:\n" + secondary_pdf_hint).strip()
    if secondary_source and secondary_source.source_type == "email_message":
        mail_text_hint = _truncate(source_plan.get("mail_text_hint", ""), max_chars=1200)
        if mail_text_hint:
            hint_text = (hint_text + "\n\nMail-Kurzinhalt:\n" + mail_text_hint).strip()

    gemini_image_path = primary_source.file_path if primary_source and primary_source.file_path else None
    gemini_custom_text = hint_text if gemini_image_path else _build_mail_text_fallback(raw_email, hint_text)

    scan_context = build_mail_scan_context(
        raw_email,
        scan_mode=mode,
        mail_source=mail_source,
        screenshot_source=screenshot_source,
        attachment_sources=attachment_sources,
        pdf_sources=any_pdf_sources,
        source_plan=source_plan,
        primary_source=primary_source,
        secondary_source=secondary_source,
        prompt_plan={},
    )
    planner_result = plan_scan_from_context(
        scan_context.to_dict(),
        scan_mode=mode,
        module_hint="modul_mail_scraper",
        fallback_source_plan=source_plan,
        fallback_primary_visual_source=dict(source_plan.get("primary_visual_source") or {}),
        fallback_secondary_context_source=dict(source_plan.get("secondary_context_source") or {}),
        fallback_should_allow_second_pass=bool(secondary_source is not None),
    )
    prompt_plan = dict(planner_result.prompt_plan or {})
    scan_decision = planner_result.decision
    scan_context.selected_profile_name = str((prompt_plan or {}).get("prompt_class", "") or scan_context.selected_profile_name)
    scan_context.context_flags["planner_used_fallback"] = bool(planner_result.used_fallback)
    scan_context.context_flags["planner_rule"] = str(planner_result.planner_rule or "")

    logging.info(
        "Mail-Quellplanung: classification=%s, scan_mode=%s, primary=%s, secondary=%s",
        source_plan.get("source_classification", ""),
        source_plan.get("scan_mode", ""),
        str((source_plan.get("primary_visual_source") or {}).get("source_type", "")),
        str((source_plan.get("secondary_context_source") or {}).get("source_type", "")),
    )

    logging.info(
        "Mail-Context: mail_id=%s, profile=%s, source_mode=%s, attachments=%s, pdfs=%s, hints=%s",
        scan_context.mail_id,
        scan_context.selected_profile_name,
        str((scan_context.source_plan or {}).get("scan_mode", "") or ""),
        len(scan_context.attachment_candidates),
        len(scan_context.pdf_candidates),
        ",".join(list(scan_context.visible_context_hints or [])[:5]),
    )

    logging.info(
        "Mail-Planner: rule=%s, fallback=%s, profile=%s, source_mode=%s",
        str(planner_result.planner_rule or ""),
        bool(planner_result.used_fallback),
        str((prompt_plan or {}).get("prompt_class", "") or ""),
        str((source_plan or {}).get("scan_mode", "") or ""),
    )

    return PreparedScanInput(
        origin_module="modul_mail_scraper",
        scan_mode=mode,
        sources=sources,
        primary_source=primary_source,
        secondary_source=secondary_source,
        gemini_image_path=gemini_image_path,
        gemini_custom_text=gemini_custom_text,
        source_plan=source_plan,
        prompt_plan=prompt_plan,
        scan_decision=scan_decision,
        scan_context=scan_context.to_dict(),
        planner_info=planner_result.to_dict(),
    )















