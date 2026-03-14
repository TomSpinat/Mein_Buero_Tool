from __future__ import annotations

from dataclasses import dataclass, field
from html.parser import HTMLParser
import mimetypes
import os
import re
from typing import Any


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

URL_RE = re.compile(r"https?://[^\s<>'\"]+", re.IGNORECASE)


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
class PreparedScanInput:
    origin_module: str
    scan_mode: str
    sources: list[ScanSource] = field(default_factory=list)
    primary_source: ScanSource | None = None
    gemini_image_path: str | None = None
    gemini_custom_text: str = ""

    def iter_temporary_paths(self):
        for source in self.sources:
            if source.file_path and source.metadata.get("temp_file"):
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


def _strip_html_quick(html_text):
    text = re.sub(r"<[^>]+>", " ", str(html_text or ""))
    return _clean_whitespace(text)


def _build_mail_hint(raw_email, tracking_links):
    parts = []
    email_date = _clean_whitespace(raw_email.get("date", ""))
    if email_date:
        parts.append(f"Empfangsdatum: {email_date}.")

    subject = _truncate(raw_email.get("subject", ""), max_chars=120)
    if subject:
        parts.append(f"Betreff: {subject}.")

    if tracking_links:
        parts.append("Moegliche Tracking-Links:")
        for link in tracking_links:
            link_text = _truncate(link.get("text") or "Link", max_chars=60)
            href = _truncate(link.get("href"), max_chars=180)
            parts.append(f"- {link_text} -> {href}")

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
                mime_type=_guess_mime_type(file_path, mime_type),
                scan_mode=mode,
                metadata={"temp_file": True},
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
    return PreparedScanInput(
        origin_module="modul_order_entry",
        scan_mode=mode,
        sources=sources,
        primary_source=primary_source,
        gemini_image_path=str(file_path) if file_path else None,
        gemini_custom_text=custom_hint,
    )


def prepare_mail_scan(raw_email, screenshot_path=None, scan_mode="einkauf"):
    mode = str(scan_mode or "einkauf").strip().lower() or "einkauf"
    raw_email = dict(raw_email or {})
    tracking_links, all_links = _tracking_links_for_mail(raw_email)
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
        },
        extras={
            "tracking_links": tracking_links,
            "link_count": len(all_links),
        },
    )

    sources = [mail_source]

    if screenshot_path:
        sources.append(
            ScanSource(
                source_type="mail_render_screenshot",
                origin_module="modul_mail_scraper",
                file_path=str(screenshot_path),
                original_name=_basename(screenshot_path, "mail_render.png"),
                mime_type=_guess_mime_type(screenshot_path, "image/png"),
                scan_mode=mode,
                metadata={"temp_file": True},
                extras={},
            )
        )

    for attachment in list(raw_email.get("attachments", []) or []):
        file_path = str(attachment.get("file_path", "") or "").strip()
        if not file_path:
            continue
        sources.append(
            ScanSource(
                source_type="mail_attachment",
                origin_module="modul_mail_scraper",
                file_path=file_path,
                original_name=_basename(attachment.get("original_name") or file_path),
                mime_type=_guess_mime_type(file_path, attachment.get("mime_type", "")),
                scan_mode=mode,
                metadata={"temp_file": bool(attachment.get("temp_file", False))},
                extras={"size_bytes": int(attachment.get("size_bytes", 0) or 0)},
            )
        )

    primary_source = None
    for source in sources:
        if source.source_type == "mail_render_screenshot" and source.file_path:
            primary_source = source
            break
    if primary_source is None:
        for source in sources:
            if source.source_type == "mail_attachment" and source.file_path:
                primary_source = source
                break

    hint_text = _build_mail_hint(raw_email, tracking_links)
    gemini_image_path = primary_source.file_path if primary_source else None
    gemini_custom_text = hint_text if gemini_image_path else _build_mail_text_fallback(raw_email, hint_text)

    return PreparedScanInput(
        origin_module="modul_mail_scraper",
        scan_mode=mode,
        sources=sources,
        primary_source=primary_source,
        gemini_image_path=gemini_image_path,
        gemini_custom_text=gemini_custom_text,
    )
