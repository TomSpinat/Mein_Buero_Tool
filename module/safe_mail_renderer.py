"""
safe_mail_renderer.py
Zentrale Sicherheits- und Renderlogik fuer HTML-E-Mails.

Ziele:
- Layout moeglichst erhalten
- keine aktive/unsichere Ausfuehrung
- externe Inhalte standardmaessig blockieren
- lokale cid:-Bilder moeglichst originalgetreu einbetten
- vertrauensbasierte Freigabe fuer Absender/Domain
"""

from __future__ import annotations

import base64
import html
import re
from dataclasses import dataclass
from email.utils import parseaddr

from PyQt6.QtCore import QUrl
from PyQt6.QtWebEngineCore import (
    QWebEnginePage,
    QWebEngineProfile,
    QWebEngineSettings,
    QWebEngineUrlRequestInterceptor,
)

from module.crash_logger import log_mail_scan_trace


TRUSTED_SENDERS_KEY = "trusted_mail_senders"
TRUSTED_DOMAINS_KEY = "trusted_mail_domains"


_BLOCK_TAG_PATTERNS = [
    re.compile(r"<script\b[^>]*>.*?</script>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<iframe\b[^>]*>.*?</iframe>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<frame\b[^>]*>.*?</frame>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<frameset\b[^>]*>.*?</frameset>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<object\b[^>]*>.*?</object>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<embed\b[^>]*>.*?</embed>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<video\b[^>]*>.*?</video>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<audio\b[^>]*>.*?</audio>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<svg\b[^>]*>.*?</svg>", re.IGNORECASE | re.DOTALL),
    re.compile(r"<form\b[^>]*>.*?</form>", re.IGNORECASE | re.DOTALL),
]

_SELF_CLOSING_BLOCKED = [
    re.compile(r"<input\b[^>]*?/?>", re.IGNORECASE),
    re.compile(r"<button\b[^>]*?/?>", re.IGNORECASE),
    re.compile(r"<select\b[^>]*?/?>", re.IGNORECASE),
    re.compile(r"<textarea\b[^>]*?/?>", re.IGNORECASE),
    re.compile(r"<base\b[^>]*?/?>", re.IGNORECASE),
    re.compile(r"<meta\b[^>]*http-equiv\s*=\s*['\"]?refresh['\"]?[^>]*?/?>", re.IGNORECASE),
]

_EVENT_ATTR_RE = re.compile(r"\s+on[a-z0-9_-]+\s*=\s*(?:\"[^\"]*\"|'[^']*'|[^\s>]+)", re.IGNORECASE)
_DANGEROUS_ATTR_RE = re.compile(r"\s+(?:formaction|srcdoc)\s*=\s*(?:\"[^\"]*\"|'[^']*'|[^\s>]+)", re.IGNORECASE)
_IMG_TAG_RE = re.compile(r"<img\b[^>]*>", re.IGNORECASE)
_LINK_TAG_RE = re.compile(r"<link\b[^>]*>", re.IGNORECASE)
_STYLE_BLOCK_RE = re.compile(r"<style\b[^>]*>(.*?)</style>", re.IGNORECASE | re.DOTALL)
_STYLE_URL_RE = re.compile(r"url\((['\"]?)(https?:|//)[^)]+\)", re.IGNORECASE)
_STYLE_IMPORT_RE = re.compile(r"@import\s+url\((['\"]?)(https?:|//)[^)]+\);?", re.IGNORECASE)
_QUOTED_ATTR_TEMPLATE = r"({name}\s*=\s*)(\"[^\"]*\"|'[^']*'|[^\s>]+)"


@dataclass
class SafeMailRenderResult:
    safe_html: str
    sender_email: str
    sender_domain: str
    trusted_sender: bool
    trusted_domain: bool
    allow_remote: bool
    blocked_remote_images: int
    blocked_remote_links: int
    blocked_active_content: int
    used_inline_cid: int

    @property
    def blocked_anything(self) -> bool:
        return bool(
            self.blocked_remote_images
            or self.blocked_remote_links
            or self.blocked_active_content
        )

    @property
    def can_trust_sender(self) -> bool:
        return bool(self.sender_email) and not self.trusted_sender

    @property
    def can_trust_domain(self) -> bool:
        return bool(self.sender_domain) and not self.trusted_domain


class _MailRequestInterceptor(QWebEngineUrlRequestInterceptor):
    def __init__(self, allow_remote=False, parent=None):
        super().__init__(parent)
        self.allow_remote = bool(allow_remote)

    def interceptRequest(self, info):
        url = info.requestUrl()
        scheme = (url.scheme() or "").lower()
        if scheme in ("about", "data"):
            return
        if scheme in ("http", "https"):
            if self.allow_remote:
                return
            info.block(True)
            return
        info.block(True)


class _MailPreviewPage(QWebEnginePage):
    def acceptNavigationRequest(self, url, nav_type, is_main_frame):
        scheme = (url.scheme() or "").lower()
        if scheme in ("about", "data"):
            return True
        return False


class SafeMailRenderer:
    @staticmethod
    def release_view_resources(view):
        if view is None:
            return
        old_page = getattr(view, "_mail_page", None)
        old_profile = getattr(view, "_mail_profile", None)
        old_interceptor = getattr(view, "_mail_interceptor", None)
        try:
            if old_page is not None:
                try:
                    old_page.deleteLater()
                except Exception:
                    pass
            if old_interceptor is not None:
                try:
                    old_interceptor.deleteLater()
                except Exception:
                    pass
            if old_profile is not None:
                try:
                    old_profile.setUrlRequestInterceptor(None)
                except Exception:
                    pass
                try:
                    old_profile.deleteLater()
                except Exception:
                    pass
        finally:
            for attr_name in ("_mail_page", "_mail_profile", "_mail_interceptor"):
                try:
                    setattr(view, attr_name, None)
                except Exception:
                    pass
        log_mail_scan_trace("safe_mail_renderer.SafeMailRenderer", "release_view_resources", extra={"view_class": type(view).__name__})

    @staticmethod
    def extract_sender_identity(sender_text):
        display_name, email_addr = parseaddr(str(sender_text or ""))
        email_addr = (email_addr or "").strip().lower()
        domain = email_addr.split("@", 1)[1] if "@" in email_addr else ""
        return {
            "display_name": (display_name or "").strip(),
            "email": email_addr,
            "domain": domain,
        }

    @classmethod
    def trust_sender(cls, settings_manager, sender_text):
        sender = cls.extract_sender_identity(sender_text)
        email_addr = sender.get("email", "")
        if not email_addr:
            return False
        values = cls._normalized_list(settings_manager.get(TRUSTED_SENDERS_KEY, []))
        if email_addr in values:
            return False
        values.append(email_addr)
        settings_manager.save_setting(TRUSTED_SENDERS_KEY, values)
        return True

    @classmethod
    def trust_domain(cls, settings_manager, sender_text):
        sender = cls.extract_sender_identity(sender_text)
        domain = sender.get("domain", "")
        if not domain:
            return False
        values = cls._normalized_list(settings_manager.get(TRUSTED_DOMAINS_KEY, []))
        if domain in values:
            return False
        values.append(domain)
        settings_manager.save_setting(TRUSTED_DOMAINS_KEY, values)
        return True

    @classmethod
    def prepare_html(
        cls,
        html_text,
        *,
        text_fallback="",
        sender_text="",
        settings_manager=None,
        inline_cid_map=None,
        allow_external=False,
    ):
        sender = cls.extract_sender_identity(sender_text)
        trusted_senders = cls._normalized_list(settings_manager.get(TRUSTED_SENDERS_KEY, [])) if settings_manager else []
        trusted_domains = cls._normalized_list(settings_manager.get(TRUSTED_DOMAINS_KEY, [])) if settings_manager else []
        trusted_sender = sender["email"] in trusted_senders if sender["email"] else False
        trusted_domain = sender["domain"] in trusted_domains if sender["domain"] else False
        allow_remote = bool(allow_external or trusted_sender or trusted_domain)

        raw_html = str(html_text or "").strip()
        if not raw_html:
            fallback_text = str(text_fallback or "").strip()
            if fallback_text:
                raw_html = cls._wrap_plaintext(fallback_text)
            else:
                raw_html = "<html><head><meta charset=\"utf-8\"></head><body><p>Keine Vorschau verfuegbar.</p></body></html>"
        elif not cls._looks_like_html(raw_html):
            raw_html = cls._wrap_plaintext(raw_html)
        elif "<html" not in raw_html.lower():
            raw_html = f"<html><head><meta charset=\"utf-8\"></head><body>{raw_html}</body></html>"

        cid_map = cls._normalize_cid_map(inline_cid_map)
        blocked_active = 0
        blocked_remote_images = 0
        blocked_remote_links = 0
        used_inline_cid = 0

        sanitized = raw_html
        for pattern in _BLOCK_TAG_PATTERNS:
            sanitized, count = pattern.subn("", sanitized)
            blocked_active += count
        for pattern in _SELF_CLOSING_BLOCKED:
            sanitized, count = pattern.subn("", sanitized)
            blocked_active += count

        sanitized, _ = re.subn(r"<meta\b([^>]*charset[^>]*)>", "<meta charset=\"utf-8\">", sanitized, flags=re.IGNORECASE)
        sanitized = _EVENT_ATTR_RE.sub("", sanitized)
        sanitized = _DANGEROUS_ATTR_RE.sub("", sanitized)
        sanitized = re.sub(r"(?i)(href|src)\s*=\s*([\"\']?)\s*(javascript:|vbscript:)[^\"\'>\s]*\2", r"\1=\"#\"", sanitized)
        sanitized = re.sub(r"(?i)(href|src)\s*=\s*([\"\']?)\s*data:(?!image/(?:png|jpeg|jpg|gif|webp|svg\+xml);base64,)[^\"\'>\s]*\2", r"\1=\"#\"", sanitized)

        def replace_img(match):
            nonlocal blocked_remote_images, used_inline_cid
            tag = match.group(0)
            safe_tag = _EVENT_ATTR_RE.sub("", tag)
            safe_tag = _DANGEROUS_ATTR_RE.sub("", safe_tag)
            safe_tag = cls._remove_attr(safe_tag, "srcset")
            src_value = cls._extract_attr(safe_tag, "src")
            if not src_value:
                return safe_tag

            resolved = src_value.strip()
            lower = resolved.lower()
            if lower.startswith("cid:"):
                cid_key = lower[4:].strip("<>").strip().lower()
                cid_data = cid_map.get(cid_key, "")
                if cid_data:
                    used_inline_cid += 1
                    return cls._set_attr(safe_tag, "src", cid_data)
                blocked_remote_images += 1
                return cls._set_blocked_img(safe_tag, resolved, "Eingebettetes Bild fehlt")

            if cls._is_remote_url(resolved):
                if allow_remote:
                    return safe_tag
                blocked_remote_images += 1
                return cls._set_blocked_img(safe_tag, resolved, "Externes Bild blockiert")

            return safe_tag

        sanitized = _IMG_TAG_RE.sub(replace_img, sanitized)

        def replace_link(match):
            nonlocal blocked_remote_links
            tag = match.group(0)
            href = cls._extract_attr(tag, "href")
            safe_tag = _EVENT_ATTR_RE.sub("", tag)
            if href and cls._is_remote_url(href) and not allow_remote:
                blocked_remote_links += 1
                return "<!-- external link resource blocked -->"
            return safe_tag

        sanitized = _LINK_TAG_RE.sub(replace_link, sanitized)

        def replace_style_block(match):
            nonlocal blocked_remote_links
            content = match.group(1)
            if allow_remote:
                return f"<style>{content}</style>"
            new_content, imports = _STYLE_IMPORT_RE.subn("", content)
            new_content, urls = _STYLE_URL_RE.subn("none", new_content)
            blocked_remote_links += imports + urls
            return f"<style>{new_content}</style>"

        sanitized = _STYLE_BLOCK_RE.sub(replace_style_block, sanitized)
        if not allow_remote:
            sanitized, style_urls = re.subn(
                r"style\s*=\s*([\"\'])(.*?)\1",
                cls._sanitize_style_attr,
                sanitized,
                flags=re.IGNORECASE | re.DOTALL,
            )
            blocked_remote_links += style_urls

        if "<head" not in sanitized.lower():
            sanitized = sanitized.replace("<html>", "<html><head><meta charset=\"utf-8\"></head>", 1)
        elif "charset" not in sanitized.lower():
            sanitized = sanitized.replace("<head>", "<head><meta charset=\"utf-8\">", 1)

        return SafeMailRenderResult(
            safe_html=sanitized,
            sender_email=sender["email"],
            sender_domain=sender["domain"],
            trusted_sender=trusted_sender,
            trusted_domain=trusted_domain,
            allow_remote=allow_remote,
            blocked_remote_images=blocked_remote_images,
            blocked_remote_links=blocked_remote_links,
            blocked_active_content=blocked_active,
            used_inline_cid=used_inline_cid,
        )

    @classmethod
    def apply_to_view(cls, view, render_result):
        cls.release_view_resources(view)
        log_mail_scan_trace(
            "safe_mail_renderer.SafeMailRenderer",
            "apply_to_view",
            extra={
                "view_class": type(view).__name__,
                "allow_remote": bool(render_result.allow_remote),
                "blocked_remote_images": int(render_result.blocked_remote_images or 0),
                "blocked_remote_links": int(render_result.blocked_remote_links or 0),
                "blocked_active_content": int(render_result.blocked_active_content or 0),
            },
        )
        profile = QWebEngineProfile(view)
        interceptor = _MailRequestInterceptor(allow_remote=render_result.allow_remote, parent=profile)
        profile.setUrlRequestInterceptor(interceptor)
        page = _MailPreviewPage(profile, view)
        cls._configure_page(page, allow_remote=render_result.allow_remote)
        view.setPage(page)
        view._mail_profile = profile
        view._mail_interceptor = interceptor
        view._mail_page = page
        view.setHtml(render_result.safe_html, QUrl("about:blank"))

    @staticmethod
    def build_notice_text(render_result):
        if not render_result.blocked_anything and render_result.used_inline_cid:
            return "Sichere Vorschau mit lokalen eingebetteten Bildern."
        if not render_result.blocked_anything:
            return "Sichere Vorschau ohne externe Nachladung."

        parts = []
        if render_result.blocked_remote_images:
            parts.append(f"{render_result.blocked_remote_images} externes Bild/Bilder blockiert")
        if render_result.blocked_remote_links:
            parts.append("externe Styles/Bilder-URLs blockiert")
        if render_result.blocked_active_content:
            parts.append("aktive Inhalte entfernt")
        if render_result.used_inline_cid:
            parts.append(f"{render_result.used_inline_cid} lokale eingebettete Bilder angezeigt")
        return "; ".join(parts) + "."

    @staticmethod
    def _configure_page(page, allow_remote=False):
        settings = page.settings()
        settings.setAttribute(QWebEngineSettings.WebAttribute.JavascriptEnabled, False)
        settings.setAttribute(QWebEngineSettings.WebAttribute.PluginsEnabled, False)
        settings.setAttribute(QWebEngineSettings.WebAttribute.FullScreenSupportEnabled, False)
        settings.setAttribute(QWebEngineSettings.WebAttribute.LocalStorageEnabled, False)
        settings.setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessFileUrls, False)
        settings.setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, bool(allow_remote))
        settings.setAttribute(QWebEngineSettings.WebAttribute.HyperlinkAuditingEnabled, False)
        settings.setAttribute(QWebEngineSettings.WebAttribute.ErrorPageEnabled, False)
        settings.setAttribute(QWebEngineSettings.WebAttribute.PdfViewerEnabled, False)
        try:
            settings.setUnknownUrlSchemePolicy(QWebEngineSettings.UnknownUrlSchemePolicy.DisallowUnknownUrlSchemes)
        except Exception:
            pass

    @staticmethod
    def _looks_like_html(text):
        lower = str(text or "").lower()
        return any(token in lower for token in ("<html", "<body", "<table", "<div", "<img", "<style", "<!doctype"))

    @staticmethod
    def _wrap_plaintext(text):
        escaped = html.escape(str(text or ""))
        return (
            "<html><head><meta charset=\"utf-8\"></head>"
            "<body><pre style='white-space: pre-wrap; font-family: Segoe UI, sans-serif;'>"
            f"{escaped}</pre></body></html>"
        )

    @staticmethod
    def _normalized_list(values):
        items = []
        for value in values or []:
            text = str(value or "").strip().lower()
            if text and text not in items:
                items.append(text)
        return items

    @staticmethod
    def _normalize_cid_map(cid_map):
        result = {}
        for key, value in (cid_map or {}).items():
            key_text = str(key or "").strip().strip("<>").lower()
            val_text = str(value or "").strip()
            if key_text and val_text:
                result[key_text] = val_text
        return result

    @staticmethod
    def _is_remote_url(value):
        text = str(value or "").strip()
        return bool(re.match(r"^(https?:)?//", text, flags=re.IGNORECASE) or text.lower().startswith("http:")) or text.lower().startswith("https:")

    @staticmethod
    def _extract_attr(tag, attr_name):
        pattern = re.compile(_QUOTED_ATTR_TEMPLATE.format(name=re.escape(attr_name)), re.IGNORECASE)
        match = pattern.search(tag)
        if not match:
            return ""
        raw_value = match.group(2).strip()
        if raw_value.startswith(('"', "'")) and raw_value.endswith(('"', "'")):
            raw_value = raw_value[1:-1]
        return raw_value

    @staticmethod
    def _remove_attr(tag, attr_name):
        pattern = re.compile(rf"\s+{re.escape(attr_name)}\s*=\s*(?:\"[^\"]*\"|'[^']*'|[^\s>]+)", re.IGNORECASE)
        return pattern.sub("", tag)

    @staticmethod
    def _set_attr(tag, attr_name, value):
        value_text = html.escape(str(value or ""), quote=True)
        pattern = re.compile(rf"({re.escape(attr_name)}\s*=\s*)(\"[^\"]*\"|'[^']*'|[^\s>]+)", re.IGNORECASE)
        replacement = rf"\1\"{value_text}\""
        if pattern.search(tag):
            return pattern.sub(replacement, tag, count=1)
        insert_at = tag.rfind(">")
        if insert_at == -1:
            return tag
        return f"{tag[:insert_at]} {attr_name}=\"{value_text}\"{tag[insert_at:]}"

    @classmethod
    def _set_blocked_img(cls, tag, original_src, label):
        placeholder = cls._placeholder_data_uri(label)
        tag = cls._set_attr(tag, "src", placeholder)
        tag = cls._set_attr(tag, "alt", label)
        tag = cls._set_attr(tag, "data-original-src", original_src)
        tag = cls._set_attr(tag, "data-remote-blocked", "1")
        return tag

    @staticmethod
    def _placeholder_data_uri(label):
        text = html.escape(str(label or "Blockiert"))
        svg = (
            "<svg xmlns='http://www.w3.org/2000/svg' width='640' height='180' viewBox='0 0 640 180'>"
            "<rect width='100%' height='100%' fill='#1f2335' stroke='#565f89' stroke-dasharray='8 6'/><text x='50%' y='50%' dominant-baseline='middle' text-anchor='middle' fill='#c0caf5' font-size='20' font-family='Segoe UI, Arial'>"
            f"{text}</text></svg>"
        )
        encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
        return f"data:image/svg+xml;base64,{encoded}"

    @staticmethod
    def _sanitize_style_attr(match):
        quote = match.group(1)
        content = match.group(2)
        new_content = _STYLE_URL_RE.sub("none", content)
        return f"style={quote}{new_content}{quote}"
