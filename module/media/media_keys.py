"""Deterministische Schluessel fuer Medienzuordnungen."""

from __future__ import annotations

import hashlib
import re
from email.utils import parseaddr


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def normalize_key_part(value) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " und ")
    text = text.replace("@", " at ")
    text = _NON_ALNUM_RE.sub("-", text)
    text = text.strip("-")
    return text


def normalize_sender_domain(value) -> str:
    domain = str(value or "").strip().lower()
    if not domain:
        return ""
    if "@" in domain:
        domain = domain.split("@", 1)[1]
    domain = domain.strip().strip(".")
    domain = re.sub(r"[^a-z0-9.-]+", "", domain)
    return domain


def extract_sender_domain(sender_text) -> str:
    _display_name, email_addr = parseaddr(str(sender_text or ""))
    return normalize_sender_domain(email_addr)


def build_shop_key(shop_name="", sender_domain="") -> str:
    normalized_shop = normalize_key_part(shop_name)
    if normalized_shop:
        return f"shop:{normalized_shop}"

    normalized_domain = normalize_sender_domain(sender_domain)
    if normalized_domain:
        return f"shop-domain:{normalize_key_part(normalized_domain)}"
    return "shop:unknown"


def shop_key_to_storage_name(shop_key) -> str:
    normalized = normalize_key_part(str(shop_key or "").replace(":", "-"))
    return normalized or "shop-unknown"


def normalize_ean_text(value) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    if digits:
        return digits
    return str(value or "").strip()


def build_product_key(product_name="", ean="", variant_text="") -> str:
    ean_text = normalize_ean_text(ean)
    if ean_text and ean_text.isdigit():
        return f"ean:{ean_text}"

    name_part = normalize_key_part(product_name)
    variant_part = normalize_key_part(variant_text)
    combined = "|".join(part for part in (name_part, variant_part) if part)
    if combined:
        return f"product:{combined}"

    digest = hashlib.sha1(f"{product_name}|{variant_text}|{ean}".encode("utf-8")).hexdigest()[:16]
    return f"product:unknown:{digest}"


def product_key_to_storage_name(product_key) -> str:
    normalized = normalize_key_part(str(product_key or "").replace(":", "-").replace("|", "-"))
    return normalized or "product-unknown"


def build_media_key(media_type, sha256="", source_ref="", original_name="") -> str:
    media_type_text = normalize_key_part(media_type) or "asset"
    digest_source = str(sha256 or "").strip().lower()
    if not digest_source:
        digest_source = "|".join(
            str(part or "").strip().lower()
            for part in (media_type_text, source_ref, original_name)
        )
        digest_source = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()
    return f"{media_type_text}:{digest_source}"
