"""Zentrale Medien-API fuer lokale Assets und ihre Verknuepfungen."""

from __future__ import annotations

import logging
import mimetypes
import os
import ssl
import urllib.request
from pathlib import Path
from urllib.parse import urljoin, urlparse
from PyQt6.QtGui import QImage

from module.module1_trace_logger import write_module1_trace
from module.crash_logger import log_exception
from module.media.media_cropper import MediaCropper
from module.media.media_keys import (
    build_media_key,
    build_product_key,
    build_shop_key,
    extract_sender_domain,
    normalize_ean_text,
    normalize_sender_domain,
    product_key_to_storage_name,
    shop_key_to_storage_name,
)
from module.media.media_store import LocalMediaStore
from module.order_visual_state import OrderVisualState


class MediaService:
    DETECTION_PRIMARY_CONFIDENCE = 0.92
    DETECTION_PRIMARY_PRIORITY = 40
    DETECTION_CANDIDATE_PRIORITY = 120

    ORDER_ITEM_STATE_CANDIDATE = "candidate"
    ORDER_ITEM_STATE_SELECTED_AUTO = "selected_auto"
    ORDER_ITEM_STATE_SELECTED_MANUAL = "selected_manual"
    ORDER_ITEM_STATE_REJECTED = "rejected"

    def __init__(self, db_manager, media_store=None):
        self.db = db_manager
        self.store = media_store or LocalMediaStore()
        self.store.ensure_structure()

    def ensure_storage(self):
        return self.store.ensure_structure()

    def register_local_asset(
        self,
        source_path,
        media_type,
        bucket,
        preferred_name="",
        source_module="",
        source_kind="",
        source_ref="",
        source_url="",
        metadata=None,
    ):
        try:
            stored = self.store.ingest_file(source_path, bucket=bucket, preferred_name=preferred_name)
            media_key = build_media_key(
                media_type=media_type,
                sha256=stored.get("sha256", ""),
                source_ref=source_ref,
                original_name=stored.get("original_name", ""),
            )
            asset = self.db.upsert_media_asset(
                media_key=media_key,
                media_type=media_type,
                file_path=stored.get("relative_path", "") or stored.get("absolute_path", ""),
                original_name=preferred_name or stored.get("original_name", ""),
                mime_type=stored.get("mime_type", ""),
                file_ext=stored.get("file_ext", ""),
                file_size_bytes=stored.get("file_size_bytes", 0),
                sha256=stored.get("sha256", ""),
                width_px=stored.get("width_px"),
                height_px=stored.get("height_px"),
                source_module=source_module,
                source_kind=source_kind,
                source_ref=source_ref,
                source_url=source_url,
                metadata=metadata or {},
            )
            if not asset:
                raise RuntimeError("Media-Asset konnte nicht in der Datenbank registriert werden.")
            return asset
        except Exception as exc:
            log_exception(__name__, exc, extra={"source_path": source_path, "media_type": media_type})
            logging.error(f"Fehler bei register_local_asset: {exc}")
            raise

    def resolve_shop_context(self, shop_name="", sender_domain="", sender_text="", payload=None):
        payload = payload if isinstance(payload, dict) else {}
        resolved_shop_name = str(shop_name or payload.get("shop_name", "")).strip()

        sender_domain_candidates = [
            sender_domain,
            payload.get("_email_sender_domain", ""),
            payload.get("sender_domain", ""),
            extract_sender_domain(sender_text or payload.get("_email_sender", "")),
            extract_sender_domain(payload.get("bestell_email", "")),
        ]
        resolved_sender_domain = ""
        for candidate in sender_domain_candidates:
            normalized = normalize_sender_domain(candidate)
            if normalized:
                resolved_sender_domain = normalized
                break

        shop_key = build_shop_key(shop_name=resolved_shop_name, sender_domain=resolved_sender_domain)
        context = {
            "shop_name": resolved_shop_name,
            "sender_domain": resolved_sender_domain,
            "sender_text": str(sender_text or payload.get("_email_sender", "")).strip(),
            "shop_key": shop_key,
        }
        if resolved_shop_name or resolved_sender_domain:
            logging.debug(
                "Shop-Kontext aufgeloest: shop_key=%s, shop_name=%s, sender_domain=%s",
                context["shop_key"],
                context["shop_name"],
                context["sender_domain"],
            )
        else:
            logging.debug("Shop-Kontext unvollstaendig: weder shop_name noch sender_domain vorhanden.")
        return context

    def resolve_shop_key(self, shop_name="", sender_domain="", sender_text="", payload=None):
        context = self.resolve_shop_context(
            shop_name=shop_name,
            sender_domain=sender_domain,
            sender_text=sender_text,
            payload=payload,
        )
        return context.get("shop_key", "shop:unknown")

    def _shop_logo_bucket(self, shop_key):
        return f"shops/{shop_key_to_storage_name(shop_key)}"

    def register_shop_logo(
        self,
        shop_name,
        source_path,
        preferred_name="",
        source_module="",
        source_kind="manual",
        source_ref="",
        source_url="",
        sender_domain="",
        sender_text="",
        is_primary=True,
        priority=100,
        confidence=1.0,
        metadata=None,
    ):
        context = self.resolve_shop_context(shop_name=shop_name, sender_domain=sender_domain, sender_text=sender_text)
        bucket = self._shop_logo_bucket(context["shop_key"])
        asset = self.register_local_asset(
            source_path=source_path,
            media_type="shop_logo",
            bucket=bucket,
            preferred_name=preferred_name or context["shop_name"] or context["sender_domain"] or "shop-logo",
            source_module=source_module,
            source_kind=source_kind,
            source_ref=source_ref or context["shop_name"] or context["sender_domain"],
            source_url=source_url,
            metadata={
                "shop_key": context["shop_key"],
                "shop_name": context["shop_name"],
                "sender_domain": context["sender_domain"],
                **dict(metadata or {}),
            },
        )
        link = self.db.upsert_shop_logo_link(
            shop_key=context["shop_key"],
            shop_name=context["shop_name"],
            sender_domain=context["sender_domain"],
            media_asset_id=asset["id"],
            is_primary=is_primary,
            priority=priority,
            source_note=str(source_kind or "").strip(),
            confidence=confidence,
            metadata=metadata or {},
        )
        if not link:
            raise RuntimeError("Shoplogo-Link konnte nicht gespeichert werden.")
        logging.info("Shoplogo registriert: shop_key=%s, asset_id=%s", context["shop_key"], asset["id"])
        self._invalidate_visuals(reason="shop_logo_registered", scope="global")
        return {"asset": asset, "link": link, "shop_key": context["shop_key"], "context": context}

    def register_shop_logo_from_file(
        self,
        shop_name,
        file_path,
        preferred_name="",
        source_module="",
        source_kind="manual",
        source_ref="",
        source_url="",
        sender_domain="",
        sender_text="",
        is_primary=True,
        priority=100,
        confidence=1.0,
        metadata=None,
    ):
        return self.register_shop_logo(
            shop_name=shop_name,
            source_path=file_path,
            preferred_name=preferred_name,
            source_module=source_module,
            source_kind=source_kind,
            source_ref=source_ref,
            source_url=source_url,
            sender_domain=sender_domain,
            sender_text=sender_text,
            is_primary=is_primary,
            priority=priority,
            confidence=confidence,
            metadata=metadata,
        )

    def link_shop_logo(
        self,
        shop_name,
        media_asset_id,
        sender_domain="",
        sender_text="",
        is_primary=True,
        priority=100,
        source_note="",
        confidence=1.0,
        metadata=None,
    ):
        context = self.resolve_shop_context(shop_name=shop_name, sender_domain=sender_domain, sender_text=sender_text)
        link = self.db.upsert_shop_logo_link(
            shop_key=context["shop_key"],
            shop_name=context["shop_name"],
            sender_domain=context["sender_domain"],
            media_asset_id=media_asset_id,
            is_primary=is_primary,
            priority=priority,
            source_note=source_note,
            confidence=confidence,
            metadata=metadata or {},
        )
        if not link:
            raise RuntimeError("Shoplogo-Link konnte nicht gespeichert werden.")
        logging.info("Shoplogo verknuepft: shop_key=%s, asset_id=%s", context["shop_key"], media_asset_id)
        self._invalidate_visuals(reason="shop_logo_linked", scope="global")
        return {"link": link, "shop_key": context["shop_key"], "context": context}

    def get_shop_logo_link(self, shop_name="", sender_domain="", sender_text="", payload=None):
        context = self.resolve_shop_context(
            shop_name=shop_name,
            sender_domain=sender_domain,
            sender_text=sender_text,
            payload=payload,
        )
        link = self.db.get_primary_shop_logo_link(
            shop_key=context.get("shop_key", ""),
            sender_domain=context.get("sender_domain", ""),
        )
        if link:
            logging.debug("Shoplogo-Link gefunden: shop_key=%s, asset_id=%s", context["shop_key"], link.get("media_asset_id"))
        else:
            logging.debug(
                "Kein Shoplogo-Link gefunden: shop_key=%s, sender_domain=%s",
                context.get("shop_key", ""),
                context.get("sender_domain", ""),
            )
        return {"link": link, "context": context}

    def resolve_shop_logo(self, shop_name="", sender_domain="", sender_text="", payload=None):
        payload_dict = payload if isinstance(payload, dict) else {}
        resolved = self.get_shop_logo_link(
            shop_name=shop_name,
            sender_domain=sender_domain,
            sender_text=sender_text,
            payload=payload,
        )
        link = resolved.get("link")
        context = resolved.get("context", {})
        if not link and payload_dict:
            ensured = self.ensure_shop_logo_from_existing_sources(
                shop_name=context.get("shop_name", "") or shop_name,
                sender_domain=context.get("sender_domain", "") or sender_domain,
                sender_text=context.get("sender_text", "") or sender_text,
                payload=payload_dict,
                source_module="resolve_shop_logo",
                source_kind="payload_logo_hint",
                priority=80,
                confidence=0.88,
            )
            if ensured and ensured.get("link"):
                return self.resolve_shop_logo(
                    shop_name=context.get("shop_name", "") or shop_name,
                    sender_domain=context.get("sender_domain", "") or sender_domain,
                    sender_text=context.get("sender_text", "") or sender_text,
                    payload=payload_dict,
                )
        if not link:
            return {"context": context, "link": None, "asset": None, "path": ""}

        asset = self.db.get_media_asset_by_id(link.get("media_asset_id")) if link.get("media_asset_id") else None
        if not asset:
            logging.warning("Shoplogo-Link ohne Asset gefunden: shop_key=%s", context.get("shop_key", ""))
            return {"context": context, "link": link, "asset": None, "path": ""}

        path_value = self._resolve_local_asset_path(asset)
        source_url = str(asset.get("source_url", "") or "")
        if not path_value and source_url:
            rematerialized = self.register_remote_shop_logo(
                shop_name=context.get("shop_name", ""),
                image_url=source_url,
                sender_domain=context.get("sender_domain", ""),
                sender_text=context.get("sender_text", ""),
                preferred_name=str(asset.get("original_name", "") or ""),
                source_module=str(asset.get("source_module", "") or "resolve_shop_logo"),
                source_kind=f"{str(asset.get('source_kind', '') or 'remote_logo').strip()}:materialized",
                source_ref=str(asset.get("source_ref", "") or source_url),
                is_primary=bool(link.get("is_primary", True)),
                priority=int(link.get("priority", 100) or 100),
                confidence=float(link.get("confidence", 0.88) or 0.88),
                metadata={"materialized_from_asset_id": asset.get("id")},
                payload=payload_dict,
            )
            if rematerialized and rematerialized.get("link"):
                return self.resolve_shop_logo(
                    shop_name=context.get("shop_name", "") or shop_name,
                    sender_domain=context.get("sender_domain", "") or sender_domain,
                    sender_text=context.get("sender_text", "") or sender_text,
                    payload=payload_dict,
                )
        if path_value:
            source_kind = str(asset.get("source_kind", "") or "").strip().lower()
            if "screenshot_logo_guess" in source_kind and not self._has_meaningful_logo_pixels(path_value):
                logging.info("Unbelastbares Screenshot-Shoplogo ignoriert: asset_id=%s", asset.get("id"))
                path_value = ""
        if not path_value:
            logging.warning("Shoplogo-Asset hat keinen gueltigen Pfad: asset_id=%s", asset.get("id"))
        return {"context": context, "link": link, "asset": asset, "path": path_value}

    def get_shop_logo_path(self, shop_name="", sender_domain="", sender_text="", payload=None):
        resolved = self.resolve_shop_logo(
            shop_name=shop_name,
            sender_domain=sender_domain,
            sender_text=sender_text,
            payload=payload,
        )
        return str(resolved.get("path", "") or "")

    def register_remote_shop_logo(
        self,
        shop_name,
        image_url,
        preferred_name="",
        source_module="",
        source_kind="existing_logo_url",
        source_ref="",
        sender_domain="",
        sender_text="",
        is_primary=True,
        priority=100,
        confidence=1.0,
        metadata=None,
        payload=None,
    ):
        context = self.resolve_shop_context(shop_name=shop_name, sender_domain=sender_domain, sender_text=sender_text, payload=payload)
        image_url_text = self._resolve_web_reference(image_url, payload=payload, sender_domain=context.get("sender_domain", ""))
        if not self._looks_like_url(image_url_text):
            logging.warning("Shoplogo-URL ungueltig oder nicht belastbar: %s", image_url)
            return None

        bucket = self._shop_logo_bucket(context["shop_key"])
        preferred = preferred_name or self._remote_name_from_url(image_url_text, fallback=context.get("shop_name", "") or context.get("sender_domain", "") or "shop-logo")
        try:
            download = self._download_remote_product_image(
                image_url=image_url_text,
                bucket=bucket,
                preferred_name=preferred,
            )
            stored_result = self.register_shop_logo_from_file(
                shop_name=context.get("shop_name", "") or shop_name,
                file_path=download["path"],
                preferred_name=preferred,
                source_module=source_module,
                source_kind=f"{str(source_kind or 'existing_logo_url').strip()}:downloaded",
                source_ref=source_ref or image_url_text,
                source_url=image_url_text,
                sender_domain=context.get("sender_domain", ""),
                sender_text=context.get("sender_text", ""),
                is_primary=is_primary,
                priority=priority,
                confidence=confidence,
                metadata={"downloaded_from_url": image_url_text, **dict(metadata or {})},
            )
            try:
                os.remove(download["path"])
            except OSError:
                pass
            logging.info("Shoplogo automatisch registriert: shop_key=%s, source_url=%s", context.get("shop_key", ""), image_url_text)
            return stored_result
        except Exception as exc:
            log_exception(__name__, exc, extra={"shop_key": context.get("shop_key", ""), "source_url": image_url_text})
            logging.warning("Shoplogo konnte nicht automatisch registriert werden: %s", exc)
            return None

    def register_shop_logo_from_screenshot_header(
        self,
        screenshot_asset_id,
        shop_name="",
        sender_domain="",
        sender_text="",
        preferred_name="",
        source_module="",
        source_kind="screenshot_logo_guess",
        source_ref="",
        priority=140,
        confidence=0.55,
        metadata=None,
        payload=None,
        source_context=None,
    ):
        context = self.resolve_shop_context(shop_name=shop_name, sender_domain=sender_domain, sender_text=sender_text)
        screenshot_resolved = self.resolve_screenshot_reference(
            screenshot_asset_id,
            payload=payload,
            source_context=source_context,
        )
        screenshot_path = str(screenshot_resolved.get("path", "") or "")
        if not screenshot_path:
            logging.debug("Screenshot fuer Shoplogo-Fallback nicht verfuegbar: screenshot_asset_id=%s", screenshot_asset_id)
            return None
        if screenshot_resolved.get("used_fallback_path"):
            logging.info(
                "Shoplogo-Fallback nutzt alternativen Screenshot-Pfad: asset_id=%s, reason=%s",
                screenshot_asset_id,
                str(screenshot_resolved.get("path_reason", "") or "fallback"),
            )

        try:
            image_width, image_height = MediaCropper.image_dimensions(screenshot_path)
            crop_width = min(image_width, max(140, min(320, int(image_width * 0.22))))
            crop_height = min(image_height, max(56, min(120, int(image_height * 0.10))))
            asset_token = self._coerce_int(screenshot_asset_id, 0)
            temp_info = self.store.build_generated_path(
                bucket=f"{self._shop_logo_bucket(context['shop_key'])}/tmp",
                preferred_name=preferred_name or context.get("shop_name", "") or context.get("sender_domain", "") or "shop-logo",
                extension=".png",
                token=f"shoplogo_guess_{asset_token or 'fallback'}",
            )
            crop_result = {"output_path": temp_info["absolute_path"]}
            crop_result = MediaCropper.crop_image(
                screenshot_path=screenshot_path,
                output_path=temp_info["absolute_path"],
                x=0,
                y=0,
                width=crop_width,
                height=crop_height,
                image_format="PNG",
                clamp=True,
            )
            try:
                if not self._has_meaningful_logo_pixels(crop_result["output_path"]):
                    logging.info("Shoplogo-Screenshot-Crop verworfen: kein belastbares Logo im Kopfbereich erkannt.")
                    return None
                metadata_payload = {
                    "guess_region": {"x": 0, "y": 0, "width": crop_width, "height": crop_height},
                    "screenshot_path_reason": str(screenshot_resolved.get("path_reason", "") or "registered_asset"),
                    **dict(metadata or {}),
                }
                if asset_token > 0:
                    metadata_payload["derived_from_asset_id"] = asset_token
                stored_result = self.register_shop_logo_from_file(
                    shop_name=context.get("shop_name", "") or shop_name,
                    file_path=crop_result["output_path"],
                    preferred_name=preferred_name or context.get("shop_name", "") or context.get("sender_domain", "") or "shop-logo",
                    source_module=source_module or "shop_logo_autoregister",
                    source_kind=source_kind,
                    source_ref=source_ref or (f"screenshot:{asset_token}" if asset_token > 0 else os.path.basename(screenshot_path)),
                    sender_domain=context.get("sender_domain", ""),
                    sender_text=context.get("sender_text", ""),
                    is_primary=True,
                    priority=priority,
                    confidence=confidence,
                    metadata=metadata_payload,
                )
                logging.info("Shoplogo aus Screenshot-Kopfbereich registriert: shop_key=%s, screenshot_asset_id=%s", context.get("shop_key", ""), screenshot_asset_id)
                return stored_result
            finally:
                try:
                    if os.path.exists(crop_result["output_path"]):
                        os.remove(crop_result["output_path"])
                except Exception:
                    pass
        except Exception as exc:
            log_exception(__name__, exc, extra={"shop_key": context.get("shop_key", ""), "screenshot_asset_id": screenshot_asset_id})
            logging.warning("Shoplogo-Fallback aus Screenshot-Kopfbereich fehlgeschlagen: %s", exc)
            return None

    def ensure_shop_logo_from_existing_sources(
        self,
        shop_name="",
        sender_domain="",
        sender_text="",
        payload=None,
        source_module="",
        source_kind="existing_shop_source",
        priority=90,
        confidence=0.85,
    ):
        payload_dict = payload if isinstance(payload, dict) else {}
        context = self.resolve_shop_context(shop_name=shop_name, sender_domain=sender_domain, sender_text=sender_text, payload=payload_dict)
        if not context.get("shop_name") and not context.get("sender_domain"):
            logging.debug("Shoplogo-Autoregistrierung uebersprungen: kein Shop-Kontext vorhanden.")
            return None
        existing = self.get_shop_logo_link(
            shop_name=context.get("shop_name", ""),
            sender_domain=context.get("sender_domain", ""),
            sender_text=context.get("sender_text", ""),
            payload=payload_dict,
        )
        if existing.get("link"):
            return existing

        logo_hints = [row for row in list(payload_dict.get("_mail_logo_hints", []) or []) if isinstance(row, dict)]
        for hint in logo_hints:
            logo_src = str(hint.get("src", "") or "").strip()
            if not logo_src:
                continue
            result = self.register_remote_shop_logo(
                shop_name=context.get("shop_name", "") or shop_name,
                image_url=logo_src,
                preferred_name=str(hint.get("alt", "") or hint.get("title", "") or context.get("shop_name", "") or context.get("sender_domain", "") or "shop-logo"),
                source_module=source_module or "shop_logo_autoregister",
                source_kind=source_kind,
                source_ref=logo_src,
                sender_domain=context.get("sender_domain", ""),
                sender_text=context.get("sender_text", ""),
                is_primary=True,
                priority=priority,
                confidence=confidence,
                metadata={
                    "alt": str(hint.get("alt", "") or ""),
                    "title": str(hint.get("title", "") or ""),
                },
                payload=payload_dict,
            )
            if result:
                return result

        screenshot_asset_id = self._resolve_payload_screenshot_asset_id(payload_dict)
        screenshot_path = self._resolve_payload_screenshot_path(payload_dict)
        if screenshot_asset_id not in (None, "") or screenshot_path:
            guessed = self.register_shop_logo_from_screenshot_header(
                screenshot_asset_id=screenshot_asset_id,
                shop_name=context.get("shop_name", "") or shop_name,
                sender_domain=context.get("sender_domain", ""),
                sender_text=context.get("sender_text", ""),
                preferred_name=context.get("shop_name", "") or context.get("sender_domain", "") or "shop-logo",
                source_module=source_module or "shop_logo_autoregister",
                source_kind="screenshot_logo_guess",
                source_ref=f"screenshot:{screenshot_asset_id}" if screenshot_asset_id not in (None, "") else os.path.basename(screenshot_path),
                priority=140,
                confidence=0.55,
                metadata={"fallback_reason": "mail_logo_hint_missing"},
                payload=payload_dict,
                source_context={"screenshot_path": screenshot_path},
            )
            if guessed:
                return guessed

        logging.debug(
            "Kein belastbarer Shoplogo-Hinweis gefunden: shop_key=%s, sender_domain=%s",
            context.get("shop_key", ""),
            context.get("sender_domain", ""),
        )
        return None

    def resolve_product_context(self, product_name="", ean="", variant_text="", item=None, payload=None):
        item = item if isinstance(item, dict) else {}
        payload = payload if isinstance(payload, dict) else {}
        resolved_name = str(product_name or item.get("produkt_name", "") or payload.get("produkt_name", "")).strip()
        resolved_variant = str(variant_text or item.get("varianten_info", "") or payload.get("varianten_info", "")).strip()
        resolved_ean = normalize_ean_text(ean or item.get("ean", "") or payload.get("ean", ""))
        product_key = build_product_key(
            product_name=resolved_name,
            ean=resolved_ean,
            variant_text=resolved_variant,
        )
        context = {
            "product_name": resolved_name,
            "variant_text": resolved_variant,
            "ean": resolved_ean,
            "product_key": product_key,
        }
        if resolved_name or resolved_ean:
            logging.debug(
                "Produkt-Kontext aufgeloest: product_key=%s, ean=%s, produkt=%s",
                context["product_key"],
                context["ean"],
                context["product_name"],
            )
        else:
            logging.debug("Produkt-Kontext unvollstaendig: weder Produktname noch EAN vorhanden.")
        return context

    def resolve_product_key(self, product_name="", ean="", variant_text="", item=None, payload=None):
        context = self.resolve_product_context(
            product_name=product_name,
            ean=ean,
            variant_text=variant_text,
            item=item,
            payload=payload,
        )
        return context.get("product_key", "product:unknown")

    def _product_image_bucket(self, product_key):
        return f"products/{product_key_to_storage_name(product_key)}"

    def _looks_like_url(self, value):
        text = str(value or "").strip().lower()
        return text.startswith("http://") or text.startswith("https://")

    def _looks_like_relative_web_path(self, value):
        text = str(value or "").strip()
        if not text or self._looks_like_url(text) or self._looks_like_relative_web_path(text):
            return False
        if text.startswith("//") or text.startswith("/") or text.startswith("./") or text.startswith("../"):
            return True
        if ":" in text[:4] or text.startswith("\\"):
            return False
        return "/" in text and not os.path.isabs(text)

    def _collect_payload_url_origins(self, payload=None, sender_domain=""):
        payload = payload if isinstance(payload, dict) else {}
        origins = []
        seen = set()

        def _add_origin(url_value):
            url_text = str(url_value or "").strip()
            if not self._looks_like_url(url_text):
                return
            parsed = urlparse(url_text)
            if not parsed.scheme or not parsed.netloc:
                return
            origin = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
            if not origin or origin in seen:
                return
            seen.add(origin)
            origins.append(origin)

        for hint_key in ("_mail_logo_hints", "_mail_image_hints"):
            for row in list(payload.get(hint_key, []) or []):
                if isinstance(row, dict):
                    _add_origin(row.get("src", ""))

        for row in list(payload.get("_scan_sources", []) or []):
            if isinstance(row, dict):
                _add_origin(row.get("source_url", ""))

        resolved_sender_domain = normalize_sender_domain(sender_domain or payload.get("_email_sender_domain", "") or payload.get("sender_domain", ""))
        if resolved_sender_domain:
            _add_origin(f"https://{resolved_sender_domain}")

        return origins

    def _resolve_web_reference(self, reference, payload=None, sender_domain=""):
        reference_text = str(reference or "").strip()
        if not reference_text:
            return ""
        if self._looks_like_url(reference_text):
            return reference_text
        if reference_text.startswith("//"):
            resolved_protocol_relative = f"https:{reference_text}"
            logging.info("Relative bild_url erfolgreich absolut aufgeloest: %s -> %s", reference_text, resolved_protocol_relative)
            return resolved_protocol_relative
        if not self._looks_like_relative_web_path(reference_text):
            return ""

        for origin in self._collect_payload_url_origins(payload=payload, sender_domain=sender_domain):
            candidate = urljoin(origin.rstrip("/") + "/", reference_text)
            if self._looks_like_url(candidate):
                logging.info("Relative bild_url erfolgreich absolut aufgeloest: %s -> %s", reference_text, candidate)
                return candidate

        logging.warning("Relative bild_url verworfen, keine belastbare Basisdomain vorhanden: %s", reference_text)
        return ""

    def _resolve_existing_local_path(self, value):
        text = str(value or "").strip()
        if not text or self._looks_like_url(text) or self._looks_like_relative_web_path(text):
            return ""
        absolute = self.store.resolve_path(text)
        if absolute and os.path.exists(absolute):
            return absolute
        return ""

    def _remote_name_from_url(self, image_url, fallback="product-image"):
        parsed = urlparse(str(image_url or "").strip())
        filename = Path(parsed.path).name
        return filename or fallback

    def _resolve_local_asset_path(self, asset):
        if not isinstance(asset, dict):
            return ""
        if str(asset.get("storage_kind", "") or "local_file").strip() != "local_file":
            return ""
        path_value = self.store.resolve_path(asset.get("file_path", ""))
        if path_value and os.path.exists(path_value):
            return path_value
        return ""

    def _get_legacy_product_path(self, product_name):
        # Alt-Tabelle `produkt_bilder` bleibt nur als Uebergangsquelle aktiv.
        legacy_value = self.db.get_image_path_by_name(product_name)
        return self._resolve_existing_local_path(legacy_value)

    def _download_remote_product_image(self, image_url, bucket, preferred_name="product-image"):
        request = urllib.request.Request(
            str(image_url or "").strip(),
            headers={
                "User-Agent": "MeinBueroTool/1.0 (+MediaService)",
                "Accept": "image/*,*/*;q=0.8",
            },
        )
        context = ssl.create_default_context()
        with urllib.request.urlopen(request, timeout=8, context=context) as response:
            payload = response.read()
            content_type = str(response.headers.get("Content-Type", "") or "").split(";", 1)[0].strip().lower()
        if not payload:
            raise ValueError("Leere Bildantwort erhalten.")

        extension = Path(urlparse(str(image_url or "").strip()).path).suffix.lower()
        if not extension and content_type:
            extension = mimetypes.guess_extension(content_type) or ".bin"
        target_info = self.store.build_generated_path(
            bucket=bucket,
            preferred_name=preferred_name,
            extension=extension or ".bin",
            token="remote-image",
        )
        with open(target_info["absolute_path"], "wb") as handle:
            handle.write(payload)
        return {
            "path": target_info["absolute_path"],
            "content_type": content_type,
        }

    def register_product_image(
        self,
        product_name,
        source_path,
        ean="",
        variant_text="",
        preferred_name="",
        source_module="",
        source_kind="manual",
        source_ref="",
        source_url="",
        is_primary=True,
        priority=100,
        metadata=None,
    ):
        context = self.resolve_product_context(product_name=product_name, ean=ean, variant_text=variant_text)
        bucket = self._product_image_bucket(context["product_key"])
        asset = self.register_local_asset(
            source_path=source_path,
            media_type="product_image",
            bucket=bucket,
            preferred_name=preferred_name or context["product_name"] or context["ean"] or "product-image",
            source_module=source_module,
            source_kind=source_kind,
            source_ref=source_ref or context["ean"] or context["product_name"] or context["product_key"],
            source_url=source_url,
            metadata={
                "product_key": context["product_key"],
                "product_name": context["product_name"],
                "ean": context["ean"],
                "variant_text": context["variant_text"],
                **dict(metadata or {}),
            },
        )
        logging.info("Produktbild lokal registriert: product_key=%s, asset_id=%s", context["product_key"], asset["id"])
        return self.link_product_image(
            product_name=context["product_name"],
            media_asset_id=asset["id"],
            ean=context["ean"],
            variant_text=context["variant_text"],
            is_primary=is_primary,
            priority=priority,
            source_note=str(source_kind or "").strip(),
            metadata=metadata,
            asset=asset,
        )

    def register_product_image_from_file(
        self,
        product_name,
        file_path,
        ean="",
        variant_text="",
        preferred_name="",
        source_module="",
        source_kind="manual",
        source_ref="",
        source_url="",
        is_primary=True,
        priority=100,
        metadata=None,
    ):
        return self.register_product_image(
            product_name=product_name,
            source_path=file_path,
            ean=ean,
            variant_text=variant_text,
            preferred_name=preferred_name,
            source_module=source_module,
            source_kind=source_kind,
            source_ref=source_ref,
            source_url=source_url,
            is_primary=is_primary,
            priority=priority,
            metadata=metadata,
        )

    def register_remote_product_image(
        self,
        product_name,
        image_url,
        ean="",
        variant_text="",
        preferred_name="",
        source_module="",
        source_kind="existing_url",
        source_ref="",
        is_primary=True,
        priority=100,
        metadata=None,
    ):
        image_url_text = str(image_url or "").strip()
        if not self._looks_like_url(image_url_text):
            logging.warning("Produktbild-URL ungueltig oder leer: %s", image_url_text)
            return None

        context = self.resolve_product_context(product_name=product_name, ean=ean, variant_text=variant_text)
        bucket = self._product_image_bucket(context["product_key"])
        preferred = preferred_name or self._remote_name_from_url(image_url_text, fallback=context["product_name"] or "product-image")

        try:
            download = self._download_remote_product_image(
                image_url=image_url_text,
                bucket=bucket,
                preferred_name=preferred,
            )
            stored_result = self.register_product_image_from_file(
                product_name=context["product_name"],
                file_path=download["path"],
                ean=context["ean"],
                variant_text=context["variant_text"],
                preferred_name=preferred,
                source_module=source_module,
                source_kind=f"{str(source_kind or 'existing_url').strip()}:downloaded",
                source_ref=source_ref or image_url_text,
                source_url=image_url_text,
                is_primary=is_primary,
                priority=priority,
                metadata={
                    "downloaded_from_url": image_url_text,
                    **dict(metadata or {}),
                },
            )
            try:
                os.remove(download["path"])
            except OSError:
                pass
            logging.info("Produktbild aus URL lokal uebernommen: product_key=%s, source_url=%s", context["product_key"], image_url_text)
            return stored_result
        except Exception as exc:
            log_exception(__name__, exc, extra={"product_key": context["product_key"], "source_url": image_url_text})
            logging.warning("Produktbild-Download fehlgeschlagen, speichere URL-Referenz: %s", exc)

        asset_source_ref = source_ref or image_url_text or context["ean"] or context["product_key"]
        media_key = build_media_key(
            media_type="product_image",
            sha256="",
            source_ref=asset_source_ref,
            original_name=preferred,
        )
        asset = self.db.upsert_media_asset(
            media_key=media_key,
            media_type="product_image",
            file_path="",
            original_name=preferred,
            mime_type="",
            file_ext=Path(urlparse(image_url_text).path).suffix.lower(),
            file_size_bytes=0,
            sha256="",
            width_px=None,
            height_px=None,
            source_module=source_module,
            source_kind=source_kind,
            source_ref=asset_source_ref,
            source_url=image_url_text,
            storage_kind="remote_url",
            metadata={
                "product_key": context["product_key"],
                "product_name": context["product_name"],
                "ean": context["ean"],
                "variant_text": context["variant_text"],
                **dict(metadata or {}),
            },
        )
        if not asset:
            raise RuntimeError("Remote-Produktbild konnte nicht als Media-Asset registriert werden.")
        logging.info("Produktbild-Referenz registriert: product_key=%s, source_url=%s", context["product_key"], image_url_text)
        return self.link_product_image(
            product_name=context["product_name"],
            media_asset_id=asset["id"],
            ean=context["ean"],
            variant_text=context["variant_text"],
            is_primary=is_primary,
            priority=priority,
            source_note=str(source_kind or "").strip(),
            metadata={"source_url": image_url_text, **dict(metadata or {})},
            asset=asset,
        )

    def register_product_image_reference(
        self,
        product_name,
        image_ref="",
        ean="",
        variant_text="",
        preferred_name="",
        source_module="",
        source_kind="existing_reference",
        source_ref="",
        source_url="",
        is_primary=True,
        priority=100,
        metadata=None,
        payload=None,
    ):
        reference = str(image_ref or source_url or "").strip()
        if not reference:
            logging.debug("Kein Produktbild-Referenzwert vorhanden fuer Produkt '%s'.", str(product_name or "").strip())
            return None

        metadata_dict = dict(metadata or {})
        sender_domain = str(metadata_dict.get("email_sender_domain", "") or metadata_dict.get("sender_domain", "") or "").strip()
        local_path = self._resolve_existing_local_path(reference)
        if local_path:
            return self.register_product_image_from_file(
                product_name=product_name,
                file_path=local_path,
                ean=ean,
                variant_text=variant_text,
                preferred_name=preferred_name,
                source_module=source_module,
                source_kind=source_kind,
                source_ref=source_ref or reference,
                source_url=source_url if self._looks_like_url(source_url) else "",
                is_primary=is_primary,
                priority=priority,
                metadata={"original_reference": reference, **metadata_dict},
            )

        resolved_reference = reference if self._looks_like_url(reference) else self._resolve_web_reference(reference, payload=payload, sender_domain=sender_domain)
        resolved_source_url = source_url if self._looks_like_url(source_url) else self._resolve_web_reference(source_url, payload=payload, sender_domain=sender_domain)
        if self._looks_like_url(resolved_reference):
            return self.register_remote_product_image(
                product_name=product_name,
                image_url=resolved_reference,
                ean=ean,
                variant_text=variant_text,
                preferred_name=preferred_name,
                source_module=source_module,
                source_kind=source_kind,
                source_ref=source_ref or reference,
                is_primary=is_primary,
                priority=priority,
                metadata={"original_reference": reference, **metadata_dict},
            )

        if self._looks_like_url(resolved_source_url):
            return self.register_remote_product_image(
                product_name=product_name,
                image_url=resolved_source_url,
                ean=ean,
                variant_text=variant_text,
                preferred_name=preferred_name,
                source_module=source_module,
                source_kind=source_kind,
                source_ref=source_ref or reference or resolved_source_url,
                is_primary=is_primary,
                priority=priority,
                metadata={"original_reference": reference, **metadata_dict},
            )

        logging.warning("Produktbild-Referenz konnte nicht uebernommen werden: %s", reference)
        return None

    def register_legacy_product_image_path(
        self,
        product_name,
        legacy_path="",
        ean="",
        variant_text="",
        is_primary=True,
        priority=100,
        metadata=None,
    ):
        return self.register_product_image_reference(
            product_name=product_name,
            image_ref=legacy_path,
            ean=ean,
            variant_text=variant_text,
            source_module="legacy_product_bilder",
            source_kind="legacy_product_path",
            source_ref=legacy_path,
            is_primary=is_primary,
            priority=priority,
            metadata=metadata,
        )

    def ensure_product_image_from_existing_sources(
        self,
        product_name,
        ean="",
        variant_text="",
        bild_url="",
        local_path="",
        source_module="",
        source_kind="existing_product_source",
        is_primary=True,
        priority=100,
        metadata=None,
        payload=None,
        item=None,
    ):
        context = self.resolve_product_context(product_name=product_name, ean=ean, variant_text=variant_text, item=item, payload=payload)
        if not context.get("product_name") and not context.get("ean"):
            logging.warning("Produktbild konnte nicht zugeordnet werden: kein brauchbarer Produktschluessel vorhanden.")
            return None

        existing = self.get_product_image_link(
            product_name=context["product_name"],
            ean=context["ean"],
            variant_text=context["variant_text"],
        )
        if existing.get("link"):
            logging.debug("Produktbild bereits vorhanden: product_key=%s", context["product_key"])
            return existing

        metadata_dict = dict(metadata or {})
        if isinstance(payload, dict):
            metadata_dict.setdefault("email_sender_domain", str(payload.get("_email_sender_domain", payload.get("sender_domain", "")) or "").strip())
            metadata_dict.setdefault("shop_name", str(payload.get("shop_name", "") or "").strip())
        legacy_path = self._get_legacy_product_path(context["product_name"])
        reference_candidates = [
            (local_path, "local_existing_path"),
            (bild_url, source_kind or "existing_product_source"),
            (legacy_path, "legacy_product_path"),
        ]
        for candidate_value, candidate_kind in reference_candidates:
            candidate_text = str(candidate_value or "").strip()
            if not candidate_text:
                continue
            result = self.register_product_image_reference(
                product_name=context["product_name"],
                image_ref=candidate_text,
                ean=context["ean"],
                variant_text=context["variant_text"],
                source_module=source_module or "product_media_bridge",
                source_kind=candidate_kind,
                source_ref=candidate_text,
                is_primary=is_primary,
                priority=priority,
                metadata=metadata_dict,
                payload=payload,
            )
            if result:
                return result

        logging.debug("Kein vorhandenes Produktbild gefunden: product_key=%s", context["product_key"])
        return None

    def link_product_image(
        self,
        product_name,
        media_asset_id,
        ean="",
        variant_text="",
        product_key="",
        is_primary=False,
        priority=100,
        source_note="",
        metadata=None,
        asset=None,
    ):
        context = self.resolve_product_context(product_name=product_name, ean=ean, variant_text=variant_text)
        explicit_product_key = str(product_key or "").strip()
        if explicit_product_key:
            context["product_key"] = explicit_product_key
        link = self.db.upsert_product_image_link(
            product_key=context["product_key"],
            product_name=context["product_name"],
            ean=context["ean"],
            variant_text=context["variant_text"],
            media_asset_id=media_asset_id,
            is_primary=is_primary,
            priority=priority,
            source_note=source_note,
            metadata=metadata or {},
        )
        if not link:
            raise RuntimeError("Produktbild-Link konnte nicht gespeichert werden.")
        logging.info("Produktbild verknuepft: product_key=%s, asset_id=%s", context["product_key"], media_asset_id)
        self._invalidate_visuals(reason="product_image_linked", scope="global")
        return {"asset": asset, "link": link, "product_key": context["product_key"], "context": context}
    def get_product_image_link(self, product_name="", ean="", variant_text="", item=None, payload=None):
        context = self.resolve_product_context(
            product_name=product_name,
            ean=ean,
            variant_text=variant_text,
            item=item,
            payload=payload,
        )
        link = self.db.get_primary_product_image_link(
            product_key=context.get("product_key", ""),
            ean=context.get("ean", ""),
            product_name=context.get("product_name", ""),
            variant_text=context.get("variant_text", ""),
        )
        if link:
            logging.debug("Produktbild-Link gefunden: product_key=%s, asset_id=%s", context["product_key"], link.get("media_asset_id"))
        else:
            logging.debug("Kein Produktbild-Link gefunden: product_key=%s", context.get("product_key", ""))
        return {"link": link, "context": context}

    def _materialize_remote_product_asset(self, asset, context, link=None, payload=None):
        asset = dict(asset or {}) if isinstance(asset, dict) else {}
        source_url = self._resolve_web_reference(
            asset.get("source_url", ""),
            payload=payload,
            sender_domain=str((context or {}).get("sender_domain", "") or ""),
        )
        if not self._looks_like_url(source_url):
            return None

        bucket = self._product_image_bucket(str((context or {}).get("product_key", "") or "product:unknown"))
        preferred = str(asset.get("original_name", "") or "").strip() or self._remote_name_from_url(
            source_url,
            fallback=str((context or {}).get("product_name", "") or "product-image"),
        )
        try:
            download = self._download_remote_product_image(
                image_url=source_url,
                bucket=bucket,
                preferred_name=preferred,
            )
            stored_result = self.register_product_image_from_file(
                product_name=str((context or {}).get("product_name", "") or ""),
                file_path=download["path"],
                ean=str((context or {}).get("ean", "") or ""),
                variant_text=str((context or {}).get("variant_text", "") or ""),
                preferred_name=preferred,
                source_module=str(asset.get("source_module", "") or "media_service"),
                source_kind=f"{str(asset.get('source_kind', '') or 'remote_url').strip()}:materialized",
                source_ref=str(asset.get("source_ref", "") or source_url),
                source_url=source_url,
                is_primary=bool((link or {}).get("is_primary", True)),
                priority=int((link or {}).get("priority", 100) or 100),
                metadata={
                    "materialized_from_asset_id": asset.get("id"),
                    "downloaded_from_url": source_url,
                },
            )
            try:
                os.remove(download["path"])
            except OSError:
                pass
            logging.info("Remote-Produktbild lokal materialisiert: asset_id=%s, source_url=%s", asset.get("id"), source_url)
            return stored_result
        except Exception as exc:
            log_exception(__name__, exc, extra={"asset_id": asset.get("id"), "source_url": source_url})
            logging.warning("Remote-Produktbild konnte nicht lokal materialisiert werden: %s", exc)
            return None

    def resolve_product_image(self, product_name="", ean="", variant_text="", item=None, payload=None):
        payload_dict = payload if isinstance(payload, dict) else {}
        item_dict = item if isinstance(item, dict) else {}
        resolved = self.get_product_image_link(
            product_name=product_name,
            ean=ean,
            variant_text=variant_text,
            item=item,
            payload=payload,
        )
        link = resolved.get("link")
        context = resolved.get("context", {})
        if not link:
            candidate_bild_url = str(item_dict.get("bild_url", "") or item_dict.get("image_url", "") or payload_dict.get("bild_url", "") or "").strip()
            candidate_local_path = str(item_dict.get("bild_pfad", "") or item_dict.get("image_path", "") or payload_dict.get("bild_pfad", "") or "").strip()
            if candidate_bild_url or candidate_local_path:
                ensured = self.ensure_product_image_from_existing_sources(
                    product_name=context.get("product_name", ""),
                    ean=context.get("ean", ""),
                    variant_text=context.get("variant_text", ""),
                    bild_url=candidate_bild_url,
                    local_path=candidate_local_path,
                    source_module="resolve_product_image",
                    source_kind="payload_image_hint",
                    is_primary=True,
                    priority=85,
                    metadata={
                        "email_sender_domain": str(payload_dict.get("_email_sender_domain", payload_dict.get("sender_domain", "")) or "").strip(),
                        "shop_name": str(payload_dict.get("shop_name", "") or "").strip(),
                    },
                    payload=payload_dict or item_dict,
                    item=item_dict,
                )
                if ensured and ensured.get("link"):
                    logging.info("Produktbild aus Payload-Hinweis nachregistriert: product_key=%s", context.get("product_key", ""))
                    return self.resolve_product_image(
                        product_name=context.get("product_name", ""),
                        ean=context.get("ean", ""),
                        variant_text=context.get("variant_text", ""),
                        item=item_dict,
                        payload=payload_dict,
                    )

            legacy_path = self._get_legacy_product_path(context.get("product_name", ""))
            if legacy_path:
                logging.debug("Legacy-Produktbildpfad gefunden, versuche Uebernahme: product_key=%s", context.get("product_key", ""))
                try:
                    adopted = self.register_legacy_product_image_path(
                        product_name=context.get("product_name", ""),
                        legacy_path=legacy_path,
                        ean=context.get("ean", ""),
                        variant_text=context.get("variant_text", ""),
                        is_primary=True,
                        priority=90,
                        metadata={"auto_adopted": True},
                    )
                    if adopted:
                        return self.resolve_product_image(
                            product_name=context.get("product_name", ""),
                            ean=context.get("ean", ""),
                            variant_text=context.get("variant_text", ""),
                            item=item_dict,
                            payload=payload_dict,
                        )
                except Exception as exc:
                    log_exception(__name__, exc, extra={"product_key": context.get("product_key", "")})
                    logging.warning("Legacy-Produktbild konnte nicht uebernommen werden: %s", exc)
                return {
                    "context": context,
                    "link": None,
                    "asset": None,
                    "path": legacy_path,
                    "source_url": "",
                    "storage_kind": "legacy_local_file",
                }
            return {"context": context, "link": None, "asset": None, "path": "", "source_url": "", "storage_kind": ""}

        asset = self.db.get_media_asset_by_id(link.get("media_asset_id")) if link.get("media_asset_id") else None
        if not asset:
            logging.warning("Produktbild-Link ohne Asset gefunden: product_key=%s", context.get("product_key", ""))
            return {"context": context, "link": link, "asset": None, "path": "", "source_url": "", "storage_kind": ""}

        path_value = self._resolve_local_asset_path(asset)
        source_url = str(asset.get("source_url", "") or "")
        storage_kind = str(asset.get("storage_kind", "") or "local_file")
        if storage_kind == "remote_url" and not path_value and source_url:
            materialized = self._materialize_remote_product_asset(
                asset=asset,
                context={
                    **dict(context or {}),
                    "sender_domain": str(payload_dict.get("_email_sender_domain", payload_dict.get("sender_domain", "")) or ""),
                },
                link=link,
                payload=payload_dict or item_dict,
            )
            if materialized and materialized.get("asset"):
                return self.resolve_product_image(
                    product_name=context.get("product_name", ""),
                    ean=context.get("ean", ""),
                    variant_text=context.get("variant_text", ""),
                    item=item_dict,
                    payload=payload_dict,
                )
        if storage_kind == "local_file" and not path_value:
            logging.warning("Produktbild-Asset hat keinen gueltigen lokalen Pfad: asset_id=%s", asset.get("id"))

        source_kind = str(asset.get("source_kind", "") or "").strip().lower()
        if source_kind == "screenshot_detection_crop":
            is_module1 = self._is_module1_ai_cropping_disabled(payload_dict)
            preview_ok, preview_metrics = self._is_previewable_detection_crop(path_value, role="product_image")
            thumbnail_like = self._looks_like_product_thumbnail(path_value)
            reason = ""
            if is_module1:
                reason = "module1_ai_cropping_disabled"
            elif not bool(link.get("is_primary")):
                reason = "non_primary_screenshot_crop"
            elif not path_value:
                reason = "missing_local_path"
            elif not preview_ok:
                reason = "not_previewable"
            elif not thumbnail_like:
                reason = "not_thumbnail_like"
            if reason:
                aspect_ratio = float(preview_metrics.get("aspect_ratio", 0.0) or 0.0)
                non_light_ratio = float(preview_metrics.get("non_light_ratio", 0.0) or 0.0)
                logging.info(
                    "Screenshot-Crop als globales Produktbild unterdrueckt: asset_id=%s, reason=%s, aspect=%.2f, non_light=%.3f",
                    asset.get("id"),
                    reason,
                    aspect_ratio,
                    non_light_ratio,
                )
                if is_module1:
                    write_module1_trace(
                        "module1_detection_candidates_suppressed",
                        asset_id=int(asset.get("id") or 0),
                        ware_index=-1,
                        reason=reason,
                    )
                    write_module1_trace(
                        "product_image_fallback_suppressed",
                        asset_id=int(asset.get("id") or 0),
                        ware_index=-1,
                        reason=reason,
                        aspect_ratio=aspect_ratio,
                        non_light_ratio=non_light_ratio,
                    )
                elif str(payload_dict.get("_origin_module", "") or "") == "modul_order_entry":
                    write_module1_trace(
                        "detection_crop_rejected_as_global_fallback",
                        asset_id=int(asset.get("id") or 0),
                        ware_index=-1,
                        reason=reason,
                        aspect_ratio=aspect_ratio,
                        non_light_ratio=non_light_ratio,
                    )
                    write_module1_trace(
                        "product_image_fallback_suppressed",
                        asset_id=int(asset.get("id") or 0),
                        ware_index=-1,
                        reason=reason,
                        aspect_ratio=aspect_ratio,
                        non_light_ratio=non_light_ratio,
                    )
                return {
                    "context": context,
                    "link": None,
                    "asset": None,
                    "path": "",
                    "source_url": "",
                    "storage_kind": "",
                }

        return {
            "context": context,
            "link": link,
            "asset": asset,
            "path": path_value,
            "source_url": source_url,
            "storage_kind": storage_kind,
        }
    def get_product_image_path(self, product_name="", ean="", variant_text="", item=None, payload=None):
        resolved = self.resolve_product_image(
            product_name=product_name,
            ean=ean,
            variant_text=variant_text,
            item=item,
            payload=payload,
        )
        return str(resolved.get("path", "") or "")

    def _selected_order_item_state(self, selection_mode="auto"):
        mode = str(selection_mode or "auto").strip().lower()
        if mode == "manual":
            return self.ORDER_ITEM_STATE_SELECTED_MANUAL
        return self.ORDER_ITEM_STATE_SELECTED_AUTO

    def _resolve_media_asset_reference(self, asset):
        if not isinstance(asset, dict):
            return {"asset": None, "path": "", "source_url": "", "storage_kind": ""}
        path_value = self._resolve_local_asset_path(asset)
        return {
            "asset": asset,
            "path": path_value,
            "source_url": str(asset.get("source_url", "") or ""),
            "storage_kind": str(asset.get("storage_kind", "") or "local_file"),
        }

    def register_order_item_image_candidate(
        self,
        order_item_id,
        media_asset_id,
        source_type="",
        source_ref="",
        metadata=None,
        auto_select=False,
        replace_existing_auto=False,
    ):
        if order_item_id in (None, "") or media_asset_id in (None, ""):
            logging.warning("Bildkandidat fuer Bestellposition uebersprungen: order_item_id=%s, media_asset_id=%s", order_item_id, media_asset_id)
            return None

        order_item_id = int(order_item_id)
        media_asset_id = int(media_asset_id)
        existing_selected = self.db.get_selected_order_item_image_link(order_item_id)
        if existing_selected and int(existing_selected.get("media_asset_id", 0) or 0) == media_asset_id:
            logging.debug("Bildkandidat bereits aktiv an Bestellposition: order_item_id=%s, media_asset_id=%s", order_item_id, media_asset_id)
            return {
                "link": existing_selected,
                "selected": bool(existing_selected.get("is_selected")),
                "decision_state": str(existing_selected.get("decision_state", "") or self.ORDER_ITEM_STATE_CANDIDATE),
            }

        link = self.db.upsert_order_item_image_link(
            order_item_id=order_item_id,
            media_asset_id=media_asset_id,
            decision_state=self.ORDER_ITEM_STATE_CANDIDATE,
            is_selected=False,
            is_rejected=False,
            selection_mode="auto",
            source_type=source_type,
            source_ref=source_ref,
            metadata=metadata or {},
        )
        if not link:
            return None

        selected = False
        if auto_select:
            allow_auto_select = True
            if existing_selected:
                existing_mode = str(existing_selected.get("selection_mode", "auto") or "auto").strip().lower()
                if existing_mode == "manual":
                    allow_auto_select = False
                elif not replace_existing_auto:
                    allow_auto_select = False
            if allow_auto_select:
                selected_link = self.db.set_order_item_image_selected(
                    order_item_id=order_item_id,
                    media_asset_id=media_asset_id,
                    decision_state=self.ORDER_ITEM_STATE_SELECTED_AUTO,
                    selection_mode="auto",
                    source_type=source_type,
                    source_ref=source_ref,
                    metadata=metadata or {},
                )
                if selected_link:
                    link = selected_link
                    selected = True
                    logging.info("Bildkandidat automatisch ausgewaehlt: order_item_id=%s, media_asset_id=%s", order_item_id, media_asset_id)
                    self._invalidate_visuals(reason="order_item_candidate_auto_selected", order_item_id=order_item_id, scope="order")
        if not selected:
            logging.info("Bildkandidat an Bestellposition registriert: order_item_id=%s, media_asset_id=%s", order_item_id, media_asset_id)
            self._invalidate_visuals(reason="order_item_candidate_registered", order_item_id=order_item_id, scope="order")
        return {
            "link": link,
            "selected": selected,
            "decision_state": str((link or {}).get("decision_state", "") or self.ORDER_ITEM_STATE_CANDIDATE),
        }

    def select_order_item_image(
        self,
        order_item_id,
        media_asset_id,
        selection_mode="manual",
        source_type="",
        source_ref="",
        metadata=None,
    ):
        if order_item_id in (None, "") or media_asset_id in (None, ""):
            logging.warning("Bildauswahl fuer Bestellposition uebersprungen: order_item_id=%s, media_asset_id=%s", order_item_id, media_asset_id)
            return None
        decision_state = self._selected_order_item_state(selection_mode)
        link = self.db.set_order_item_image_selected(
            order_item_id=int(order_item_id),
            media_asset_id=int(media_asset_id),
            decision_state=decision_state,
            selection_mode=selection_mode,
            source_type=source_type,
            source_ref=source_ref,
            metadata=metadata or {},
        )
        if link:
            logging.info("Bild an Bestellposition ausgewaehlt: order_item_id=%s, media_asset_id=%s, mode=%s", order_item_id, media_asset_id, selection_mode)
            self._invalidate_visuals(reason="order_item_image_selected", order_item_id=order_item_id, scope="order")
        return link

    def reject_order_item_image(
        self,
        order_item_id,
        media_asset_id,
        selection_mode="manual",
        source_type="",
        source_ref="",
        metadata=None,
    ):
        if order_item_id in (None, "") or media_asset_id in (None, ""):
            logging.warning("Bildverwerfung fuer Bestellposition uebersprungen: order_item_id=%s, media_asset_id=%s", order_item_id, media_asset_id)
            return None
        link = self.db.reject_order_item_image_link(
            order_item_id=int(order_item_id),
            media_asset_id=int(media_asset_id),
            selection_mode=selection_mode,
            source_type=source_type,
            source_ref=source_ref,
            metadata=metadata or {},
        )
        if link:
            logging.info("Bildkandidat an Bestellposition verworfen: order_item_id=%s, media_asset_id=%s", order_item_id, media_asset_id)
            self._invalidate_visuals(reason="order_item_image_rejected", order_item_id=order_item_id, scope="order")
        return link

    def set_manual_order_item_image(
        self,
        order_item_id,
        media_asset_id,
        source_type="manual",
        source_ref="",
        metadata=None,
    ):
        return self.select_order_item_image(
            order_item_id=order_item_id,
            media_asset_id=media_asset_id,
            selection_mode="manual",
            source_type=source_type,
            source_ref=source_ref,
            metadata=metadata,
        )

    def _coerce_int(self, value, default=0):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _resolve_visual_order_id(self, order_item_id):
        row = self.db.get_order_item_row(order_item_id) if order_item_id not in (None, "") else None
        return self._coerce_int((row or {}).get("einkauf_id"), 0)

    def _invalidate_visuals(self, reason="", einkauf_id=None, order_item_id=None, scope="global"):
        resolved_order_id = self._coerce_int(einkauf_id, 0)
        if resolved_order_id <= 0 and order_item_id not in (None, ""):
            resolved_order_id = self._resolve_visual_order_id(order_item_id)
        return OrderVisualState.invalidate(
            reason=reason,
            einkauf_id=resolved_order_id or None,
            scope=scope,
        )

    def _media_source_label(self, source_type="", source_kind="", source_note=""):
        token = " ".join(
            part for part in (str(source_type or "").strip(), str(source_kind or "").strip(), str(source_note or "").strip()) if part
        ).lower()
        if not token:
            return "Unbekannt"
        if "screenshot_detection_crop" in token or "crop" in token:
            return "Screenshot-Crop"
        if "global_product_image" in token:
            return "Globales Produktbild"
        if "legacy" in token:
            return "Legacy"
        if "ean" in token:
            return "EAN"
        if "manual" in token:
            return "Manuell"
        if "url" in token:
            return "bild_url"
        if "fallback" in token:
            return "Fallback"
        return (str(source_type or source_kind or source_note or "Unbekannt").replace("_", " ").strip() or "Unbekannt").title()

    def _build_order_item_candidate_view(
        self,
        media_asset_id=None,
        asset=None,
        link=None,
        path="",
        source_url="",
        storage_kind="",
        source_type="",
        source_kind="",
        source_note="",
        source_ref="",
        order_item_id=None,
        source_row_index=None,
        attached_to_order_item=False,
    ):
        link = dict(link or {}) if isinstance(link, dict) else None
        asset = dict(asset or {}) if isinstance(asset, dict) else None
        media_asset_id = self._coerce_int(media_asset_id or (asset or {}).get("id") or (link or {}).get("media_asset_id"), 0)
        decision_state = str((link or {}).get("decision_state", "") or self.ORDER_ITEM_STATE_CANDIDATE)
        is_selected = bool((link or {}).get("is_selected")) and not bool((link or {}).get("is_rejected"))
        is_rejected = bool((link or {}).get("is_rejected"))
        selection_mode = str((link or {}).get("selection_mode", "") or "auto").strip() or "auto"
        status_key = "candidate"
        status_label = "Kandidat"
        if is_rejected or decision_state == self.ORDER_ITEM_STATE_REJECTED:
            status_key = "rejected"
            status_label = "Verworfen"
        elif is_selected and decision_state == self.ORDER_ITEM_STATE_SELECTED_MANUAL:
            status_key = "selected_manual"
            status_label = "Manuell gesetzt"
        elif is_selected:
            status_key = "selected_auto"
            status_label = "Ausgewaehlt"
        elif not attached_to_order_item:
            status_key = "unmapped"
            status_label = "Noch nicht gemappt"

        return {
            "media_asset_id": media_asset_id,
            "asset": asset,
            "link": link,
            "path": str(path or ""),
            "source_url": str(source_url or ""),
            "storage_kind": str(storage_kind or ""),
            "source_type": str(source_type or (link or {}).get("source_type", "") or ""),
            "source_kind": str(source_kind or (asset or {}).get("source_kind", "") or ""),
            "source_note": str(source_note or (link or {}).get("source_ref", "") or ""),
            "source_ref": str(source_ref or (link or {}).get("source_ref", "") or (asset or {}).get("source_ref", "") or ""),
            "source_label": self._media_source_label(source_type or (link or {}).get("source_type", ""), source_kind or (asset or {}).get("source_kind", ""), source_note or (link or {}).get("source_ref", "")),
            "decision_state": decision_state,
            "selection_mode": selection_mode,
            "status_key": status_key,
            "status_label": status_label,
            "is_selected": is_selected,
            "is_rejected": is_rejected,
            "is_manual": selection_mode == "manual" or decision_state == self.ORDER_ITEM_STATE_SELECTED_MANUAL,
            "attached_to_order_item": bool(attached_to_order_item),
            "order_item_id": self._coerce_int(order_item_id or (link or {}).get("order_item_id"), 0),
            "source_row_index": self._coerce_int(source_row_index, -1),
        }

    def _dedupe_media_candidates(self, candidates):
        deduped = []
        seen = set()
        for candidate in candidates or []:
            if not isinstance(candidate, dict):
                continue
            key = (
                self._coerce_int(candidate.get("media_asset_id"), 0),
                str(candidate.get("path", "") or "").strip(),
                str(candidate.get("source_url", "") or "").strip(),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(candidate)
        return deduped

    def get_order_item_image_candidates(self, order_item_id, fallback_to_product=True):
        order_row = self.db.get_order_item_row(order_item_id)
        links = list(self.db.get_order_item_image_links(order_item_id) or [])
        selected_links = [row for row in links if bool(row.get("is_selected")) and not bool(row.get("is_rejected"))]
        if len(selected_links) > 1:
            logging.warning("Mehrere aktive Bildkandidaten fuer Bestellposition entdeckt: order_item_id=%s", order_item_id)

        candidates = []
        for link in links:
            asset = self.db.get_media_asset_by_id(link.get("media_asset_id")) if link.get("media_asset_id") else None
            resolved_asset = self._resolve_media_asset_reference(asset)
            candidates.append(
                self._build_order_item_candidate_view(
                    media_asset_id=(link or {}).get("media_asset_id"),
                    asset=resolved_asset.get("asset"),
                    link=link,
                    path=resolved_asset.get("path", ""),
                    source_url=resolved_asset.get("source_url", ""),
                    storage_kind=resolved_asset.get("storage_kind", ""),
                    order_item_id=order_item_id,
                    attached_to_order_item=True,
                )
            )

        candidates = self._dedupe_media_candidates(candidates)
        selected = next((candidate for candidate in candidates if candidate.get("is_selected") and not candidate.get("is_rejected")), None)
        fallback = None
        if not selected and fallback_to_product and order_row:
            fallback_resolved = self.resolve_product_image(
                product_name=order_row.get("produkt_name", ""),
                ean=order_row.get("ean", ""),
                variant_text=order_row.get("varianten_info", ""),
            )
            if fallback_resolved and (fallback_resolved.get("asset") or fallback_resolved.get("path") or fallback_resolved.get("source_url")):
                fallback = self._build_order_item_candidate_view(
                    media_asset_id=(fallback_resolved.get("asset") or {}).get("id"),
                    asset=fallback_resolved.get("asset"),
                    link=None,
                    path=fallback_resolved.get("path", ""),
                    source_url=fallback_resolved.get("source_url", ""),
                    storage_kind=fallback_resolved.get("storage_kind", ""),
                    source_type="global_product_image",
                    source_kind=str((fallback_resolved.get("asset") or {}).get("source_kind", "") or "global_product_image"),
                    source_note="global_product_fallback",
                    order_item_id=order_item_id,
                    attached_to_order_item=False,
                )
        return {
            "order_item": order_row,
            "candidates": candidates,
            "selected": selected,
            "fallback_global": fallback,
        }

    def resolve_order_item_selected_image(self, order_item_id, fallback_to_product=True):
        resolved = self.get_order_item_image_candidates(order_item_id, fallback_to_product=fallback_to_product)
        selected = resolved.get("selected")
        if selected:
            return {
                "order_item": resolved.get("order_item"),
                "link": selected.get("link"),
                "asset": selected.get("asset"),
                "path": selected.get("path", ""),
                "source_url": selected.get("source_url", ""),
                "storage_kind": selected.get("storage_kind", ""),
                "source": "order_item_selection",
            }
        fallback = resolved.get("fallback_global") or {}
        return {
            "order_item": resolved.get("order_item"),
            "link": fallback.get("link"),
            "asset": fallback.get("asset"),
            "path": str(fallback.get("path", "") or ""),
            "source_url": str(fallback.get("source_url", "") or ""),
            "storage_kind": str(fallback.get("storage_kind", "") or ""),
            "source": "global_product_fallback" if fallback else "",
        }

    def register_manual_item_candidate_from_file(
        self,
        product_name,
        file_path,
        ean="",
        variant_text="",
        order_item_id=None,
        source_module="wizard_image_review",
        source_ref="",
        metadata=None,
    ):
        product_result = self.register_product_image_from_file(
            product_name=product_name,
            file_path=file_path,
            ean=ean,
            variant_text=variant_text,
            preferred_name="manual-product-image",
            source_module=source_module,
            source_kind="manual_file",
            source_ref=source_ref or file_path,
            is_primary=False,
            priority=35,
            metadata=metadata or {},
        )
        asset = dict((product_result or {}).get("asset") or {})
        if not asset:
            return None
        if order_item_id not in (None, ""):
            self.register_order_item_image_candidate(
                order_item_id=order_item_id,
                media_asset_id=asset.get("id"),
                source_type="manual_file",
                source_ref=source_ref or file_path,
                metadata=metadata or {},
                auto_select=False,
                replace_existing_auto=False,
            )
            link = self.set_manual_order_item_image(
                order_item_id=order_item_id,
                media_asset_id=asset.get("id"),
                source_type="manual_file",
                source_ref=source_ref or file_path,
                metadata=metadata or {},
            )
            resolved = self._resolve_media_asset_reference(asset)
            return self._build_order_item_candidate_view(
                media_asset_id=asset.get("id"),
                asset=resolved.get("asset"),
                link=link,
                path=resolved.get("path", ""),
                source_url=resolved.get("source_url", ""),
                storage_kind=resolved.get("storage_kind", ""),
                source_type="manual_file",
                source_kind=str(asset.get("source_kind", "") or "manual_file"),
                source_note=source_ref or file_path,
                order_item_id=order_item_id,
                attached_to_order_item=True,
            )
        resolved = self._resolve_media_asset_reference(asset)
        return self._build_order_item_candidate_view(
            media_asset_id=asset.get("id"),
            asset=resolved.get("asset"),
            link=None,
            path=resolved.get("path", ""),
            source_url=resolved.get("source_url", ""),
            storage_kind=resolved.get("storage_kind", ""),
            source_type="manual_file",
            source_kind=str(asset.get("source_kind", "") or "manual_file"),
            source_note=source_ref or file_path,
            order_item_id=None,
            attached_to_order_item=False,
        )

    def _is_module1_ai_cropping_disabled(self, payload=None):
        return isinstance(payload, dict) and str(payload.get("_origin_module", "") or "").strip() == "modul_order_entry"

    def _build_module1_detection_skip_result(self, reason="module1_ai_cropping_disabled"):
        return {
            "processed": False,
            "reason": str(reason or "module1_ai_cropping_disabled"),
            "created": [],
            "rejected": [],
            "created_count": 0,
            "rejected_count": 0,
            "result_version": 4,
        }

    def _ensure_payload_detection_result(self, payload, source_module="wizard_image_review"):
        if not isinstance(payload, dict):
            return {"processed": False, "reason": "invalid_payload", "created": [], "rejected": [], "created_count": 0, "rejected_count": 0}
        if self._is_module1_ai_cropping_disabled(payload):
            logging.info(
                "module1_screenshot_detection_persist_skipped: source_module=%s, reason=%s",
                source_module,
                "phase_a_disable_module1_ai_cropping",
            )
            write_module1_trace(
                "module1_screenshot_detection_persist_skipped",
                source_module=str(source_module or ""),
                reason="phase_a_disable_module1_ai_cropping",
            )
            result = self._build_module1_detection_skip_result()
            payload["_wizard_detection_result"] = result
            return result
        cached = payload.get("_wizard_detection_result")
        if isinstance(cached, dict):
            try:
                cached_version = int(cached.get("result_version", 1) or 1)
            except (TypeError, ValueError):
                cached_version = 1
            if cached_version >= 3:
                rejected_errors = " | ".join(
                    str(row.get("error", "") or "")
                    for row in list(cached.get("rejected", []) or [])
                    if isinstance(row, dict)
                )
                if int(cached.get("created_count", 0) or 0) <= 0 and (
                    "Registrierter Screenshot nicht verfuegbar" in rejected_errors
                    or "Kein nutzbarer Screenshot-Pfad verfuegbar" in rejected_errors
                ):
                    logging.info("Wizard-Detektionscache wird wegen altem Screenshot-Fehler neu aufgebaut.")
                else:
                    return cached
            else:
                logging.info("Wizard-Detektionscache wird neu aufgebaut: alte_version=%s", cached_version)
        result = self.register_payload_screenshot_detections(
            payload,
            source_module=source_module,
            source_kind="wizard_payload_detection",
        )
        payload["_wizard_detection_result"] = result
        return result

    def get_payload_item_image_candidates(self, payload, source_row_index, review_row=None, fallback_to_product=True, source_module="wizard_image_review"):
        payload = payload if isinstance(payload, dict) else {}
        try:
            source_row_index = int(source_row_index)
        except (TypeError, ValueError):
            source_row_index = -1
        waren = list(payload.get("waren", []) or [])
        if source_row_index < 0 or source_row_index >= len(waren):
            return {"source_row_index": source_row_index, "order_item_id": None, "item": {}, "candidates": [], "selected": None, "fallback_global": None}

        item = waren[source_row_index] if isinstance(waren[source_row_index], dict) else {}
        review_row = review_row if isinstance(review_row, dict) else {}
        order_item_id = self._coerce_int(review_row.get("match_position_id"), 0)
        module1_ai_cropping_disabled = self._is_module1_ai_cropping_disabled(payload)
        base_candidates = []
        selected = None
        fallback = None
        if order_item_id > 0:
            existing = self.get_order_item_image_candidates(order_item_id, fallback_to_product=fallback_to_product)
            existing_candidates = list(existing.get("candidates", []) or [])
            if module1_ai_cropping_disabled:
                filtered_candidates = [
                    row
                    for row in existing_candidates
                    if isinstance(row, dict) and str(row.get("source_type", "") or "").strip() != "screenshot_detection_crop"
                ]
                suppressed_count = len(existing_candidates) - len(filtered_candidates)
                if suppressed_count > 0:
                    logging.info(
                        "module1_detection_candidates_suppressed: source_module=%s, row=%s, suppressed=%s, reason=%s",
                        source_module,
                        source_row_index,
                        suppressed_count,
                        "phase_a_disable_module1_ai_cropping",
                    )
                    write_module1_trace(
                        "module1_detection_candidates_suppressed",
                        source_row_index=int(source_row_index),
                        suppressed_count=int(suppressed_count),
                        reason="phase_a_disable_module1_ai_cropping",
                    )
                existing_candidates = filtered_candidates
                selected_row = existing.get("selected") if isinstance(existing.get("selected"), dict) else None
                selected = selected_row if isinstance(selected_row, dict) and str(selected_row.get("source_type", "") or "").strip() != "screenshot_detection_crop" else None
                fallback_row = existing.get("fallback_global") if isinstance(existing.get("fallback_global"), dict) else None
                fallback = fallback_row if isinstance(fallback_row, dict) and str(fallback_row.get("source_type", "") or "").strip() != "screenshot_detection_crop" else None
            else:
                selected = existing.get("selected")
                fallback = existing.get("fallback_global")
            base_candidates.extend(existing_candidates)

        if not fallback and fallback_to_product:
            fallback_resolved = self.resolve_product_image(item=item, payload=payload or item)
            if fallback_resolved and (fallback_resolved.get("asset") or fallback_resolved.get("path") or fallback_resolved.get("source_url")):
                fallback_path = str(fallback_resolved.get("path", "") or "")
                fallback_asset = fallback_resolved.get("asset") if isinstance(fallback_resolved.get("asset"), dict) else {}
                fallback_link = fallback_resolved.get("link") if isinstance(fallback_resolved.get("link"), dict) else {}
                fallback_source_kind = str((fallback_asset or {}).get("source_kind", "") or "global_product_image")
                fallback_reason = ""
                fallback_metrics = {"aspect_ratio": 0.0, "non_light_ratio": 0.0}
                if fallback_source_kind == "screenshot_detection_crop":
                    fallback_ok, fallback_metrics = self._is_previewable_detection_crop(fallback_path, role="product_image")
                    fallback_thumb = self._looks_like_product_thumbnail(fallback_path)
                    if not bool((fallback_link or {}).get("is_primary")):
                        fallback_reason = "non_primary_screenshot_crop"
                    elif not fallback_path:
                        fallback_reason = "missing_local_path"
                    elif not fallback_ok:
                        fallback_reason = "not_previewable"
                    elif not fallback_thumb:
                        fallback_reason = "not_thumbnail_like"
                    if fallback_reason:
                        aspect_ratio = float(fallback_metrics.get("aspect_ratio", 0.0) or 0.0)
                        non_light_ratio = float(fallback_metrics.get("non_light_ratio", 0.0) or 0.0)
                        logging.info(
                            "Screenshot-Crop als globaler Fallback verworfen: row=%s, asset_id=%s, reason=%s, aspect=%.2f, non_light=%.3f",
                            source_row_index,
                            int((fallback_asset or {}).get("id") or 0),
                            fallback_reason,
                            aspect_ratio,
                            non_light_ratio,
                        )
                        if str(payload.get("_origin_module", "") or "") == "modul_order_entry":
                            write_module1_trace(
                                "detection_crop_rejected_as_global_fallback",
                                asset_id=int((fallback_asset or {}).get("id") or 0),
                                ware_index=int(source_row_index),
                                reason=fallback_reason,
                                aspect_ratio=aspect_ratio,
                                non_light_ratio=non_light_ratio,
                            )
                            write_module1_trace(
                                "product_image_fallback_suppressed",
                                asset_id=int((fallback_asset or {}).get("id") or 0),
                                ware_index=int(source_row_index),
                                reason=fallback_reason,
                                aspect_ratio=aspect_ratio,
                                non_light_ratio=non_light_ratio,
                            )
                        fallback_resolved = None
                if fallback_resolved:
                    fallback = self._build_order_item_candidate_view(
                        media_asset_id=(fallback_asset or {}).get("id"),
                        asset=fallback_asset,
                        link=None,
                        path=fallback_resolved.get("path", ""),
                        source_url=fallback_resolved.get("source_url", ""),
                        storage_kind=fallback_resolved.get("storage_kind", ""),
                        source_type="global_product_image",
                        source_kind=fallback_source_kind,
                        source_note="wizard_payload",
                        order_item_id=order_item_id,
                        source_row_index=source_row_index,
                        attached_to_order_item=False,
                    )

        detection_result = self._build_module1_detection_skip_result() if module1_ai_cropping_disabled else self._ensure_payload_detection_result(payload, source_module=source_module)
        if module1_ai_cropping_disabled:
            logging.info(
                "module1_detection_candidates_suppressed: source_module=%s, row=%s, reason=%s",
                source_module,
                source_row_index,
                "phase_a_disable_module1_ai_cropping",
            )
            write_module1_trace(
                "module1_detection_candidates_suppressed",
                source_row_index=int(source_row_index),
                suppressed_count=0,
                reason="phase_a_disable_module1_ai_cropping",
            )
        for created in list((detection_result or {}).get("created", []) or []):
            detection = created.get("validated_detection", created.get("detection", {})) if isinstance(created, dict) else {}
            crop_asset = created.get("crop_asset", {}) if isinstance(created, dict) else {}
            asset_id = (crop_asset or {}).get("id") if isinstance(crop_asset, dict) else None
            if not asset_id:
                continue
            resolved_row = self._resolve_detection_source_row_index(
                payload,
                detection,
                log_context=f"wizard_row_{int(source_row_index)}",
            )
            detection_index = int(resolved_row.get("source_row_index", -1) or -1)
            if detection_index != source_row_index:
                continue
            logging.info("Crop-Kandidat fuer Wizard-Zeile erzeugt: row=%s, asset_id=%s, reason=%s", source_row_index, asset_id, str(resolved_row.get("reason", "") or ""))
            resolved_crop = self._resolve_media_asset_reference(crop_asset)
            crop_path = str(resolved_crop.get("path", "") or "")
            detection_role = self._effective_detection_preview_role(detection, crop_path)
            preview_ok, preview_metrics = self._is_previewable_detection_crop(crop_path, detection=detection, role=detection_role)
            if not preview_ok:
                logging.info(
                    "Detection-Crop fuer Vorschau verworfen: row=%s, asset_id=%s, role=%s, aspect=%.2f, non_light=%.3f, distinct=%s",
                    source_row_index,
                    asset_id,
                    detection_role,
                    float(preview_metrics.get("aspect_ratio", 0.0) or 0.0),
                    float(preview_metrics.get("non_light_ratio", 0.0) or 0.0),
                    int(preview_metrics.get("distinct_count", 0) or 0),
                )
                if str(payload.get("_origin_module", "") or "") == "modul_order_entry":
                    write_module1_trace(
                        "wizard_detection_crop_preview_rejected",
                        source_row_index=int(source_row_index),
                        asset_id=int(asset_id or 0),
                        detection_role=str(detection_role or ""),
                        aspect_ratio=float(preview_metrics.get("aspect_ratio", 0.0) or 0.0),
                        non_light_ratio=float(preview_metrics.get("non_light_ratio", 0.0) or 0.0),
                        distinct_count=int(preview_metrics.get("distinct_count", 0) or 0),
                        crop_path=crop_path,
                    )
                continue
            if detection_role == "product_image":
                logging.info(
                    "Detection-Crop als Produktthumbnail eingestuft: row=%s, asset_id=%s, aspect=%.2f",
                    source_row_index,
                    asset_id,
                    float(preview_metrics.get("aspect_ratio", 0.0) or 0.0),
                )
            candidate_view = self._build_order_item_candidate_view(
                media_asset_id=asset_id,
                asset=resolved_crop.get("asset"),
                link=None,
                path=crop_path,
                source_url=resolved_crop.get("source_url", ""),
                storage_kind=resolved_crop.get("storage_kind", ""),
                source_type="screenshot_detection_crop",
                source_kind=str((crop_asset or {}).get("source_kind", "") or "screenshot_detection_crop"),
                source_note=str(detection.get("produkt_name_hint", "") or detection_role or "wizard-crop"),
                order_item_id=order_item_id,
                source_row_index=source_row_index,
                attached_to_order_item=False,
            )
            candidate_view["detection_role"] = detection_role
            candidate_view["preview_priority"] = self._preview_priority_for_detection_role(detection_role)
            base_candidates.append(candidate_view)

        if fallback and not any(self._coerce_int(row.get("media_asset_id"), 0) == self._coerce_int(fallback.get("media_asset_id"), 0) and self._coerce_int(fallback.get("media_asset_id"), 0) > 0 for row in base_candidates):
            base_candidates.insert(0, fallback)

        base_candidates = self._dedupe_media_candidates(base_candidates)
        if module1_ai_cropping_disabled:
            before_count = len(list(base_candidates or []))
            base_candidates = [
                row
                for row in list(base_candidates or [])
                if isinstance(row, dict) and str(row.get("source_type", "") or "").strip() != "screenshot_detection_crop"
            ]
            if isinstance(selected, dict) and str(selected.get("source_type", "") or "").strip() == "screenshot_detection_crop":
                selected = None
            if isinstance(fallback, dict) and str(fallback.get("source_type", "") or "").strip() == "screenshot_detection_crop":
                fallback = None
            suppressed_after_dedupe = before_count - len(base_candidates)
            if suppressed_after_dedupe > 0:
                write_module1_trace(
                    "module1_detection_candidates_suppressed",
                    source_row_index=int(source_row_index),
                    suppressed_count=int(suppressed_after_dedupe),
                    reason="phase_a_disable_module1_ai_cropping_post_dedupe",
                )
        if not selected:
            selected = next((row for row in base_candidates if row.get("is_selected") and not row.get("is_rejected")), None)
        if str(payload.get("_origin_module", "") or "") == "modul_order_entry":
            write_module1_trace(
                "wizard_image_candidates_resolved",
                source_row_index=int(source_row_index),
                candidate_count=len(list(base_candidates or [])),
                selected_asset_id=selected.get("media_asset_id") if isinstance(selected, dict) else None,
                fallback_asset_id=fallback.get("media_asset_id") if isinstance(fallback, dict) else None,
                detection_result=dict(detection_result or {}),
                candidate_roles=[
                    {
                        "asset_id": row.get("media_asset_id"),
                        "source_type": row.get("source_type"),
                        "detection_role": row.get("detection_role"),
                        "path": str(row.get("path", "") or ""),
                    }
                    for row in list(base_candidates or [])
                    if isinstance(row, dict)
                ],
            )
        return {
            "source_row_index": source_row_index,
            "order_item_id": order_item_id or None,
            "item": item,
            "candidates": base_candidates,
            "selected": selected,
            "fallback_global": fallback,
            "detection_result": detection_result,
        }

    def apply_payload_image_decisions(self, einkauf_id, payload, decisions, source_module="wizard_image_review", source_ref=""):
        if einkauf_id in (None, "") or not isinstance(payload, dict) or not isinstance(decisions, dict):
            return {"processed": False, "reason": "invalid_input", "applied_count": 0, "rejected_count": 0, "mapped_rows": 0}

        mapping = self.build_order_item_source_map(einkauf_id, payload)
        source_row_map = mapping.get("source_row_map", {}) or {}
        applied_count = 0
        rejected_count = 0
        mapped_rows = 0

        for raw_index, raw_decision in (decisions or {}).items():
            try:
                source_row_index = int(raw_index)
            except (TypeError, ValueError):
                continue
            decision = dict(raw_decision or {}) if isinstance(raw_decision, dict) else {}
            order_items = list(source_row_map.get(source_row_index, []) or [])
            if not order_items:
                logging.info("Bildentscheidung fuer Wizard-Zeile konnte noch keiner Bestellposition zugeordnet werden: source_row_index=%s", source_row_index)
                continue
            mapped_rows += 1
            candidate_asset_ids = {
                self._coerce_int(asset_id, 0)
                for asset_id in list(decision.get("candidate_asset_ids", []) or [])
                if self._coerce_int(asset_id, 0) > 0
            }
            rejected_asset_ids = {
                self._coerce_int(asset_id, 0)
                for asset_id in list(decision.get("rejected_asset_ids", []) or [])
                if self._coerce_int(asset_id, 0) > 0
            }
            selected_asset_id = self._coerce_int(decision.get("selected_asset_id"), 0)
            selected_mode = str(decision.get("selected_mode", "manual") or "manual").strip() or "manual"
            if selected_asset_id > 0:
                candidate_asset_ids.add(selected_asset_id)

            for order_item in order_items:
                order_item_id = self._coerce_int((order_item or {}).get("id"), 0)
                if order_item_id <= 0:
                    continue
                for asset_id in sorted(candidate_asset_ids):
                    self.register_order_item_image_candidate(
                        order_item_id=order_item_id,
                        media_asset_id=asset_id,
                        source_type="wizard_candidate",
                        source_ref=source_ref or f"row:{source_row_index}",
                        metadata={"source_row_index": source_row_index, "source_module": source_module},
                        auto_select=False,
                        replace_existing_auto=False,
                    )
                for asset_id in sorted(rejected_asset_ids):
                    self.reject_order_item_image(
                        order_item_id=order_item_id,
                        media_asset_id=asset_id,
                        selection_mode="manual",
                        source_type="wizard_reject",
                        source_ref=source_ref or f"row:{source_row_index}",
                        metadata={"source_row_index": source_row_index, "source_module": source_module},
                    )
                    rejected_count += 1
                if selected_asset_id > 0:
                    self.select_order_item_image(
                        order_item_id=order_item_id,
                        media_asset_id=selected_asset_id,
                        selection_mode=selected_mode,
                        source_type="wizard_manual_selection" if selected_mode == "manual" else "wizard_selection",
                        source_ref=source_ref or f"row:{source_row_index}",
                        metadata={"source_row_index": source_row_index, "source_module": source_module},
                    )
                    applied_count += 1

        if applied_count or rejected_count or mapped_rows:
            self._invalidate_visuals(reason="payload_image_decisions_applied", einkauf_id=einkauf_id, scope="order")
        return {
            "processed": True,
            "reason": "ok",
            "applied_count": applied_count,
            "rejected_count": rejected_count,
            "mapped_rows": mapped_rows,
        }

    def build_order_item_source_map(self, einkauf_id, payload):
        payload = payload if isinstance(payload, dict) else {}
        order_items = list(self.db.get_order_items_for_order(einkauf_id) or [])
        source_units = []
        for source_index, ware in enumerate(payload.get("waren", []) or []):
            if not isinstance(ware, dict):
                continue
            try:
                quantity = max(1, int(self.db._to_int(ware.get("menge", 1), default=1)))
            except Exception:
                quantity = 1
            unit_template = {
                "source_row_index": int(source_index),
                "produkt_name": str(ware.get("produkt_name", "") or "").strip(),
                "varianten_info": str(ware.get("varianten_info", "") or "").strip(),
                "ean": str(ware.get("ean", "") or "").strip(),
            }
            for _ in range(quantity):
                source_units.append(dict(unit_template))

        used_source_units = set()
        source_row_map = {}
        unmatched_order_item_ids = []
        for order_item in order_items:
            best_index = None
            best_score = 0.0
            for unit_index, source_unit in enumerate(source_units):
                if unit_index in used_source_units:
                    continue
                score = float(
                    self.db._score_inventory_match(
                        {"produkt_name": source_unit.get("produkt_name", ""), "ean": source_unit.get("ean", "")},
                        order_item,
                    )
                    or 0.0
                )
                if str(source_unit.get("varianten_info", "") or "").strip() and str(source_unit.get("varianten_info", "") or "").strip() == str(order_item.get("varianten_info", "") or "").strip():
                    score += 0.05
                if score > best_score:
                    best_score = score
                    best_index = unit_index
            if best_index is None or best_score < 0.40:
                unmatched_order_item_ids.append(int(order_item.get("id", 0) or 0))
                continue
            used_source_units.add(best_index)
            source_row_index = int(source_units[best_index].get("source_row_index", -1) or -1)
            source_row_map.setdefault(source_row_index, []).append(dict(order_item))
        return {
            "einkauf_id": int(einkauf_id),
            "order_items": order_items,
            "source_row_map": source_row_map,
            "unmatched_order_item_ids": [row_id for row_id in unmatched_order_item_ids if row_id],
        }

    def register_order_item_candidates_from_payload(
        self,
        einkauf_id,
        payload,
        detection_result=None,
        source_module="",
        source_kind="order_item_image_payload",
    ):
        if einkauf_id in (None, "") or not isinstance(payload, dict):
            return {"processed": False, "reason": "invalid_input", "candidate_count": 0, "selected_count": 0, "mapping": {}}

        mapping = self.build_order_item_source_map(einkauf_id, payload)
        source_row_map = mapping.get("source_row_map", {}) or {}
        candidate_count = 0
        selected_count = 0
        bestellnummer = str(payload.get("bestellnummer", "") or "").strip()

        for source_index, ware in enumerate(payload.get("waren", []) or []):
            if not isinstance(ware, dict):
                continue
            order_items = source_row_map.get(int(source_index), []) or []
            if not order_items:
                continue
            image_link = self.get_product_image_link(
                product_name=str(ware.get("produkt_name", "") or "").strip(),
                ean=str(ware.get("ean", "") or "").strip(),
                variant_text=str(ware.get("varianten_info", "") or "").strip(),
            )
            link_row = image_link.get("link") if isinstance(image_link, dict) else None
            media_asset_id = link_row.get("media_asset_id") if isinstance(link_row, dict) else None
            if not media_asset_id:
                continue
            for order_item in order_items:
                registered = self.register_order_item_image_candidate(
                    order_item_id=order_item.get("id"),
                    media_asset_id=media_asset_id,
                    source_type="global_product_image",
                    source_ref=bestellnummer or f"order:{einkauf_id}",
                    metadata={
                        "bestellnummer": bestellnummer,
                        "source_row_index": int(source_index),
                        "source_kind": source_kind,
                    },
                    auto_select=True,
                    replace_existing_auto=False,
                )
                if registered:
                    candidate_count += 1
                    if registered.get("selected"):
                        selected_count += 1

        created_detections = list((detection_result or {}).get("created", []) or [])
        for created in created_detections:
            detection = created.get("validated_detection", created.get("detection", {})) if isinstance(created, dict) else {}
            crop_asset = created.get("crop_asset", {}) if isinstance(created, dict) else {}
            resolved_row = self._resolve_detection_source_row_index(payload, detection, log_context=f"order_candidates_{int(einkauf_id or 0)}")
            source_index = int(resolved_row.get("source_row_index", -1) or -1)
            crop_asset_id = crop_asset.get("id") if isinstance(crop_asset, dict) else None
            if source_index < 0 or not crop_asset_id:
                continue
            order_items = source_row_map.get(source_index, []) or []
            if not order_items:
                logging.debug("Kein Mapping fuer Crop-Kandidat an Bestellposition gefunden: einkauf_id=%s, source_row_index=%s", einkauf_id, source_index)
                continue
            for order_item in order_items:
                registered = self.register_order_item_image_candidate(
                    order_item_id=order_item.get("id"),
                    media_asset_id=crop_asset_id,
                    source_type="screenshot_detection_crop",
                    source_ref=str(created.get("primary_reason", "") or f"crop:{crop_asset_id}"),
                    metadata={
                        "bestellnummer": bestellnummer,
                        "source_row_index": source_index,
                        "confidence": created.get("confidence", 0.0),
                        "region_id": (created.get("region") or {}).get("id") if isinstance(created.get("region"), dict) else None,
                        "is_primary_candidate": bool(created.get("is_primary", False)),
                    },
                    auto_select=bool(created.get("is_primary", False)),
                    replace_existing_auto=bool(created.get("is_primary", False)),
                )
                if registered:
                    candidate_count += 1
                    if registered.get("selected"):
                        selected_count += 1

        logging.info(
            "Bildkandidaten fuer Bestellpositionen verarbeitet: einkauf_id=%s, kandidaten=%s, ausgewaehlt=%s, ungemappt=%s",
            einkauf_id,
            candidate_count,
            selected_count,
            len(mapping.get("unmatched_order_item_ids", []) or []),
        )
        if candidate_count or selected_count:
            self._invalidate_visuals(reason="payload_image_candidates_registered", einkauf_id=einkauf_id, scope="order")
        return {
            "processed": True,
            "reason": "ok",
            "candidate_count": int(candidate_count),
            "selected_count": int(selected_count),
            "mapping": mapping,
        }

    def build_order_visual_preview(self, einkauf_id, shop_name="", sender_domain="", payload=None, max_item_images=2):
        order_items = list(self.db.get_order_items_for_order(einkauf_id) or [])
        previews = []
        seen_keys = set()
        for order_item in order_items:
            resolved = self.resolve_order_item_selected_image(order_item.get("id"), fallback_to_product=True)
            key = str((resolved.get("asset") or {}).get("id") or resolved.get("path") or resolved.get("source_url") or "").strip()
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            previews.append(
                {
                    "order_item_id": int(order_item.get("id", 0) or 0),
                    "produkt_name": str(order_item.get("produkt_name", "") or "").strip(),
                    "path": str(resolved.get("path", "") or ""),
                    "source_url": str(resolved.get("source_url", "") or ""),
                    "storage_kind": str(resolved.get("storage_kind", "") or ""),
                    "source": str(resolved.get("source", "") or ""),
                }
            )
            if len(previews) >= max(1, int(max_item_images or 2)):
                break
        shop_resolved = self.resolve_shop_logo(shop_name=shop_name, sender_domain=sender_domain, payload=payload)
        return {
            "einkauf_id": int(einkauf_id),
            "shop": shop_resolved,
            "item_previews": previews,
            "remaining_item_count": max(0, len(order_items) - len(previews)),
        }
    def build_package_visual_preview(self, ausgangs_paket_id, max_item_images=2):
        package_items = list(self.db.get_order_items_for_package(ausgangs_paket_id) or [])
        previews = []
        seen_keys = set()
        order_stats = {}
        for index, order_item in enumerate(package_items):
            order_id = self._coerce_int(order_item.get("einkauf_id"), 0)
            if order_id <= 0:
                continue
            stats = order_stats.setdefault(
                order_id,
                {
                    "count": 0,
                    "first_index": index,
                    "shop_name": str(order_item.get("shop_name", "") or "").strip(),
                    "bestellnummer": str(order_item.get("bestellnummer", "") or "").strip(),
                },
            )
            stats["count"] += 1
            if not stats.get("shop_name"):
                stats["shop_name"] = str(order_item.get("shop_name", "") or "").strip()
            if not stats.get("bestellnummer"):
                stats["bestellnummer"] = str(order_item.get("bestellnummer", "") or "").strip()

        primary_order_id = 0
        primary_shop_name = ""
        primary_bestellnummer = ""
        if order_stats:
            primary_order_id, primary_stats = sorted(
                order_stats.items(),
                key=lambda entry: (-int(entry[1].get("count", 0) or 0), int(entry[1].get("first_index", 0) or 0), int(entry[0] or 0)),
            )[0]
            primary_shop_name = str(primary_stats.get("shop_name", "") or "").strip()
            primary_bestellnummer = str(primary_stats.get("bestellnummer", "") or "").strip()

        for order_item in package_items:
            resolved = self.resolve_order_item_selected_image(order_item.get("id"), fallback_to_product=True)
            key = str((resolved.get("asset") or {}).get("id") or resolved.get("path") or resolved.get("source_url") or "").strip()
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            previews.append(
                {
                    "order_item_id": int(order_item.get("id", 0) or 0),
                    "einkauf_id": int(order_item.get("einkauf_id", 0) or 0),
                    "bestellnummer": str(order_item.get("bestellnummer", "") or "").strip(),
                    "produkt_name": str(order_item.get("produkt_name", "") or "").strip(),
                    "path": str(resolved.get("path", "") or ""),
                    "source_url": str(resolved.get("source_url", "") or ""),
                    "storage_kind": str(resolved.get("storage_kind", "") or ""),
                    "source": str(resolved.get("source", "") or ""),
                }
            )
            if len(previews) >= max(1, int(max_item_images or 2)):
                break

        shop_resolved = self.resolve_shop_logo(shop_name=primary_shop_name, payload={"shop_name": primary_shop_name})
        return {
            "ausgangs_paket_id": int(ausgangs_paket_id or 0),
            "primary_order_id": int(primary_order_id or 0),
            "bestellnummer": primary_bestellnummer,
            "shop": shop_resolved,
            "item_previews": previews,
            "remaining_item_count": max(0, len(package_items) - len(previews)),
            "order_count": len(order_stats),
        }

    def _safe_detection_confidence(self, value, default=0.0):
        try:
            score = float(value)
        except (TypeError, ValueError):
            score = float(default)
        if score < 0.0:
            return 0.0
        if score > 1.0:
            return 1.0
        return score

    def _detection_role(self, detection):
        if not isinstance(detection, dict):
            return ""
        role = str(detection.get("product_key", "") or detection.get("role", "") or detection.get("kind", "") or "").strip().lower()
        if role:
            return role
        hint = str(detection.get("produkt_name_hint", "") or "").strip().lower()
        if hint.startswith("product image") or hint.startswith("image for ") or " product image " in f" {hint} ":
            return "product_image"
        return ""

    def _is_generic_detection_role(self, value):
        token = str(value or "").strip().lower()
        return token in {"product_image", "quantity", "price", "product_name", "name", "title", "image", "qty"}

    def _preview_priority_for_detection_role(self, value):
        token = str(value or "").strip().lower()
        if token == "product_image":
            return 0
        if token in {"product_name", "name", "title"}:
            return 1
        if token in {"quantity", "price", "qty"}:
            return 5
        return 2

    def _sample_preview_image_metrics(self, image_path):
        image = QImage(str(image_path or ""))
        if image.isNull():
            return {"valid": False, "width": 0, "height": 0, "aspect_ratio": 0.0, "non_light_ratio": 0.0, "distinct_count": 0}
        width = int(image.width())
        height = int(image.height())
        if width <= 0 or height <= 0:
            return {"valid": False, "width": width, "height": height, "aspect_ratio": 0.0, "non_light_ratio": 0.0, "distinct_count": 0}
        step_x = max(1, width // 48)
        step_y = max(1, height // 48)
        total = 0
        non_light = 0
        distinct = set()
        for y in range(0, height, step_y):
            for x in range(0, width, step_x):
                color = image.pixelColor(x, y)
                total += 1
                red = int(color.red())
                green = int(color.green())
                blue = int(color.blue())
                if min(red, green, blue) < 245 or (max(red, green, blue) - min(red, green, blue)) > 16:
                    non_light += 1
                distinct.add((red // 24, green // 24, blue // 24))
        return {
            "valid": True,
            "width": width,
            "height": height,
            "aspect_ratio": (float(max(width, height)) / float(max(1, min(width, height)))),
            "non_light_ratio": (float(non_light) / float(max(1, total))),
            "distinct_count": len(distinct),
        }

    def _looks_like_product_thumbnail(self, image_path, detection=None):
        metrics = self._sample_preview_image_metrics(image_path)
        width = int(metrics.get("width", 0) or 0)
        height = int(metrics.get("height", 0) or 0)
        if (width <= 0 or height <= 0) and isinstance(detection, dict):
            width = self._coerce_int(detection.get("width"), 0)
            height = self._coerce_int(detection.get("height"), 0)
        min_edge = min(width, height) if width > 0 and height > 0 else 0
        aspect_ratio = float(metrics.get("aspect_ratio", 0.0) or 0.0)
        non_light_ratio = float(metrics.get("non_light_ratio", 0.0) or 0.0)
        distinct_count = int(metrics.get("distinct_count", 0) or 0)
        strict_thumbnail = bool(
            min_edge >= 24
            and aspect_ratio <= 2.35
            and non_light_ratio >= 0.06
            and distinct_count >= 6
        )
        relaxed_small_thumbnail = bool(
            min_edge >= 22
            and aspect_ratio <= 2.6
            and non_light_ratio >= 0.05
            and distinct_count >= 3
        )
        return bool(strict_thumbnail or relaxed_small_thumbnail)

    def _is_previewable_detection_crop(self, image_path, detection=None, role=""):
        metrics = self._sample_preview_image_metrics(image_path)
        width = int(metrics.get("width", 0) or 0)
        height = int(metrics.get("height", 0) or 0)
        if width <= 0 or height <= 0:
            return False, metrics
        min_edge = min(width, height)
        aspect_ratio = float(metrics.get("aspect_ratio", 0.0) or 0.0)
        non_light_ratio = float(metrics.get("non_light_ratio", 0.0) or 0.0)
        distinct_count = int(metrics.get("distinct_count", 0) or 0)
        token = str(role or "").strip().lower()
        if token == "product_image":
            strict_ok = bool(min_edge >= 22 and aspect_ratio <= 2.8 and non_light_ratio >= 0.04 and distinct_count >= 5)
            relaxed_ok = bool(min_edge >= 20 and aspect_ratio <= 2.8 and non_light_ratio >= 0.06 and distinct_count >= 3)
            return bool(strict_ok or relaxed_ok), metrics
        strict_generic_ok = bool(min_edge >= 18 and aspect_ratio <= 3.4 and non_light_ratio >= 0.035 and distinct_count >= 4)
        relaxed_generic_ok = bool(min_edge >= 20 and aspect_ratio <= 2.8 and non_light_ratio >= 0.055 and distinct_count >= 3)
        return bool(strict_generic_ok or relaxed_generic_ok), metrics

    def _effective_detection_preview_role(self, detection, image_path=""):
        detection_role = self._detection_role(detection)
        if detection_role:
            return detection_role
        if self._looks_like_product_thumbnail(image_path, detection=detection):
            return "product_image"
        return ""

    def _has_meaningful_logo_pixels(self, image_path):
        image = QImage(str(image_path or ""))
        if image.isNull():
            return False
        width = int(image.width())
        height = int(image.height())
        if width <= 0 or height <= 0:
            return False
        step_x = max(1, width // 48)
        step_y = max(1, height // 24)
        total = 0
        non_light = 0
        distinct = set()
        for y in range(0, height, step_y):
            for x in range(0, width, step_x):
                color = image.pixelColor(x, y)
                total += 1
                red = int(color.red())
                green = int(color.green())
                blue = int(color.blue())
                if min(red, green, blue) < 240 or (max(red, green, blue) - min(red, green, blue)) > 18:
                    non_light += 1
                distinct.add((red // 24, green // 24, blue // 24))
        if total <= 0:
            return False
        non_light_ratio = float(non_light) / float(total)
        has_content = non_light_ratio >= 0.035 or len(distinct) >= 10
        logging.info(
            "Shoplogo-Crop geprueft: path=%s, non_light_ratio=%.3f, distinct_colors=%s, accepted=%s",
            str(image_path or ""),
            non_light_ratio,
            len(distinct),
            has_content,
        )
        return bool(has_content)

    def normalize_detection_input(self, detection):
        normalized = MediaCropper.normalize_detection(detection)
        logging.info(
            "Detektion normalisiert: produkt_name_hint=%s, product_key=%s, ean=%s",
            normalized.get("produkt_name_hint", ""),
            normalized.get("product_key", ""),
            normalized.get("ean", ""),
        )
        return normalized

    def _resolve_detection_product_context(self, detection):
        normalized = self.normalize_detection_input(detection)
        raw_product_key = str(normalized.get("product_key", "") or "").strip()
        detection_role = self._detection_role(normalized)
        explicit_product_key = "" if self._is_generic_detection_role(raw_product_key) else raw_product_key
        context = self.resolve_product_context(
            product_name=normalized.get("produkt_name_hint", ""),
            ean=normalized.get("ean", ""),
            variant_text=normalized.get("variant_text", ""),
        )
        mapping_quality = "unknown"
        if explicit_product_key:
            context["product_key"] = explicit_product_key
            mapping_quality = "explicit"
        elif context.get("ean"):
            mapping_quality = "ean"
        elif context.get("product_name"):
            mapping_quality = "hint"
        context["produkt_name_hint"] = normalized.get("produkt_name_hint", "")
        context["mapping_quality"] = mapping_quality
        context["explicit_product_key"] = explicit_product_key
        context["detection_role"] = detection_role
        return context

    def _product_crop_bucket(self, product_key=""):
        resolved_key = str(product_key or "").strip() or "product:unknown"
        return f"{self._product_image_bucket(resolved_key)}/crops"

    def _should_set_detection_primary(self, product_context, confidence):
        confidence_value = self._safe_detection_confidence(confidence)
        if confidence_value < self.DETECTION_PRIMARY_CONFIDENCE:
            return False, "confidence_below_threshold"
        mapping_quality = str(product_context.get("mapping_quality", "unknown") or "unknown")
        if mapping_quality not in ("explicit", "ean"):
            return False, "mapping_not_strong_enough"
        existing = self.db.get_primary_product_image_link(
            product_key=product_context.get("product_key", ""),
            ean=product_context.get("ean", ""),
            product_name=product_context.get("product_name", ""),
            variant_text=product_context.get("variant_text", ""),
        )
        if existing and existing.get("media_asset_id"):
            return False, "primary_already_exists"
        return True, "high_confidence_crop"

    def register_product_crop_from_detection(
        self,
        screenshot_asset_id,
        detection,
        source_module="",
        source_kind="ki_detection",
        source_ref="",
        model_name="",
        payload=None,
        source_context=None,
        clamp=True,
    ):
        screenshot_resolved = self.resolve_screenshot_reference(
            screenshot_asset_id,
            payload=payload,
            source_context=source_context,
        )
        screenshot_asset = screenshot_resolved.get("asset")
        screenshot_path = str(screenshot_resolved.get("path", "") or "")
        if not screenshot_asset:
            raise ValueError(f"Registrierter Screenshot nicht verfuegbar: {screenshot_asset_id}")
        if not screenshot_path:
            raise ValueError(f"Kein nutzbarer Screenshot-Pfad verfuegbar: {screenshot_asset_id}")
        if screenshot_resolved.get("used_fallback_path"):
            logging.info(
                "Produkt-Crop nutzt alternativen Screenshot-Pfad: asset_id=%s, reason=%s",
                screenshot_asset_id,
                str(screenshot_resolved.get("path_reason", "") or "fallback"),
            )

        validated = MediaCropper.validate_detection_box(screenshot_path, detection, clamp=clamp)
        logging.info(
            "Detektions-Box validiert: screenshot_asset_id=%s, x=%s, y=%s, width=%s, height=%s, clamped=%s",
            screenshot_asset_id,
            validated.get("x"),
            validated.get("y"),
            validated.get("width"),
            validated.get("height"),
            validated.get("was_clamped"),
        )
        if str((payload or {}).get("_origin_module", "") or "") == "modul_order_entry":
            if str(validated.get("normalization_mode", "") or "") == "relative_1000_to_image":
                write_module1_trace(
                    "detection_relative_1000_normalized",
                    screenshot_asset_id=int(screenshot_asset_id),
                    raw_detection=dict(detection or {}) if isinstance(detection, dict) else detection,
                    normalized_detection=dict(validated or {}),
                )
        product_context = self._resolve_detection_product_context(validated)
        confidence_value = self._safe_detection_confidence(validated.get("confidence"), 0.0)
        label = (
            product_context.get("product_name")
            or product_context.get("produkt_name_hint")
            or product_context.get("product_key")
            or f"crop_{screenshot_asset_id}"
        )
        bucket = self._product_crop_bucket(product_context.get("product_key", ""))
        target_info = self.store.build_generated_path(
            bucket=bucket,
            preferred_name=label,
            extension=".png",
            token=f"crop_{screenshot_asset_id}",
        )
        crop_result = MediaCropper.crop_image(
            screenshot_path=screenshot_path,
            output_path=target_info["absolute_path"],
            x=validated["x"],
            y=validated["y"],
            width=validated["width"],
            height=validated["height"],
            image_format="PNG",
            clamp=clamp,
        )
        crop_asset = self.register_local_asset(
            source_path=crop_result["output_path"],
            media_type="product_image",
            bucket=bucket,
            preferred_name=label,
            source_module=source_module or str(screenshot_asset.get("source_module", "") or ""),
            source_kind="screenshot_detection_crop",
            source_ref=source_ref or label or f"screenshot:{screenshot_asset_id}",
            metadata={
                "derived_from_asset_id": int(screenshot_asset_id),
                "detection": validated,
                "model_name": str(model_name or "").strip(),
                "source_context": dict(source_context or {}),
                "product_context": dict(product_context or {}),
            },
        )
        stored_crop_path = self.store.resolve_path(crop_asset.get("file_path", ""))
        generated_crop_path = os.path.abspath(str(crop_result["output_path"]))
        if stored_crop_path and stored_crop_path != generated_crop_path and os.path.exists(generated_crop_path):
            try:
                os.remove(generated_crop_path)
            except OSError:
                pass

        is_primary, primary_reason = self._should_set_detection_primary(product_context, confidence_value)
        product_link = None
        mapping_quality = str(product_context.get("mapping_quality", "unknown") or "unknown")
        if mapping_quality != "unknown":
            product_link = self.link_product_image(
                product_name=product_context.get("product_name") or product_context.get("produkt_name_hint") or label,
                media_asset_id=crop_asset["id"],
                ean=product_context.get("ean", ""),
                variant_text=product_context.get("variant_text", ""),
                product_key=product_context.get("explicit_product_key", ""),
                is_primary=is_primary,
                priority=self.DETECTION_PRIMARY_PRIORITY if is_primary else self.DETECTION_CANDIDATE_PRIORITY,
                source_note="screenshot_detection_crop",
                metadata={
                    "confidence": confidence_value,
                    "mapping_quality": mapping_quality,
                    "primary_reason": primary_reason,
                    "screenshot_asset_id": int(screenshot_asset_id),
                    "model_name": str(model_name or "").strip(),
                    "source_context": dict(source_context or {}),
                },
                asset=crop_asset,
            )
            if is_primary:
                logging.info("Crop als primaeres Produktbild gesetzt: product_key=%s, asset_id=%s", product_context.get("product_key", ""), crop_asset.get("id"))
            else:
                logging.info("Crop als Kandidat gespeichert: product_key=%s, asset_id=%s, reason=%s", product_context.get("product_key", ""), crop_asset.get("id"), primary_reason)
        else:
            logging.debug("Crop keinem Produkt sicher zugeordnet: screenshot_asset_id=%s, label=%s", screenshot_asset_id, label)

        region = self.db.create_screenshot_region(
            screenshot_asset_id=screenshot_asset_id,
            crop_asset_id=crop_asset["id"],
            region_kind="product_detection",
            label=label,
            x=validated["x"],
            y=validated["y"],
            width=validated["width"],
            height=validated["height"],
            source_kind=source_kind,
            source_ref=source_ref or label,
            metadata={
                "produkt_name_hint": validated.get("produkt_name_hint", ""),
                "product_key": product_context.get("product_key", ""),
                "ean": product_context.get("ean", ""),
                "detection_confidence": confidence_value,
                "model_name": str(model_name or "").strip(),
                "source_context": dict(source_context or {}),
                "mapping_quality": mapping_quality,
                "is_primary": bool(is_primary),
                "primary_reason": primary_reason,
                "was_clamped": bool(validated.get("was_clamped", False)),
            },
        )
        if not region:
            raise RuntimeError("Screenshot-Region konnte fuer den Detection-Crop nicht gespeichert werden.")

        if str((payload or {}).get("_origin_module", "") or "") == "modul_order_entry":
            write_module1_trace(
                "detection_crop_created",
                screenshot_asset_id=int(screenshot_asset_id),
                screenshot_path=screenshot_path,
                detection=dict(validated or {}),
                crop_asset_id=crop_asset.get("id"),
                crop_path=stored_crop_path,
                mapping_quality=mapping_quality,
                primary_reason=primary_reason,
                detection_role=str(product_context.get("detection_role", "") or ""),
            )
        return {
            "screenshot_asset": screenshot_asset,
            "crop_asset": crop_asset,
            "region": region,
            "product_link": product_link,
            "product_context": product_context,
            "validated_detection": validated,
            "confidence": confidence_value,
            "is_primary": bool(is_primary),
            "primary_reason": primary_reason,
        }

    def register_screenshot_detections(
        self,
        screenshot_asset_id,
        detections,
        source_module="",
        source_kind="ki_detection_batch",
        source_ref="",
        model_name="",
        payload=None,
        source_context=None,
        clamp=True,
    ):
        created = []
        rejected = []
        for index, detection in enumerate(list(detections or [])):
            try:
                result = self.register_product_crop_from_detection(
                    screenshot_asset_id=screenshot_asset_id,
                    detection=detection,
                    source_module=source_module,
                    source_kind=source_kind,
                    source_ref=source_ref or f"detection:{index}",
                    model_name=model_name,
                    payload=payload,
                    source_context=source_context,
                    clamp=clamp,
                )
                created.append(result)
            except Exception as exc:
                log_exception(__name__, exc, extra={"screenshot_asset_id": screenshot_asset_id, "detection_index": index})
                detection_row = dict(detection or {}) if isinstance(detection, dict) else {}
                logging.warning("Detektion verworfen: screenshot_asset_id=%s, index=%s, ware_index=%s, product_key=%s, box=(%s,%s,%s,%s), reason=%s", screenshot_asset_id, index, str(detection_row.get("ware_index", "") or detection_row.get("waren_index", "")), str(detection_row.get("product_key", "") or ""), str(detection_row.get("x", "") or ""), str(detection_row.get("y", "") or ""), str(detection_row.get("width", "") or ""), str(detection_row.get("height", "") or ""), exc)
                if str((payload or {}).get("_origin_module", "") or "") == "modul_order_entry":
                    write_module1_trace(
                        "detection_rejected",
                        screenshot_asset_id=int(screenshot_asset_id),
                        detection_index=int(index),
                        detection=detection_row,
                        reason=str(exc),
                    )
                rejected.append({
                    "index": int(index),
                    "error": str(exc),
                    "detection": dict(detection or {}) if isinstance(detection, dict) else detection,
                })
        return {
            "screenshot_asset_id": int(screenshot_asset_id),
            "created": created,
            "rejected": rejected,
            "created_count": len(created),
            "rejected_count": len(rejected),
        }

    def _extract_payload_detections(self, payload):
        if not isinstance(payload, dict):
            return []
        direct = payload.get("screenshot_detections")
        if isinstance(direct, list):
            return list(direct)
        internal = payload.get("_screenshot_detections")
        if isinstance(internal, list):
            return list(internal)
        provider_meta = payload.get("_provider_meta", {}) if isinstance(payload.get("_provider_meta"), dict) else {}
        provider_extra = provider_meta.get("meta", {}) if isinstance(provider_meta.get("meta"), dict) else {}
        if isinstance(provider_extra.get("screenshot_detections"), list):
            return list(provider_extra.get("screenshot_detections") or [])
        return []

    def _is_screenshot_source_row(self, row):
        if not isinstance(row, dict):
            return False
        source_type = str(row.get("source_type", "") or "").strip().lower()
        mime_type = str(row.get("mime_type", "") or "").strip().lower()
        file_path = str(row.get("file_path", "") or "").strip().lower()
        if "screenshot" in source_type:
            return True
        if mime_type.startswith("image/"):
            return True
        return file_path.endswith((".png", ".jpg", ".jpeg", ".webp", ".bmp", ".gif"))

    def _looks_like_screenshot_path(self, path_value):
        path_text = str(path_value or "").strip().lower()
        return path_text.endswith((".png", ".jpg", ".jpeg", ".webp", ".bmp", ".gif"))

    def _resolve_payload_screenshot_asset_id(self, payload):
        if not isinstance(payload, dict):
            return None
        direct_asset_id = payload.get("_screenshot_media_asset_id")
        if direct_asset_id not in (None, ""):
            return direct_asset_id
        primary_asset_id = payload.get("_primary_scan_media_asset_id")
        primary_scan_path = str(payload.get("_primary_scan_file_path", "") or "").strip().lower()
        if primary_asset_id not in (None, "") and primary_scan_path.endswith((".png", ".jpg", ".jpeg", ".webp", ".bmp", ".gif")):
            return primary_asset_id
        for row in list(payload.get("_scan_sources", []) or []):
            if not self._is_screenshot_source_row(row):
                continue
            media_asset_id = row.get("media_asset_id") if isinstance(row, dict) else None
            if media_asset_id not in (None, ""):
                return media_asset_id
        return None

    def _resolve_existing_file_path(self, path_value):
        path_text = str(path_value or "").strip()
        if not path_text:
            return ""
        candidates = []
        resolved_path = self.store.resolve_path(path_text)
        if resolved_path:
            candidates.append(str(resolved_path))
        if os.path.isabs(path_text):
            candidates.append(path_text)
        else:
            candidates.append(os.path.abspath(path_text))
        seen = set()
        for candidate in candidates:
            candidate_text = str(candidate or "").strip()
            if not candidate_text or candidate_text in seen:
                continue
            seen.add(candidate_text)
            if os.path.exists(candidate_text):
                return candidate_text
        return ""

    def _collect_screenshot_path_candidates(self, payload=None, source_context=None):
        payload_dict = payload if isinstance(payload, dict) else {}
        context_dict = source_context if isinstance(source_context, dict) else {}
        candidates = []
        seen = set()

        def _append_candidate(path_value, reason):
            if not self._looks_like_screenshot_path(path_value):
                return
            resolved_path = self._resolve_existing_file_path(path_value)
            if not resolved_path or resolved_path in seen:
                return
            seen.add(resolved_path)
            candidates.append({"path": resolved_path, "reason": reason})

        _append_candidate(payload_dict.get("_registered_screenshot_path"), "payload_registered_screenshot_path")
        _append_candidate(payload_dict.get("_primary_scan_file_path"), "payload_primary_scan_file_path")
        _append_candidate(payload_dict.get("screenshot_path"), "payload_screenshot_path")
        _append_candidate(context_dict.get("screenshot_path"), "context_screenshot_path")
        _append_candidate(context_dict.get("primary_scan_file_path"), "context_primary_scan_file_path")

        for row in list(payload_dict.get("_scan_sources", []) or []):
            if isinstance(row, dict) and self._is_screenshot_source_row(row):
                _append_candidate(row.get("file_path"), "payload_scan_source")

        for row in list(context_dict.get("scan_sources", []) or []):
            if isinstance(row, dict) and self._is_screenshot_source_row(row):
                _append_candidate(row.get("file_path"), "context_scan_source")

        return candidates

    def _resolve_payload_screenshot_path(self, payload, source_context=None):
        candidates = self._collect_screenshot_path_candidates(payload=payload, source_context=source_context)
        if not candidates:
            return ""
        return str(candidates[0].get("path", "") or "")

    def resolve_screenshot_reference(self, screenshot_asset_id=None, payload=None, source_context=None):
        resolved = self.resolve_screenshot_asset(screenshot_asset_id)
        screenshot_asset = resolved.get("asset")
        screenshot_path = str(resolved.get("path", "") or "")
        if screenshot_path:
            return {
                "asset": screenshot_asset,
                "path": screenshot_path,
                "used_fallback_path": False,
                "path_reason": "registered_asset",
            }

        candidates = self._collect_screenshot_path_candidates(payload=payload, source_context=source_context)
        if candidates:
            candidate = dict(candidates[0] or {})
            logging.warning(
                "Screenshot-Pfad aus Fallback-Quelle verwendet: asset_id=%s, reason=%s, path=%s",
                screenshot_asset_id,
                str(candidate.get("reason", "") or "fallback"),
                str(candidate.get("path", "") or ""),
            )
            return {
                "asset": screenshot_asset,
                "path": str(candidate.get("path", "") or ""),
                "used_fallback_path": True,
                "path_reason": str(candidate.get("reason", "") or "fallback"),
            }

        if screenshot_asset_id not in (None, ""):
            logging.warning("Kein nutzbarer Screenshot-Pfad gefunden: asset_id=%s", screenshot_asset_id)
        return {
            "asset": screenshot_asset,
            "path": "",
            "used_fallback_path": False,
            "path_reason": "missing",
        }
    def _tokenize_detection_match_text(self, value):
        text = str(value or "").strip().lower()
        if not text:
            return []
        cleaned = []
        for char in text:
            cleaned.append(char if char.isalnum() else " ")
        return [token for token in "".join(cleaned).split() if len(token) >= 2]

    def _score_detection_payload_row_match(self, ware, detection):
        if not isinstance(ware, dict) or not isinstance(detection, dict):
            return 0
        detection_name = str(detection.get("produkt_name_hint") or detection.get("product_name_hint") or "").strip()
        detection_variant = str(detection.get("variant_text") or detection.get("varianten_info") or "").strip()
        detection_ean = normalize_ean_text(detection.get("ean", ""))

        ware_name = str(ware.get("produkt_name", "") or "").strip()
        ware_variant = str(ware.get("varianten_info", ware.get("variant_text", "")) or "").strip()
        ware_ean = normalize_ean_text(ware.get("ean", ""))

        score = 0
        if detection_ean and ware_ean and detection_ean == ware_ean:
            score += 100

        detection_name_tokens = self._tokenize_detection_match_text(detection_name)
        ware_name_tokens = self._tokenize_detection_match_text(ware_name)
        if detection_name_tokens and ware_name_tokens:
            overlap = len(set(detection_name_tokens).intersection(ware_name_tokens))
            if overlap:
                score += overlap * 12
                if overlap >= min(len(set(detection_name_tokens)), len(set(ware_name_tokens))):
                    score += 30
                elif overlap >= 2:
                    score += 12

        detection_variant_tokens = self._tokenize_detection_match_text(detection_variant)
        ware_variant_tokens = self._tokenize_detection_match_text(ware_variant)
        if detection_variant_tokens and ware_variant_tokens:
            variant_overlap = len(set(detection_variant_tokens).intersection(ware_variant_tokens))
            if variant_overlap:
                score += variant_overlap * 6
                if variant_overlap >= 2:
                    score += 8

        return score

    def _resolve_detection_source_row_index(self, payload, detection, log_context="detection"):
        if not isinstance(payload, dict) or not isinstance(detection, dict):
            return {"source_row_index": -1, "reason": "invalid_input"}
        waren = list(payload.get("waren", []) or [])
        if not waren:
            return {"source_row_index": -1, "reason": "no_waren"}

        ware_index_raw = detection.get("ware_index", detection.get("waren_index", ""))
        ware_index_text = str(ware_index_raw or "").strip()
        if ware_index_text:
            try:
                ware_index = int(ware_index_text)
            except (TypeError, ValueError):
                logging.warning("Detection-Zuordnung: ungueltiger ware_index verworfen: context=%s, ware_index=%s", log_context, ware_index_text)
            else:
                if 0 <= ware_index < len(waren):
                    logging.info("Detection mit ware_index uebernommen: context=%s, ware_index=%s", log_context, ware_index)
                    return {"source_row_index": ware_index, "reason": "ware_index"}
                logging.warning(
                    "Detection-Zuordnung: ware_index ausserhalb des Bereichs: context=%s, ware_index=%s, waren=%s",
                    log_context,
                    ware_index,
                    len(waren),
                )

        scored = []
        for row_index, ware in enumerate(waren):
            score = self._score_detection_payload_row_match(ware, detection)
            if score > 0:
                scored.append({"row_index": int(row_index), "score": int(score)})
        if not scored:
            logging.debug("Detection keiner Zeile sicher zugeordnet: context=%s", log_context)
            return {"source_row_index": -1, "reason": "no_plausible_fallback"}

        scored.sort(key=lambda row: (-int(row.get("score", 0) or 0), int(row.get("row_index", -1) or -1)))
        best = scored[0]
        second_score = int(scored[1].get("score", 0) or 0) if len(scored) > 1 else -1
        best_score = int(best.get("score", 0) or 0)
        best_index = int(best.get("row_index", -1) or -1)
        if best_score >= 60 and (second_score < 0 or best_score - second_score >= 15):
            logging.info(
                "Detection ueber produktbezogenen Fallback zugeordnet: context=%s, source_row_index=%s, score=%s",
                log_context,
                best_index,
                best_score,
            )
            return {"source_row_index": best_index, "reason": "produkt_name_fallback", "score": best_score}

        logging.debug(
            "Detection keiner Zeile sicher zugeordnet: context=%s, best_score=%s, second_score=%s",
            log_context,
            best_score,
            second_score,
        )
        return {"source_row_index": -1, "reason": "ambiguous_fallback", "score": best_score}
    def _resolve_payload_item_context(self, payload, detection):
        if not isinstance(payload, dict) or not isinstance(detection, dict):
            return {}
        resolved_row = self._resolve_detection_source_row_index(payload, detection, log_context="payload_item_context")
        ware_index = int(resolved_row.get("source_row_index", -1) or -1)
        waren = list(payload.get("waren", []) or [])
        if ware_index < 0 or ware_index >= len(waren):
            return {}
        ware = waren[ware_index]
        if not isinstance(ware, dict):
            return {}
        return {
            "ware_index": ware_index,
            "produkt_name_hint": str(ware.get("produkt_name", "") or "").strip(),
            "ean": str(ware.get("ean", "") or "").strip(),
            "variant_text": str(ware.get("varianten_info", ware.get("variant_text", "")) or "").strip(),
            "mapping_reason": str(resolved_row.get("reason", "") or ""),
        }

    def _enrich_detection_from_payload(self, payload, detection):
        if not isinstance(detection, dict):
            return detection
        enriched = dict(detection)
        item_context = self._resolve_payload_item_context(payload, enriched)
        if item_context.get("produkt_name_hint") and not str(enriched.get("produkt_name_hint") or enriched.get("product_name_hint") or "").strip():
            enriched["produkt_name_hint"] = item_context["produkt_name_hint"]
        if item_context.get("ean") and not str(enriched.get("ean", "") or "").strip():
            enriched["ean"] = item_context["ean"]
        if item_context.get("variant_text") and not str(enriched.get("variant_text") or enriched.get("varianten_info") or "").strip():
            enriched["variant_text"] = item_context["variant_text"]
        resolved_row = self._resolve_detection_source_row_index(payload, enriched, log_context="enrich_detection")
        if int(resolved_row.get("source_row_index", -1) or -1) >= 0:
            enriched["ware_index"] = int(resolved_row.get("source_row_index", -1) or -1)
        return enriched

    def _repair_missing_product_image_detections(self, payload, detections, screenshot_path):
        payload_dict = payload if isinstance(payload, dict) else {}
        rows = list(payload_dict.get("waren", []) or [])
        prepared = [dict(row) if isinstance(row, dict) else row for row in list(detections or [])]
        if not prepared or not rows:
            return prepared
        if not self._looks_like_screenshot_path(screenshot_path):
            return prepared

        screenshot_image = QImage(str(screenshot_path or ""))
        if screenshot_image.isNull():
            return prepared
        image_width = int(screenshot_image.width())
        image_height = int(screenshot_image.height())
        if image_width <= 0 or image_height <= 0:
            return prepared

        def _is_non_light(color):
            red = int(color.red())
            green = int(color.green())
            blue = int(color.blue())
            return bool(min(red, green, blue) < 245 or (max(red, green, blue) - min(red, green, blue)) > 16)

        def _region_metrics(x, y, width, height):
            x = max(0, int(x))
            y = max(0, int(y))
            width = max(1, int(width))
            height = max(1, int(height))
            if x >= image_width or y >= image_height:
                return None
            max_width = max(1, image_width - x)
            max_height = max(1, image_height - y)
            width = min(width, max_width)
            height = min(height, max_height)
            if width <= 0 or height <= 0:
                return None
            step_x = max(1, width // 40)
            step_y = max(1, height // 40)
            total = 0
            non_light = 0
            distinct = set()
            for py in range(y, y + height, step_y):
                for px in range(x, x + width, step_x):
                    color = screenshot_image.pixelColor(px, py)
                    total += 1
                    red = int(color.red())
                    green = int(color.green())
                    blue = int(color.blue())
                    if _is_non_light(color):
                        non_light += 1
                    distinct.add((red // 24, green // 24, blue // 24))
            if total <= 0:
                return None
            min_edge = min(width, height)
            aspect_ratio = float(max(width, height)) / float(max(1, min_edge))
            non_light_ratio = float(non_light) / float(max(1, total))
            distinct_count = len(distinct)
            is_thumbnail = bool(
                min_edge >= 22
                and aspect_ratio <= 2.8
                and non_light_ratio >= 0.045
                and distinct_count >= 5
            )
            return {
                "x": x,
                "y": y,
                "width": width,
                "height": height,
                "aspect_ratio": aspect_ratio,
                "non_light_ratio": non_light_ratio,
                "distinct_count": distinct_count,
                "is_thumbnail": is_thumbnail,
            }

        def _scan_compact_thumbnail_for_oob_detection(detection):
            if not isinstance(detection, dict):
                return None
            raw_x = self._coerce_int(detection.get("x"), -1)
            raw_y = self._coerce_int(detection.get("y"), -1)
            raw_w = self._coerce_int(detection.get("width"), 0)
            raw_h = self._coerce_int(detection.get("height"), 0)
            if raw_x < 0 or raw_w <= 0 or raw_h <= 0:
                return None
            if raw_y >= 0 and raw_y < image_height:
                return None
            if raw_x >= image_width:
                return None

            scan_w = max(24, min(raw_w, min(260, image_width)))
            scan_h = max(24, min(raw_h, min(220, image_height)))
            target_center_x = float(raw_x + (scan_w / 2.0))

            search_left = max(0, raw_x - max(18, scan_w // 2))
            search_right = min(image_width, raw_x + scan_w + max(20, scan_w))
            if search_right - search_left < scan_w:
                search_right = min(image_width, search_left + scan_w)
            if search_right - search_left < scan_w:
                return None

            scan_top = max(0, int(image_height * 0.30))
            max_scan_y = image_height - scan_h
            if max_scan_y <= scan_top:
                scan_top = max(0, max_scan_y)
            if max_scan_y < 0:
                return None

            step_x = max(2, scan_w // 6)
            step_y = max(2, scan_h // 6)
            best = None

            for candidate_y in range(scan_top, max_scan_y + 1, step_y):
                max_scan_x = search_right - scan_w
                for candidate_x in range(search_left, max_scan_x + 1, step_x):
                    metrics = _region_metrics(candidate_x, candidate_y, scan_w, scan_h)
                    if not metrics:
                        continue
                    if not bool(metrics.get("is_thumbnail")):
                        continue
                    aspect_ratio = float(metrics.get("aspect_ratio", 0.0) or 0.0)
                    non_light_ratio = float(metrics.get("non_light_ratio", 0.0) or 0.0)
                    distinct_count = int(metrics.get("distinct_count", 0) or 0)
                    if aspect_ratio > 2.8 or non_light_ratio < 0.055 or distinct_count < 5:
                        continue

                    center_x = float(candidate_x + (scan_w / 2.0))
                    x_alignment = max(0.0, 1.0 - (abs(center_x - target_center_x) / float(max(1, scan_w * 3))))
                    vertical_bias = float(candidate_y) / float(max(1, image_height))
                    score = (non_light_ratio * 4.2) + (min(distinct_count, 18) * 0.04) + (x_alignment * 0.25) + (vertical_bias * 0.10)
                    if best is None or score > float(best.get("score", -9999.0)):
                        best = {"score": float(score), "metrics": metrics}

            if not best:
                return None
            return dict(best.get("metrics", {}) or {})

        row_map = {}
        for index, detection in enumerate(list(prepared or [])):
            if not isinstance(detection, dict):
                continue
            resolved = self._resolve_detection_source_row_index(payload_dict, detection, log_context="repair_detection")
            row_index = int(resolved.get("source_row_index", -1) or -1)
            if row_index < 0 or row_index >= len(rows):
                continue
            detection["ware_index"] = row_index
            row_map.setdefault(row_index, []).append((index, detection))

            role = self._detection_role(detection)
            if role:
                continue
            try:
                validated = MediaCropper.validate_detection_box(screenshot_path, detection, clamp=True)
            except Exception:
                validated = None
            metrics = _region_metrics(
                (validated or {}).get("x", 0),
                (validated or {}).get("y", 0),
                (validated or {}).get("width", 0),
                (validated or {}).get("height", 0),
            ) if validated else None
            if metrics and bool(metrics.get("is_thumbnail")):
                detection["product_key"] = "product_image"
                detection.setdefault("coord_origin", "top_left")
                detection.setdefault("coord_units", "px")
                detection.setdefault("source_image_width", image_width)
                detection.setdefault("source_image_height", image_height)

        is_module1 = str(payload_dict.get("_origin_module", "") or "") == "modul_order_entry"
        for row_index in range(len(rows)):
            row_entries = list(row_map.get(row_index, []) or [])
            if not row_entries:
                continue

            has_good_product_image = False
            oob_repair_candidate = None
            saw_oob_detection = False
            anchors = []
            for _, detection in row_entries:
                if not isinstance(detection, dict):
                    continue
                try:
                    validated = MediaCropper.validate_detection_box(screenshot_path, detection, clamp=True)
                except Exception:
                    detection_y = self._coerce_int(detection.get("y"), -1)
                    if detection_y < 0 or detection_y >= image_height:
                        saw_oob_detection = True
                    candidate_metrics = _scan_compact_thumbnail_for_oob_detection(detection)
                    if candidate_metrics:
                        candidate_score = (
                            float(candidate_metrics.get("non_light_ratio", 0.0) or 0.0) * 3.8
                            + float(min(int(candidate_metrics.get("distinct_count", 0) or 0), 18)) * 0.04
                        )
                        if (oob_repair_candidate is None) or (candidate_score > float(oob_repair_candidate.get("score", -9999.0))):
                            oob_repair_candidate = {
                                "score": float(candidate_score),
                                "metrics": dict(candidate_metrics or {}),
                                "detection": detection,
                            }
                    continue
                metrics = _region_metrics(validated.get("x", 0), validated.get("y", 0), validated.get("width", 0), validated.get("height", 0))
                if not metrics:
                    continue
                role = self._detection_role(detection)
                if role == "product_image" and bool(metrics.get("is_thumbnail")):
                    has_good_product_image = True
                    break
                hint_text = str(detection.get("produkt_name_hint") or detection.get("product_name_hint") or "").strip()
                is_flat_anchor = int(validated.get("width", 0) or 0) >= max(150, int(validated.get("height", 0) or 0) * 3)
                if hint_text or is_flat_anchor:
                    score = int(validated.get("width", 0) or 0) + (40 if hint_text else 0)
                    anchors.append((score, dict(validated), detection))

            if has_good_product_image:
                continue
            if oob_repair_candidate:
                synth_metrics = dict(oob_repair_candidate.get("metrics", {}) or {})
                source_detection = dict(oob_repair_candidate.get("detection", {}) or {})
                ware = rows[row_index] if row_index < len(rows) and isinstance(rows[row_index], dict) else {}
                synthetic_detection = {
                    "produkt_name_hint": str(ware.get("produkt_name", "") or source_detection.get("produkt_name_hint", "") or source_detection.get("product_name_hint", "") or "").strip(),
                    "product_key": "product_image",
                    "ean": str(ware.get("ean", "") or "").strip(),
                    "variant_text": str(ware.get("varianten_info", ware.get("variant_text", "")) or "").strip(),
                    "x": int(synth_metrics.get("x", 0) or 0),
                    "y": int(synth_metrics.get("y", 0) or 0),
                    "width": int(synth_metrics.get("width", 0) or 0),
                    "height": int(synth_metrics.get("height", 0) or 0),
                    "confidence": "",
                    "ware_index": int(row_index),
                    "coord_units": "px",
                    "coord_origin": "top_left",
                    "source_image_width": int(image_width),
                    "source_image_height": int(image_height),
                    "synthetic": True,
                }
                prepared.append(synthetic_detection)
                if is_module1:
                    write_module1_trace(
                        "synthetic_product_image_detection_created",
                        asset_id=0,
                        ware_index=int(row_index),
                        reason="out_of_bounds_vertical_scan",
                        aspect_ratio=float(synth_metrics.get("aspect_ratio", 0.0) or 0.0),
                        non_light_ratio=float(synth_metrics.get("non_light_ratio", 0.0) or 0.0),
                    )
                logging.info(
                    "Synthetische product_image-Detection (out-of-bounds repair) erzeugt: ware_index=%s, box=(%s,%s,%s,%s)",
                    row_index,
                    synthetic_detection["x"],
                    synthetic_detection["y"],
                    synthetic_detection["width"],
                    synthetic_detection["height"],
                )
                continue
            if not anchors:
                if is_module1:
                    reject_reason = "out_of_bounds_vertical_scan_no_candidate" if saw_oob_detection else "no_anchor_detection"
                    write_module1_trace(
                        "synthetic_product_image_detection_rejected",
                        asset_id=0,
                        ware_index=int(row_index),
                        reason=reject_reason,
                        aspect_ratio=0.0,
                        non_light_ratio=0.0,
                    )
                continue

            _, anchor_box, anchor_detection = sorted(anchors, key=lambda row: row[0], reverse=True)[0]
            anchor_x = int(anchor_box.get("x", 0) or 0)
            anchor_y = int(anchor_box.get("y", 0) or 0)
            anchor_w = int(anchor_box.get("width", 0) or 0)
            anchor_h = int(anchor_box.get("height", 0) or 0)

            search_left = max(0, anchor_x - max(140, int(anchor_w * 0.55)))
            search_right = max(search_left + 1, anchor_x - 4)
            search_top = max(0, anchor_y - max(16, int(anchor_h * 0.9)))
            search_bottom = min(image_height, anchor_y + anchor_h + max(20, int(anchor_h * 1.8)))

            if search_right <= search_left or search_bottom <= search_top:
                if is_module1:
                    write_module1_trace(
                        "synthetic_product_image_detection_rejected",
                        asset_id=0,
                        ware_index=int(row_index),
                        reason="invalid_search_window",
                        aspect_ratio=0.0,
                        non_light_ratio=0.0,
                    )
                continue

            non_light_count = 0
            min_x = image_width
            min_y = image_height
            max_x = -1
            max_y = -1
            window_width = max(1, search_right - search_left)
            window_height = max(1, search_bottom - search_top)
            scan_step_x = max(1, window_width // 120)
            scan_step_y = max(1, window_height // 120)

            for py in range(search_top, search_bottom, scan_step_y):
                for px in range(search_left, search_right, scan_step_x):
                    color = screenshot_image.pixelColor(px, py)
                    if not _is_non_light(color):
                        continue
                    non_light_count += 1
                    if px < min_x:
                        min_x = px
                    if py < min_y:
                        min_y = py
                    if px > max_x:
                        max_x = px
                    if py > max_y:
                        max_y = py

            if non_light_count <= 0 or max_x < min_x or max_y < min_y:
                if is_module1:
                    write_module1_trace(
                        "synthetic_product_image_detection_rejected",
                        asset_id=0,
                        ware_index=int(row_index),
                        reason="search_window_empty",
                        aspect_ratio=0.0,
                        non_light_ratio=0.0,
                    )
                continue

            synth_x = max(0, min_x - 2)
            synth_y = max(0, min_y - 2)
            synth_w = min(image_width - synth_x, (max_x - min_x + 1) + 4)
            synth_h = min(image_height - synth_y, (max_y - min_y + 1) + 4)
            synth_metrics = _region_metrics(synth_x, synth_y, synth_w, synth_h)
            if not synth_metrics or not bool(synth_metrics.get("is_thumbnail")):
                if is_module1:
                    write_module1_trace(
                        "synthetic_product_image_detection_rejected",
                        asset_id=0,
                        ware_index=int(row_index),
                        reason="synthetic_region_not_thumbnail",
                        aspect_ratio=float((synth_metrics or {}).get("aspect_ratio", 0.0) or 0.0),
                        non_light_ratio=float((synth_metrics or {}).get("non_light_ratio", 0.0) or 0.0),
                    )
                continue

            ware = rows[row_index] if row_index < len(rows) and isinstance(rows[row_index], dict) else {}
            synthetic_detection = {
                "produkt_name_hint": str(ware.get("produkt_name", "") or anchor_detection.get("produkt_name_hint", "") or anchor_detection.get("product_name_hint", "") or "").strip(),
                "product_key": "product_image",
                "ean": str(ware.get("ean", "") or "").strip(),
                "variant_text": str(ware.get("varianten_info", ware.get("variant_text", "")) or "").strip(),
                "x": int(synth_metrics.get("x", 0) or 0),
                "y": int(synth_metrics.get("y", 0) or 0),
                "width": int(synth_metrics.get("width", 0) or 0),
                "height": int(synth_metrics.get("height", 0) or 0),
                "confidence": "",
                "ware_index": int(row_index),
                "coord_units": "px",
                "coord_origin": "top_left",
                "source_image_width": int(image_width),
                "source_image_height": int(image_height),
                "synthetic": True,
            }
            prepared.append(synthetic_detection)
            if is_module1:
                write_module1_trace(
                    "synthetic_product_image_detection_created",
                    asset_id=0,
                    ware_index=int(row_index),
                    reason="left_window_repair",
                    aspect_ratio=float(synth_metrics.get("aspect_ratio", 0.0) or 0.0),
                    non_light_ratio=float(synth_metrics.get("non_light_ratio", 0.0) or 0.0),
                )
            logging.info(
                "Synthetische product_image-Detection erzeugt: ware_index=%s, box=(%s,%s,%s,%s)",
                row_index,
                synthetic_detection["x"],
                synthetic_detection["y"],
                synthetic_detection["width"],
                synthetic_detection["height"],
            )

        return prepared

    def register_payload_screenshot_detections(
        self,
        payload,
        source_module="",
        source_kind="workflow_detection_batch",
        source_ref="",
        clamp=True,
    ):
        if not isinstance(payload, dict):
            return {"processed": False, "reason": "invalid_payload", "created": [], "rejected": [], "created_count": 0, "rejected_count": 0}
        if self._is_module1_ai_cropping_disabled(payload):
            logging.info(
                "module1_screenshot_detection_persist_skipped: source_module=%s, source_kind=%s, reason=%s",
                source_module,
                source_kind,
                "phase_a_disable_module1_ai_cropping",
            )
            write_module1_trace(
                "module1_screenshot_detection_persist_skipped",
                source_module=str(source_module or ""),
                source_kind=str(source_kind or ""),
                reason="phase_a_disable_module1_ai_cropping",
            )
            return self._build_module1_detection_skip_result()

        detections = self._extract_payload_detections(payload)
        if not detections:
            logging.debug("Keine Screenshot-Detektionen im Payload vorhanden.")
            return {"processed": False, "reason": "no_detections", "created": [], "rejected": [], "created_count": 0, "rejected_count": 0}

        screenshot_asset_id = self._resolve_payload_screenshot_asset_id(payload)
        if screenshot_asset_id in (None, ""):
            logging.warning("Screenshot-Detektionen uebersprungen: kein registrierter Screenshot im Payload gefunden.")
            return {"processed": False, "reason": "missing_screenshot_asset", "created": [], "rejected": [], "created_count": 0, "rejected_count": len(detections)}

        provider_meta = payload.get("_provider_meta", {}) if isinstance(payload.get("_provider_meta"), dict) else {}
        screenshot_path = self._resolve_payload_screenshot_path(payload)
        enriched = [self._enrich_detection_from_payload(payload, detection) for detection in detections]
        enriched = self._repair_missing_product_image_detections(payload, enriched, screenshot_path)
        source_context = {
            "bestellnummer": str(payload.get("bestellnummer", "") or "").strip(),
            "shop_name": str(payload.get("shop_name", "") or "").strip(),
            "email_sender_domain": str(payload.get("_email_sender_domain", "") or "").strip(),
            "primary_scan_file_path": str(payload.get("_primary_scan_file_path", "") or "").strip(),
            "screenshot_path": screenshot_path,
            "scan_sources": [dict(row) for row in list(payload.get("_scan_sources", []) or []) if isinstance(row, dict)],
        }
        result = self.register_screenshot_detections(
            screenshot_asset_id=screenshot_asset_id,
            detections=enriched,
            source_module=source_module,
            source_kind=source_kind,
            source_ref=source_ref or source_context.get("bestellnummer") or source_context.get("primary_scan_file_path") or f"screenshot:{screenshot_asset_id}",
            model_name=str(provider_meta.get("provider", "") or "").strip(),
            payload=payload,
            source_context=source_context,
            clamp=clamp,
        )
        result["processed"] = True
        result["reason"] = "ok"
        result["screenshot_asset_id"] = int(screenshot_asset_id)
        result["result_version"] = 4
        logging.info(
            "Screenshot-Detektionen verarbeitet: screenshot_asset_id=%s, created=%s, rejected=%s",
            screenshot_asset_id,
            result.get("created_count", 0),
            result.get("rejected_count", 0),
        )
        if str(payload.get("_origin_module", "") or "") == "modul_order_entry":
            write_module1_trace(
                "payload_screenshot_detections_processed",
                screenshot_asset_id=int(screenshot_asset_id),
                screenshot_path=screenshot_path,
                detection_count=len(list(enriched or [])),
                raw_detection_count=len(list(detections or [])),
                created_count=int(result.get("created_count", 0) or 0),
                rejected_count=int(result.get("rejected_count", 0) or 0),
                rejected=list(result.get("rejected", []) or []),
                created=list(result.get("created", []) or []),
            )
        return result
    def _screenshot_bucket(self, source_kind="", context_key=""):
        kind = self.store.sanitize_filename(source_kind or "generic", fallback="generic")
        parts = ["screenshots", kind]
        if str(context_key or "").strip():
            parts.append(self.store.sanitize_filename(context_key, fallback="item"))
        return "/".join(parts)

    def register_screenshot(
        self,
        source_path,
        preferred_name="",
        source_module="",
        source_kind="screenshot_render",
        source_ref="",
        source_url="",
        context_key="",
        metadata=None,
    ):
        screenshot_path = str(source_path or "").strip()
        if not screenshot_path:
            raise ValueError("Es wurde kein Screenshot-Pfad uebergeben.")
        resolved_path = self.store.resolve_path(screenshot_path)
        if not resolved_path or not os.path.exists(resolved_path):
            raise FileNotFoundError(screenshot_path)

        bucket = self._screenshot_bucket(source_kind=source_kind, context_key=context_key)
        asset = self.register_local_asset(
            source_path=resolved_path,
            media_type="screenshot",
            bucket=bucket,
            preferred_name=preferred_name or Path(resolved_path).stem or "screenshot",
            source_module=source_module,
            source_kind=source_kind,
            source_ref=source_ref or context_key or Path(resolved_path).name,
            source_url=source_url,
            metadata=metadata or {},
        )
        path_value = self.store.resolve_path(asset.get("file_path", ""))
        logging.info("Screenshot registriert: asset_id=%s, path=%s", asset.get("id"), path_value)
        return {"asset": asset, "path": path_value, "asset_id": asset.get("id")}

    def register_screenshot_from_file(
        self,
        file_path,
        preferred_name="",
        source_module="",
        source_kind="screenshot_render",
        source_ref="",
        source_url="",
        context_key="",
        metadata=None,
    ):
        return self.register_screenshot(
            source_path=file_path,
            preferred_name=preferred_name,
            source_module=source_module,
            source_kind=source_kind,
            source_ref=source_ref,
            source_url=source_url,
            context_key=context_key,
            metadata=metadata,
        )

    def resolve_screenshot_asset(self, asset_id):
        if asset_id in (None, ""):
            return {"asset": None, "path": ""}
        asset = self.db.get_media_asset_by_id(asset_id)
        if not asset:
            logging.debug("Screenshot-Asset nicht gefunden: asset_id=%s", asset_id)
            return {"asset": None, "path": ""}
        path_value = self.store.resolve_path(asset.get("file_path", ""))
        if path_value and os.path.exists(path_value):
            return {"asset": asset, "path": path_value}
        logging.warning("Screenshot-Asset ohne gueltigen Pfad gefunden: asset_id=%s", asset_id)
        return {"asset": asset, "path": ""}

    def get_screenshot_path(self, asset_id):
        resolved = self.resolve_screenshot_asset(asset_id)
        return str(resolved.get("path", "") or "")

    def register_screenshot_crop(
        self,
        screenshot_asset_id,
        x,
        y,
        width,
        height,
        label="",
        region_kind="product_candidate",
        source_kind="manual_region",
        source_ref="",
        metadata=None,
    ):
        screenshot_asset = self.db.get_media_asset_by_id(screenshot_asset_id)
        if not screenshot_asset:
            raise ValueError(f"Screenshot-Asset nicht gefunden: {screenshot_asset_id}")

        screenshot_path = self.store.resolve_path(screenshot_asset.get("file_path", ""))
        if not screenshot_path:
            raise ValueError("Screenshot-Asset hat keinen gueltigen Dateipfad.")

        target_info = self.store.build_generated_path(
            bucket="screenshots/crops",
            preferred_name=label or f"crop_{screenshot_asset_id}",
            extension=".png",
            token=f"crop_{screenshot_asset_id}",
        )
        crop_result = MediaCropper.crop_image(
            screenshot_path=screenshot_path,
            output_path=target_info["absolute_path"],
            x=x,
            y=y,
            width=width,
            height=height,
            image_format="PNG",
        )
        crop_asset = self.register_local_asset(
            source_path=crop_result["output_path"],
            media_type="crop",
            bucket="screenshots/crops",
            preferred_name=label or f"crop_{screenshot_asset_id}",
            source_module=str(screenshot_asset.get("source_module", "") or ""),
            source_kind="screenshot_crop",
            source_ref=source_ref or label or f"screenshot:{screenshot_asset_id}",
            metadata={
                "derived_from_asset_id": int(screenshot_asset_id),
                "region": crop_result.get("region", {}),
                **dict(metadata or {}),
            },
        )
        stored_crop_path = self.store.resolve_path(crop_asset.get("file_path", ""))
        generated_crop_path = os.path.abspath(str(crop_result["output_path"]))
        if stored_crop_path and stored_crop_path != generated_crop_path and os.path.exists(generated_crop_path):
            try:
                os.remove(generated_crop_path)
            except OSError:
                pass

        region = self.db.create_screenshot_region(
            screenshot_asset_id=screenshot_asset_id,
            crop_asset_id=crop_asset["id"],
            region_kind=region_kind,
            label=label,
            x=x,
            y=y,
            width=width,
            height=height,
            source_kind=source_kind,
            source_ref=source_ref,
            metadata=metadata or {},
        )
        if not region:
            raise RuntimeError("Screenshot-Region konnte nicht gespeichert werden.")
        return {"screenshot_asset": screenshot_asset, "crop_asset": crop_asset, "region": region}


































































