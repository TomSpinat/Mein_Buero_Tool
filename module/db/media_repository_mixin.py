"""Ausgelagerte Repository-Methoden fuer DatabaseManager."""

import json
import logging

from module.crash_logger import log_exception


class MediaRepositoryMixin:
    def _json_dump(self, value):
        if value in (None, "", {}):
            return None
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        except Exception:
            return json.dumps({"raw": str(value)}, ensure_ascii=False, sort_keys=True)

    def get_image_path_by_name(self, produkt_name):
        """
        Legacy-Kompatibilitaetszugriff fuer Produktbilder.

        Fuehrend ist inzwischen die Medienarchitektur ueber
        `product_image_links` + `media_assets`. Nur wenn dort kein lokaler
        Pfad hinterlegt ist, faellt die Methode auf `produkt_bilder` zurueck.
        """
        try:
            produkt_name_text = str(produkt_name or "").strip()
            if not produkt_name_text:
                return None

            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT ma.file_path, ma.storage_kind
                FROM product_image_links pil
                JOIN media_assets ma ON ma.id = pil.media_asset_id
                WHERE pil.product_name = %s
                ORDER BY pil.is_primary DESC, pil.priority ASC, pil.updated_at DESC
                LIMIT 1
                """,
                (produkt_name_text,),
            )
            media_row = cursor.fetchone()
            cursor.close()

            if media_row:
                storage_kind = str(media_row.get("storage_kind", "") or "local_file").strip()
                file_path = str(media_row.get("file_path", "") or "").strip()
                if storage_kind == "local_file" and file_path:
                    conn.close()
                    return file_path

            cursor = conn.cursor()
            cursor.execute("SELECT bild_pfad FROM produkt_bilder WHERE produkt_name = %s", (produkt_name_text,))
            result = cursor.fetchone()

            cursor.close()
            conn.close()

            if result and result[0]:
                return result[0]
            return None
        except Exception as e:
            log_exception(__name__, e)
            logging.error(f"Fehler bei get_image_path_by_name: {e}")
            return None

    def save_image_path(self, produkt_name, bild_pfad):
        """
        Legacy-Uebergangswrite fuer bestehende Aufrufer.

        Neue Medien sollen ueber `module.media.media_service.MediaService`
        registriert werden. Diese Methode behaelt `produkt_bilder` nur fuer
        rueckwaertskompatible Restpfade aktuell.
        """
        try:
            produkt_name_text = str(produkt_name or "").strip()
            bild_pfad_text = str(bild_pfad or "").strip()
            if not produkt_name_text or not bild_pfad_text:
                return False

            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                INSERT INTO produkt_bilder (produkt_name, bild_pfad)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE bild_pfad = VALUES(bild_pfad)
                """,
                (produkt_name_text, bild_pfad_text),
            )

            conn.commit()
            cursor.close()
            conn.close()
            logging.debug("Legacy-Produktbildpfad aktualisiert: %s", produkt_name_text)
            return True
        except Exception as e:
            log_exception(__name__, e)
            logging.error(f"Fehler bei save_image_path: {e}")
            return False

    def get_media_asset_by_id(self, asset_id):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM media_assets WHERE id = %s LIMIT 1", (int(asset_id),))
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"asset_id": asset_id})
            logging.error(f"Fehler bei get_media_asset_by_id: {e}")
            return None

    def get_media_asset_by_key(self, media_key):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM media_assets WHERE media_key = %s LIMIT 1", (str(media_key or "").strip(),))
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"media_key": media_key})
            logging.error(f"Fehler bei get_media_asset_by_key: {e}")
            return None

    def upsert_media_asset(
        self,
        media_key,
        media_type,
        file_path,
        original_name="",
        mime_type="",
        file_ext="",
        file_size_bytes=0,
        sha256="",
        width_px=None,
        height_px=None,
        source_module="",
        source_kind="",
        source_ref="",
        source_url="",
        storage_kind="local_file",
        metadata=None,
    ):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                INSERT INTO media_assets (
                    media_key, media_type, storage_kind, file_path, original_name,
                    mime_type, file_ext, file_size_bytes, sha256, width_px, height_px,
                    source_module, source_kind, source_ref, source_url, metadata_json
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    media_type = VALUES(media_type),
                    storage_kind = VALUES(storage_kind),
                    file_path = VALUES(file_path),
                    original_name = VALUES(original_name),
                    mime_type = VALUES(mime_type),
                    file_ext = VALUES(file_ext),
                    file_size_bytes = VALUES(file_size_bytes),
                    sha256 = VALUES(sha256),
                    width_px = VALUES(width_px),
                    height_px = VALUES(height_px),
                    source_module = VALUES(source_module),
                    source_kind = VALUES(source_kind),
                    source_ref = VALUES(source_ref),
                    source_url = VALUES(source_url),
                    metadata_json = VALUES(metadata_json)
                """,
                (
                    str(media_key or "").strip(),
                    str(media_type or "").strip(),
                    str(storage_kind or "local_file").strip(),
                    str(file_path or "").strip(),
                    str(original_name or "").strip(),
                    str(mime_type or "").strip(),
                    str(file_ext or "").strip(),
                    int(file_size_bytes or 0),
                    str(sha256 or "").strip(),
                    int(width_px) if width_px not in (None, "") else None,
                    int(height_px) if height_px not in (None, "") else None,
                    str(source_module or "").strip(),
                    str(source_kind or "").strip(),
                    str(source_ref or "").strip(),
                    str(source_url or "").strip(),
                    self._json_dump(metadata),
                ),
            )
            conn.commit()
            cursor.execute("SELECT * FROM media_assets WHERE media_key = %s LIMIT 1", (str(media_key or "").strip(),))
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"media_key": media_key, "media_type": media_type})
            logging.error(f"Fehler bei upsert_media_asset: {e}")
            return None

    def upsert_shop_logo_link(
        self,
        shop_key,
        shop_name,
        media_asset_id,
        sender_domain="",
        is_primary=True,
        priority=100,
        source_note="",
        confidence=1.0,
        metadata=None,
    ):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                INSERT INTO shop_logo_links (
                    shop_key, shop_name, sender_domain, media_asset_id,
                    is_primary, priority, source_note, confidence, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    shop_name = VALUES(shop_name),
                    sender_domain = VALUES(sender_domain),
                    is_primary = VALUES(is_primary),
                    priority = VALUES(priority),
                    source_note = VALUES(source_note),
                    confidence = VALUES(confidence),
                    metadata_json = VALUES(metadata_json),
                    last_seen_at = CURRENT_TIMESTAMP
                """,
                (
                    str(shop_key or "").strip(),
                    str(shop_name or "").strip(),
                    str(sender_domain or "").strip().lower(),
                    int(media_asset_id),
                    bool(is_primary),
                    int(priority or 100),
                    str(source_note or "").strip(),
                    float(confidence or 0.0),
                    self._json_dump(metadata),
                ),
            )
            conn.commit()
            cursor.execute(
                """
                SELECT * FROM shop_logo_links
                WHERE shop_key = %s AND media_asset_id = %s
                LIMIT 1
                """,
                (str(shop_key or "").strip(), int(media_asset_id)),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"shop_key": shop_key, "media_asset_id": media_asset_id})
            logging.error(f"Fehler bei upsert_shop_logo_link: {e}")
            return None

    def get_primary_shop_logo_link(self, shop_key="", sender_domain=""):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            shop_key_text = str(shop_key or "").strip()
            sender_domain_text = str(sender_domain or "").strip().lower()

            if shop_key_text:
                cursor.execute(
                    """
                    SELECT *
                    FROM shop_logo_links
                    WHERE shop_key = %s
                    ORDER BY is_primary DESC, priority ASC, confidence DESC, updated_at DESC
                    LIMIT 1
                    """,
                    (shop_key_text,),
                )
                row = cursor.fetchone()
                if row:
                    cursor.close()
                    conn.close()
                    return row

            if sender_domain_text:
                cursor.execute(
                    """
                    SELECT *
                    FROM shop_logo_links
                    WHERE sender_domain = %s
                    ORDER BY is_primary DESC, priority ASC, confidence DESC, updated_at DESC
                    LIMIT 1
                    """,
                    (sender_domain_text,),
                )
                row = cursor.fetchone()
                cursor.close()
                conn.close()
                return row or None

            cursor.close()
            conn.close()
            return None
        except Exception as e:
            log_exception(__name__, e, extra={"shop_key": shop_key, "sender_domain": sender_domain})
            logging.error(f"Fehler bei get_primary_shop_logo_link: {e}")
            return None

    def get_primary_shop_logo_link_by_name(self, shop_name=""):
        """Sucht einen Shop-Logo-Link ueber den gespeicherten shop_name (Fallback).

        Nuetzlich wenn aeltere Eintraege einen anderen shop_key-Format hatten
        aber den richtigen shop_name enthalten.
        """
        try:
            name_text = str(shop_name or "").strip()
            if not name_text:
                return None
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT *
                FROM shop_logo_links
                WHERE LOWER(shop_name) = LOWER(%s)
                ORDER BY is_primary DESC, priority ASC, confidence DESC, updated_at DESC
                LIMIT 1
                """,
                (name_text,),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"shop_name": shop_name})
            logging.error(f"Fehler bei get_primary_shop_logo_link_by_name: {e}")
            return None

    def upsert_product_image_link(
        self,
        product_key,
        product_name,
        media_asset_id,
        ean="",
        variant_text="",
        is_primary=False,
        priority=100,
        source_note="",
        metadata=None,
    ):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                INSERT INTO product_image_links (
                    product_key, product_name, ean, variant_text, media_asset_id,
                    is_primary, priority, source_note, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    product_name = VALUES(product_name),
                    ean = VALUES(ean),
                    variant_text = VALUES(variant_text),
                    is_primary = VALUES(is_primary),
                    priority = VALUES(priority),
                    source_note = VALUES(source_note),
                    metadata_json = VALUES(metadata_json)
                """,
                (
                    str(product_key or "").strip(),
                    str(product_name or "").strip(),
                    str(ean or "").strip(),
                    str(variant_text or "").strip(),
                    int(media_asset_id),
                    bool(is_primary),
                    int(priority or 100),
                    str(source_note or "").strip(),
                    self._json_dump(metadata),
                ),
            )
            conn.commit()
            cursor.execute(
                """
                SELECT * FROM product_image_links
                WHERE product_key = %s AND media_asset_id = %s
                LIMIT 1
                """,
                (str(product_key or "").strip(), int(media_asset_id)),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"product_key": product_key, "media_asset_id": media_asset_id})
            logging.error(f"Fehler bei upsert_product_image_link: {e}")
            return None

    def get_primary_product_image_link(self, product_key="", ean="", product_name="", variant_text=""):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            product_key_text = str(product_key or "").strip()
            ean_text = str(ean or "").strip()
            product_name_text = str(product_name or "").strip()
            variant_text_value = str(variant_text or "").strip()

            def _fetch(query, params):
                cursor.execute(query, params)
                return cursor.fetchone()

            if product_key_text:
                row = _fetch(
                    """
                    SELECT *
                    FROM product_image_links
                    WHERE product_key = %s
                      AND is_primary = TRUE
                    ORDER BY priority ASC, updated_at DESC
                    LIMIT 1
                    """,
                    (product_key_text,),
                )
                if row:
                    cursor.close()
                    conn.close()
                    return row

                row = _fetch(
                    """
                    SELECT *
                    FROM product_image_links
                    WHERE product_key = %s
                    ORDER BY is_primary DESC, priority ASC, updated_at DESC
                    LIMIT 1
                    """,
                    (product_key_text,),
                )
                if row:
                    cursor.close()
                    conn.close()
                    return row

            if ean_text:
                row = _fetch(
                    """
                    SELECT *
                    FROM product_image_links
                    WHERE ean = %s
                      AND is_primary = TRUE
                    ORDER BY priority ASC, updated_at DESC
                    LIMIT 1
                    """,
                    (ean_text,),
                )
                if row:
                    cursor.close()
                    conn.close()
                    return row

                row = _fetch(
                    """
                    SELECT *
                    FROM product_image_links
                    WHERE ean = %s
                    ORDER BY is_primary DESC, priority ASC, updated_at DESC
                    LIMIT 1
                    """,
                    (ean_text,),
                )
                if row:
                    cursor.close()
                    conn.close()
                    return row

            if product_name_text:
                row = _fetch(
                    """
                    SELECT *
                    FROM product_image_links
                    WHERE product_name = %s
                      AND (%s = '' OR variant_text = %s)
                      AND is_primary = TRUE
                    ORDER BY priority ASC, updated_at DESC
                    LIMIT 1
                    """,
                    (product_name_text, variant_text_value, variant_text_value),
                )
                if row:
                    cursor.close()
                    conn.close()
                    return row

                row = _fetch(
                    """
                    SELECT *
                    FROM product_image_links
                    WHERE product_name = %s
                      AND (%s = '' OR variant_text = %s)
                    ORDER BY is_primary DESC, priority ASC, updated_at DESC
                    LIMIT 1
                    """,
                    (product_name_text, variant_text_value, variant_text_value),
                )
                cursor.close()
                conn.close()
                return row or None

            cursor.close()
            conn.close()
            return None
        except Exception as e:
            log_exception(
                __name__,
                e,
                extra={
                    "product_key": product_key,
                    "ean": ean,
                    "product_name": product_name,
                    "variant_text": variant_text,
                },
            )
            logging.error(f"Fehler bei get_primary_product_image_link: {e}")
            return None

    def upsert_order_item_image_link(
        self,
        order_item_id,
        media_asset_id,
        decision_state="candidate",
        is_selected=False,
        is_rejected=False,
        selection_mode="auto",
        source_type="",
        source_ref="",
        metadata=None,
    ):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            decided_at = None
            if bool(is_selected) or bool(is_rejected):
                cursor.execute(
                    """
                    INSERT INTO order_item_image_links (
                        order_item_id, media_asset_id, decision_state,
                        is_selected, is_rejected, selection_mode,
                        source_type, source_ref, metadata_json, decided_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                        decision_state = VALUES(decision_state),
                        is_selected = VALUES(is_selected),
                        is_rejected = VALUES(is_rejected),
                        selection_mode = VALUES(selection_mode),
                        source_type = VALUES(source_type),
                        source_ref = VALUES(source_ref),
                        metadata_json = VALUES(metadata_json),
                        decided_at = CURRENT_TIMESTAMP
                    """,
                    (
                        int(order_item_id),
                        int(media_asset_id),
                        str(decision_state or "candidate").strip(),
                        bool(is_selected),
                        bool(is_rejected),
                        str(selection_mode or "auto").strip(),
                        str(source_type or "").strip(),
                        str(source_ref or "").strip(),
                        self._json_dump(metadata),
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO order_item_image_links (
                        order_item_id, media_asset_id, decision_state,
                        is_selected, is_rejected, selection_mode,
                        source_type, source_ref, metadata_json, decided_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
                    ON DUPLICATE KEY UPDATE
                        decision_state = VALUES(decision_state),
                        is_selected = VALUES(is_selected),
                        is_rejected = VALUES(is_rejected),
                        selection_mode = VALUES(selection_mode),
                        source_type = VALUES(source_type),
                        source_ref = VALUES(source_ref),
                        metadata_json = VALUES(metadata_json)
                    """,
                    (
                        int(order_item_id),
                        int(media_asset_id),
                        str(decision_state or "candidate").strip(),
                        False,
                        False,
                        str(selection_mode or "auto").strip(),
                        str(source_type or "").strip(),
                        str(source_ref or "").strip(),
                        self._json_dump(metadata),
                    ),
                )
            conn.commit()
            cursor.execute(
                """
                SELECT * FROM order_item_image_links
                WHERE order_item_id = %s AND media_asset_id = %s
                LIMIT 1
                """,
                (int(order_item_id), int(media_asset_id)),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"order_item_id": order_item_id, "media_asset_id": media_asset_id})
            logging.error(f"Fehler bei upsert_order_item_image_link: {e}")
            return None

    def set_order_item_image_selected(
        self,
        order_item_id,
        media_asset_id,
        decision_state="selected_auto",
        selection_mode="auto",
        source_type="",
        source_ref="",
        metadata=None,
    ):
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                UPDATE order_item_image_links
                SET is_selected = FALSE,
                    decision_state = CASE WHEN is_rejected = TRUE THEN 'rejected' ELSE 'candidate' END
                WHERE order_item_id = %s
                  AND media_asset_id != %s
                  AND is_selected = TRUE
                """,
                (int(order_item_id), int(media_asset_id)),
            )
            cursor.execute(
                """
                INSERT INTO order_item_image_links (
                    order_item_id, media_asset_id, decision_state,
                    is_selected, is_rejected, selection_mode,
                    source_type, source_ref, metadata_json, decided_at
                ) VALUES (%s, %s, %s, TRUE, FALSE, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    decision_state = VALUES(decision_state),
                    is_selected = TRUE,
                    is_rejected = FALSE,
                    selection_mode = VALUES(selection_mode),
                    source_type = VALUES(source_type),
                    source_ref = VALUES(source_ref),
                    metadata_json = VALUES(metadata_json),
                    decided_at = CURRENT_TIMESTAMP
                """,
                (
                    int(order_item_id),
                    int(media_asset_id),
                    str(decision_state or "selected_auto").strip(),
                    str(selection_mode or "auto").strip(),
                    str(source_type or "").strip(),
                    str(source_ref or "").strip(),
                    self._json_dump(metadata),
                ),
            )
            conn.commit()
            cursor.execute(
                """
                SELECT * FROM order_item_image_links
                WHERE order_item_id = %s AND media_asset_id = %s
                LIMIT 1
                """,
                (int(order_item_id), int(media_asset_id)),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"order_item_id": order_item_id, "media_asset_id": media_asset_id})
            logging.error(f"Fehler bei set_order_item_image_selected: {e}")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            return None
        finally:
            try:
                if cursor:
                    cursor.close()
            except Exception:
                pass
            try:
                if conn and conn.is_connected():
                    conn.close()
            except Exception:
                pass

    def reject_order_item_image_link(
        self,
        order_item_id,
        media_asset_id,
        selection_mode="manual",
        source_type="",
        source_ref="",
        metadata=None,
    ):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                INSERT INTO order_item_image_links (
                    order_item_id, media_asset_id, decision_state,
                    is_selected, is_rejected, selection_mode,
                    source_type, source_ref, metadata_json, decided_at
                ) VALUES (%s, %s, 'rejected', FALSE, TRUE, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    decision_state = 'rejected',
                    is_selected = FALSE,
                    is_rejected = TRUE,
                    selection_mode = VALUES(selection_mode),
                    source_type = VALUES(source_type),
                    source_ref = VALUES(source_ref),
                    metadata_json = VALUES(metadata_json),
                    decided_at = CURRENT_TIMESTAMP
                """,
                (
                    int(order_item_id),
                    int(media_asset_id),
                    str(selection_mode or "manual").strip(),
                    str(source_type or "").strip(),
                    str(source_ref or "").strip(),
                    self._json_dump(metadata),
                ),
            )
            conn.commit()
            cursor.execute(
                """
                SELECT * FROM order_item_image_links
                WHERE order_item_id = %s AND media_asset_id = %s
                LIMIT 1
                """,
                (int(order_item_id), int(media_asset_id)),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"order_item_id": order_item_id, "media_asset_id": media_asset_id})
            logging.error(f"Fehler bei reject_order_item_image_link: {e}")
            return None

    def get_order_item_image_links(self, order_item_id):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT *
                FROM order_item_image_links
                WHERE order_item_id = %s
                ORDER BY is_selected DESC,
                         CASE WHEN selection_mode = 'manual' THEN 0 ELSE 1 END ASC,
                         is_rejected ASC,
                         updated_at DESC,
                         id DESC
                """,
                (int(order_item_id),),
            )
            rows = cursor.fetchall() or []
            cursor.close()
            conn.close()
            return rows
        except Exception as e:
            log_exception(__name__, e, extra={"order_item_id": order_item_id})
            logging.error(f"Fehler bei get_order_item_image_links: {e}")
            return []

    def get_selected_order_item_image_link(self, order_item_id):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT *
                FROM order_item_image_links
                WHERE order_item_id = %s
                  AND is_selected = TRUE
                  AND is_rejected = FALSE
                ORDER BY CASE WHEN selection_mode = 'manual' THEN 0 ELSE 1 END ASC,
                         updated_at DESC,
                         id DESC
                LIMIT 1
                """,
                (int(order_item_id),),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"order_item_id": order_item_id})
            logging.error(f"Fehler bei get_selected_order_item_image_link: {e}")
            return None

    def get_order_item_row(self, order_item_id):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT id, einkauf_id, produkt_name, varianten_info, ean, menge,
                       ekp_brutto, bezugskosten_anteil_brutto, einstand_brutto, status
                FROM waren_positionen
                WHERE id = %s
                LIMIT 1
                """,
                (int(order_item_id),),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"order_item_id": order_item_id})
            logging.error(f"Fehler bei get_order_item_row: {e}")
            return None

    def get_order_items_for_order(self, einkauf_id):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT id, einkauf_id, produkt_name, varianten_info, ean, menge,
                       ekp_brutto, bezugskosten_anteil_brutto, einstand_brutto, status
                FROM waren_positionen
                WHERE einkauf_id = %s
                ORDER BY id ASC
                """,
                (int(einkauf_id),),
            )
            rows = cursor.fetchall() or []
            cursor.close()
            conn.close()
            return rows
        except Exception as e:
            log_exception(__name__, e, extra={"einkauf_id": einkauf_id})
            logging.error(f"Fehler bei get_order_items_for_order: {e}")
            return []
    def get_order_items_for_package(self, ausgangs_paket_id):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT w.id, w.einkauf_id, w.ausgangs_paket_id, w.produkt_name,
                       w.varianten_info, w.ean, w.menge, w.ekp_brutto,
                       w.bezugskosten_anteil_brutto, w.einstand_brutto, w.status,
                       e.shop_name, e.bestellnummer
                FROM waren_positionen w
                LEFT JOIN einkauf_bestellungen e ON e.id = w.einkauf_id
                WHERE w.ausgangs_paket_id = %s
                ORDER BY w.id ASC
                """,
                (int(ausgangs_paket_id),),
            )
            rows = cursor.fetchall() or []
            cursor.close()
            conn.close()
            return rows
        except Exception as e:
            log_exception(__name__, e, extra={"ausgangs_paket_id": ausgangs_paket_id})
            logging.error(f"Fehler bei get_order_items_for_package: {e}")
            return []

    def create_screenshot_region(
        self,
        screenshot_asset_id,
        x,
        y,
        width,
        height,
        crop_asset_id=None,
        region_kind="product_candidate",
        label="",
        source_kind="",
        source_ref="",
        coord_origin="top_left",
        coord_units="px",
        metadata=None,
    ):
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                INSERT INTO screenshot_regions (
                    screenshot_asset_id, crop_asset_id, region_kind, label,
                    x, y, width, height, coord_origin, coord_units,
                    source_kind, source_ref, metadata_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    int(screenshot_asset_id),
                    int(crop_asset_id) if crop_asset_id not in (None, "") else None,
                    str(region_kind or "product_candidate").strip(),
                    str(label or "").strip(),
                    int(x),
                    int(y),
                    int(width),
                    int(height),
                    str(coord_origin or "top_left").strip(),
                    str(coord_units or "px").strip(),
                    str(source_kind or "").strip(),
                    str(source_ref or "").strip(),
                    self._json_dump(metadata),
                ),
            )
            region_id = cursor.lastrowid
            conn.commit()
            cursor.execute("SELECT * FROM screenshot_regions WHERE id = %s LIMIT 1", (int(region_id),))
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row or None
        except Exception as e:
            log_exception(__name__, e, extra={"screenshot_asset_id": screenshot_asset_id})
            logging.error(f"Fehler bei create_screenshot_region: {e}")
            return None




