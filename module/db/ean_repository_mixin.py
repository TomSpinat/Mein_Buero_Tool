"""Ausgelagerte Repository-Methoden fuer DatabaseManager."""

import json
from module.crash_logger import log_exception, log_message

class EanRepositoryMixin:
    def _safe_json_dump(self, value):
        return json.dumps(value, ensure_ascii=False)

    def _upsert_local_ean_mapping_cursor(self, cursor, produkt_name, ean, varianten_info="", bild_url="", quelle="manual", confidence=1.0):
        """Speichert eine EAN-Zuordnung zentral in ean_katalog (innerhalb bestehender Transaktion)."""
        name = str(produkt_name or "").strip()
        ean_txt = str(ean or "").strip()
        if not name or not ean_txt:
            return

        variant_txt = str(varianten_info or "").strip()
        image_txt = str(bild_url or "").strip()
        source_txt = str(quelle or "manual").strip() or "manual"
        conf = float(confidence or 1.0)
        if conf < 0.0:
            conf = 0.0
        if conf > 1.0:
            conf = 1.0

        cursor.execute(
            """
            INSERT INTO ean_katalog (produkt_name, varianten_info, ean, bild_url, quelle, confidence, last_seen_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                bild_url = IF(VALUES(bild_url) != '', VALUES(bild_url), bild_url),
                quelle = IF(VALUES(quelle) != '', VALUES(quelle), quelle),
                confidence = GREATEST(COALESCE(confidence, 0), VALUES(confidence)),
                last_seen_at = NOW()
            """,
            (name, variant_txt, ean_txt, image_txt, source_txt, conf)
        )

    def _upsert_ean_alias_cache_cursor(
        self,
        cursor,
        raw_name,
        cleaned_name="",
        varianten_info="",
        chosen_query="",
        matched_title="",
        ean="",
        brand="",
        model_code="",
        category_hint="",
        confidence=0.5,
        source="manual",
    ):
        """Speichert einen lokalen Alias fuer chaotische Produkttitel."""
        raw_txt = str(raw_name or "").strip()
        ean_txt = str(ean or "").strip()
        if not raw_txt or not ean_txt:
            return

        cleaned_txt = str(cleaned_name or "").strip()
        variant_txt = str(varianten_info or "").strip()
        query_txt = str(chosen_query or "").strip()
        title_txt = str(matched_title or "").strip()
        brand_txt = str(brand or "").strip()
        model_txt = str(model_code or "").strip()
        category_txt = str(category_hint or "").strip()
        source_txt = str(source or "manual").strip() or "manual"
        conf = float(confidence or 0.5)
        if conf < 0.0:
            conf = 0.0
        if conf > 1.0:
            conf = 1.0

        cursor.execute(
            """
            INSERT INTO ean_alias_cache (
                raw_name, cleaned_name, varianten_info, chosen_query, matched_title,
                ean, brand, model_code, category_hint, confidence, source, last_used_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, NOW()
            )
            ON DUPLICATE KEY UPDATE
                varianten_info = IF(VALUES(varianten_info) != '', VALUES(varianten_info), varianten_info),
                chosen_query = IF(VALUES(chosen_query) != '', VALUES(chosen_query), chosen_query),
                matched_title = IF(VALUES(matched_title) != '', VALUES(matched_title), matched_title),
                brand = IF(VALUES(brand) != '', VALUES(brand), brand),
                model_code = IF(VALUES(model_code) != '', VALUES(model_code), model_code),
                category_hint = IF(VALUES(category_hint) != '', VALUES(category_hint), category_hint),
                confidence = GREATEST(COALESCE(confidence, 0), VALUES(confidence)),
                source = IF(VALUES(source) != '', VALUES(source), source),
                last_used_at = NOW()
            """,
            (
                raw_txt,
                cleaned_txt,
                variant_txt,
                query_txt,
                title_txt,
                ean_txt,
                brand_txt,
                model_txt,
                category_txt,
                conf,
                source_txt,
            )
        )

    def upsert_local_ean_mapping(self, produkt_name, ean, varianten_info="", bild_url="", quelle="manual", confidence=1.0):
        """Public Wrapper: Speichert eine EAN-Zuordnung zentral in ean_katalog."""
        conn = self._get_connection()
        if not conn.is_connected():
            return False

        cursor = None
        try:
            cursor = conn.cursor()
            self._upsert_local_ean_mapping_cursor(
                cursor,
                produkt_name,
                ean,
                varianten_info=varianten_info,
                bild_url=bild_url,
                quelle=quelle,
                confidence=confidence
            )
            conn.commit()
            return True
        except Exception as e:
            log_exception(__name__, e)
            if conn:
                conn.rollback()
            return False
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def upsert_ean_alias_cache(
        self,
        raw_name,
        cleaned_name="",
        varianten_info="",
        chosen_query="",
        matched_title="",
        ean="",
        brand="",
        model_code="",
        category_hint="",
        confidence=0.5,
        source="manual",
    ):
        """Public Wrapper: Speichert einen gelernten Alias fuer chaotische Produktnamen."""
        conn = self._get_connection()
        if not conn.is_connected():
            return False

        cursor = None
        try:
            cursor = conn.cursor()
            self._upsert_ean_alias_cache_cursor(
                cursor,
                raw_name,
                cleaned_name=cleaned_name,
                varianten_info=varianten_info,
                chosen_query=chosen_query,
                matched_title=matched_title,
                ean=ean,
                brand=brand,
                model_code=model_code,
                category_hint=category_hint,
                confidence=confidence,
                source=source,
            )
            conn.commit()
            return True
        except Exception as e:
            log_exception(__name__, e)
            if conn:
                conn.rollback()
            return False
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def search_ean_alias_candidates(self, raw_name, cleaned_name="", limit=10):
        """Sucht in lokal gelernten Aliasen fuer wiederkehrende chaotische Produkttitel."""
        raw_txt = str(raw_name or "").strip()
        cleaned_txt = str(cleaned_name or "").strip()
        if not raw_txt and not cleaned_txt:
            return []

        max_limit = max(1, min(int(limit or 10), 50))
        like_raw = self._build_contains_like_pattern(raw_txt) if raw_txt else ""
        like_cleaned = self._build_contains_like_pattern(cleaned_txt) if cleaned_txt else ""
        results = []
        seen = set()

        conn = self._get_connection()
        if not conn.is_connected():
            return []

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            query = f"""
                SELECT
                    a.raw_name,
                    a.cleaned_name,
                    a.varianten_info,
                    a.chosen_query,
                    a.matched_title,
                    a.ean,
                    a.brand,
                    a.model_code,
                    a.category_hint,
                    COALESCE(a.confidence, 0.5) AS confidence,
                    COALESCE(a.source, 'ean_alias_cache') AS source,
                    COALESCE(img.bild_url, '') AS bild_url
                FROM ean_alias_cache a
                LEFT JOIN (
                    SELECT ean, MAX(COALESCE(NULLIF(bild_url, ''), '')) AS bild_url
                    FROM ean_katalog
                    GROUP BY ean
                ) img ON img.ean = a.ean
                WHERE (
                    LOWER(a.raw_name) = LOWER(%s)
                    OR (%s != '' AND a.raw_name LIKE %s ESCAPE '\\')
                    OR (%s != '' AND LOWER(a.cleaned_name) = LOWER(%s))
                    OR (%s != '' AND a.cleaned_name LIKE %s ESCAPE '\\')
                )
                ORDER BY
                    CASE
                        WHEN LOWER(a.raw_name) = LOWER(%s) THEN 0
                        WHEN %s != '' AND LOWER(a.cleaned_name) = LOWER(%s) THEN 1
                        ELSE 2
                    END ASC,
                    confidence DESC,
                    last_used_at DESC
                LIMIT {max_limit}
            """
            cursor.execute(
                query,
                (
                    raw_txt,
                    raw_txt,
                    like_raw,
                    cleaned_txt,
                    cleaned_txt,
                    cleaned_txt,
                    like_cleaned,
                    raw_txt,
                    cleaned_txt,
                    cleaned_txt,
                ),
            )
            rows = cursor.fetchall() or []

            for row in rows:
                ean_txt = str(row.get("ean", "")).strip()
                product_title = str(row.get("matched_title", "")).strip() or str(row.get("raw_name", "")).strip()
                key = (ean_txt, product_title)
                if not ean_txt or key in seen:
                    continue
                seen.add(key)

                brand_txt = str(row.get("brand", "")).strip()
                model_txt = str(row.get("model_code", "")).strip()
                category_txt = str(row.get("category_hint", "")).strip()
                variant_txt = str(row.get("varianten_info", "")).strip()
                if not variant_txt:
                    variant_txt = " | ".join(part for part in [brand_txt, model_txt, category_txt] if part)

                conf = float(row.get("confidence", 0.5) or 0.5)
                conf = min(1.0, conf + 0.12)
                source_txt = str(row.get("source", "ean_alias_cache")).strip() or "ean_alias_cache"
                chosen_query = str(row.get("chosen_query", "")).strip()
                cleaned_row = str(row.get("cleaned_name", "")).strip()

                results.append({
                    "produkt_name": product_title,
                    "varianten_info": variant_txt,
                    "ean": ean_txt,
                    "bild_url": str(row.get("bild_url", "")).strip(),
                    "quelle": f"ean_alias_cache:{source_txt}",
                    "confidence": conf,
                    "brand": brand_txt,
                    "model_code": model_txt,
                    "category_hint": category_txt,
                    "query_used": chosen_query,
                    "_lookup_meta": {
                        "raw_name": raw_txt,
                        "cleaned_name": cleaned_row,
                        "chosen_query": chosen_query,
                        "matched_title": product_title,
                        "ean": ean_txt,
                        "brand": brand_txt,
                        "model_code": model_txt,
                        "category_hint": category_txt,
                        "confidence": conf,
                        "source": source_txt,
                    },
                })

            if results:
                log_message(
                    f"{__name__}.search_ean_alias_candidates",
                    "alias candidates found",
                    extra={
                        "raw_name": raw_txt,
                        "cleaned_name": cleaned_txt,
                        "count": len(results),
                    },
                )
            return results
        except Exception as e:
            log_exception(__name__, e)
            return []
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def search_local_ean_candidates(self, produkt_name, varianten_info="", limit=10):
        """
        Sucht EAN-Kandidaten lokal in ean_katalog und (fallback) in waren_positionen.
        Rueckgabe: Liste von Dicts mit produkt_name, varianten_info, ean, bild_url, quelle, confidence.
        """
        name = str(produkt_name or "").strip()
        if not name:
            return []

        max_limit = max(1, min(int(limit or 10), 50))
        variant = str(varianten_info or "").strip()
        like_name = self._build_contains_like_pattern(name)
        results = []
        seen = set()

        conn = self._get_connection()
        if not conn.is_connected():
            return []

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            query_main = f"""
                SELECT produkt_name, varianten_info, ean,
                       COALESCE(bild_url, '') AS bild_url,
                       COALESCE(quelle, 'ean_katalog') AS quelle,
                       COALESCE(confidence, 1.0) AS confidence
                FROM ean_katalog
                WHERE (LOWER(produkt_name) = LOWER(%s) OR produkt_name LIKE %s ESCAPE '\\')
                  AND ean IS NOT NULL AND ean != ''
                ORDER BY
                    CASE WHEN LOWER(produkt_name) = LOWER(%s) THEN 0 ELSE 1 END ASC,
                    CASE WHEN %s != '' AND LOWER(varianten_info) = LOWER(%s) THEN 0 ELSE 1 END ASC,
                    confidence DESC,
                    last_seen_at DESC
                LIMIT {max_limit}
            """
            cursor.execute(query_main, (name, like_name, name, variant, variant))
            rows = cursor.fetchall() or []

            for row in rows:
                key = (str(row.get("ean", "")).strip(), str(row.get("produkt_name", "")).strip())
                if not key[0] or key in seen:
                    continue
                seen.add(key)
                results.append({
                    "produkt_name": str(row.get("produkt_name", "")).strip(),
                    "varianten_info": str(row.get("varianten_info", "")).strip(),
                    "ean": str(row.get("ean", "")).strip(),
                    "bild_url": str(row.get("bild_url", "")).strip(),
                    "quelle": str(row.get("quelle", "ean_katalog")).strip(),
                    "confidence": float(row.get("confidence", 1.0) or 1.0),
                })

            if len(results) < max_limit:
                rest = max_limit - len(results)
                query_fallback = f"""
                    SELECT
                        produkt_name,
                        COALESCE(varianten_info, '') AS varianten_info,
                        ean,
                        MAX(id) AS newest_id
                    FROM waren_positionen
                    WHERE (LOWER(produkt_name) = LOWER(%s) OR produkt_name LIKE %s ESCAPE '\\')
                      AND ean IS NOT NULL AND ean != ''
                    GROUP BY produkt_name, varianten_info, ean
                    ORDER BY newest_id DESC
                    LIMIT {rest}
                """
                cursor.execute(query_fallback, (name, like_name))
                rows2 = cursor.fetchall() or []
                for row in rows2:
                    key = (str(row.get("ean", "")).strip(), str(row.get("produkt_name", "")).strip())
                    if not key[0] or key in seen:
                        continue
                    seen.add(key)
                    results.append({
                        "produkt_name": str(row.get("produkt_name", "")).strip(),
                        "varianten_info": str(row.get("varianten_info", "")).strip(),
                        "ean": str(row.get("ean", "")).strip(),
                        "bild_url": "",
                        "quelle": "waren_positionen",
                        "confidence": 0.6,
                    })

            return results
        except Exception as e:
            log_exception(__name__, e)
            return []
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def find_product_candidates_by_ean(self, ean, limit=20):
        """Liefert lokale Produktkandidaten zu einer gescannten EAN."""
        ean_txt = str(ean or "").strip()
        if not ean_txt:
            return []

        max_limit = max(1, min(int(limit or 20), 100))
        results = []
        seen = set()

        conn = self._get_connection()
        if not conn.is_connected():
            return []

        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            query_main = f"""
                SELECT produkt_name, varianten_info, ean,
                       COALESCE(bild_url, '') AS bild_url,
                       COALESCE(quelle, 'ean_katalog') AS quelle,
                       COALESCE(confidence, 1.0) AS confidence
                FROM ean_katalog
                WHERE ean = %s
                ORDER BY confidence DESC, last_seen_at DESC
                LIMIT {max_limit}
            """
            cursor.execute(query_main, (ean_txt,))
            rows = cursor.fetchall() or []
            for row in rows:
                key = (str(row.get("ean", "")).strip(), str(row.get("produkt_name", "")).strip(), str(row.get("varianten_info", "")).strip())
                if key in seen:
                    continue
                seen.add(key)
                results.append({
                    "produkt_name": str(row.get("produkt_name", "")).strip(),
                    "varianten_info": str(row.get("varianten_info", "")).strip(),
                    "ean": str(row.get("ean", "")).strip(),
                    "bild_url": str(row.get("bild_url", "")).strip(),
                    "quelle": str(row.get("quelle", "ean_katalog")).strip(),
                    "confidence": float(row.get("confidence", 1.0) or 1.0),
                })

            if len(results) < max_limit:
                rest = max_limit - len(results)
                query_fallback = f"""
                    SELECT produkt_name, COALESCE(varianten_info, '') AS varianten_info, ean, MAX(id) AS newest_id
                    FROM waren_positionen
                    WHERE ean = %s
                    GROUP BY produkt_name, varianten_info, ean
                    ORDER BY newest_id DESC
                    LIMIT {rest}
                """
                cursor.execute(query_fallback, (ean_txt,))
                rows2 = cursor.fetchall() or []
                for row in rows2:
                    key = (str(row.get("ean", "")).strip(), str(row.get("produkt_name", "")).strip(), str(row.get("varianten_info", "")).strip())
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append({
                        "produkt_name": str(row.get("produkt_name", "")).strip(),
                        "varianten_info": str(row.get("varianten_info", "")).strip(),
                        "ean": str(row.get("ean", "")).strip(),
                        "bild_url": "",
                        "quelle": "waren_positionen",
                        "confidence": 0.6,
                    })

            return results
        except Exception as e:
            log_exception(__name__, e)
            return []
        finally:
            if conn and conn.is_connected():
                if cursor:
                    cursor.close()
                conn.close()

    def get_ean_by_name(self, produkt_name):
        """Versucht, eine bekannte EAN fuer ein Produkt zu finden (zentral ueber ean_katalog)."""
        candidates = self.search_local_ean_candidates(produkt_name, limit=1)
        if candidates:
            return str(candidates[0].get("ean", "")).strip()
        return ""

