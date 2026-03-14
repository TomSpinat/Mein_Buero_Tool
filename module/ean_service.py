"""
ean_service.py
Zentraler Service fuer EAN/Barcode-Funktionen.

Ziel:
- lokale EAN-Wissensbasis und Alias-Lernspeicher nutzen
- chaotische Produkttitel fuer UPCitemdb normalisieren
- mehrere priorisierte Query-Varianten probieren
- API-Treffer bewerten und lokal lernen
"""

import json
import ssl
import urllib.parse
import urllib.request

from module.crash_logger import (
    AppError,
    classify_upcitemdb_error,
    error_category_priority,
    error_to_payload,
    log_classified_error,
    log_message,
)
from module.database_manager import DatabaseManager
from module.media.media_service import MediaService
from module.upcitemdb_matcher import UpcitemdbMatcher
from module.upcitemdb_normalizer import ProductNameNormalizer


class EanService:
    def __init__(self, settings_manager, db_manager=None):
        self.settings_manager = settings_manager
        self.db = db_manager or DatabaseManager(settings_manager)
        self.normalizer = ProductNameNormalizer()
        self.matcher = UpcitemdbMatcher(self.normalizer)
        self.media = MediaService(self.db)

    def find_local_candidates_by_name(self, produkt_name, varianten_info="", limit=10):
        """Liefert rohe lokale EAN-Kandidaten fuer Name/Variante."""
        return self.db.search_local_ean_candidates(
            produkt_name=produkt_name,
            varianten_info=varianten_info,
            limit=limit,
        )

    def find_candidates_by_name(self, produkt_name, varianten_info="", limit=25, allow_api_fallback=True):
        result = self.lookup_candidates_by_name(
            produkt_name,
            varianten_info=varianten_info,
            limit=limit,
            allow_api_fallback=allow_api_fallback,
        )
        return result.get("candidates", [])

    def lookup_candidates_by_name(self, produkt_name, varianten_info="", limit=25, allow_api_fallback=True):
        name = str(produkt_name or "").strip()
        variant = str(varianten_info or "").strip()
        max_limit = max(1, min(int(limit or 25), 50))
        normalized = self.normalizer.normalize_for_upcitemdb(name, varianten_info=variant)

        log_message(
            f"{__name__}.lookup_candidates_by_name",
            "normalized product query created",
            extra=normalized.to_dict(),
        )

        base_result = {
            "normalized": normalized.to_dict(),
            "candidates": [],
            "used_api": False,
            "local_hit": False,
            "error": None,
        }

        local_candidates = self._search_local_candidates(name, variant, normalized, max_limit)
        if local_candidates:
            base_result["candidates"] = local_candidates[:max_limit]
            base_result["local_hit"] = True
            return base_result

        if not allow_api_fallback or not self._api_enabled():
            return base_result

        api_candidates, api_error = self._search_upcitemdb_candidates(normalized, max_limit)
        base_result["used_api"] = True
        base_result["candidates"] = api_candidates[:max_limit]
        if api_error and not api_candidates:
            base_result["error"] = api_error
        return base_result

    def find_best_local_ean_by_name(self, produkt_name, varianten_info=""):
        """Liefert die beste lokal gefundene EAN oder ''."""
        name = str(produkt_name or "").strip()
        variant = str(varianten_info or "").strip()
        if not name:
            return ""
        normalized = self.normalizer.normalize_for_upcitemdb(name, varianten_info=variant)
        candidates = self._search_local_candidates(name, variant, normalized, 1)
        if not candidates:
            return ""
        return str(candidates[0].get("ean", "")).strip()

    def _normalize_ean(self, value):
        txt = str(value or "").strip()
        if not txt:
            return ""
        digits = "".join(ch for ch in txt if ch.isdigit())
        if len(digits) in (8, 12, 13, 14):
            return digits
        return txt

    def _safe_float(self, value, default=0.0):
        try:
            return float(value or default)
        except (TypeError, ValueError):
            return default

    def _api_enabled(self):
        raw = self.settings_manager.get("ean_api_enabled", True)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("1", "true", "yes", "on")

    def _build_local_search_terms(self, raw_name, normalized):
        terms = []

        def _add(term):
            txt = str(term or "").strip()
            if not txt:
                return
            key = txt.lower()
            if key in seen:
                return
            seen.add(key)
            terms.append(txt)

        seen = set()
        _add(raw_name)
        _add(normalized.cleaned_name)
        _add(normalized.english_core_query)
        if normalized.brand and normalized.model_code:
            _add(f"{normalized.brand} {normalized.model_code}")
        if normalized.brand and normalized.product_family:
            _add(f"{normalized.brand} {normalized.product_family}")
        if normalized.brand and normalized.product_family and normalized.color:
            _add(f"{normalized.brand} {normalized.product_family} {normalized.color}")
        return terms[:6]

    def _candidate_key(self, candidate):
        ean = str(candidate.get("ean", "")).strip()
        title = str(candidate.get("produkt_name", "")).strip().lower()
        if ean:
            return f"ean::{ean}"
        return f"title::{title}"

    def _merge_candidate(self, bucket, candidate, confidence_boost=0.0):
        merged = dict(candidate or {})
        merged["confidence"] = min(1.0, self._safe_float(merged.get("confidence", 0.5), 0.5) + confidence_boost)
        key = self._candidate_key(merged)
        current = bucket.get(key)
        if current is None or self._safe_float(merged.get("confidence"), 0.0) > self._safe_float(current.get("confidence"), 0.0):
            bucket[key] = merged

    def _search_local_candidates(self, raw_name, varianten_info, normalized, limit):
        bucket = {}

        alias_hits = self.db.search_ean_alias_candidates(
            raw_name,
            cleaned_name=normalized.cleaned_name,
            limit=limit,
        )
        for candidate in alias_hits:
            self._merge_candidate(bucket, candidate, confidence_boost=0.10)

        for idx, term in enumerate(self._build_local_search_terms(raw_name, normalized)):
            boost = 0.10 if idx == 0 else 0.06 if idx == 1 else 0.03
            hits = self.db.search_local_ean_candidates(
                produkt_name=term,
                varianten_info=varianten_info if idx == 0 else "",
                limit=limit,
            )
            for candidate in hits:
                self._merge_candidate(bucket, candidate, confidence_boost=boost)

        results = sorted(bucket.values(), key=lambda item: self._safe_float(item.get("confidence", 0.0), 0.0), reverse=True)
        if results:
            log_message(
                f"{__name__}._search_local_candidates",
                "local ean candidates found",
                extra={
                    "raw_name": raw_name,
                    "cleaned_name": normalized.cleaned_name,
                    "count": len(results),
                    "top_terms": self._build_local_search_terms(raw_name, normalized),
                },
            )
        return results[:limit]

    def _fetch_upcitemdb_items(self, query_text):
        query = str(query_text or "").strip()
        if len(query) < 2:
            return [], None

        api_url = str(
            self.settings_manager.get("upcitemdb_api_url", "https://api.upcitemdb.com/prod/trial/search")
        ).strip() or "https://api.upcitemdb.com/prod/trial/search"
        timeout_sec = int(self.settings_manager.get("upcitemdb_timeout_sec", 8) or 8)

        full_url = f"{api_url}?{urllib.parse.urlencode({'s': query})}"
        req = urllib.request.Request(
            full_url,
            headers={
                "User-Agent": "MeinBueroTool/1.0 (+EAN-Search)",
                "Accept": "application/json",
            },
        )

        api_key = str(self.settings_manager.get("upcitemdb_api_key", "")).strip()
        if api_key:
            req.add_header("Authorization", f"Bearer {api_key}")

        status_code = None
        raw_body = ""
        try:
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, timeout=timeout_sec, context=ctx) as resp:
                status_code = int(getattr(resp, "status", 200) or 200)
                raw_body = resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            app_error = classify_upcitemdb_error(exc, query_text=query)
            log_classified_error(
                f"{__name__}._fetch_upcitemdb_items",
                app_error.category,
                app_error.user_message,
                status_code=app_error.status_code,
                service=app_error.service,
                exc=exc,
                extra={"query": query, "api_url": api_url},
            )
            return [], app_error

        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            app_error = classify_upcitemdb_error(exc, query_text=query, status_code=status_code)
            log_classified_error(
                f"{__name__}._fetch_upcitemdb_items",
                app_error.category,
                app_error.user_message,
                status_code=app_error.status_code,
                service=app_error.service,
                exc=exc,
                extra={"query": query, "response_len": len(raw_body)},
            )
            return [], app_error

        if not isinstance(payload, dict):
            app_error = AppError(
                category="invalid_response",
                user_message="UPCitemdb hat eine ungueltige Antwort geliefert.",
                technical_message=f"payload type: {type(payload).__name__}",
                status_code=status_code,
                service="upcitemdb",
                retryable=False,
            )
            log_classified_error(
                f"{__name__}._fetch_upcitemdb_items",
                app_error.category,
                app_error.user_message,
                status_code=app_error.status_code,
                service=app_error.service,
                extra={"query": query},
            )
            return [], app_error

        items = payload.get("items", [])
        if items is None:
            items = []
        if not isinstance(items, list):
            app_error = AppError(
                category="invalid_response",
                user_message="UPCitemdb Antwort enthaelt kein gueltiges Treffer-Format.",
                technical_message=f"items type: {type(items).__name__}",
                status_code=status_code,
                service="upcitemdb",
                retryable=False,
            )
            log_classified_error(
                f"{__name__}._fetch_upcitemdb_items",
                app_error.category,
                app_error.user_message,
                status_code=app_error.status_code,
                service=app_error.service,
                extra={"query": query},
            )
            return [], app_error

        return items, None

    def _pick_image_url(self, raw_item):
        images = raw_item.get("images", [])
        if isinstance(images, list) and images:
            return str(images[0] or "").strip()
        image = raw_item.get("image")
        return str(image or "").strip()

    def _extract_category_text(self, raw_item):
        for key in ("category", "category_name"):
            value = raw_item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, list):
                parts = [str(x).strip() for x in value if str(x).strip()]
                if parts:
                    return " / ".join(parts)
        return ""

    def _search_upcitemdb_candidates(self, normalized, limit=25):
        max_queries = max(1, min(int(self.settings_manager.get("upcitemdb_max_queries", 5) or 5), 8))
        min_score = self._safe_float(self.settings_manager.get("upcitemdb_min_score", 0.16), 0.16)
        bucket = {}
        api_errors = []

        query_variants = list(normalized.query_variants)[:max_queries]
        log_message(
            f"{__name__}._search_upcitemdb_candidates",
            "starting upcitemdb lookup",
            extra={
                "raw_name": normalized.raw_name,
                "cleaned_name": normalized.cleaned_name,
                "queries": query_variants,
            },
        )

        for query_info in query_variants:
            query_text = str(query_info.get("query", "")).strip()
            if not query_text:
                continue

            raw_items, fetch_error = self._fetch_upcitemdb_items(query_text)
            if fetch_error is not None:
                api_errors.append(
                    {
                        "query": query_text,
                        "kind": str(query_info.get("kind", "")),
                        "error": fetch_error,
                    }
                )

            log_message(
                f"{__name__}._search_upcitemdb_candidates",
                "query completed",
                extra={
                    "query": query_text,
                    "kind": query_info.get("kind", ""),
                    "count": len(raw_items),
                    "error_category": fetch_error.category if fetch_error is not None else "",
                },
            )

            for raw_item in raw_items:
                ean = self._normalize_ean(raw_item.get("ean") or raw_item.get("upc") or raw_item.get("gtin"))
                if not ean:
                    continue

                title = str(raw_item.get("title", "") or "").strip() or normalized.raw_name
                brand = str(raw_item.get("brand", "") or "").strip()
                model = str(raw_item.get("model", "") or "").strip()
                category = self._extract_category_text(raw_item)
                score_info = self.matcher.score_api_item(
                    normalized,
                    raw_item,
                    query_kind=str(query_info.get("kind", "")),
                    query_text=query_text,
                )
                score = self._safe_float(score_info.get("score", 0.0), 0.0)
                if score < min_score:
                    continue

                item_norm = score_info.get("item_normalized", {}) if isinstance(score_info, dict) else {}
                best_brand = brand or str(item_norm.get("brand", "")).strip() or normalized.brand
                best_model = model or str(item_norm.get("model_code", "")).strip() or normalized.model_code
                best_category = category or str(item_norm.get("category_hint", "")).strip() or normalized.category_hint
                variant_text = " | ".join(part for part in [best_brand, best_model, best_category] if part)
                source_txt = f"upcitemdb_api:{str(query_info.get('kind', 'query')).strip() or 'query'}"

                candidate = {
                    "produkt_name": title,
                    "varianten_info": variant_text,
                    "ean": ean,
                    "bild_url": self._pick_image_url(raw_item),
                    "quelle": source_txt,
                    "confidence": score,
                    "brand": best_brand,
                    "model_code": best_model,
                    "category_hint": best_category,
                    "query_used": query_text,
                    "match_reasons": ", ".join(score_info.get("reasons", [])),
                    "_lookup_meta": {
                        "raw_name": normalized.raw_name,
                        "cleaned_name": normalized.cleaned_name,
                        "chosen_query": query_text,
                        "matched_title": title,
                        "ean": ean,
                        "brand": best_brand,
                        "model_code": best_model,
                        "category_hint": best_category,
                        "confidence": score,
                        "source": source_txt,
                    },
                }
                self._merge_candidate(bucket, candidate, confidence_boost=0.0)

        results = sorted(bucket.values(), key=lambda item: self._safe_float(item.get("confidence", 0.0), 0.0), reverse=True)
        if results:
            log_message(
                f"{__name__}._search_upcitemdb_candidates",
                "api candidates scored",
                extra={
                    "raw_name": normalized.raw_name,
                    "best_candidates": [
                        {
                            "produkt_name": str(item.get("produkt_name", "")),
                            "ean": str(item.get("ean", "")),
                            "confidence": self._safe_float(item.get("confidence", 0.0), 0.0),
                            "quelle": str(item.get("quelle", "")),
                            "query_used": str(item.get("query_used", "")),
                        }
                        for item in results[:5]
                    ],
                },
            )

        api_error_payload = None
        if not results and api_errors:
            api_errors = sorted(
                api_errors,
                key=lambda item: error_category_priority(getattr(item.get("error"), "category", "unknown")),
            )
            chosen = api_errors[0]
            api_error_payload = error_to_payload(chosen.get("error"))
            api_error_payload["query"] = str(chosen.get("query", ""))
            api_error_payload["query_kind"] = str(chosen.get("kind", ""))

        return results[:limit], api_error_payload

    def search_upcitemdb_candidates_by_name(self, produkt_name, varianten_info="", limit=25):
        normalized = self.normalizer.normalize_for_upcitemdb(produkt_name, varianten_info=varianten_info)
        return self._search_upcitemdb_candidates(normalized, limit=limit)

    def remember_mapping(self, produkt_name, ean, varianten_info="", bild_url="", quelle="manual", confidence=1.0):
        """Speichert eine EAN-Zuordnung in der lokalen Wissensbasis."""
        ok = self.db.upsert_local_ean_mapping(
            produkt_name=produkt_name,
            ean=ean,
            varianten_info=varianten_info,
            bild_url=bild_url,
            quelle=quelle,
            confidence=confidence,
        )
        if ok and str(bild_url or "").strip():
            try:
                self.media.ensure_product_image_from_existing_sources(
                    product_name=produkt_name,
                    ean=ean,
                    variant_text=varianten_info,
                    bild_url=bild_url,
                    source_module="ean_service",
                    source_kind=str(quelle or "ean_mapping").strip() or "ean_mapping",
                    is_primary=True,
                    priority=80,
                    metadata={
                        "confidence": float(confidence or 0.0),
                        "source_scope": "ean_mapping",
                    },
                )
                log_message(
                    f"{__name__}.remember_mapping",
                    "product image reference bridged into media assets",
                    extra={
                        "produkt_name": str(produkt_name or "").strip(),
                        "ean": str(ean or "").strip(),
                        "bild_url": str(bild_url or "").strip(),
                        "quelle": str(quelle or "").strip(),
                    },
                )
            except Exception as exc:
                log_classified_error(
                    f"{__name__}.remember_mapping",
                    "product_image_bridge_failed",
                    "Produktbild-Referenz konnte nicht in die neue Medienstruktur uebernommen werden.",
                    service="media_service",
                    exc=exc,
                    extra={
                        "produkt_name": str(produkt_name or "").strip(),
                        "ean": str(ean or "").strip(),
                        "bild_url": str(bild_url or "").strip(),
                    },
                )
        return ok

    def remember_candidate_selection(self, raw_name, selected_candidate, varianten_info=""):
        """Speichert eine bestaetigte Auswahl sowohl im EAN-Katalog als auch im Alias-Lernspeicher."""
        if not isinstance(selected_candidate, dict):
            return False

        raw_txt = str(raw_name or "").strip()
        variant_txt = str(varianten_info or "").strip()
        meta = dict(selected_candidate.get("_lookup_meta") or {})
        ean_txt = self._normalize_ean(selected_candidate.get("ean") or meta.get("ean"))
        if not raw_txt or not ean_txt:
            return False

        normalized = self.normalizer.normalize_for_upcitemdb(raw_txt, varianten_info=variant_txt)
        matched_title = str(meta.get("matched_title") or selected_candidate.get("produkt_name") or raw_txt).strip()
        chosen_query = str(meta.get("chosen_query") or selected_candidate.get("query_used") or "").strip()
        if not chosen_query and normalized.search_queries:
            chosen_query = normalized.search_queries[0]

        brand = str(meta.get("brand") or selected_candidate.get("brand") or normalized.brand).strip()
        model_code = str(meta.get("model_code") or selected_candidate.get("model_code") or normalized.model_code).strip()
        category_hint = str(meta.get("category_hint") or selected_candidate.get("category_hint") or normalized.category_hint).strip()
        image_url = str(selected_candidate.get("bild_url", "")).strip()
        source_txt = str(meta.get("source") or selected_candidate.get("quelle") or "manual").strip() or "manual"
        confidence = self._safe_float(meta.get("confidence", selected_candidate.get("confidence", 0.75)), 0.75)

        ok_raw = self.remember_mapping(
            produkt_name=raw_txt,
            ean=ean_txt,
            varianten_info=variant_txt,
            bild_url=image_url,
            quelle=source_txt,
            confidence=confidence,
        )

        ok_clean = True
        if normalized.cleaned_name and normalized.cleaned_name.lower() not in (raw_txt.lower(), matched_title.lower()):
            ok_clean = self.remember_mapping(
                produkt_name=normalized.cleaned_name,
                ean=ean_txt,
                varianten_info=variant_txt,
                bild_url=image_url,
                quelle=f"{source_txt}:cleaned",
                confidence=max(confidence - 0.05, 0.4),
            )

        ok_title = True
        if matched_title and matched_title.lower() != raw_txt.lower():
            ok_title = self.remember_mapping(
                produkt_name=matched_title,
                ean=ean_txt,
                varianten_info=variant_txt,
                bild_url=image_url,
                quelle=f"{source_txt}:title",
                confidence=confidence,
            )

        ok_alias = self.db.upsert_ean_alias_cache(
            raw_name=raw_txt,
            cleaned_name=normalized.cleaned_name,
            varianten_info=variant_txt,
            chosen_query=chosen_query,
            matched_title=matched_title,
            ean=ean_txt,
            brand=brand,
            model_code=model_code,
            category_hint=category_hint,
            confidence=confidence,
            source=source_txt,
        )

        log_message(
            f"{__name__}.remember_candidate_selection",
            "ean selection stored",
            extra={
                "raw_name": raw_txt,
                "cleaned_name": normalized.cleaned_name,
                "matched_title": matched_title,
                "chosen_query": chosen_query,
                "ean": ean_txt,
                "brand": brand,
                "model_code": model_code,
                "category_hint": category_hint,
                "confidence": confidence,
                "source": source_txt,
            },
        )
        return bool(ok_raw and ok_clean and ok_title and ok_alias)

    def find_local_products_by_ean(self, ean, limit=20):
        """Liefert lokale Produktkandidaten fuer eine gescannte EAN."""
        return self.db.find_product_candidates_by_ean(ean, limit=limit)






