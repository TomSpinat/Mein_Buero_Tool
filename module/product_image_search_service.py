import json
import logging
import ssl
import urllib.error
import urllib.parse
import urllib.request
from urllib.parse import urlparse

from module.crash_logger import log_exception
from module.upcitemdb_normalizer import ProductNameNormalizer


class ProductImageSearchError(Exception):
    pass


class ProductImageSearchService:
    NEGATIVE_TEXT_TERMS = {
        'logo', 'icon', 'sprite', 'banner', 'placeholder', 'favicon', 'header', 'category page',
        'brand page', 'shop header', 'shop logo', 'store logo', 'vector', 'symbol', 'clipart'
    }
    NEGATIVE_DOMAIN_TERMS = {'wikipedia.org', 'wikimedia.org', 'facebook.com', 'instagram.com', 'pinterest.'}
    POSITIVE_DOMAIN_TERMS = {'images', 'cdn', 'media', 'product', 'shop', 'store'}

    def __init__(self, settings_manager):
        self.settings_manager = settings_manager
        self.normalizer = ProductNameNormalizer()
        self.provider = str(settings_manager.get('product_image_search_provider', 'brave') or 'brave').strip().lower()
        self.api_url = str(settings_manager.get('product_image_search_api_url', '') or '').strip()
        self.api_key = str(settings_manager.get('product_image_search_api_key', '') or '').strip()
        self.cx = str(settings_manager.get('product_image_search_cx', '') or '').strip()
        self.enabled = bool(settings_manager.get('product_image_search_enabled', True))
        self.timeout_sec = max(3, int(settings_manager.get('product_image_search_timeout_sec', 8) or 8))
        self.max_results = max(1, min(6, int(settings_manager.get('product_image_search_max_results', 6) or 6)))
        self.raw_provider_limit = max(self.max_results * 3, 10)

    def search_candidates(self, produkt_name, varianten_info='', ean='', limit=None, context=None):
        produkt_name = str(produkt_name or '').strip()
        varianten_info = str(varianten_info or '').strip()
        ean = str(ean or '').strip()
        context = dict(context or {}) if isinstance(context, dict) else {}
        output_limit = max(1, min(6, int(limit or self.max_results or 6)))
        if not produkt_name:
            raise ProductImageSearchError('Fuer die Bildsuche fehlt der Produktname.')
        if not self.enabled:
            raise ProductImageSearchError('Die Web-Bildsuche ist in den Einstellungen deaktiviert.')
        if self.provider not in ('brave', 'google'):
            raise ProductImageSearchError(f"Der konfigurierte Bildsuch-Provider '{self.provider}' wird aktuell noch nicht unterstuetzt.")
        if self.provider == 'brave' and not self.api_url:
            raise ProductImageSearchError('Die Bildsuch-API-URL fehlt in den Einstellungen (für Brave).')
        if not self.api_key:
            raise ProductImageSearchError('Fuer die Bildsuche fehlt noch der API-Key in den Einstellungen.')
        if self.provider == 'google' and not self.cx:
            raise ProductImageSearchError('Fuer die Google Bildsuche fehlt die existierende Suchmaschinen-ID (CX).')

        normalized = self.normalizer.normalize_for_upcitemdb(produkt_name, varianten_info)
        query_plan = self._build_query_plan(normalized)
        logging.info(
            'product_image_search_started: produkt_name=%s, ean=%s, query_count=%s, context=%s',
            produkt_name,
            ean,
            len(query_plan),
            context,
        )

        raw_candidates = []
        for entry in query_plan:
            query_text = str(entry.get('query', '') or '').strip()
            if not query_text:
                continue
            logging.info(
                'product_image_search_query_built: produkt_name=%s, query_kind=%s, query=%s',
                produkt_name,
                str(entry.get('kind', '') or ''),
                query_text,
            )
            provider_rows = self._fetch_provider_results(query_text)
            for original_rank, row in enumerate(provider_rows, start=1):
                normalized_row = self._normalize_provider_row(
                    row,
                    produkt_name=produkt_name,
                    varianten_info=varianten_info,
                    normalized=normalized,
                    query_entry=entry,
                    original_rank=original_rank,
                )
                if normalized_row:
                    raw_candidates.append(normalized_row)

        ranked = self._rank_candidates(raw_candidates, normalized)
        deduped = self._dedupe_candidates(ranked)
        top_candidates = deduped[:output_limit]
        logging.info(
            'product_image_search_candidates_ranked: produkt_name=%s, candidate_count=%s, returned=%s',
            produkt_name,
            len(deduped),
            len(top_candidates),
        )
        return {
            'provider': self.provider,
            'produkt_name': produkt_name,
            'varianten_info': varianten_info,
            'ean': ean,
            'normalized_lookup_meta': normalized.to_dict(),
            'query_plan': query_plan,
            'candidates': top_candidates,
            'candidate_count': len(top_candidates),
            'total_ranked_count': len(deduped),
        }

    def _build_query_plan(self, normalized):
        normalized_meta = normalized.to_dict() if hasattr(normalized, 'to_dict') else {}
        brand = str(normalized_meta.get('brand', '') or '').strip()
        model_code = str(normalized_meta.get('model_code', '') or '').strip()
        family = str(normalized_meta.get('product_family', '') or '').strip()
        category = str(normalized_meta.get('category_hint', '') or '').strip()
        size = str(normalized_meta.get('size_or_capacity', '') or '').strip()
        platform = str(normalized_meta.get('platform', '') or '').strip()
        cleaned_name = str(normalized_meta.get('cleaned_name', '') or '').strip()

        plan = []
        plan.append({'kind': 'strict', 'query': self._join_query_parts(brand, model_code or family, size, category, platform)})
        plan.append({'kind': 'core_family', 'query': self._join_query_parts(brand, family or model_code, size, category)})
        plan.append({'kind': 'soft_fallback', 'query': self._join_query_parts(brand, family or cleaned_name, category)})

        for variant in list(normalized_meta.get('query_variants', []) or []):
            if len(plan) >= 6:
                break
            plan.append({'kind': str(variant.get('kind', 'normalizer') or 'normalizer'), 'query': str(variant.get('query', '') or '').strip()})

        deduped = []
        seen = set()
        for entry in plan:
            query = str(entry.get('query', '') or '').strip()
            if not query:
                continue
            key = query.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append({'kind': str(entry.get('kind', '') or 'query'), 'query': query})
            if len(deduped) >= 5:
                break
        return deduped

    def _join_query_parts(self, *parts):
        ordered = []
        seen = set()
        for part in parts:
            text = str(part or '').strip()
            if not text:
                continue
            token = self.normalizer.compact_token(text)
            if token and token in seen:
                continue
            if token:
                seen.add(token)
            ordered.append(text)
        return ' '.join(ordered).strip()

    def _fetch_provider_results(self, query_text):
        if self.provider == 'google':
            return self._fetch_google_results(query_text)
        return self._fetch_brave_results(query_text)

    def _fetch_google_results(self, query_text):
        encoded_query = urllib.parse.urlencode({
            'q': query_text,
            'key': self.api_key,
            'cx': self.cx,
            'searchType': 'image',
            'num': min(10, self.raw_provider_limit),
            'safe': 'active',
        })
        base_url = 'https://customsearch.googleapis.com/customsearch/v1'
        request_url = f"{base_url}?{encoded_query}"
        logging.info('product_image_search_api_request: provider=google, url=hidden')
        request = urllib.request.Request(
            request_url,
            headers={
                'Accept': 'application/json',
                'User-Agent': 'MeinBueroTool/1.0',
            },
        )
        return self._execute_request(request, query_text, 'google')

    def _fetch_brave_results(self, query_text):
        encoded_query = urllib.parse.urlencode({
            'q': query_text,
            'count': self.raw_provider_limit,
            'search_lang': 'en',
            'spellcheck': 0,
            'safesearch': 'moderate',
        })
        request_url = f"{self.api_url}?{encoded_query}"
        logging.info('product_image_search_api_request: provider=brave, url=%s', request_url)
        request = urllib.request.Request(
            request_url,
            headers={
                'Accept': 'application/json',
                'User-Agent': 'MeinBueroTool/1.0',
                'X-Subscription-Token': self.api_key,
            },
        )
        return self._execute_request(request, query_text, 'brave')

    def _execute_request(self, request, query_text, provider):
        try:
            context = ssl.create_default_context()
            with urllib.request.urlopen(request, timeout=self.timeout_sec, context=context) as response:
                raw_data = response.read()
        except urllib.error.HTTPError as exc:
            log_exception(__name__, exc, extra={'provider': self.provider, 'query': query_text, 'status_code': exc.code})
            logging.warning('product_image_search_failed: provider=%s, status_code=%s, reason=%s', self.provider, exc.code, exc)
            if int(exc.code) in (401, 403):
                raise ProductImageSearchError('Die Bildsuche wurde vom Provider abgelehnt. Bitte API-Key und Berechtigung pruefen.') from exc
            if int(exc.code) == 429:
                raise ProductImageSearchError('Die Bildsuche wurde gerade vom Provider gedrosselt. Bitte spaeter erneut versuchen.') from exc
            raise ProductImageSearchError(f'Die Bildsuche antwortet mit HTTP {exc.code}.') from exc
        except urllib.error.URLError as exc:
            log_exception(__name__, exc, extra={'provider': self.provider, 'query': query_text})
            logging.warning('product_image_search_failed: provider=%s, reason=%s', self.provider, exc)
            raise ProductImageSearchError('Die Bildsuch-API ist aktuell nicht erreichbar.') from exc
        except Exception as exc:
            log_exception(__name__, exc, extra={'provider': self.provider, 'query': query_text})
            logging.warning('product_image_search_failed: provider=%s, reason=%s', self.provider, exc)
            raise ProductImageSearchError(f'Die Bildsuche konnte nicht geladen werden: {exc}') from exc

        try:
            payload = json.loads(raw_data.decode('utf-8', errors='replace'))
        except Exception as exc:
            log_exception(__name__, exc, extra={'provider': self.provider, 'query': query_text})
            raise ProductImageSearchError('Die Bildsuch-API hat eine ungueltige Antwort geliefert.') from exc

        results = self._extract_provider_results(payload)
        logging.info(
            'product_image_search_api_response: provider=%s, query=%s, result_count=%s',
            self.provider,
            query_text,
            len(results),
        )
        return results

    def _extract_provider_results(self, payload):
        if not isinstance(payload, dict):
            return []
            for key in ('items', 'results', 'images'):
                rows = payload.get(key)
                if isinstance(rows, list):
                    return rows
        data = payload.get('data')
        if isinstance(data, dict):
            for key in ('items', 'results', 'images'):
                rows = data.get(key)
                if isinstance(rows, list):
                    return rows
        return []

    def _normalize_provider_row(self, row, produkt_name, varianten_info, normalized, query_entry, original_rank):
        if not isinstance(row, dict):
            return None

        image_url = self._normalize_url(
            self._first_text(
                self._deep_get(row, 'properties', 'url'),
                self._deep_get(row, 'image', 'url'),
                row.get('image_url'),
                self._deep_get(row, 'thumbnail', 'src'),
            )
        )
        if not image_url or not image_url.startswith(('http://', 'https://')):
            return None

        thumbnail_url = self._normalize_url(
            self._first_text(
                self._deep_get(row, 'thumbnail', 'src'),
                self._deep_get(row, 'thumbnail', 'url'),
                row.get('thumbnail_url'),
            )
        )
        source_page_url = self._normalize_url(
            self._first_text(
                row.get('source'),
                row.get('page_url'),
                row.get('url'),
                self._deep_get(row, 'meta_url', 'url'),
                self._deep_get(row, 'url_source', 'url'),
            )
        )
        title = self._first_text(row.get('title'), row.get('alt'), row.get('image_title'))
        snippet = self._first_text(row.get('description'), row.get('snippet'), row.get('caption'))
        width = self._to_int(self._first_text(self._deep_get(row, 'properties', 'width'), row.get('width')))
        height = self._to_int(self._first_text(self._deep_get(row, 'properties', 'height'), row.get('height')))
        source_domain = self._domain_from_url(source_page_url or image_url)

        return {
            'title': title or source_domain or produkt_name,
            'snippet': snippet,
            'image_url': image_url,
            'thumbnail_url': thumbnail_url or image_url,
            'source_domain': source_domain,
            'source_page_url': source_page_url,
            'width': width,
            'height': height,
            'confidence': 0.0,
            'query_used': str(query_entry.get('query', '') or '').strip(),
            'query_kind': str(query_entry.get('kind', '') or '').strip(),
            'original_rank': int(original_rank),
            'produkt_name': produkt_name,
            'varianten_info': varianten_info,
            'normalized_lookup_meta': normalized.to_dict() if hasattr(normalized, 'to_dict') else {},
            'score_details': {},
        }

    def _rank_candidates(self, candidates, normalized):
        ranked = []
        normalized_meta = normalized.to_dict() if hasattr(normalized, 'to_dict') else {}
        brand_token = self.normalizer.compact_token(normalized_meta.get('brand', ''))
        model_token = self.normalizer.compact_token(normalized_meta.get('model_code', ''))
        family_token = self.normalizer.compact_token(normalized_meta.get('product_family', ''))
        size_token = self.normalizer.compact_token(normalized_meta.get('size_or_capacity', ''))
        category_token = self.normalizer.compact_token(normalized_meta.get('category_hint', ''))

        for candidate in candidates:
            row = dict(candidate or {})
            title = str(row.get('title', '') or '').strip()
            snippet = str(row.get('snippet', '') or '').strip()
            source_domain = str(row.get('source_domain', '') or '').strip().lower()
            search_text = ' '.join(part for part in (title, snippet, source_domain) if part).lower()
            compact_text = self.normalizer.compact_token(search_text)

            score = 0.0
            details = {}

            if brand_token and brand_token in compact_text:
                score += 2.2
                details['brand'] = 2.2
            if model_token and model_token in compact_text:
                score += 3.0
                details['model_code'] = 3.0
            if family_token and family_token in compact_text:
                score += 2.0
                details['product_family'] = 2.0
            if size_token and size_token in compact_text:
                score += 1.2
                details['size_or_capacity'] = 1.2
            if category_token and category_token in compact_text:
                score += 1.0
                details['category_hint'] = 1.0

            width = self._to_int(row.get('width'))
            height = self._to_int(row.get('height'))
            if width >= 300 and height >= 300:
                score += 1.0
                details['image_size'] = 1.0
            elif width >= 160 and height >= 160:
                score += 0.5
                details['image_size'] = 0.5
            else:
                score -= 0.6
                details['image_size'] = -0.6

            if width > 0 and height > 0:
                ratio = max(width, height) / max(1.0, min(width, height))
                if ratio <= 1.8:
                    score += 0.6
                    details['aspect_ratio'] = 0.6
                elif ratio >= 3.0:
                    score -= 1.0
                    details['aspect_ratio'] = -1.0

            if any(term in search_text for term in self.NEGATIVE_TEXT_TERMS):
                score -= 3.0
                details['negative_text'] = -3.0
            if any(term in source_domain for term in self.NEGATIVE_DOMAIN_TERMS):
                score -= 1.2
                details['negative_domain'] = -1.2
            if any(term in source_domain for term in self.POSITIVE_DOMAIN_TERMS):
                score += 0.4
                details['positive_domain'] = 0.4

            query_kind = str(row.get('query_kind', '') or '').strip().lower()
            if query_kind == 'strict':
                score += 0.8
                details['query_kind'] = 0.8
            elif query_kind == 'core_family':
                score += 0.5
                details['query_kind'] = 0.5

            confidence = max(0.01, min(0.99, round((score + 4.5) / 10.0, 3)))
            row['confidence'] = confidence
            row['ranking_score'] = round(score, 3)
            row['score_details'] = details
            ranked.append(row)

        ranked.sort(key=lambda item: (-float(item.get('ranking_score', 0.0) or 0.0), -float(item.get('confidence', 0.0) or 0.0), int(item.get('original_rank', 999) or 999)))
        return ranked

    def _dedupe_candidates(self, candidates):
        deduped = []
        seen = set()
        for candidate in candidates:
            image_url = str(candidate.get('image_url', '') or '').strip().lower()
            if not image_url or image_url in seen:
                continue
            seen.add(image_url)
            deduped.append(candidate)
        return deduped

    def _deep_get(self, data, *keys):
        current = data
        for key in keys:
            if not isinstance(current, dict):
                return ''
            current = current.get(key)
        return current

    def _first_text(self, *values):
        for value in values:
            if isinstance(value, (dict, list, tuple, set)):
                continue
            text = str(value or '').strip()
            if text:
                return text
        return ''

    def _normalize_url(self, value):
        text = str(value or "").strip()
        if not text:
            return ""
        if text.startswith("//"):
            return f"https:{text}"
        if text.startswith(("http://", "https://")):
            return text
        return ""

    def _to_int(self, value):
        try:
            return int(float(str(value or '0').strip()))
        except Exception:
            return 0

    def _domain_from_url(self, value):
        try:
            parsed = urlparse(str(value or '').strip())
            return str(parsed.netloc or '').lower().lstrip('www.')
        except Exception:
            return ''



