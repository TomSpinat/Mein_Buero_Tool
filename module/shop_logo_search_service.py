import json
import logging
import ssl
import urllib.error
import urllib.parse
import urllib.request
from urllib.parse import urlparse

from module.crash_logger import log_exception
from module.media.media_keys import build_shop_key, extract_sender_domain, normalize_sender_domain


class ShopLogoSearchError(Exception):
    pass


class ShopLogoSearchService:
    NEGATIVE_TEXT_TERMS = {
        'product',
        'produkte',
        'banner',
        'header',
        'screenshot',
        'store screenshot',
        'shop screenshot',
        'press',
        'social',
        'avatar',
        'cover',
        'placeholder',
        'mockup',
    }
    POSITIVE_TEXT_TERMS = {'logo', 'official', 'brand'}
    NEGATIVE_DOMAIN_TERMS = {'facebook.com', 'instagram.com', 'pinterest.', 'wikipedia.org', 'wikimedia.org'}
    POSITIVE_DOMAIN_TERMS = {'brand', 'logo', 'press', 'cdn', 'media', 'assets'}

    def __init__(self, settings_manager):
        self.settings_manager = settings_manager
        self.provider = str(settings_manager.get('shop_logo_search_provider', settings_manager.get('product_image_search_provider', 'brave')) or 'brave').strip().lower()
        self.api_url = str(settings_manager.get('shop_logo_search_api_url', settings_manager.get('product_image_search_api_url', '')) or '').strip()
        self.api_key = str(settings_manager.get('shop_logo_search_api_key', settings_manager.get('product_image_search_api_key', '')) or '').strip()
        self.cx = str(settings_manager.get('shop_logo_search_google_cx', settings_manager.get('shop_logo_search_cx', settings_manager.get('product_image_search_google_cx', ''))) or '').strip()
        self.enabled = bool(settings_manager.get('shop_logo_search_enabled', settings_manager.get('product_image_search_enabled', True)))
        self.timeout_sec = max(3, int(settings_manager.get('shop_logo_search_timeout_sec', settings_manager.get('product_image_search_timeout_sec', 8)) or 8))
        self.max_results = max(1, min(6, int(settings_manager.get('shop_logo_search_max_results', settings_manager.get('product_image_search_max_results', 6)) or 6)))
        self.raw_provider_limit = max(self.max_results * 3, 10)

    def search_candidates(self, canonical_shop_name='', sender_domain='', shop_key='', context=None, limit=None):
        resolved_context = self._resolve_context(
            canonical_shop_name=canonical_shop_name,
            sender_domain=sender_domain,
            shop_key=shop_key,
            context=context,
        )
        output_limit = max(1, min(6, int(limit or self.max_results or 6)))
        if not self.enabled:
            raise ShopLogoSearchError('Die Web-Logosuche ist in den Einstellungen deaktiviert.')
        if self.provider not in ('brave', 'google'):
            raise ShopLogoSearchError(f"Der konfigurierte Logo-Suchprovider '{self.provider}' wird aktuell noch nicht unterstuetzt.")
        if self.provider == 'brave' and not self.api_url:
            raise ShopLogoSearchError('Die Logo-Such-API-URL fehlt in den Einstellungen (fuer Brave).')
        if not self.api_key:
            raise ShopLogoSearchError('Fuer die Logo-Suche fehlt noch ein API-Key in den Einstellungen.')
        if self.provider == 'google' and not self.cx:
            raise ShopLogoSearchError('Fuer die Google Logo-Suche fehlt die existierende Suchmaschinen-ID (CX).')
        if not resolved_context.get('canonical_shop_name') and not resolved_context.get('sender_domain'):
            raise ShopLogoSearchError('Fuer die Logo-Suche fehlt noch ein belastbarer Shop-Kontext.')

        query_plan = self._build_query_plan(resolved_context)
        logging.info(
            'shop_logo_search_started: canonical_shop_name=%s, sender_domain=%s, shop_key=%s, query_count=%s',
            resolved_context.get('canonical_shop_name', ''),
            resolved_context.get('sender_domain', ''),
            resolved_context.get('shop_key', ''),
            len(query_plan),
        )

        raw_candidates = []
        for entry in query_plan:
            query_text = str(entry.get('query', '') or '').strip()
            if not query_text:
                continue
            logging.info(
                'shop_logo_search_query_built: canonical_shop_name=%s, query_kind=%s, query=%s',
                resolved_context.get('canonical_shop_name', ''),
                str(entry.get('kind', '') or ''),
                query_text,
            )
            provider_rows = self._fetch_provider_results(query_text)
            for original_rank, row in enumerate(provider_rows, start=1):
                normalized_row = self._normalize_provider_row(
                    row,
                    resolved_context=resolved_context,
                    query_entry=entry,
                    original_rank=original_rank,
                )
                if normalized_row:
                    raw_candidates.append(normalized_row)

        ranked = self._rank_candidates(raw_candidates, resolved_context)
        deduped = self._dedupe_candidates(ranked)
        top_candidates = deduped[:output_limit]
        logging.info(
            'shop_logo_candidates_ranked: canonical_shop_name=%s, candidate_count=%s, returned=%s',
            resolved_context.get('canonical_shop_name', ''),
            len(deduped),
            len(top_candidates),
        )
        return {
            'provider': self.provider,
            'canonical_shop_name': resolved_context.get('canonical_shop_name', ''),
            'sender_domain': resolved_context.get('sender_domain', ''),
            'shop_key': resolved_context.get('shop_key', ''),
            'shop_lookup_meta': dict(resolved_context),
            'query_plan': query_plan,
            'candidates': top_candidates,
            'candidate_count': len(top_candidates),
            'total_ranked_count': len(deduped),
        }

    def _resolve_context(self, canonical_shop_name='', sender_domain='', shop_key='', context=None):
        context = dict(context or {}) if isinstance(context, dict) else {}
        resolved_name = str(
            canonical_shop_name
            or context.get('canonical_shop_name', '')
            or context.get('shop_name', '')
        ).strip()
        resolved_sender_domain = normalize_sender_domain(
            sender_domain
            or context.get('sender_domain', '')
            or context.get('_email_sender_domain', '')
            or extract_sender_domain(context.get('bestell_email', ''))
            or extract_sender_domain(context.get('_email_sender', ''))
        )
        resolved_shop_key = str(shop_key or context.get('shop_key', '') or '').strip()
        if not resolved_shop_key:
            resolved_shop_key = build_shop_key(shop_name=resolved_name, sender_domain=resolved_sender_domain)
        return {
            'canonical_shop_name': resolved_name,
            'sender_domain': resolved_sender_domain,
            'shop_key': resolved_shop_key,
            'raw_shop_name': str(context.get('raw_shop_name', context.get('shop_name', '')) or '').strip(),
            'bestell_email': str(context.get('bestell_email', '') or '').strip(),
        }

    def _build_query_plan(self, resolved_context):
        canonical_shop_name = str(resolved_context.get('canonical_shop_name', '') or '').strip()
        sender_domain = str(resolved_context.get('sender_domain', '') or '').strip()
        domain_label = sender_domain.lstrip('www.').strip()
        short_name = canonical_shop_name or self._domain_stub(domain_label)

        plan = [
            {'kind': 'strict', 'query': self._join_query_parts(canonical_shop_name, 'logo')},
            {'kind': 'domain_logo', 'query': self._join_query_parts(domain_label, 'logo')},
            {'kind': 'official_logo', 'query': self._join_query_parts(short_name, 'official logo')},
        ]

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
            token = text.lower()
            if token in seen:
                continue
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
        logging.info('shop_logo_search_api_request: provider=google, url=hidden')
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
            'safesearch': 'off',
        })
        request_url = f"{self.api_url}?{encoded_query}"
        logging.info('shop_logo_search_api_request: provider=brave, url=%s', request_url)
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
            logging.warning('shop_logo_search_failed: provider=%s, status_code=%s, reason=%s', self.provider, exc.code, exc)
            if int(exc.code) in (401, 403):
                raise ShopLogoSearchError('Die Logo-Suche wurde vom Provider abgelehnt. Bitte API-Key und Berechtigung pruefen.') from exc
            if int(exc.code) == 429:
                raise ShopLogoSearchError('Die Logo-Suche wurde gerade vom Provider gedrosselt. Bitte spaeter erneut versuchen.') from exc
            raise ShopLogoSearchError(f'Die Logo-Suche antwortet mit HTTP {exc.code}.') from exc
        except urllib.error.URLError as exc:
            log_exception(__name__, exc, extra={'provider': self.provider, 'query': query_text})
            logging.warning('shop_logo_search_failed: provider=%s, reason=%s', self.provider, exc)
            raise ShopLogoSearchError('Die Logo-Such-API ist aktuell nicht erreichbar.') from exc
        except Exception as exc:
            log_exception(__name__, exc, extra={'provider': self.provider, 'query': query_text})
            logging.warning('shop_logo_search_failed: provider=%s, reason=%s', self.provider, exc)
            raise ShopLogoSearchError(f'Die Logo-Suche konnte nicht geladen werden: {exc}') from exc

        try:
            payload = json.loads(raw_data.decode('utf-8', errors='replace'))
        except Exception as exc:
            log_exception(__name__, exc, extra={'provider': self.provider, 'query': query_text})
            raise ShopLogoSearchError('Die Logo-Such-API hat eine ungueltige Antwort geliefert.') from exc

        results = self._extract_provider_results(payload)
        logging.info(
            'shop_logo_search_api_response: provider=%s, query=%s, result_count=%s',
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

    def _normalize_provider_row(self, row, resolved_context, query_entry, original_rank):
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
            'title': title or source_domain or resolved_context.get('canonical_shop_name', '') or 'Shoplogo',
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
            'canonical_shop_name': str(resolved_context.get('canonical_shop_name', '') or '').strip(),
            'sender_domain': str(resolved_context.get('sender_domain', '') or '').strip(),
            'shop_lookup_meta': dict(resolved_context),
            'score_details': {},
        }

    def _rank_candidates(self, candidates, resolved_context):
        ranked = []
        canonical_shop_name = str(resolved_context.get('canonical_shop_name', '') or '').strip().lower()
        sender_domain = str(resolved_context.get('sender_domain', '') or '').strip().lower()
        domain_stub = self._domain_stub(sender_domain)

        for candidate in candidates:
            row = dict(candidate or {})
            title = str(row.get('title', '') or '').strip()
            snippet = str(row.get('snippet', '') or '').strip()
            source_domain = str(row.get('source_domain', '') or '').strip().lower()
            search_text = ' '.join(part for part in (title, snippet, source_domain) if part).lower()

            score = 0.0
            details = {}

            if canonical_shop_name and canonical_shop_name in search_text:
                score += 3.2
                details['canonical_shop_name'] = 3.2
            elif domain_stub and domain_stub in search_text:
                score += 2.0
                details['domain_stub'] = 2.0

            if sender_domain and sender_domain in search_text:
                score += 1.8
                details['sender_domain'] = 1.8

            positive_hits = sum(1 for term in self.POSITIVE_TEXT_TERMS if term in search_text)
            if positive_hits:
                delta = min(1.5, positive_hits * 0.6)
                score += delta
                details['positive_text'] = round(delta, 2)

            width = self._to_int(row.get('width'))
            height = self._to_int(row.get('height'))
            if width >= 120 and height >= 40:
                score += 0.6
                details['image_size'] = 0.6
            elif width > 0 and height > 0 and min(width, height) < 24:
                score -= 1.0
                details['image_size'] = -1.0

            if width > 0 and height > 0:
                smaller = max(1.0, min(width, height))
                ratio = max(width, height) / smaller
                if ratio <= 3.2:
                    score += 0.5
                    details['aspect_ratio'] = 0.5
                elif ratio >= 7.5:
                    score -= 0.8
                    details['aspect_ratio'] = -0.8
                if max(width, height) <= 32:
                    score -= 1.1
                    details['tiny_favicon_like'] = -1.1

            image_url = str(row.get('image_url', '') or '').strip().lower()
            if image_url.endswith(('.svg', '.png', '.webp')):
                score += 0.4
                details['logo_friendly_format'] = 0.4

            negative_hits = sum(1 for term in self.NEGATIVE_TEXT_TERMS if term in search_text)
            if negative_hits:
                delta = min(3.5, negative_hits * 0.8)
                score -= delta
                details['negative_text'] = -round(delta, 2)

            if any(term in source_domain for term in self.NEGATIVE_DOMAIN_TERMS):
                score -= 1.0
                details['negative_domain'] = -1.0
            if any(term in source_domain for term in self.POSITIVE_DOMAIN_TERMS):
                score += 0.4
                details['positive_domain'] = 0.4

            query_kind = str(row.get('query_kind', '') or '').strip().lower()
            if query_kind == 'strict':
                score += 0.7
                details['query_kind'] = 0.7
            elif query_kind == 'domain_logo':
                score += 0.5
                details['query_kind'] = 0.5

            confidence = max(0.01, min(0.99, round((score + 4.0) / 10.0, 3)))
            row['confidence'] = confidence
            row['ranking_score'] = round(score, 3)
            row['score_details'] = details
            ranked.append(row)

        ranked.sort(
            key=lambda item: (
                -float(item.get('ranking_score', 0.0) or 0.0),
                -float(item.get('confidence', 0.0) or 0.0),
                int(item.get('original_rank', 999) or 999),
            )
        )
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

    def _domain_stub(self, sender_domain):
        text = str(sender_domain or '').strip().lower()
        if not text:
            return ''
        return text.split('.', 1)[0]

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
        text = str(value or '').strip()
        if not text:
            return ''
        if text.startswith('//'):
            return f'https:{text}'
        if text.startswith(('http://', 'https://')):
            return text
        return ''

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
