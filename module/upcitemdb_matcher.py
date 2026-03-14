"""
upcitemdb_matcher.py
Bewertet UPCitemdb-Treffer gegen die normalisierten Suchinformationen.
"""

import difflib

from module.upcitemdb_normalizer import ProductNameNormalizer


class UpcitemdbMatcher:
    def __init__(self, normalizer=None):
        self.normalizer = normalizer or ProductNameNormalizer()

    def score_api_item(self, normalized_query, raw_item, query_kind="", query_text=""):
        title = str(raw_item.get("title", "") or "").strip()
        brand = str(raw_item.get("brand", "") or "").strip()
        model = str(raw_item.get("model", "") or "").strip()
        category = self._extract_category(raw_item)
        compare_text = " ".join(part for part in [brand, title, model, category] if part)
        item_norm = self.normalizer.normalize_for_upcitemdb(compare_text)

        score = 0.0
        reasons = []

        brand_score = self._score_brand(normalized_query.brand, brand or item_norm.brand, title)
        score += brand_score
        if brand_score > 0:
            reasons.append("brand")

        model_score = self._score_model(normalized_query.model_code, model, title, item_norm.product_family)
        score += model_score
        if model_score > 0:
            reasons.append("model")

        family_basis = normalized_query.product_family or normalized_query.cleaned_name
        item_family_basis = item_norm.product_family or item_norm.cleaned_name or title
        family_ratio = self._ratio(family_basis, item_family_basis)
        score += 0.22 * family_ratio
        if family_ratio >= 0.55:
            reasons.append("title")

        title_ratio = self._ratio(normalized_query.english_core_query or normalized_query.cleaned_name, item_norm.cleaned_name or title)
        score += 0.18 * title_ratio

        color_score = self._score_exact_attr(normalized_query.color, item_norm.color or title, hit_bonus=0.08, miss_penalty=-0.03)
        score += color_score
        if color_score > 0:
            reasons.append("color")

        size_score = self._score_exact_attr(normalized_query.size_or_capacity, item_norm.size_or_capacity or title, hit_bonus=0.10, miss_penalty=-0.04)
        score += size_score
        if size_score > 0:
            reasons.append("capacity")

        category_score = self._score_exact_attr(normalized_query.category_hint, item_norm.category_hint or category or title, hit_bonus=0.08, miss_penalty=-0.03)
        score += category_score
        if category_score > 0:
            reasons.append("category")

        platform_score = self._score_exact_attr(normalized_query.platform, item_norm.platform or title, hit_bonus=0.06, miss_penalty=-0.02)
        score += platform_score
        if platform_score > 0:
            reasons.append("platform")

        edition_score = self._score_exact_attr(normalized_query.edition, item_norm.edition or title, hit_bonus=0.06, miss_penalty=-0.02)
        score += edition_score
        if edition_score > 0:
            reasons.append("edition")

        if query_kind == "brand_model" and model_score > 0.20:
            score += 0.06
        elif query_kind == "brand_family" and family_ratio > 0.60:
            score += 0.04
        elif query_kind == "core_en" and title_ratio > 0.60:
            score += 0.03

        if query_text and self._contains_compact(title, query_text):
            score += 0.02

        score = max(0.0, min(round(score, 4), 0.99))
        return {
            "score": score,
            "reasons": reasons,
            "item_normalized": item_norm.to_dict(),
        }

    def _extract_category(self, raw_item):
        for key in ("category", "category_name"):
            value = raw_item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, list):
                parts = [str(x).strip() for x in value if str(x).strip()]
                if parts:
                    return " ".join(parts)
        return ""

    def _score_brand(self, expected_brand, item_brand, title):
        if not expected_brand:
            return 0.0
        expected = self.normalizer.compact_token(expected_brand)
        item_token = self.normalizer.compact_token(item_brand)
        title_token = self.normalizer.compact_token(title)

        if item_token and item_token == expected:
            return 0.30
        if expected and expected in title_token:
            return 0.25
        if item_token and item_token != expected:
            return -0.12
        return 0.0

    def _score_model(self, expected_model, item_model, title, item_family):
        if not expected_model:
            return 0.0
        expected = self.normalizer.compact_token(expected_model)
        if not expected:
            return 0.0

        for candidate in (item_model, title, item_family):
            compact = self.normalizer.compact_token(candidate)
            if not compact:
                continue
            if compact == expected:
                return 0.35
            if expected in compact or compact in expected:
                return 0.24

        token_hits = 0
        expected_parts = [self.normalizer.compact_token(x) for x in str(expected_model).split() if self.normalizer.compact_token(x)]
        compare_parts = [self.normalizer.compact_token(x) for x in f"{item_model} {title}".split() if self.normalizer.compact_token(x)]
        for part in expected_parts:
            if part in compare_parts:
                token_hits += 1
        if expected_parts and token_hits:
            return min(0.28, 0.10 + (token_hits / len(expected_parts)) * 0.18)
        return 0.0

    def _score_exact_attr(self, expected_value, compare_value, hit_bonus=0.1, miss_penalty=0.0):
        expected = self.normalizer.compact_token(expected_value)
        compare = self.normalizer.compact_token(compare_value)
        if not expected:
            return 0.0
        if expected and expected in compare:
            return hit_bonus
        if compare:
            return miss_penalty
        return 0.0

    def _ratio(self, left, right):
        left_txt = str(left or "").strip().lower()
        right_txt = str(right or "").strip().lower()
        if not left_txt or not right_txt:
            return 0.0
        return difflib.SequenceMatcher(None, left_txt, right_txt).ratio()

    def _contains_compact(self, text, fragment):
        compact_text = self.normalizer.compact_token(text)
        compact_fragment = self.normalizer.compact_token(fragment)
        if not compact_text or not compact_fragment:
            return False
        return compact_fragment in compact_text
