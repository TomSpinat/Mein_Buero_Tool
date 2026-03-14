"""
upcitemdb_normalizer.py
Bereitet chaotische Produktnamen fuer die UPCitemdb-Suche auf.

Die Logik ist bewusst heuristisch und kontrolliert:
- keine freie Uebersetzung
- nur kleine Mapping-Listen
- mehrere priorisierte Query-Varianten statt nur ein Suchstring
"""

from dataclasses import dataclass, field
import re
import unicodedata


@dataclass
class QueryVariant:
    kind: str
    language: str
    query: str
    priority: int


@dataclass
class NormalizedProductQuery:
    raw_name: str
    cleaned_name: str
    brand: str = ""
    model_code: str = ""
    product_family: str = ""
    category_hint: str = ""
    color: str = ""
    size_or_capacity: str = ""
    pack_size: str = ""
    platform: str = ""
    edition: str = ""
    language_hint: str = "neutral"
    english_core_query: str = ""
    german_core_query: str = ""
    mixed_query: str = ""
    search_queries: list[str] = field(default_factory=list)
    query_variants: list[dict] = field(default_factory=list)

    def to_dict(self):
        return {
            "raw_name": self.raw_name,
            "cleaned_name": self.cleaned_name,
            "brand": self.brand,
            "model_code": self.model_code,
            "product_family": self.product_family,
            "category_hint": self.category_hint,
            "color": self.color,
            "size_or_capacity": self.size_or_capacity,
            "pack_size": self.pack_size,
            "platform": self.platform,
            "edition": self.edition,
            "language_hint": self.language_hint,
            "english_core_query": self.english_core_query,
            "german_core_query": self.german_core_query,
            "mixed_query": self.mixed_query,
            "search_queries": list(self.search_queries),
            "query_variants": [dict(x) for x in self.query_variants],
        }


class ProductNameNormalizer:
    TOKEN_MAP_EN = {
        "weiss": "white",
        "weib": "white",
        "weisses": "white",
        "weis": "white",
        "weisser": "white",
        "weissfarben": "white",
        "schwarz": "black",
        "schwarze": "black",
        "schwarzer": "black",
        "graphit": "graphite",
        "grau": "gray",
        "graues": "gray",
        "silber": "silver",
        "blau": "blue",
        "rot": "red",
        "gruen": "green",
        "grun": "green",
        "kabellos": "wireless",
        "kabellose": "wireless",
        "drahtlos": "wireless",
        "kopfhoerer": "headphones",
        "maus": "mouse",
        "ladecase": "charging case",
        "ladegehause": "charging case",
        "ladegehaeuse": "charging case",
        "festplatte": "hard drive",
        "speicher": "storage",
        "speicherplatz": "storage",
        "stueck": "pack",
        "stuck": "pack",
        "gen": "gen",
        "generation": "gen",
    }

    TOKEN_MAP_DE = {
        "white": "weiss",
        "black": "schwarz",
        "graphite": "graphit",
        "gray": "grau",
        "silver": "silber",
        "blue": "blau",
        "red": "rot",
        "green": "gruen",
        "wireless": "kabellos",
        "headphones": "kopfhoerer",
        "mouse": "maus",
        "charging": "lade",
        "case": "case",
        "hard": "fest",
        "drive": "platte",
        "storage": "speicher",
    }

    BRAND_RULES = [
        (("sony", "playstation", "dualsense"), "Sony"),
        (("nintendo", "switch"), "Nintendo"),
        (("apple", "airpods", "magsafe"), "Apple"),
        (("samsung", "galaxy", "990 pro"), "Samsung"),
        (("logitech", "mx master"), "Logitech"),
        (("microsoft", "xbox"), "Microsoft"),
        (("western digital", "wd"), "Western Digital"),
        (("sandisk",), "SanDisk"),
        (("seagate",), "Seagate"),
        (("razer",), "Razer"),
        (("corsair",), "Corsair"),
        (("steelseries",), "SteelSeries"),
        (("asus",), "ASUS"),
        (("acer",), "Acer"),
        (("lenovo",), "Lenovo"),
        (("hp", "hewlett packard"), "HP"),
        (("bose",), "Bose"),
        (("anker",), "Anker"),
        (("jbl",), "JBL"),
    ]

    FAMILY_RULES = [
        (re.compile(r"\bmx\s+master\s+3s\b", re.I), {"family": "MX Master 3S", "brand": "Logitech", "category": "mouse", "model_code": "MX Master 3S"}),
        (re.compile(r"\bairpods\s+pro\b", re.I), {"family": "AirPods Pro", "brand": "Apple", "category": "headphones", "model_code": "AirPods Pro"}),
        (re.compile(r"\bdualsense\b", re.I), {"family": "DualSense Controller", "brand": "Sony", "category": "controller", "model_code": "DualSense"}),
        (re.compile(r"\b990\s*pro\b", re.I), {"family": "990 PRO", "brand": "Samsung", "category": "ssd", "model_code": "990 PRO"}),
        (re.compile(r"\bpro\s+controller\b", re.I), {"family": "Pro Controller", "category": "controller", "model_code": "Pro Controller"}),
        (re.compile(r"\bwireless\s+controller\b", re.I), {"family": "Wireless Controller", "category": "controller"}),
    ]

    CATEGORY_RULES = [
        (("controller", "gamepad"), "controller"),
        (("mouse",), "mouse"),
        (("headphones", "earbuds", "headset", "airpods"), "headphones"),
        (("ssd", "nvme", "m.2"), "ssd"),
        (("hard drive", "hdd"), "hard drive"),
        (("keyboard",), "keyboard"),
        (("case", "charging case"), "case"),
        (("console",), "console"),
    ]

    PLATFORM_RULES = [
        (re.compile(r"\bplaystation\s*5\b|\bps5\b", re.I), "PS5"),
        (re.compile(r"\bnintendo\s+switch\b|\bswitch\b", re.I), "Nintendo Switch"),
        (re.compile(r"\bxbox\s+series\s+x\b", re.I), "Xbox Series X"),
        (re.compile(r"\bxbox\s+series\s+s\b", re.I), "Xbox Series S"),
        (re.compile(r"\bxbox\b", re.I), "Xbox"),
        (re.compile(r"\bpc\b", re.I), "PC"),
        (re.compile(r"\bmac\b|\bmacbook\b", re.I), "Mac"),
    ]

    COLOR_RULES = [
        (re.compile(r"\bwhite\b|\bweiss\b|\bweisses\b|\bweis\b|\bweib\b", re.I), ("white", "weiss")),
        (re.compile(r"\bblack\b|\bschwarz\b|\bschwarze\b|\bschwarzer\b", re.I), ("black", "schwarz")),
        (re.compile(r"\bgraphite\b|\bgraphit\b", re.I), ("graphite", "graphit")),
        (re.compile(r"\bgray\b|\bgrey\b|\bgrau\b", re.I), ("gray", "grau")),
        (re.compile(r"\bsilver\b|\bsilber\b", re.I), ("silver", "silber")),
        (re.compile(r"\bblue\b|\bblau\b", re.I), ("blue", "blau")),
        (re.compile(r"\bred\b|\brot\b", re.I), ("red", "rot")),
    ]

    EDITION_RULES = [
        (re.compile(r"\b2\.\s*gen\b|\b2nd\s+gen\b|\b2nd\s+generation\b", re.I), ("2nd Gen", "2. Gen")),
        (re.compile(r"\b3\.\s*gen\b|\b3rd\s+gen\b|\b3rd\s+generation\b", re.I), ("3rd Gen", "3. Gen")),
        (re.compile(r"\bv\s*2\b|\bv2\b", re.I), ("V2", "V2")),
        (re.compile(r"\blimited\s+edition\b", re.I), ("Limited Edition", "Limited Edition")),
        (re.compile(r"\bspecial\s+edition\b", re.I), ("Special Edition", "Special Edition")),
    ]

    NOISE_PATTERNS = [
        re.compile(r"\b(rechnung|invoice|lieferschein|versand|shipping|lieferung|zustellung)\b", re.I),
        re.compile(r"\b(inkl\.?\s*mwst|inklusive\s*mwst|vat\s*included|incl\.?\s*vat)\b", re.I),
        re.compile(r"\b(menge|qty|quantity)\b", re.I),
        re.compile(r"\b(sofort\s+lieferbar|prime|fulfilled\s+by|verkauf\s+durch|sold\s+by)\b", re.I),
        re.compile(r"\b(bestellnummer|bestellnr|order\s*number|order\s*no|auftragsnummer|auftragsnr)\b[:#\s-]*[a-z0-9_-]+", re.I),
        re.compile(r"\b(sku|artikelnummer|article\s*number)\b[:#\s-]*[a-z0-9_-]+", re.I),
        re.compile(r"\b(neu|brandneu|lagernd|sofort)\b", re.I),
    ]

    STOPWORDS = {
        "mit", "und", "the", "for", "fuer", "fur", "inkl", "inklusive", "mitgeliefert",
        "nur", "of", "by", "von", "der", "die", "das", "ein", "eine", "new",
        "usb", "model", "modell", "original", "neuware",
    }

    CAPACITY_PATTERNS = [
        re.compile(r"\b\d+\s?(tb|gb|mb)\b", re.I),
        re.compile(r"\b\d+\s?(mah|wh|w)\b", re.I),
        re.compile(r"\b\d+\s?(cm|mm|inch|in)\b", re.I),
    ]

    PACK_PATTERNS = [
        re.compile(r"\b(\d+)\s?(stk|stueck|stuck|pcs|pieces|pack)\b", re.I),
        re.compile(r"\bpack\s+of\s+(\d+)\b", re.I),
    ]

    MODEL_TOKEN_PATTERNS = [
        re.compile(r"^[a-z]{1,4}\d+[a-z0-9-]*$", re.I),
        re.compile(r"^\d+[a-z]{1,4}$", re.I),
        re.compile(r"^v\d+$", re.I),
        re.compile(r"^usb-c$", re.I),
        re.compile(r"^m\.?2$", re.I),
        re.compile(r"^nvme$", re.I),
        re.compile(r"^\d{2,4}$", re.I),
        re.compile(r"^\d+[a-z]$", re.I),
    ]

    SPECIAL_TOKEN_CASE = {
        "ps5": "PS5",
        "nvme": "NVMe",
        "m.2": "M.2",
        "usb-c": "USB-C",
        "ssd": "SSD",
        "hdd": "HDD",
        "tb": "TB",
        "gb": "GB",
        "v2": "V2",
    }

    def normalize_for_upcitemdb(self, name: str, varianten_info: str = "") -> NormalizedProductQuery:
        raw_name = " ".join(part for part in [str(name or "").strip(), str(varianten_info or "").strip()] if part).strip()
        folded = self._fold(raw_name)
        base_text = self._normalize_base_text(folded)
        neutral_text = self._strip_noise(base_text)
        neutral_text = self._collapse_spaces(neutral_text)

        color_en, color_de = self._detect_color(neutral_text)
        size_or_capacity = self._extract_first(neutral_text, self.CAPACITY_PATTERNS)
        pack_size = self._extract_pack_size(neutral_text)
        platform = self._detect_platform(neutral_text)
        edition_en, edition_de = self._detect_edition(neutral_text)
        family_info = self._detect_family(neutral_text)
        brand = family_info.get("brand", "") or self._detect_brand(neutral_text)
        category_hint = family_info.get("category", "") or self._detect_category(neutral_text)
        product_family = family_info.get("family", "") or self._build_family_fallback(neutral_text, brand, category_hint, platform)
        model_code = family_info.get("model_code", "") or self._extract_model_code(neutral_text, brand, product_family, size_or_capacity, platform)

        language_hint = self._detect_language_hint(neutral_text)
        cleaned_name = self._build_cleaned_name(
            brand=brand,
            product_family=product_family,
            category_hint=category_hint,
            platform=platform,
            edition=edition_en,
            size_or_capacity=size_or_capacity,
            color=color_en,
            pack_size=pack_size,
            fallback_text=neutral_text,
        )

        english_core_query = self._build_query(
            brand=brand,
            product_family=product_family,
            category_hint=category_hint,
            platform=platform,
            edition=edition_en,
            size_or_capacity=size_or_capacity,
            color=color_en,
            pack_size=pack_size,
            fallback=cleaned_name,
        )

        german_core_query = self._build_query(
            brand=brand,
            product_family=self._family_to_german(product_family),
            category_hint=self._category_to_german(category_hint),
            platform=platform,
            edition=edition_de,
            size_or_capacity=size_or_capacity,
            color=color_de,
            pack_size=pack_size,
            fallback=self._to_german_fallback(cleaned_name),
        )

        mixed_query = self._build_query(
            brand=brand,
            product_family=product_family or self._first_tokens(neutral_text, 4),
            category_hint=category_hint,
            platform=platform,
            edition=edition_en or edition_de,
            size_or_capacity=size_or_capacity,
            color=color_en or color_de,
            pack_size=pack_size,
            fallback=neutral_text,
        )

        query_variants = self._build_query_variants(
            raw_name=raw_name,
            cleaned_name=cleaned_name,
            brand=brand,
            model_code=model_code,
            product_family=product_family,
            category_hint=category_hint,
            platform=platform,
            edition_en=edition_en,
            edition_de=edition_de,
            size_or_capacity=size_or_capacity,
            color_en=color_en,
            color_de=color_de,
            pack_size=pack_size,
            english_core_query=english_core_query,
            german_core_query=german_core_query,
            mixed_query=mixed_query,
            language_hint=language_hint,
        )

        search_queries = [q["query"] for q in query_variants]

        return NormalizedProductQuery(
            raw_name=raw_name,
            cleaned_name=cleaned_name,
            brand=brand,
            model_code=model_code,
            product_family=product_family,
            category_hint=category_hint,
            color=color_en,
            size_or_capacity=size_or_capacity,
            pack_size=pack_size,
            platform=platform,
            edition=edition_en,
            language_hint=language_hint,
            english_core_query=english_core_query,
            german_core_query=german_core_query,
            mixed_query=mixed_query,
            search_queries=search_queries,
            query_variants=query_variants,
        )

    def compact_token(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", self._fold(value).lower())

    def _fold(self, value: str) -> str:
        value = str(value or "")
        normalized = unicodedata.normalize("NFKD", value)
        return normalized.encode("ascii", "ignore").decode("ascii")

    def _normalize_base_text(self, text: str) -> str:
        text = text.replace("&", " and ")
        text = text.replace("/", " / ")
        text = text.replace("_", " ")
        text = re.sub(r"(?i)\bplaystation\s*5\b", " PS5 ", text)
        text = re.sub(r"(?i)\bnintendo\s+switch\b", " Nintendo Switch ", text)
        text = re.sub(r"(?i)\busb\s*c\b", " USB-C ", text)
        text = re.sub(r"(?i)\bm\s*\.?\s*2\b", " M.2 ", text)
        text = re.sub(r"[\[\]\(\)\{\}|,;:]+", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip().lower()

    def _strip_noise(self, text: str) -> str:
        clean = text
        for pattern in self.NOISE_PATTERNS:
            clean = pattern.sub(" ", clean)
        clean = re.sub(r"\s+", " ", clean)
        return clean.strip()

    def _collapse_spaces(self, value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    def _extract_first(self, text: str, patterns: list) -> str:
        for pattern in patterns:
            match = pattern.search(text)
            if match:
                return self._pretty_query_text(match.group(0))
        return ""

    def _extract_pack_size(self, text: str) -> str:
        for pattern in self.PACK_PATTERNS:
            match = pattern.search(text)
            if not match:
                continue
            count = match.group(1)
            return f"{count} Pack"
        return ""

    def _detect_brand(self, text: str) -> str:
        compact_text = self._collapse_spaces(text)
        for aliases, canonical in self.BRAND_RULES:
            for alias in aliases:
                if alias in compact_text:
                    return canonical
        return ""

    def _detect_family(self, text: str) -> dict:
        for pattern, info in self.FAMILY_RULES:
            if pattern.search(text):
                return dict(info)
        return {}

    def _detect_category(self, text: str) -> str:
        for aliases, canonical in self.CATEGORY_RULES:
            for alias in aliases:
                if alias in text:
                    return canonical
        return ""

    def _detect_platform(self, text: str) -> str:
        for pattern, canonical in self.PLATFORM_RULES:
            if pattern.search(text):
                return canonical
        return ""

    def _detect_color(self, text: str) -> tuple[str, str]:
        for pattern, values in self.COLOR_RULES:
            if pattern.search(text):
                return values
        return "", ""

    def _detect_edition(self, text: str) -> tuple[str, str]:
        for pattern, values in self.EDITION_RULES:
            if pattern.search(text):
                return values
        return "", ""

    def _detect_language_hint(self, text: str) -> str:
        german_signals = 0
        english_signals = 0

        for token in ("weiss", "schwarz", "kabellos", "maus", "ladecase", "kopfhoerer", "mit", "fuer", "versand"):
            if token in text:
                german_signals += 1

        for token in ("white", "black", "wireless", "mouse", "charging", "case", "controller", "shipping"):
            if token in text:
                english_signals += 1

        if german_signals > 0 and english_signals > 0:
            return "mixed"
        if german_signals > 0:
            return "de"
        if english_signals > 0:
            return "en"
        return "neutral"

    def _build_family_fallback(self, text: str, brand: str, category_hint: str, platform: str) -> str:
        translated = self._translate_text_tokens(text, target_language="en")
        tokens = translated.split()
        ignored = {
            self.compact_token(brand),
            self.compact_token(category_hint),
            self.compact_token(platform),
        }
        family_tokens = []
        for token in tokens:
            compact = self.compact_token(token)
            if not compact or compact in ignored:
                continue
            if token in ("wireless", "charging", "case", "pack"):
                continue
            if token in self.STOPWORDS:
                continue
            family_tokens.append(self._pretty_token(token))
            if len(family_tokens) >= 4:
                break
        return " ".join(family_tokens)

    def _extract_model_code(self, text: str, brand: str, product_family: str, size_or_capacity: str, platform: str) -> str:
        if product_family and any(ch.isdigit() for ch in product_family):
            return product_family

        if product_family and self.compact_token(product_family) in {
            self.compact_token("DualSense"),
            self.compact_token("AirPods Pro"),
            self.compact_token("Pro Controller"),
        }:
            return product_family

        tokens = self._translate_text_tokens(text, target_language="en").split()
        ignored = {
            self.compact_token(brand),
            self.compact_token(product_family),
            self.compact_token(size_or_capacity),
            self.compact_token(platform),
        }

        picked = []
        for token in tokens:
            compact = self.compact_token(token)
            if not compact or compact in ignored:
                continue
            if any(pattern.match(token) for pattern in self.MODEL_TOKEN_PATTERNS):
                picked.append(self._pretty_token(token))
            if len(picked) >= 3:
                break
        return " ".join(picked)

    def _translate_text_tokens(self, text: str, target_language: str) -> str:
        mapping = self.TOKEN_MAP_EN if target_language == "en" else self.TOKEN_MAP_DE
        parts = []
        for token in re.findall(r"[a-z0-9.+-]+", text):
            mapped = mapping.get(token, token)
            parts.extend(str(mapped).split())
        return self._collapse_spaces(" ".join(parts))

    def _build_cleaned_name(
        self,
        brand: str,
        product_family: str,
        category_hint: str,
        platform: str,
        edition: str,
        size_or_capacity: str,
        color: str,
        pack_size: str,
        fallback_text: str,
    ) -> str:
        query = self._build_query(
            brand=brand,
            product_family=product_family,
            category_hint=category_hint,
            platform=platform,
            edition=edition,
            size_or_capacity=size_or_capacity,
            color=color,
            pack_size=pack_size,
            fallback=self._translate_text_tokens(fallback_text, target_language="en"),
        )
        return query

    def _build_query(
        self,
        brand: str = "",
        product_family: str = "",
        category_hint: str = "",
        platform: str = "",
        edition: str = "",
        size_or_capacity: str = "",
        color: str = "",
        pack_size: str = "",
        fallback: str = "",
    ) -> str:
        parts = []
        for part in (brand, product_family, category_hint, platform, edition, size_or_capacity, color, pack_size):
            txt = self._pretty_query_text(part)
            if txt:
                parts.append(txt)

        if not parts:
            return self._pretty_query_text(fallback)

        query = self._dedupe_phrase_parts(parts)
        return self._collapse_spaces(query)

    def _build_query_variants(
        self,
        raw_name: str,
        cleaned_name: str,
        brand: str,
        model_code: str,
        product_family: str,
        category_hint: str,
        platform: str,
        edition_en: str,
        edition_de: str,
        size_or_capacity: str,
        color_en: str,
        color_de: str,
        pack_size: str,
        english_core_query: str,
        german_core_query: str,
        mixed_query: str,
        language_hint: str,
    ) -> list[dict]:
        variants = []

        if brand and model_code:
            variants.append(QueryVariant("brand_model", "neutral", self._build_query(brand=brand, product_family=model_code), 1))

        family_query = self._build_query(
            brand=brand,
            product_family=product_family or model_code,
            category_hint="" if product_family else category_hint,
            platform=platform if platform and self.compact_token(platform) not in self.compact_token(product_family or "") else "",
            edition=edition_en,
            size_or_capacity=size_or_capacity,
            color=color_en,
            pack_size=pack_size,
            fallback=cleaned_name,
        )
        if family_query:
            variants.append(QueryVariant("brand_family", "en", family_query, 2))

        if english_core_query:
            variants.append(QueryVariant("core_en", "en", english_core_query, 3))

        if language_hint in ("de", "mixed") and german_core_query:
            variants.append(QueryVariant("core_de", "de", german_core_query, 4))

        if language_hint == "mixed" and mixed_query:
            variants.append(QueryVariant("core_mixed", "mixed", mixed_query, 5))

        if cleaned_name:
            variants.append(QueryVariant("fallback_cleaned", "neutral", cleaned_name, 6))

        if raw_name:
            variants.append(QueryVariant("fallback_raw", "neutral", self._pretty_query_text(raw_name), 7))

        deduped = []
        seen = set()
        for variant in variants:
            query = self._collapse_spaces(variant.query)
            if not query:
                continue
            key = query.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append({
                "kind": variant.kind,
                "language": variant.language,
                "query": query,
                "priority": variant.priority,
            })
        return deduped

    def _pretty_query_text(self, value: str) -> str:
        tokens = re.findall(r"[A-Za-z0-9.+-]+", str(value or ""))
        return " ".join(self._pretty_token(token) for token in tokens if token)

    def _pretty_token(self, token: str) -> str:
        raw = str(token or "").strip()
        if not raw:
            return ""

        lower = raw.lower()
        if lower in self.SPECIAL_TOKEN_CASE:
            return self.SPECIAL_TOKEN_CASE[lower]

        if re.match(r"^\d+\s?(tb|gb|mb)$", lower):
            number = re.findall(r"\d+", lower)[0]
            unit = re.findall(r"(tb|gb|mb)$", lower)[0].upper()
            return f"{number}{unit}"

        if re.match(r"^\d+(st|nd|rd|th)$", lower):
            return lower[:-2] + lower[-2:]

        if raw.isupper():
            return raw
        if any(ch.isdigit() for ch in raw):
            return raw.upper() if len(raw) <= 4 else raw
        return raw.title()

    def _dedupe_phrase_parts(self, parts: list[str]) -> str:
        tokens = []
        seen = set()
        for part in parts:
            for token in str(part).split():
                compact = self.compact_token(token)
                if not compact or compact in seen:
                    continue
                seen.add(compact)
                tokens.append(token)
        return " ".join(tokens)

    def _category_to_german(self, category_hint: str) -> str:
        mapping = {
            "controller": "Controller",
            "mouse": "Maus",
            "headphones": "Kopfhoerer",
            "ssd": "SSD",
            "hard drive": "Festplatte",
            "keyboard": "Tastatur",
            "case": "Case",
            "console": "Konsole",
        }
        return mapping.get(str(category_hint or "").strip().lower(), category_hint)

    def _family_to_german(self, product_family: str) -> str:
        family = str(product_family or "").strip()
        if not family:
            return ""
        replacements = {
            "Wireless": "Wireless",
            "Controller": "Controller",
            "Charging": "Lade",
            "Case": "Case",
            "Mouse": "Maus",
        }
        parts = []
        for token in family.split():
            parts.append(replacements.get(token, token))
        return " ".join(parts)

    def _to_german_fallback(self, cleaned_name: str) -> str:
        pieces = []
        for token in self._fold(cleaned_name).lower().split():
            mapped = self.TOKEN_MAP_DE.get(token, token)
            pieces.extend(str(mapped).split())
        return self._pretty_query_text(" ".join(pieces))

    def _first_tokens(self, text: str, count: int) -> str:
        tokens = re.findall(r"[a-z0-9.+-]+", text)
        return self._pretty_query_text(" ".join(tokens[:count]))
