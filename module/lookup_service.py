"""Zentraler LookupService – einzige Quelle der Wahrheit fuer alle Feld-Lookups.

Wird von Modul 1, Modul 2 und dem Mail-Wizard identisch verwendet.
Der Service fuehrt Lookups in fester Reihenfolge aus:
    1. mapping.json (lokal, synchron)
    2. Lokale Datenbank (shop_logo_links, ean_katalog, product_image_links …)
    3. Externe API (Brave, UPCitemdb) – nur wenn DB-Lookup leer

Wichtige Designregeln:
    - EAN-Bild aus ean_katalog.bild_url wird BEWUSST IGNORIERT (oft unpassend).
    - Produktbilder kommen ausschliesslich aus product_image_links → media_assets.
    - EAN-Lookups liefern IMMER needs_confirm=True (User muss bestaetigen).
    - Der Service enthaelt KEINE UI-Logik. Dialoge werden ueber Callbacks ausgeloest.
"""

from __future__ import annotations

import logging
import os
import re
from typing import TYPE_CHECKING, Callable, Optional

from module.lookup_results import (
    FieldState,
    FieldType,
    LookupResult,
    LookupSource,
)
from module.normalization_dialog import load_mapping, resolve_known_mapping
from module.amazon_country_dialog import is_generic_amazon_shop
from module.media.media_keys import build_shop_key, build_product_key
from module.crash_logger import log_exception, log_message

if TYPE_CHECKING:
    from module.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Amazon-Pattern fuer shop_name Erkennung
# ---------------------------------------------------------------------------
_AMAZON_PATTERN = re.compile(r"\bamazon\b", re.IGNORECASE)


def _is_amazon(text: str) -> bool:
    """Prueft ob ein Shop-Name Amazon ist (generisch, nicht schon spezifisch)."""
    return bool(_AMAZON_PATTERN.search(text)) and is_generic_amazon_shop(text)


def _normalize_shop_key(name: str) -> str:
    """Erzeugt einen normalisierten shop_key aus einem Shop-Namen.

    'Amazon DE' → 'amazon_de', 'Notebooksbilliger.de' → 'notebooksbilliger_de'
    """
    key = str(name or "").strip().lower()
    key = re.sub(r"[^a-z0-9]+", "_", key)
    key = key.strip("_")
    return key


class LookupService:
    """Zentraler Lookup-Orchestrator fuer alle Eingabefeld-Typen.

    Instanzierung: einmal pro App-Session, wird an alle Module weitergereicht.

    Args:
        db: DatabaseManager-Instanz (hat alle Mixin-Methoden).
    """

    def __init__(self, db: "DatabaseManager"):
        self.db = db

    # ===================================================================
    #  SHOP-NAME LOOKUP
    # ===================================================================

    def lookup_shop(
        self,
        shop_name: str,
        sender_domain: str = "",
    ) -> LookupResult:
        """Vollstaendige Lookup-Chain fuer ein shop_name-Feld.

        Reihenfolge:
            1. Amazon-Pattern erkennen → needs_confirm (AmazonCountryDialog)
            2. mapping.json pruefen → normalisierter Wert
            3. shop_logo_links via shop_key (exakt)
            4. shop_logo_links via shop_name (LIKE-Fallback)
            5. shop_logo_links via sender_domain (nur wenn vorhanden)

        Gibt ein LookupResult zurueck. Wenn `needs_confirm=True` und
        `source=AMAZON_DIALOG`, muss die UI den AmazonCountryDialog oeffnen.
        """
        text = str(shop_name or "").strip()
        if not text:
            return LookupResult(
                state=FieldState.EMPTY,
                source=LookupSource.NONE,
                field_type=FieldType.SHOP_NAME,
            )

        # --- 1. Amazon-Sonderfall ---
        if _is_amazon(text):
            return LookupResult(
                state=FieldState.UNMAPPED,
                source=LookupSource.AMAZON_DIALOG,
                field_type=FieldType.SHOP_NAME,
                data={"raw_shop_name": text},
                needs_confirm=True,
                normalized_value="",
            )

        # --- 2. mapping.json ---
        mapped = resolve_known_mapping("shops", text)
        if mapped is not None:
            normalized = str(mapped).strip()
            logo_result = self._find_shop_logo(
                shop_key=build_shop_key(normalized),
                shop_name=normalized,
                sender_domain=sender_domain,
            )
            return LookupResult(
                state=FieldState.HIT_LOCAL,
                source=LookupSource.MAPPING_JSON,
                field_type=FieldType.SHOP_NAME,
                data={"raw_shop_name": text, "mapped_shop_name": normalized},
                confidence=1.0,
                needs_confirm=False,
                normalized_value=normalized,
                logo_path=logo_result.logo_path,
            )

        # --- 3+4+5. DB Logo-Suche (shop_key → shop_name → sender_domain) ---
        shop_key = build_shop_key(text, sender_domain)
        logo_result = self._find_shop_logo(
            shop_key=shop_key,
            shop_name=text,
            sender_domain=sender_domain,
        )

        if logo_result.found:
            return LookupResult(
                state=FieldState.HIT_LOCAL,
                source=logo_result.source,
                field_type=FieldType.SHOP_NAME,
                data={"raw_shop_name": text, "shop_key": shop_key},
                confidence=logo_result.confidence,
                needs_confirm=False,
                normalized_value=text,
                logo_path=logo_result.logo_path,
            )

        # --- Kein Treffer: Normalisierung noetig ---
        return LookupResult(
            state=FieldState.UNMAPPED,
            source=LookupSource.NORMALIZATION_DIALOG,
            field_type=FieldType.SHOP_NAME,
            data={"raw_shop_name": text, "shop_key": shop_key, "shop_name": text},
            needs_confirm=True,
            normalized_value="",
        )

    def _find_shop_logo(
        self,
        shop_key: str = "",
        shop_name: str = "",
        sender_domain: str = "",
    ) -> LookupResult:
        """Sucht ein Shop-Logo in der DB (4-stufig + shop_name-Fallback).

        Stufe 1: shop_key exakt (mit domain)
        Stufe 2: shop_key nur aus shop_name (ohne domain, Fallback)
        Stufe 3: sender_domain
        Stufe 4: shop_name exact match in shop_logo_links.shop_name (fuer aeltere Eintraege)
        """
        logger.debug(
            "_find_shop_logo: shop_key=%r shop_name=%r sender_domain=%r",
            shop_key, shop_name, sender_domain,
        )
        try:
            # Stufe 1: shop_key exakt (z.B. "shop:amazon-de")
            if shop_key:
                link_row = self.db.get_primary_shop_logo_link(shop_key=shop_key)
                logger.debug("Stufe1 shop_key=%r → link_row=%s", shop_key, bool(link_row))
                if link_row:
                    path = self._resolve_media_path(link_row.get("media_asset_id"))
                    logger.debug("Stufe1 path=%r", path)
                    if path:
                        return LookupResult(
                            state=FieldState.HIT_LOCAL,
                            source=LookupSource.DB_SHOP_KEY,
                            field_type=FieldType.SHOP_NAME,
                            confidence=float(link_row.get("confidence", 1.0) or 1.0),
                            logo_path=path,
                        )

            # Stufe 2: Nur shop_name ohne domain (Fallback wenn Logo ohne domain gespeichert)
            if shop_name:
                name_only_key = build_shop_key(shop_name)  # kein sender_domain
                if name_only_key and name_only_key != shop_key:
                    link_row = self.db.get_primary_shop_logo_link(shop_key=name_only_key)
                    logger.debug("Stufe2 name_only_key=%r → link_row=%s", name_only_key, bool(link_row))
                    if link_row:
                        path = self._resolve_media_path(link_row.get("media_asset_id"))
                        if path:
                            return LookupResult(
                                state=FieldState.HIT_LOCAL,
                                source=LookupSource.DB_SHOP_NAME,
                                field_type=FieldType.SHOP_NAME,
                                confidence=float(link_row.get("confidence", 1.0) or 1.0),
                                logo_path=path,
                            )

            # Stufe 3: sender_domain
            if sender_domain:
                domain_text = str(sender_domain).strip().lower()
                link_row = self.db.get_primary_shop_logo_link(sender_domain=domain_text)
                logger.debug("Stufe3 sender_domain=%r → link_row=%s", domain_text, bool(link_row))
                if link_row:
                    path = self._resolve_media_path(link_row.get("media_asset_id"))
                    if path:
                        return LookupResult(
                            state=FieldState.HIT_LOCAL,
                            source=LookupSource.DB_SENDER_DOMAIN,
                            field_type=FieldType.SHOP_NAME,
                            confidence=float(link_row.get("confidence", 1.0) or 1.0),
                            logo_path=path,
                        )

            # Stufe 4: shop_name exakt in shop_logo_links.shop_name (Fallback fuer aeltere Keys)
            if shop_name:
                link_row = self.db.get_primary_shop_logo_link_by_name(shop_name=shop_name)
                logger.debug("Stufe4 shop_name=%r → link_row=%s", shop_name, bool(link_row))
                if link_row:
                    path = self._resolve_media_path(link_row.get("media_asset_id"))
                    if path:
                        return LookupResult(
                            state=FieldState.HIT_LOCAL,
                            source=LookupSource.DB_SHOP_NAME,
                            field_type=FieldType.SHOP_NAME,
                            confidence=float(link_row.get("confidence", 1.0) or 1.0),
                            logo_path=path,
                        )

        except Exception as exc:
            log_exception(__name__, exc)

        logger.debug("_find_shop_logo: kein Treffer fuer shop_key=%r shop_name=%r", shop_key, shop_name)
        return LookupResult(
            state=FieldState.NOT_FOUND,
            source=LookupSource.NONE,
            field_type=FieldType.SHOP_NAME,
        )

    # ===================================================================
    #  EAN LOOKUP  (EAN → Produktname, KEIN Bild aus ean_katalog!)
    # ===================================================================

    def lookup_ean(self, ean: str) -> LookupResult:
        """Lookup-Chain fuer ein EAN-Feld.

        Reihenfolge:
            1. ean_katalog WHERE ean = X → produkt_name (KEIN bild_url!)
            2. product_image_links WHERE ean = X → Produktbild separat

        IMMER needs_confirm=True (User muss bestaetigen).
        """
        ean_text = str(ean or "").strip()
        if not ean_text:
            return LookupResult(
                state=FieldState.EMPTY,
                source=LookupSource.NONE,
                field_type=FieldType.EAN,
            )

        try:
            # --- 1. ean_katalog: Produktname ermitteln ---
            candidates = self.db.find_product_candidates_by_ean(ean_text, limit=5)
            suggestions = []
            best_name = ""
            best_confidence = 0.0

            for cand in candidates:
                name = str(cand.get("produkt_name", "")).strip()
                variant = str(cand.get("varianten_info", "")).strip()
                conf = float(cand.get("confidence", 0.6) or 0.6)
                source = str(cand.get("quelle", "")).strip()

                suggestions.append({
                    "produkt_name": name,
                    "varianten_info": variant,
                    "ean": ean_text,
                    "confidence": conf,
                    "source": source,
                })

                if conf > best_confidence:
                    best_confidence = conf
                    best_name = name

            # --- 2. Produktbild separat suchen (nicht aus ean_katalog!) ---
            image_path = ""
            if ean_text:
                image_path = self._find_product_image(ean=ean_text)

            if suggestions:
                return LookupResult(
                    state=FieldState.HIT_LOCAL,
                    source=LookupSource.DB_EAN_KATALOG,
                    field_type=FieldType.EAN,
                    data={
                        "ean": ean_text,
                        "produkt_name": best_name,
                    },
                    confidence=best_confidence,
                    needs_confirm=True,  # IMMER bestaetigen!
                    suggestions=suggestions,
                    image_path=image_path,
                    normalized_value=best_name,
                )

            return LookupResult(
                state=FieldState.NOT_FOUND,
                source=LookupSource.NONE,
                field_type=FieldType.EAN,
                data={"ean": ean_text},
                image_path=image_path,
            )

        except Exception as exc:
            log_exception(__name__, exc)
            return LookupResult(
                state=FieldState.NOT_FOUND,
                source=LookupSource.NONE,
                field_type=FieldType.EAN,
                error=str(exc),
            )

    # ===================================================================
    #  PRODUKTNAME LOOKUP  (Name → EAN-Vorschlag + Bild)
    # ===================================================================

    def lookup_product_name(self, produkt_name: str, varianten_info: str = "") -> LookupResult:
        """Lookup-Chain fuer ein produkt_name-Feld.

        Reihenfolge:
            1. ean_alias_cache → EAN-Vorschlag (needs_confirm=True)
            2. ean_katalog → EAN-Vorschlag (Fallback)
            3. product_image_links → Produktbild

        IMMER needs_confirm=True fuer EAN-Vorschlaege.
        """
        name = str(produkt_name or "").strip()
        if not name:
            return LookupResult(
                state=FieldState.EMPTY,
                source=LookupSource.NONE,
                field_type=FieldType.PRODUKT_NAME,
            )

        try:
            # --- 1. ean_alias_cache ---
            alias_candidates = self.db.search_ean_alias_candidates(raw_name=name, limit=5)
            suggestions = []

            for cand in alias_candidates:
                suggestions.append({
                    "produkt_name": str(cand.get("produkt_name", "")).strip(),
                    "varianten_info": str(cand.get("varianten_info", "")).strip(),
                    "ean": str(cand.get("ean", "")).strip(),
                    "confidence": float(cand.get("confidence", 0.5) or 0.5),
                    "source": str(cand.get("quelle", "ean_alias_cache")).strip(),
                })

            # --- 2. ean_katalog (Fallback) ---
            if not suggestions:
                local_candidates = self.db.search_local_ean_candidates(
                    produkt_name=name,
                    varianten_info=varianten_info,
                    limit=5,
                )
                for cand in local_candidates:
                    suggestions.append({
                        "produkt_name": str(cand.get("produkt_name", "")).strip(),
                        "varianten_info": str(cand.get("varianten_info", "")).strip(),
                        "ean": str(cand.get("ean", "")).strip(),
                        "confidence": float(cand.get("confidence", 0.6) or 0.6),
                        "source": str(cand.get("quelle", "ean_katalog")).strip(),
                    })

            # --- 3. Produktbild suchen ---
            image_path = self._find_product_image(product_name=name)

            best_ean = ""
            best_confidence = 0.0
            source = LookupSource.NONE

            if suggestions:
                best = suggestions[0]
                best_ean = best.get("ean", "")
                best_confidence = best.get("confidence", 0.0)
                source = (
                    LookupSource.DB_EAN_ALIAS
                    if "alias" in best.get("source", "")
                    else LookupSource.DB_EAN_KATALOG
                )

            state = FieldState.HIT_LOCAL if suggestions or image_path else FieldState.NOT_FOUND

            return LookupResult(
                state=state,
                source=source if suggestions else (LookupSource.DB_PRODUCT_IMAGE if image_path else LookupSource.NONE),
                field_type=FieldType.PRODUKT_NAME,
                data={
                    "produkt_name": name,
                    "ean_vorschlag": best_ean,
                },
                confidence=best_confidence,
                needs_confirm=True,  # IMMER bestaetigen!
                suggestions=suggestions,
                image_path=image_path,
            )

        except Exception as exc:
            log_exception(__name__, exc)
            return LookupResult(
                state=FieldState.NOT_FOUND,
                source=LookupSource.NONE,
                field_type=FieldType.PRODUKT_NAME,
                error=str(exc),
            )

    # ===================================================================
    #  ZAHLUNGSART LOOKUP
    # ===================================================================

    def lookup_zahlungsart(self, raw_value: str) -> LookupResult:
        """Lookup-Chain fuer ein zahlungsart-Feld.

        Reihenfolge:
            1. mapping.json['zahlungsarten'] → normalisierter Wert
            2. Kein Treffer → UNMAPPED (NormalizationDialog noetig)
        """
        text = str(raw_value or "").strip()
        if not text:
            return LookupResult(
                state=FieldState.EMPTY,
                source=LookupSource.NONE,
                field_type=FieldType.ZAHLUNGSART,
            )

        mapped = resolve_known_mapping("zahlungsarten", text)
        if mapped is not None:
            return LookupResult(
                state=FieldState.HIT_LOCAL,
                source=LookupSource.MAPPING_JSON,
                field_type=FieldType.ZAHLUNGSART,
                data={"raw_zahlungsart": text, "mapped_zahlungsart": mapped},
                confidence=1.0,
                needs_confirm=False,
                normalized_value=str(mapped).strip(),
            )

        return LookupResult(
            state=FieldState.UNMAPPED,
            source=LookupSource.NORMALIZATION_DIALOG,
            field_type=FieldType.ZAHLUNGSART,
            data={"raw_zahlungsart": text},
            needs_confirm=True,
        )

    # ===================================================================
    #  REVERSE LOOKUP: EAN → Produktname (nur lokale DB)
    # ===================================================================

    def reverse_lookup_ean_to_name(self, ean: str) -> LookupResult:
        """EAN → Produktname NUR aus lokaler DB. Kein API-Call.

        Wird im Wareneingang-Scanner verwendet wenn eine EAN gescannt
        wird und der Produktname noch nicht bekannt ist.
        """
        ean_text = str(ean or "").strip()
        if not ean_text:
            return LookupResult(
                state=FieldState.EMPTY,
                source=LookupSource.NONE,
                field_type=FieldType.EAN,
            )

        try:
            candidates = self.db.find_product_candidates_by_ean(ean_text, limit=3)
            if candidates:
                best = candidates[0]
                return LookupResult(
                    state=FieldState.HIT_LOCAL,
                    source=LookupSource.DB_EAN_KATALOG,
                    field_type=FieldType.EAN,
                    data={
                        "ean": ean_text,
                        "produkt_name": str(best.get("produkt_name", "")).strip(),
                        "varianten_info": str(best.get("varianten_info", "")).strip(),
                    },
                    confidence=float(best.get("confidence", 0.6) or 0.6),
                    needs_confirm=True,
                    normalized_value=str(best.get("produkt_name", "")).strip(),
                )

            return LookupResult(
                state=FieldState.NOT_FOUND,
                source=LookupSource.NONE,
                field_type=FieldType.EAN,
                data={"ean": ean_text},
            )

        except Exception as exc:
            log_exception(__name__, exc)
            return LookupResult(
                state=FieldState.NOT_FOUND,
                source=LookupSource.NONE,
                field_type=FieldType.EAN,
                error=str(exc),
            )

    # ===================================================================
    #  BESTELLNUMMER LOOKUP  (existiert bereits in DB?)
    # ===================================================================

    def lookup_bestellnummer(self, bestellnummer: str) -> LookupResult:
        """Prueft ob eine Bestellnummer bereits in einkauf_bestellungen existiert.

        Ergebnis:
            OVERWRITE  → Bestellnummer schon vorhanden (gelb)
            USER_CONFIRMED → neu, noch nicht gespeichert (gruen)
        """
        nr = str(bestellnummer or "").strip()
        if not nr:
            return LookupResult(
                state=FieldState.EMPTY,
                source=LookupSource.NONE,
                field_type=FieldType.BESTELLNUMMER,
            )
        try:
            exists = self.db.bestellnummer_exists(nr)
            if exists:
                return LookupResult(
                    state=FieldState.OVERWRITE,
                    source=LookupSource.DB_WAREN_POSITIONEN,
                    field_type=FieldType.BESTELLNUMMER,
                    data={"bestellnummer": nr},
                    confidence=1.0,
                    needs_confirm=False,
                    normalized_value=nr,
                )
            return LookupResult(
                state=FieldState.USER_CONFIRMED,
                source=LookupSource.USER_MANUAL,
                field_type=FieldType.BESTELLNUMMER,
                data={"bestellnummer": nr},
                confidence=1.0,
                needs_confirm=False,
                normalized_value=nr,
            )
        except Exception as exc:
            log_exception(__name__, exc)
            return LookupResult(
                state=FieldState.NOT_FOUND,
                source=LookupSource.NONE,
                field_type=FieldType.BESTELLNUMMER,
                error=str(exc),
            )

    # ===================================================================
    #  KAUFDATUM LOOKUP  (immer USER_CONFIRMED – nur visuelles Feedback)
    # ===================================================================

    def lookup_kaufdatum(self, kaufdatum: str) -> LookupResult:
        """Kein DB-Lookup noetig – gibt sofort USER_CONFIRMED zurueck.

        Das Datum ist ein freier Eingabewert; Enter bestaetigt ihn lediglich
        visuell als 'vom User geprueft'.
        """
        val = str(kaufdatum or "").strip()
        if not val:
            return LookupResult(
                state=FieldState.EMPTY,
                source=LookupSource.NONE,
                field_type=FieldType.KAUFDATUM,
            )
        return LookupResult(
            state=FieldState.USER_CONFIRMED,
            source=LookupSource.USER_MANUAL,
            field_type=FieldType.KAUFDATUM,
            data={"kaufdatum": val},
            confidence=1.0,
            needs_confirm=False,
            normalized_value=val,
        )

    # ===================================================================
    #  CONVENIENCE: Dispatcher nach FieldType
    # ===================================================================

    def lookup(
        self,
        field_type: FieldType,
        value: str,
        sender_domain: str = "",
        varianten_info: str = "",
    ) -> LookupResult:
        """Generischer Dispatcher – ruft die passende Lookup-Methode auf.

        Wird von `FieldLookupBinding` aufgerufen. Erleichtert die Anbindung,
        weil die UI nur field_type und value kennen muss.
        """
        if field_type == FieldType.SHOP_NAME:
            return self.lookup_shop(value, sender_domain=sender_domain)
        elif field_type == FieldType.ZAHLUNGSART:
            return self.lookup_zahlungsart(value)
        elif field_type == FieldType.EAN:
            return self.lookup_ean(value)
        elif field_type == FieldType.PRODUKT_NAME:
            return self.lookup_product_name(value, varianten_info=varianten_info)
        elif field_type == FieldType.BESTELLNUMMER:
            return self.lookup_bestellnummer(value)
        elif field_type == FieldType.KAUFDATUM:
            return self.lookup_kaufdatum(value)
        else:
            return LookupResult(
                state=FieldState.NOT_FOUND,
                source=LookupSource.NONE,
                field_type=field_type,
                error=f"Unbekannter FieldType: {field_type}",
            )

    # ===================================================================
    #  INTERNE HELPER
    # ===================================================================

    def _resolve_media_path(self, media_asset_id) -> str:
        """Laedt den lokalen Dateipfad eines media_assets-Eintrags.

        Unterstuetzt absolute und relative Pfade (relativ zum MediaStore-Basisverzeichnis).
        """
        if not media_asset_id:
            return ""
        try:
            asset = self.db.get_media_asset_by_id(int(media_asset_id))
            if not asset:
                logger.debug("_resolve_media_path: kein Asset fuer id=%s", media_asset_id)
                return ""
            storage = str(asset.get("storage_kind", "")).strip()
            path = str(asset.get("file_path", "")).strip()
            if not (storage == "local_file" and path):
                return ""
            # Direkter Pfadtest (funktioniert wenn CWD == App-Root)
            if os.path.exists(path):
                return path
            # Fallback: Pfad relativ zum MediaStore-Basisverzeichnis aufloesen
            from module.media.media_store import LocalMediaStore
            abs_path = LocalMediaStore().resolve_path(path)
            if abs_path and os.path.exists(abs_path):
                return abs_path
            logger.debug(
                "_resolve_media_path: Datei nicht gefunden – id=%s path=%r abs=%r",
                media_asset_id, path, abs_path,
            )
        except Exception as exc:
            log_exception(__name__, exc)
        return ""

    def _find_product_image(
        self,
        product_name: str = "",
        ean: str = "",
        variant_text: str = "",
    ) -> str:
        """Sucht ein Produktbild in product_image_links → media_assets.

        NICHT in ean_katalog.bild_url (bewusst ignoriert!).
        """
        try:
            product_key = build_product_key(product_name, ean, variant_text)
            link = self.db.get_primary_product_image_link(
                product_key=product_key,
                ean=ean,
                product_name=product_name,
                variant_text=variant_text,
            )
            if link:
                path = self._resolve_media_path(link.get("media_asset_id"))
                if path:
                    return path
        except Exception as exc:
            log_exception(__name__, exc)
        return ""
