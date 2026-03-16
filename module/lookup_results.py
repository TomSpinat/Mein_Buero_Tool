"""Zentrale Ergebnis-Typen und Feld-Status-Enums fuer den LookupService.

Dieses Modul definiert die standardisierten Rueckgabe-Typen aller Lookup-
Operationen. Jede Lookup-Methode in `lookup_service.py` gibt ein
`LookupResult` zurueck. Die UI liest daraus `FieldState` ab und setzt
die passende Farbcodierung.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
#  FieldType: Was fuer ein Feld wird gesucht?
# ---------------------------------------------------------------------------
class FieldType(str, Enum):
    """Identifiziert das Eingabefeld, das den Lookup ausgeloest hat."""

    SHOP_NAME = "shop_name"
    ZAHLUNGSART = "zahlungsart"
    EAN = "ean"
    PRODUKT_NAME = "produkt_name"
    BESTELLNUMMER = "bestellnummer"
    KAUFDATUM = "kaufdatum"


# ---------------------------------------------------------------------------
#  FieldState: Visueller Zustand eines Eingabefeldes
# ---------------------------------------------------------------------------
class FieldState(str, Enum):
    """Bestimmt die Farbcodierung eines Eingabefeldes.

    Die zugehoerigen Hex-Werte werden in `einkauf_ui.py` unter
    FIELD_STATE_STYLES definiert.
    """

    EMPTY = "empty"
    """Feld ist leer – grauer Placeholder."""

    AI_SUGGESTED = "ai_suggested"
    """KI hat einen Wert erkannt und eingetragen (change_kind='add')."""

    USER_CONFIRMED = "user_confirmed"
    """User hat Enter gedrueckt oder manuell eingegeben → gruen."""

    OVERWRITE = "overwrite"
    """Wert wuerde bestehenden DB-Wert ueberschreiben → gelb."""

    LOOKUP_RUNNING = "lookup_running"
    """Lookup laeuft gerade (async Worker aktiv) → gruen + Spinner."""

    HIT_LOCAL = "hit_local"
    """Treffer aus lokaler Datenbank → gruen + 'DB'-Badge."""

    HIT_API = "hit_api"
    """Treffer aus externer API (Brave, UPCitemdb) → gruen + 'API'-Badge."""

    NOT_FOUND = "not_found"
    """Kein Treffer – User muss manuell ergaenzen → cyan."""

    UNMAPPED = "unmapped"
    """Wert bekannt aber nicht gemappt (Normalisierung noetig) → cyan."""


# ---------------------------------------------------------------------------
#  LookupSource: Woher stammt das Ergebnis?
# ---------------------------------------------------------------------------
class LookupSource(str, Enum):
    """Herkunft eines Lookup-Treffers."""

    MAPPING_JSON = "mapping_json"
    """Treffer aus mapping.json (Shop-/Zahlungsart-Normalisierung)."""

    DB_SHOP_KEY = "db_shop_key"
    """shop_logo_links via shop_key (exakte Suche)."""

    DB_SHOP_NAME = "db_shop_name"
    """shop_logo_links via shop_name (LIKE-Fallback)."""

    DB_SENDER_DOMAIN = "db_sender_domain"
    """shop_logo_links via sender_domain."""

    DB_EAN_KATALOG = "db_ean_katalog"
    """Treffer in ean_katalog (EAN → Produktname, NICHT Bild!)."""

    DB_EAN_ALIAS = "db_ean_alias"
    """Treffer in ean_alias_cache (Produktname → EAN-Vorschlag)."""

    DB_PRODUCT_IMAGE = "db_product_image"
    """Treffer in product_image_links → media_assets."""

    DB_WAREN_POSITIONEN = "db_waren_positionen"
    """Fallback-Treffer aus waren_positionen."""

    API_UPCITEMDB = "api_upcitemdb"
    """Treffer aus UPCitemdb API (nur Name/EAN, KEIN Bild!)."""

    API_BRAVE_LOGO = "api_brave_logo"
    """Logo-Bild via Brave Image Search."""

    API_BRAVE_PRODUCT = "api_brave_product"
    """Produktbild via Brave Image Search."""

    AMAZON_DIALOG = "amazon_dialog"
    """User hat Amazon-Land via AmazonCountryDialog gewaehlt."""

    NORMALIZATION_DIALOG = "normalization_dialog"
    """User hat Wert via NormalizationDialog gemappt."""

    USER_MANUAL = "user_manual"
    """Manuell vom User eingegeben, kein automatisches Ergebnis."""

    NONE = "none"
    """Kein Ergebnis / Lookup fehlgeschlagen."""


# ---------------------------------------------------------------------------
#  LookupResult: Standardisiertes Ergebnis aller Lookups
# ---------------------------------------------------------------------------
@dataclass
class LookupResult:
    """Einheitliches Ergebnis jeder Lookup-Operation.

    Wird von `LookupService`-Methoden zurueckgegeben und von
    `FieldLookupBinding` in visuelle Zustaende uebersetzt.

    Attribute:
        state:          Visueller Zustand des Feldes nach diesem Lookup.
        source:         Woher das Ergebnis stammt.
        field_type:     Welches Feld hat den Lookup ausgeloest.
        data:           Nutzdaten – Inhalt haengt vom Lookup-Typ ab.
        confidence:     Vertrauenswert 0.0–1.0.
        needs_confirm:  True → User muss Ergebnis explizit bestaetigen.
        suggestions:    Optionale Liste weiterer Kandidaten.
        logo_path:      Lokaler Dateipfad eines gefundenen Logos.
        image_path:     Lokaler Dateipfad eines gefundenen Produktbilds.
        normalized_value: Normalisierter Wert (z.B. 'Amazon DE' statt 'amazon').
        error:          Fehlermeldung falls Lookup gescheitert.
    """

    state: FieldState = FieldState.NOT_FOUND
    source: LookupSource = LookupSource.NONE
    field_type: FieldType = FieldType.SHOP_NAME
    data: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    needs_confirm: bool = False
    suggestions: list[dict[str, Any]] = field(default_factory=list)
    logo_path: str = ""
    image_path: str = ""
    normalized_value: str = ""
    error: str = ""

    # -- Convenience properties --

    @property
    def found(self) -> bool:
        """True wenn mindestens ein Treffer vorliegt."""
        return self.state in (
            FieldState.HIT_LOCAL,
            FieldState.HIT_API,
            FieldState.USER_CONFIRMED,
            FieldState.AI_SUGGESTED,
        )

    @property
    def is_local(self) -> bool:
        """True wenn Ergebnis aus lokaler DB stammt (kein API-Call noetig)."""
        return self.source in (
            LookupSource.MAPPING_JSON,
            LookupSource.DB_SHOP_KEY,
            LookupSource.DB_SHOP_NAME,
            LookupSource.DB_SENDER_DOMAIN,
            LookupSource.DB_EAN_KATALOG,
            LookupSource.DB_EAN_ALIAS,
            LookupSource.DB_PRODUCT_IMAGE,
            LookupSource.DB_WAREN_POSITIONEN,
        )

    @property
    def is_api(self) -> bool:
        """True wenn Ergebnis aus externer API stammt."""
        return self.source in (
            LookupSource.API_UPCITEMDB,
            LookupSource.API_BRAVE_LOGO,
            LookupSource.API_BRAVE_PRODUCT,
        )

    @property
    def has_logo(self) -> bool:
        return bool(self.logo_path)

    @property
    def has_image(self) -> bool:
        return bool(self.image_path)

    @property
    def has_suggestions(self) -> bool:
        return bool(self.suggestions)

    def merge(self, other: "LookupResult") -> "LookupResult":
        """Fuegt Daten aus einem weiteren Lookup-Ergebnis hinzu.

        Nuetzlich wenn z.B. der Shop-Lookup ein Logo findet und
        separat noch ein Mapping-Treffer existiert. Der 'bessere'
        State gewinnt.
        """
        _STATE_PRIO = {
            FieldState.HIT_LOCAL: 6,
            FieldState.HIT_API: 5,
            FieldState.USER_CONFIRMED: 4,
            FieldState.AI_SUGGESTED: 3,
            FieldState.UNMAPPED: 2,
            FieldState.NOT_FOUND: 1,
            FieldState.LOOKUP_RUNNING: 0,
            FieldState.EMPTY: 0,
            FieldState.OVERWRITE: 0,
        }
        best_state = self.state if _STATE_PRIO.get(self.state, 0) >= _STATE_PRIO.get(other.state, 0) else other.state
        best_source = self.source if _STATE_PRIO.get(self.state, 0) >= _STATE_PRIO.get(other.state, 0) else other.source

        merged_data = {**self.data, **other.data}
        merged_suggestions = self.suggestions + [s for s in other.suggestions if s not in self.suggestions]

        return LookupResult(
            state=best_state,
            source=best_source,
            field_type=self.field_type,
            data=merged_data,
            confidence=max(self.confidence, other.confidence),
            needs_confirm=self.needs_confirm or other.needs_confirm,
            suggestions=merged_suggestions,
            logo_path=self.logo_path or other.logo_path,
            image_path=self.image_path or other.image_path,
            normalized_value=self.normalized_value or other.normalized_value,
            error=self.error or other.error,
        )


# ---------------------------------------------------------------------------
#  FieldState Styling – konsistente Hex-Werte fuer alle Module
# ---------------------------------------------------------------------------
FIELD_STATE_STYLES: dict[FieldState, dict[str, str]] = {
    FieldState.EMPTY: {
        "bg": "#24283b",
        "fg": "#565f89",
        "border": "#414868",
        "badge": "",
    },
    FieldState.AI_SUGGESTED: {
        "bg": "#203225",
        "fg": "#9ece6a",
        "border": "#9ece6a",
        "badge": "KI",
    },
    FieldState.USER_CONFIRMED: {
        "bg": "#203225",
        "fg": "#9ece6a",
        "border": "#9ece6a",
        "badge": "",
    },
    FieldState.OVERWRITE: {
        "bg": "#3a3117",
        "fg": "#f7c66f",
        "border": "#f7a34b",
        "badge": "",
    },
    FieldState.LOOKUP_RUNNING: {
        "bg": "#203225",
        "fg": "#9ece6a",
        "border": "#9ece6a",
        "badge": "\u231b",
    },
    FieldState.HIT_LOCAL: {
        "bg": "#203225",
        "fg": "#9ece6a",
        "border": "#9ece6a",
        "badge": "DB",
    },
    FieldState.HIT_API: {
        "bg": "#203225",
        "fg": "#9ece6a",
        "border": "#9ece6a",
        "badge": "API",
    },
    FieldState.NOT_FOUND: {
        "bg": "#1f3340",
        "fg": "#7dcfff",
        "border": "#4ea1d3",
        "badge": "?",
    },
    FieldState.UNMAPPED: {
        "bg": "#1f3340",
        "fg": "#7dcfff",
        "border": "#4ea1d3",
        "badge": "\u26a0",
    },
}
