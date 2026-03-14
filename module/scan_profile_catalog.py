from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class ScanProfile:
    name: str
    description: str
    use_cases: tuple[str, ...] = ()
    extraction_focus: tuple[str, ...] = ()
    source_kind: str = "visual"
    expects_screenshot_detections: bool = False
    prioritizes_tracking: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "use_cases": list(self.use_cases),
            "extraction_focus": list(self.extraction_focus),
            "source_kind": self.source_kind,
            "expects_screenshot_detections": bool(self.expects_screenshot_detections),
            "prioritizes_tracking": bool(self.prioritizes_tracking),
        }


@dataclass
class ScanDecision:
    profile_name: str
    scan_mode: str = "einkauf"
    primary_visual_source: dict[str, Any] = field(default_factory=dict)
    secondary_context_source: dict[str, Any] = field(default_factory=dict)
    should_allow_second_pass: bool = False
    source_reasoning_summary: str = ""
    prompt_score: int = 0
    module_hint: str = ""
    source_classification: str = ""
    prompt_hints: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        profile = get_scan_profile(self.profile_name)
        return {
            "profile_name": profile.name,
            "scan_mode": str(self.scan_mode or "einkauf"),
            "primary_visual_source": dict(self.primary_visual_source or {}),
            "secondary_context_source": dict(self.secondary_context_source or {}),
            "should_allow_second_pass": bool(self.should_allow_second_pass),
            "source_reasoning_summary": str(self.source_reasoning_summary or ""),
            "prompt_score": int(self.prompt_score or 0),
            "module_hint": str(self.module_hint or ""),
            "source_classification": str(self.source_classification or ""),
            "prompt_hints": dict(self.prompt_hints or {}),
            "profile": profile.to_dict(),
        }

    def __str__(self) -> str:
        primary_type = str((self.primary_visual_source or {}).get("source_type", "") or "")
        secondary_type = str((self.secondary_context_source or {}).get("source_type", "") or "")
        parts = [
            f"profile={self.profile_name}",
            f"scan_mode={self.scan_mode}",
        ]
        if primary_type:
            parts.append(f"primary={primary_type}")
        if secondary_type:
            parts.append(f"secondary={secondary_type}")
        if self.should_allow_second_pass:
            parts.append("second_pass=allowed")
        return "ScanDecision(" + ", ".join(parts) + ")"


_PROFILE_LIST = (
    ScanProfile(
        name="purchase_visual_generic",
        description="Allgemeiner Einkaufs-Screenshot oder visueller Beleg ohne klare Mail- oder PDF-Dominanz.",
        use_cases=("neutraler Screenshot", "einfacher Einkaufsbeleg", "teilweise sichtbare Produktansicht"),
        extraction_focus=("produkte", "mengen", "preise", "sichtbare bildhinweise"),
        source_kind="visual",
        expects_screenshot_detections=True,
        prioritizes_tracking=False,
    ),
    ScanProfile(
        name="order_overview_visual",
        description="Shop- oder Bestelluebersicht mit sichtbaren Produktkarten, Listen oder Statusangaben.",
        use_cases=("Amazon-/Shop-Bestelluebersicht", "Konto- oder Orders-Seite", "visuelle Produktliste"),
        extraction_focus=("produkte", "produktbilder", "mengen", "bestellstatus"),
        source_kind="visual",
        expects_screenshot_detections=True,
        prioritizes_tracking=False,
    ),
    ScanProfile(
        name="ecommerce_order_detail_visual",
        description="Detaillierte Bestellansicht eines Shops mit Produkt-, Adress- und Summenblock.",
        use_cases=("Bestelldetailseite", "Order Details", "eCommerce-Bestellansicht"),
        extraction_focus=("produkte", "adressen", "summen", "bildhinweise"),
        source_kind="visual",
        expects_screenshot_detections=True,
        prioritizes_tracking=True,
    ),
    ScanProfile(
        name="purchase_document_pdf",
        description="Dokument- oder PDF-Fall mit Fokus auf Rechnungs-, Summen- und Artikelblock.",
        use_cases=("Rechnung", "Bestell-PDF", "Beleg-PDF"),
        extraction_focus=("artikelzeilen", "summen", "adressen", "rechnungsdaten"),
        source_kind="pdf",
        expects_screenshot_detections=False,
        prioritizes_tracking=False,
    ),
    ScanProfile(
        name="invoice_pdf",
        description="Rechnungs-PDF mit starker Prioritaet auf Rechnungsnummer, Summen und Steuerangaben.",
        use_cases=("Invoice", "Rechnungsbeleg", "steuerrelevantes PDF"),
        extraction_focus=("rechnungsnummer", "summen", "steuer", "artikelzeilen"),
        source_kind="pdf",
        expects_screenshot_detections=False,
        prioritizes_tracking=False,
    ),
    ScanProfile(
        name="mail_order_visual",
        description="Bestellmail oder Mail-Screenshot mit Fokus auf sichtbare Produkte und Bestelldetails.",
        use_cases=("Bestellmail", "Mail-Screenshot", "maildominanter Bestellfall"),
        extraction_focus=("produkte", "produktbilder", "bestellnummer", "summen"),
        source_kind="mail",
        expects_screenshot_detections=True,
        prioritizes_tracking=False,
    ),
    ScanProfile(
        name="mail_shipping_visual",
        description="Versand- oder Tracking-Mail mit Fokus auf Paketstatus und Tracking-Hinweise.",
        use_cases=("Versandmail", "Tracking-Update", "Zustellmail"),
        extraction_focus=("tracking", "paketdienst", "sendungsstatus", "lieferdatum"),
        source_kind="mail",
        expects_screenshot_detections=False,
        prioritizes_tracking=True,
    ),
    ScanProfile(
        name="order_mail_primary",
        description="Mail ist die Hauptquelle; Produkte und sichtbare Bilder kommen bevorzugt aus Mail oder Screenshot.",
        use_cases=("Bestellmail mit Screenshot", "maildominanter Hybridfall"),
        extraction_focus=("produkte", "produktbilder", "tracking", "bestelldaten"),
        source_kind="mail",
        expects_screenshot_detections=True,
        prioritizes_tracking=True,
    ),
    ScanProfile(
        name="order_mail_pdf_primary",
        description="PDF ist die staerkere Hauptquelle; Mail liefert nur Zusatzkontext.",
        use_cases=("PDF-dominante Bestellmail", "Rechnungs-PDF als Hauptquelle"),
        extraction_focus=("artikelzeilen", "summen", "bestellnummer", "tracking aus mail falls noetig"),
        source_kind="hybrid",
        expects_screenshot_detections=False,
        prioritizes_tracking=True,
    ),
    ScanProfile(
        name="order_mail_hybrid",
        description="Mail und PDF ergaenzen sich; Produkte, Tracking und Rechnungsdaten werden komplementaer gelesen.",
        use_cases=("hybrider Mail/PDF-Fall", "Bestellmail mit hilfreicher PDF"),
        extraction_focus=("produkte", "tracking", "summen", "adressen"),
        source_kind="hybrid",
        expects_screenshot_detections=True,
        prioritizes_tracking=True,
    ),
    ScanProfile(
        name="discord_ticket_sales",
        description="Verkaufs- oder Ticket-Screenshot mit Fokus auf Chat-/Ticket-Positionen.",
        use_cases=("Discord-Ticket", "Verkaufschat", "Ticket-Screenshot"),
        extraction_focus=("waren", "preise", "ticketdaten", "kaeuferdaten"),
        source_kind="chat",
        expects_screenshot_detections=False,
        prioritizes_tracking=False,
    ),
    ScanProfile(
        name="purchase_document_generic",
        description="Generischer Kauf-/Dokumentfall als defensiver Rueckfall.",
        use_cases=("Fallback-Profil", "unbekannter Dokumenttyp"),
        extraction_focus=("grunddaten", "produkte", "summen"),
        source_kind="mixed",
        expects_screenshot_detections=False,
        prioritizes_tracking=False,
    ),
)

_SCAN_PROFILE_ALIASES = {
    "order_detail_visual": "ecommerce_order_detail_visual",
    "verkauf_ticket": "discord_ticket_sales",
}

_SCAN_PROFILE_MAP = {profile.name: profile for profile in _PROFILE_LIST}


def list_scan_profiles() -> list[ScanProfile]:
    return list(_PROFILE_LIST)


def resolve_scan_profile_name(profile_name: str, fallback: str = "purchase_visual_generic") -> str:
    name = str(profile_name or "").strip()
    if not name:
        name = str(fallback or "purchase_visual_generic").strip() or "purchase_visual_generic"
    name = _SCAN_PROFILE_ALIASES.get(name, name)
    if name in _SCAN_PROFILE_MAP:
        return name
    fallback_name = _SCAN_PROFILE_ALIASES.get(str(fallback or "purchase_visual_generic").strip(), str(fallback or "purchase_visual_generic").strip())
    return fallback_name if fallback_name in _SCAN_PROFILE_MAP else "purchase_visual_generic"


def get_scan_profile(profile_name: str, fallback: str = "purchase_visual_generic") -> ScanProfile:
    resolved = resolve_scan_profile_name(profile_name, fallback=fallback)
    return _SCAN_PROFILE_MAP[resolved]


def build_prompt_plan(prompt_class: str, prompt_score: int, reasoning: str, module_hint: str = "", extras: dict[str, Any] | None = None) -> dict[str, Any]:
    profile = get_scan_profile(prompt_class, fallback="purchase_document_generic")
    return {
        "prompt_class": profile.name,
        "prompt_score": int(prompt_score or 0),
        "prompt_reasoning_summary": str(reasoning or "").strip(),
        "module_hint": str(module_hint or ""),
        "extras": dict(extras or {}),
        "profile_name": profile.name,
        "profile_description": profile.description,
        "profile_source_kind": profile.source_kind,
        "profile_use_cases": list(profile.use_cases),
        "profile_extraction_focus": list(profile.extraction_focus),
        "expects_screenshot_detections": bool(profile.expects_screenshot_detections),
        "prioritizes_tracking": bool(profile.prioritizes_tracking),
    }


def build_scan_decision(
    profile_name: str,
    scan_mode: str = "einkauf",
    primary_visual_source: Mapping[str, Any] | None = None,
    secondary_context_source: Mapping[str, Any] | None = None,
    should_allow_second_pass: bool = False,
    source_reasoning_summary: str = "",
    prompt_score: int = 0,
    module_hint: str = "",
    source_classification: str = "",
    prompt_hints: Mapping[str, Any] | None = None,
) -> ScanDecision:
    profile = get_scan_profile(profile_name, fallback="purchase_document_generic")
    return ScanDecision(
        profile_name=profile.name,
        scan_mode=str(scan_mode or "einkauf"),
        primary_visual_source=dict(primary_visual_source or {}),
        secondary_context_source=dict(secondary_context_source or {}),
        should_allow_second_pass=bool(should_allow_second_pass),
        source_reasoning_summary=str(source_reasoning_summary or "").strip(),
        prompt_score=int(prompt_score or 0),
        module_hint=str(module_hint or ""),
        source_classification=str(source_classification or ""),
        prompt_hints=dict(prompt_hints or {}),
    )


def build_scan_decision_from_existing(
    prompt_plan: Mapping[str, Any] | None = None,
    *,
    scan_mode: str = "einkauf",
    source_plan: Mapping[str, Any] | None = None,
    primary_visual_source: Mapping[str, Any] | None = None,
    secondary_context_source: Mapping[str, Any] | None = None,
    should_allow_second_pass: bool = False,
) -> ScanDecision:
    prompt_plan = dict(prompt_plan or {})
    source_plan = dict(source_plan or {})
    return build_scan_decision(
        profile_name=str(prompt_plan.get("prompt_class", "") or "purchase_document_generic"),
        scan_mode=scan_mode,
        primary_visual_source=primary_visual_source if primary_visual_source is not None else source_plan.get("primary_visual_source", {}),
        secondary_context_source=secondary_context_source if secondary_context_source is not None else source_plan.get("secondary_context_source", {}),
        should_allow_second_pass=bool(should_allow_second_pass),
        source_reasoning_summary=str(source_plan.get("source_reasoning_summary", "") or prompt_plan.get("prompt_reasoning_summary", "") or ""),
        prompt_score=int(prompt_plan.get("prompt_score", 0) or 0),
        module_hint=str(prompt_plan.get("module_hint", "") or ""),
        source_classification=str(source_plan.get("source_classification", "") or ""),
        prompt_hints={
            "prompt_plan_extras": dict(prompt_plan.get("extras") or {}),
            "source_scan_mode": str(source_plan.get("scan_mode", "") or ""),
        },
    )


def coerce_scan_decision(value: Any) -> ScanDecision | None:
    if isinstance(value, ScanDecision):
        return value
    if not isinstance(value, Mapping):
        return None
    return build_scan_decision(
        profile_name=str(value.get("profile_name", value.get("prompt_class", "")) or "purchase_document_generic"),
        scan_mode=str(value.get("scan_mode", "einkauf") or "einkauf"),
        primary_visual_source=value.get("primary_visual_source") if isinstance(value.get("primary_visual_source"), Mapping) else {},
        secondary_context_source=value.get("secondary_context_source") if isinstance(value.get("secondary_context_source"), Mapping) else {},
        should_allow_second_pass=bool(value.get("should_allow_second_pass", False)),
        source_reasoning_summary=str(value.get("source_reasoning_summary", "") or ""),
        prompt_score=int(value.get("prompt_score", value.get("decision_score", 0)) or 0),
        module_hint=str(value.get("module_hint", "") or ""),
        source_classification=str(value.get("source_classification", "") or ""),
        prompt_hints=value.get("prompt_hints") if isinstance(value.get("prompt_hints"), Mapping) else {},
    )
