from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Mapping, Optional

from module.scan_output_contract import get_scan_output_schema_json
from module.scan_profile_catalog import build_scan_decision_from_existing, coerce_scan_decision, get_scan_profile


@dataclass
class GeminiPromptRenderResult:
    prompt: str
    resolved_profile: str
    decision: dict[str, Any]
    profile_hint: str = ""
    guidance_text: str = ""
    renderer_name: str = "central_gemini_renderer"
    used_fallback: bool = False

    def to_meta(self) -> dict[str, Any]:
        decision = dict(self.decision or {})
        primary = decision.get("primary_visual_source", {}) if isinstance(decision.get("primary_visual_source"), dict) else {}
        secondary = decision.get("secondary_context_source", {}) if isinstance(decision.get("secondary_context_source"), dict) else {}
        return {
            "renderer_name": str(self.renderer_name or "central_gemini_renderer"),
            "used_fallback": bool(self.used_fallback),
            "resolved_profile": str(self.resolved_profile or ""),
            "source_classification": str(decision.get("source_classification", "") or ""),
            "primary_source_type": str(primary.get("source_type", "") or ""),
            "secondary_source_type": str(secondary.get("source_type", "") or ""),
        }


def resolve_prompt_profile_from_request(scan_mode: str, prompt_profile: str = "", prompt_plan: Optional[Dict[str, Any]] = None, scan_decision: Optional[Dict[str, Any]] = None) -> str:
    mode = str(scan_mode or "einkauf").strip().lower()
    fallback = "discord_ticket_sales" if mode == "verkauf" else "purchase_visual_generic"
    explicit = str(prompt_profile or "").strip()
    if explicit:
        return get_scan_profile(explicit, fallback=fallback).name
    decision = coerce_scan_decision(scan_decision)
    if decision is not None and str(decision.profile_name or "").strip():
        return get_scan_profile(decision.profile_name, fallback=fallback).name
    if isinstance(prompt_plan, dict):
        planned = str(prompt_plan.get("prompt_class", "") or "").strip()
        if planned:
            return get_scan_profile(planned, fallback=fallback).name
    return get_scan_profile(fallback, fallback=fallback).name


def resolve_scan_decision_from_request(scan_mode: str, prompt_profile: str = "", prompt_plan: Optional[Dict[str, Any]] = None, scan_decision: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    decision = coerce_scan_decision(scan_decision)
    if decision is None:
        resolved_profile = resolve_prompt_profile_from_request(scan_mode, prompt_profile, prompt_plan, scan_decision)
        decision = build_scan_decision_from_existing(
            prompt_plan=dict(prompt_plan or {"prompt_class": resolved_profile}),
            scan_mode=scan_mode,
            should_allow_second_pass=False,
        )
    return decision.to_dict()


def _is_module1_ai_cropping_disabled(scan_decision: Optional[Dict[str, Any]] = None, prompt_plan: Optional[Dict[str, Any]] = None) -> bool:
    decision = dict(scan_decision or {})
    plan = dict(prompt_plan or {})
    module_hint = str(decision.get("module_hint", "") or plan.get("module_hint", "") or plan.get("origin_module", "") or "").strip().lower()
    if module_hint == "modul_order_entry":
        return True
    prompt_hints = decision.get("prompt_hints") if isinstance(decision.get("prompt_hints"), Mapping) else {}
    if isinstance(prompt_hints, Mapping):
        context_channel = str(prompt_hints.get("context_channel", "") or prompt_hints.get("source_channel", "") or "").strip().lower()
        if context_channel == "order_entry":
            return True
    extras = plan.get("extras") if isinstance(plan.get("extras"), Mapping) else {}
    if isinstance(extras, Mapping):
        context_channel = str(extras.get("context_channel", "") or extras.get("source_channel", "") or "").strip().lower()
        if context_channel == "order_entry":
            return True
    return False


def _profile_catalog_hint(prompt_profile: str, scan_mode: str = "einkauf", allow_screenshot_detections: bool = True) -> str:
    fallback = "discord_ticket_sales" if str(scan_mode or "").strip().lower() == "verkauf" else "purchase_visual_generic"
    profile = get_scan_profile(prompt_profile, fallback=fallback)
    parts: List[str] = [f"Fachprofil: {profile.description}."]
    if profile.extraction_focus:
        parts.append("Schwerpunkte: " + ", ".join(list(profile.extraction_focus)[:4]) + ".")
    if allow_screenshot_detections and profile.expects_screenshot_detections:
        parts.append("Produktbezogene screenshot_detections sind sinnvoll, wenn sie klar sichtbar ableitbar sind.")
    if profile.prioritizes_tracking:
        parts.append("Tracking hat in diesem Profil besondere Prioritaet.")
    return "\n".join(parts).strip()


def _describe_source_for_prompt(source: Mapping[str, Any] | None, fallback_label: str = "Quelle") -> str:
    row = dict(source or {})
    if not row:
        return ""
    label = str(row.get("original_name", "") or row.get("source_type", "") or fallback_label).strip()
    source_type = str(row.get("source_type", "") or "").strip()
    if label and source_type and source_type not in label:
        return f"{label} ({source_type})"
    return label or fallback_label


def _build_prompt_plan_hint(prompt_profile: str, prompt_plan: Optional[Dict[str, Any]] = None, scan_decision: Optional[Dict[str, Any]] = None, scan_mode: str = "einkauf") -> str:
    parts: List[str] = []
    profile = resolve_prompt_profile_from_request(scan_mode, prompt_profile, prompt_plan, scan_decision)
    allow_screenshot_detections = not _is_module1_ai_cropping_disabled(scan_decision=scan_decision, prompt_plan=prompt_plan)
    if profile:
        parts.append(f"Aktives Prompt-Profil: {profile}.")
        catalog_hint = _profile_catalog_hint(profile, scan_mode=scan_mode, allow_screenshot_detections=allow_screenshot_detections)
        if catalog_hint:
            parts.append(catalog_hint)
    decision = dict(resolve_scan_decision_from_request(scan_mode, prompt_profile, prompt_plan, scan_decision) or {})
    try:
        score = int(decision.get("prompt_score", 0) or 0)
    except Exception:
        score = 0
    if score > 0:
        parts.append(f"Profilbewertung: {score}/100.")
    reasoning = str(decision.get("source_reasoning_summary", "") or "").strip()
    if reasoning:
        parts.append(f"Quell-Begruendung: {reasoning}.")
    primary_label = _describe_source_for_prompt(decision.get("primary_visual_source"), fallback_label="Primaerquelle")
    secondary_label = _describe_source_for_prompt(decision.get("secondary_context_source"), fallback_label="Sekundaerquelle")
    if primary_label:
        parts.append(f"Primaere visuelle Quelle: {primary_label}.")
    if secondary_label:
        parts.append(f"Sekundaere Zusatzquelle: {secondary_label}.")
    if bool(decision.get("should_allow_second_pass", False)):
        parts.append("Ein gezielter Ergaenzungspass ist fachlich erlaubt, aber optional.")
    return "\n".join(parts).strip()


def _profile_guidance_for_verkauf(prompt_profile: str) -> str:
    profile = get_scan_profile(prompt_profile, fallback="discord_ticket_sales").name.lower()
    if profile in {"discord_ticket_sales", "verkauf_ticket"}:
        return (
            "Profilfokus:\n"
            "- Es handelt sich typischerweise um ein Discord-Ticket, einen Chat-Verlauf oder einen Ticket-Screenshot.\n"
            "- Extrahiere ticket_name, kaeufer, zahlungsziel und waren[] bevorzugt aus Ticket-Headern, sichtbaren Nachrichten, Embeds, Produktlisten und Preisangaben.\n"
            "- Produkte muessen aus klar sichtbaren Chat- oder Listeninhalten stammen. Wenn Mengen oder Preise nicht klar erkennbar sind, leer lassen statt raten.\n"
            "- Trenne mehrere Produkte sauber in waren[] und mische keine Varianten zusammen."
        )
    return (
        "Profilfokus:\n"
        "- Extrahiere Ticket- und Produktdaten nur aus klar sichtbaren Verkaufsinformationen.\n"
        "- Fehlende Werte leer lassen statt sie aus Kontext zu erraten."
    )


def _profile_guidance_for_einkauf(prompt_profile: str, allow_screenshot_detections: bool = True) -> str:
    profile = get_scan_profile(prompt_profile, fallback="purchase_visual_generic").name.lower()
    guidance = {
        "purchase_document_pdf": (
            "Profilfokus:\n"
            "- Die Hauptquelle ist ein Dokument oder eine PDF. Priorisiere Rechnungs-, Bestell-, Summen-, Adress- und Artikelbloecke aus dem Dokument.\n"
            "- Produkte sollen aus klaren Produktzeilen oder sichtbaren Produktbereichen der gerenderten PDF-Seite kommen.\n"
            "- Wenn die PDF keine Produktbilder zeigt, erfinde keine screenshot_detections."
        ),
        "order_overview_visual": (
            "Profilfokus:\n"
            "- Die Hauptquelle ist eine visuelle Bestelluebersicht oder ein Shop-Screenshot.\n"
            "- Priorisiere sichtbare Produktkarten, Listenzeilen, Artikelnamen, Mengen, Preise und erkennbare Produktbilder aus dem Screenshot."
            if not allow_screenshot_detections else
            "Profilfokus:\n"
            "- Die Hauptquelle ist eine visuelle Bestelluebersicht oder ein Shop-Screenshot.\n"
            "- Priorisiere sichtbare Produktkarten, Listenzeilen, Artikelnamen, Mengen, Preise und erkennbare Produktbilder aus dem Screenshot.\n"
            "- Nutze screenshot_detections besonders dann, wenn sichtbare Produktbilder oder Kacheln klar abgegrenzt sind. Fuer echte Produktthumbnails liefere nach Moeglichkeit einen separaten Detection-Eintrag mit product_key oder role = product_image. Diese Box soll nur das Thumbnail selbst umfassen, nicht die ganze Produktzeile, nicht Text, Preis oder Buttons. Solche product_image-Boxen sind in Bestellscreenshots meist kompakt und eher quadratisch oder leicht rechteckig. Vermeide fuer product_image lange flache Streifen oder ganze Zeilenboxen. Die Boxen muessen sich auf die echte Pixelgroesse der Primaerdatei beziehen, nicht auf eine UI-Vorschau."
        ),
        "ecommerce_order_detail_visual": (
            "Profilfokus:\n"
            "- Die Hauptquelle ist eine detaillierte Bestellansicht eines Shops, zum Beispiel Order Details mit Summen-, Adress- oder Statusblock.\n"
            "- Priorisiere Bestellnummer, sichtbare Produktzeilen, Produktbilder, Summen, Adressen und erkennbare Tracking- oder Statushinweise aus genau dieser Detailansicht."
            if not allow_screenshot_detections else
            "Profilfokus:\n"
            "- Die Hauptquelle ist eine detaillierte Bestellansicht eines Shops, zum Beispiel Order Details mit Summen-, Adress- oder Statusblock.\n"
            "- Priorisiere Bestellnummer, sichtbare Produktzeilen, Produktbilder, Summen, Adressen und erkennbare Tracking- oder Statushinweise aus genau dieser Detailansicht.\n"
            "- Nutze screenshot_detections fuer sichtbare Produktbereiche nur dann, wenn Bildboxen oder Karten sauber zur Bestellung gehoeren. Wenn ein Produktthumbnail sichtbar ist, liefere bevorzugt einen separaten Detection-Eintrag mit product_key oder role = product_image. Diese Box soll nur das Bildthumbnail umfassen, nicht die ganze Produktzeile. In Shop-Order-Details sind solche Bildboxen meist kompakt und grob quadratisch oder leicht rechteckig; vermeide fuer product_image extrem breite Zeilenboxen, lange Textstreifen oder halbierte Randboxen. Die Boxen muessen auf die Originaldatei in echten Pixeln referenzieren; source_image_width und source_image_height moeglichst immer mitgeben."
        ),
        "purchase_visual_generic": (
            "Profilfokus:\n"
            "- Die Hauptquelle ist ein allgemeiner Einkaufs-Screenshot oder Beleg ohne klare Mail- oder PDF-Dominanz.\n"
            "- Produkte, Mengen und Preise sollen aus der staerksten sichtbaren Quelle kommen.\n"
            "- Wenn nur Teilinformationen sichtbar sind, nur diese uebernehmen und den Rest leer lassen."
        ),
        "order_mail_primary": (
            "Profilfokus:\n"
            "- Die Mail bzw. ihr Screenshot ist die primaere visuelle Quelle. Produkte und Produktbilder sollen zuerst aus der Maildarstellung kommen.\n"
            "- Die sekundaere Quelle darf Tracking, Betraege, Adressen oder Rechnungsdetails ergaenzen, aber sichtbare Produkte der Mail nicht verdraengen.\n"
            "- Tracking aktiv aus sichtbaren Versandhinweisen, Buttons, Linktexten und URL-Hints der Mail ziehen."
        ),
        "order_mail_pdf_primary": (
            "Profilfokus:\n"
            "- In diesem Mail-Fall ist die PDF die staerkere Hauptquelle fuer Produkte und Rechnungsdetails.\n"
            "- Die Mail bleibt Zusatzkontext fuer Tracking, Versandstatus, Buttons, Linktexte oder Hinweise, die in der PDF fehlen.\n"
            "- Produkte und bild_url sollen zuerst aus der PDF bzw. ihrer gerenderten Seite kommen, wenn dort die direkte Evidenz staerker ist."
        ),
        "order_mail_hybrid": (
            "Profilfokus:\n"
            "- Mail und PDF ergaenzen sich. Nutze die visuell staerkere Quelle fuer Produkte und Produktbilder.\n"
            "- Nutze die andere Quelle fuer Tracking, Summen, Adressen oder Rechnungsdetails, wenn sie dort klarer erkennbar sind.\n"
            "- Kombiniere nur komplementaere Informationen; keine Mischprodukte und keine Doppelzaehlungen."
        ),
    }
    return guidance.get(
        profile,
        (
            "Profilfokus:\n"
            "- Extrahiere Einkaufsdaten quelltreu aus der staerksten sichtbaren Quelle.\n"
            "- Fehlende Werte lieber leer lassen als aus Kontext zu raten."
        ),
    )


def render_gemini_prompt_from_decision(scan_mode: str, custom_text: str = "", prompt_profile: str = "", prompt_plan: Optional[Dict[str, Any]] = None, scan_decision: Optional[Dict[str, Any]] = None, schema_text: str = "") -> GeminiPromptRenderResult:
    mode = str(scan_mode or "einkauf").strip().lower()
    schema = str(schema_text or "").strip() or get_scan_output_schema_json(mode)
    custom = str(custom_text or "").strip()
    resolved_profile = resolve_prompt_profile_from_request(mode, prompt_profile, prompt_plan, scan_decision)
    resolved_decision = resolve_scan_decision_from_request(mode, prompt_profile, prompt_plan, scan_decision)
    module1_ai_cropping_disabled = mode == "einkauf" and _is_module1_ai_cropping_disabled(scan_decision=resolved_decision, prompt_plan=prompt_plan)
    if module1_ai_cropping_disabled:
        logging.info(
            "module1_screenshot_detection_prompt_skipped: profile=%s, scan_mode=%s, reason=%s",
            resolved_profile,
            mode,
            "phase_a_disable_module1_ai_cropping",
        )
    profile_hint = _build_prompt_plan_hint(resolved_profile, prompt_plan, scan_decision=resolved_decision, scan_mode=mode)
    if mode == "verkauf":
        profile_guidance = _profile_guidance_for_verkauf(resolved_profile)
        prompt = f"""
Du analysierst eine Verkaufsquelle. Das ist meist ein Discord-Ticket, ein Chat-Screenshot oder ein Ticket-Verlauf.

{profile_hint}

{profile_guidance}

Wichtige Regeln:
1) Keine erfundenen Werte.
2) Wenn ein Wert fehlt: leere Zeichenkette "". Fuer Listen nutze [] statt Freitext.
3) Antworte nur als gueltiges JSON-Objekt, ohne Markdown, ohne Code-Fences.
4) Nutze exakt die Schluessel aus dem folgenden Vertrag.
5) Produkte, Mengen und Preise nur uebernehmen, wenn sie im Ticket klar sichtbar oder genannt sind.
6) Zerlege erkennbare Einzelpositionen sauber in waren[].
7) Wenn der Screenshot abgeschnitten oder unvollstaendig ist, nur den sichtbaren Teil verwenden.

Zusatztext vom Nutzer:
{custom}

Interner Ausgabevertrag (JSON-Schema-Hinweis):
{schema}
""".strip()
    else:
        profile_guidance = _profile_guidance_for_einkauf(resolved_profile, allow_screenshot_detections=not module1_ai_cropping_disabled)
        screenshot_guidance = (
            """
Screenshot-Detektionen:
19) screenshot_detections aktiv liefern, wenn in der primaeren visuellen Quelle sichtbare Produkte mit erkennbaren Bildbereichen vorhanden sind.
    Fuer ein sichtbares Produktthumbnail moeglichst einen eigenen Detection-Eintrag fuer product_image liefern.
20) Setze Bounding-Box-artige Koordinaten nur, wenn sie belastbar aus der sichtbaren Quelle ableitbar sind.
    Nutze fuer jeden Detection-Eintrag moeglichst explizit coord_origin und coord_units.
    Bevorzugt: coord_origin=top_left und coord_units=px mit x, y, width, height in absoluten Pixeln der primaeren visuellen Quelle.
    Wenn nur aus einer skalierten Ansicht sicher ableitbar: coord_units=relative_1000 verwenden (Wertebereich 0..1000) und source_image_width/source_image_height der Referenz mitgeben.
    Nicht zwischen Einheiten mischen, keine stillen Umrechnungen erraten.
    product_image-Boxen sollen kompakt und eher quadratisch oder leicht rechteckig sein. Markiere nicht die ganze Produktzeile, keine langen flachen Streifen und keine Buttons oder Preisbereiche als product_image.
    Keine Fantasieboxen. Wenn unklar: screenshot_detections leer lassen.
21) Wenn screenshot_detections gesetzt werden, ordne sie moeglichst passenden Produkten in waren[] zu und liefere dafuer bevorzugt den passenden ware_index der Warenzeile. Wenn kein sicherer Index ableitbar ist, ware_index lieber leer lassen statt raten.
""".strip()
            if not module1_ai_cropping_disabled else
            """
Screenshot-Detektionen:
19) Fuer diesen Modul-1-Scan screenshot_detections leer lassen.
20) Keine Produktbild-Boxen, keine KI-Crops und keine Bounding-Boxes fuer Produktbilder zurueckgeben.
""".strip()
        )
        prompt = f"""
Du analysierst eine Einkaufsquelle. Je nach Prompt-Profil kann das eine Bestellmail, ein Mail-Screenshot,
eine Bestelluebersicht, eine Rechnungs-PDF, ein Beleg oder ein Hybrid aus primaerer visueller Quelle plus Zusatzkontext sein.

{profile_hint}

{profile_guidance}

Arbeitsweise:
1) Keine erfundenen Werte.
2) Wenn ein Wert fehlt: leere Zeichenkette "". Fuer Listen nutze [] statt Freitext.
3) Antworte nur als gueltiges JSON-Objekt, ohne Markdown, ohne Code-Fences.
4) Nutze exakt die Schluessel aus dem folgenden Vertrag.
5) Menge korrekt erfassen.
6) ekp_brutto ist Produkt-Stueckpreis; Versand und Nebenkosten getrennt in eigene Felder.
7) Lies den Zusatztext sorgfaeltig: Dort koennen Quellmodus, Primaerquelle, Zusatzquelle, Tracking-Hinweise, PDF-Kurzinhalte oder Bild-Hinweise stehen.
8) Die primaere visuelle Quelle ist die wichtigste visuelle Grundlage. Die sekundaere Quelle darf ergaenzen,
   aber eine klare Produktliste oder sichtbare Produktdarstellung der Primaerquelle nicht blind verdraengen.
9) Wenn Quellen sich widersprechen, bevorzuge die Quelle mit klarerer direkter Evidenz. Keine Mischprodukte erfinden.

Produkte und Bilder:
10) Fuelle waren[] aktiv, sobald sichtbare oder klar genannte Produkte vorhanden sind.
11) produkt_name soll moeglichst aus sichtbaren Artikelbereichen, Produktkarten oder klaren Produktzeilen stammen.
12) Wenn die Primaerquelle ein Screenshot ist, priorisiere sichtbare Produktbereiche, Produktnamen und Produktbilder aus diesem Screenshot.
13) Wenn die Primaerquelle eine PDF ist, priorisiere sichtbare Produktbereiche und Produktzeilen aus der gerenderten PDF-Seite.
14) bild_url soll gesetzt werden, wenn im Dokument oder im Zusatztext eine belastbare Produktbild-URL oder ein klarer Bildhinweis vorliegt.
    Wenn keine belastbare URL vorliegt: bild_url leer lassen.
15) Leere oder irrelevante PDFs duerfen die Warenextraktion nicht blockieren.

Tracking und Versand:
16) tracking_nummer_einkauf und paketdienst aktiv aus allen relevanten Hinweisen suchen, mit Prioritaet auf
    sichtbare Versandhinweise, Versandbestaetigungen, Buttons, Linktexte, URL-Hints,
    Trackingnummern im Dokument und Hinweise auf Versanddienstleister.
17) Tracking darf nicht leer bleiben, wenn eine Quelle klare Tracking-Hinweise hat, auch falls die andere Quelle dazu nichts enthaelt.
18) Versanddaten, Status und Lieferhinweise komplementaer aus Primaer- und Zusatzquelle nutzen, ohne etwas zu erfinden.

Steuer / Reverse Charge:
18a) Setze reverse_charge = true, wenn die Rechnung einen der folgenden Hinweise traegt:
     "§13b UStG", "Steuerschuldnerschaft des Leistungsempfaengers", "Reverse Charge",
     "VAT: 0%" kombiniert mit einem erkennbaren EU-Absender, oder vergleichbare Formulierungen.
     In allen anderen Faellen: reverse_charge = false.

{screenshot_guidance}

Quellkombination:
22) Zusatzquellen duerfen Tracking-, Betrags-, Adress- oder Rechnungsdetails ergaenzen, wenn sie dort klarer sind.
23) Produkte und Produktbilder sollen aus der visuell staerksten Quelle kommen, nicht aus einer schwachen Nebenquelle.
24) Fehlende Werte lieber leer lassen als raten.

Zusatztext vom Nutzer:
{custom}

Interner Ausgabevertrag (JSON-Schema-Hinweis):
{schema}
""".strip()
    return GeminiPromptRenderResult(
        prompt=prompt,
        resolved_profile=resolved_profile,
        decision=resolved_decision,
        profile_hint=profile_hint,
        guidance_text=profile_guidance,
    )
