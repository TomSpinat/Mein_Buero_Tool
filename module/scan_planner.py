from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from module.scan_profile_catalog import (
    ScanDecision,
    build_prompt_plan,
    build_scan_decision,
    build_scan_decision_from_existing,
    get_scan_profile,
)


@dataclass
class ScanPlannerResult:
    prompt_plan: dict[str, Any]
    decision: ScanDecision
    used_fallback: bool = False
    planner_name: str = "central_scan_planner"
    planner_rule: str = ""
    context_channel: str = ""
    reasoning_summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "prompt_plan": dict(self.prompt_plan or {}),
            "decision": self.decision.to_dict() if isinstance(self.decision, ScanDecision) else {},
            "used_fallback": bool(self.used_fallback),
            "planner_name": str(self.planner_name or "central_scan_planner"),
            "planner_rule": str(self.planner_rule or ""),
            "context_channel": str(self.context_channel or ""),
            "reasoning_summary": str(self.reasoning_summary or ""),
        }


def _descriptor_from_row(row: Mapping[str, Any] | None) -> dict[str, Any]:
    row = dict(row or {})
    if not row:
        return {}
    return {
        "source_type": str(row.get("source_type", "") or ""),
        "original_name": str(row.get("original_name", "") or ""),
        "file_path": str(row.get("file_path", "") or ""),
        "mime_type": str(row.get("mime_type", "") or ""),
    }


def _first_descriptor(rows: Any) -> dict[str, Any]:
    for row in list(rows or []):
        descriptor = _descriptor_from_row(row)
        if descriptor:
            return descriptor
    return {}


def _profile_score(profile_name: str, fallback_prompt_plan: Mapping[str, Any] | None = None) -> int:
    if isinstance(fallback_prompt_plan, Mapping):
        fallback_class = str(fallback_prompt_plan.get("prompt_class", "") or "")
        fallback_score = int(fallback_prompt_plan.get("prompt_score", 0) or 0)
        if fallback_class and fallback_class == profile_name and fallback_score > 0:
            return fallback_score
    defaults = {
        "discord_ticket_sales": 100,
        "purchase_document_pdf": 90,
        "invoice_pdf": 92,
        "ecommerce_order_detail_visual": 92,
        "order_overview_visual": 88,
        "order_mail_hybrid": 96,
        "order_mail_pdf_primary": 91,
        "order_mail_primary": 89,
        "purchase_visual_generic": 72,
        "purchase_document_generic": 60,
    }
    return int(defaults.get(profile_name, 70))


def _resolve_order_entry_plan(context: Mapping[str, Any], scan_mode: str, module_hint: str, fallback_prompt_plan: Mapping[str, Any] | None = None) -> ScanPlannerResult | None:
    profile_name = get_scan_profile(
        str(context.get("suggested_profile_name", "") or "purchase_visual_generic"),
        fallback="discord_ticket_sales" if scan_mode == "verkauf" else "purchase_visual_generic",
    ).name
    primary_source = _descriptor_from_row(context.get("primary_candidate"))
    input_kind = str(context.get("input_kind", "") or "")
    if not primary_source and input_kind != "text":
        return None
    source_classification = {
        "pdf": "single_document_source",
        "image": "single_visual_source",
        "other": "single_file_source",
        "text": "single_text_source",
    }.get(input_kind, "single_source")
    source_scan_mode = {
        "pdf": "pdf_primary",
        "image": "visual_primary",
        "other": "file_primary",
        "text": "text_primary",
    }.get(input_kind, "single_primary")
    reasoning = str(context.get("source_reasoning_summary", "") or "").strip() or "Order-Entry-Context wurde zentral ausgewertet."
    prompt_plan = build_prompt_plan(
        prompt_class=profile_name,
        prompt_score=_profile_score(profile_name, fallback_prompt_plan),
        reasoning=reasoning,
        module_hint=module_hint,
        extras={
            "source_channel": str(context.get("source_channel", "order_entry") or "order_entry"),
            "input_kind": input_kind,
            "document_guess": str(context.get("document_guess", "") or ""),
            "visible_context_hints": list(context.get("visible_context_hints", []) or []),
            "context_flags": dict(context.get("context_flags", {}) or {}),
            "planner_origin": "central",
        },
    )
    decision = build_scan_decision(
        profile_name=profile_name,
        scan_mode=scan_mode,
        primary_visual_source=primary_source,
        secondary_context_source={},
        should_allow_second_pass=False,
        source_reasoning_summary=reasoning,
        prompt_score=int(prompt_plan.get("prompt_score", 0) or 0),
        module_hint=module_hint,
        source_classification=source_classification,
        prompt_hints={
            "source_scan_mode": source_scan_mode,
            "planner_rule": "order_entry_context",
            "context_channel": str(context.get("source_channel", "order_entry") or "order_entry"),
        },
    )
    return ScanPlannerResult(
        prompt_plan=prompt_plan,
        decision=decision,
        used_fallback=False,
        planner_rule="order_entry_context",
        context_channel=str(context.get("source_channel", "order_entry") or "order_entry"),
        reasoning_summary=reasoning,
    )


def _resolve_mail_plan(context: Mapping[str, Any], scan_mode: str, module_hint: str, fallback_prompt_plan: Mapping[str, Any] | None = None) -> ScanPlannerResult | None:
    source_plan = dict(context.get("source_plan", {}) or {})
    profile_name = str(context.get("selected_profile_name", "") or "")
    if not profile_name:
        classification = str(source_plan.get("source_classification", "") or "")
        if classification == "hybrid":
            profile_name = "order_mail_hybrid"
        elif classification == "pdf_dominant":
            profile_name = "order_mail_pdf_primary"
        else:
            profile_name = "order_mail_primary"
    profile_name = get_scan_profile(profile_name, fallback="order_mail_primary").name
    primary_source = _descriptor_from_row(source_plan.get("primary_visual_source")) or _first_descriptor(context.get("primary_candidates"))
    secondary_source = _descriptor_from_row(source_plan.get("secondary_context_source")) or _first_descriptor(context.get("secondary_candidates"))
    reasoning = str(context.get("source_reasoning_summary", "") or source_plan.get("source_reasoning_summary", "") or "").strip()
    if not reasoning:
        reasoning = "Mail-Context wurde zentral ausgewertet."
    should_allow_second_pass = bool(context.get("should_allow_second_pass", False) or secondary_source)
    prompt_plan = build_prompt_plan(
        prompt_class=profile_name,
        prompt_score=_profile_score(profile_name, fallback_prompt_plan),
        reasoning=reasoning,
        module_hint=module_hint,
        extras={
            "source_channel": str(context.get("source_channel", "mail") or "mail"),
            "sender_domain": str(context.get("sender_domain", "") or ""),
            "visible_context_hints": list(context.get("visible_context_hints", []) or []),
            "context_flags": dict(context.get("context_flags", {}) or {}),
            "primary_type": str(primary_source.get("source_type", "") or ""),
            "secondary_type": str(secondary_source.get("source_type", "") or ""),
            "planner_origin": "central",
        },
    )
    decision = build_scan_decision(
        profile_name=profile_name,
        scan_mode=scan_mode,
        primary_visual_source=primary_source,
        secondary_context_source=secondary_source,
        should_allow_second_pass=should_allow_second_pass,
        source_reasoning_summary=reasoning,
        prompt_score=int(prompt_plan.get("prompt_score", 0) or 0),
        module_hint=module_hint,
        source_classification=str(source_plan.get("source_classification", "") or ""),
        prompt_hints={
            "source_scan_mode": str(source_plan.get("scan_mode", "") or ""),
            "planner_rule": "mail_context",
            "context_channel": str(context.get("source_channel", "mail") or "mail"),
        },
    )
    return ScanPlannerResult(
        prompt_plan=prompt_plan,
        decision=decision,
        used_fallback=False,
        planner_rule="mail_context",
        context_channel=str(context.get("source_channel", "mail") or "mail"),
        reasoning_summary=reasoning,
    )


def _build_order_entry_legacy_fallback_plan(context: Mapping[str, Any], scan_mode: str, module_hint: str) -> tuple[dict[str, Any], str]:
    fallback_profile = "discord_ticket_sales" if str(scan_mode or "").strip().lower() == "verkauf" else "purchase_visual_generic"
    profile_name = get_scan_profile(str(context.get("suggested_profile_name", "") or fallback_profile), fallback=fallback_profile).name
    reasoning = str(context.get("source_reasoning_summary", "") or "").strip() or "Legacy-Fallback fuer Order Entry wurde verwendet."
    extras = {
        "source_channel": str(context.get("source_channel", "order_entry") or "order_entry"),
        "input_kind": str(context.get("input_kind", "other") or "other"),
        "document_guess": str(context.get("document_guess", "") or ""),
        "visible_context_hints": list(context.get("visible_context_hints", []) or []),
        "context_flags": dict(context.get("context_flags", {}) or {}),
        "planner_origin": "legacy_fallback",
    }
    if profile_name == "discord_ticket_sales":
        extras["channel_type"] = "discord_ticket"
        score = 100
    elif profile_name == "purchase_document_pdf":
        score = 90 if str(context.get("file_name", "") or "").lower().endswith(".pdf") else 82
    elif profile_name == "ecommerce_order_detail_visual":
        score = 92
    elif profile_name == "order_overview_visual":
        score = 88
    else:
        profile_name = "purchase_visual_generic" if profile_name not in {"discord_ticket_sales", "purchase_document_pdf", "ecommerce_order_detail_visual", "order_overview_visual"} else profile_name
        score = 72
    return (
        build_prompt_plan(
            prompt_class=profile_name,
            prompt_score=score,
            reasoning=reasoning,
            module_hint=module_hint,
            extras=extras,
        ),
        "legacy_order_entry_prompt_plan",
    )


def _build_mail_legacy_fallback_plan(context: Mapping[str, Any], module_hint: str) -> tuple[dict[str, Any], str]:
    source_plan = dict(context.get("source_plan", {}) or {})
    classification = str(source_plan.get("source_classification", "mail_dominant") or "mail_dominant")
    primary_type = str((source_plan.get("primary_visual_source") or {}).get("source_type", "") or "")
    secondary_type = str((source_plan.get("secondary_context_source") or {}).get("source_type", "") or "")
    reasoning = str(context.get("source_reasoning_summary", "") or source_plan.get("source_reasoning_summary", "") or "").strip()
    if classification == "hybrid":
        profile_name = "order_mail_hybrid"
        score = 96
        rule = "legacy_mail_prompt_plan_hybrid"
        reasoning = reasoning or "Legacy-Mail-Fallback nutzt weiter den Hybrid-Fall."
    elif classification == "pdf_dominant" or primary_type == "mail_attachment":
        profile_name = "order_mail_pdf_primary"
        score = 91
        rule = "legacy_mail_prompt_plan_pdf"
        reasoning = reasoning or "Legacy-Mail-Fallback nutzt weiter den PDF-dominierten Fall."
    else:
        profile_name = "order_mail_primary"
        has_visual = bool(primary_type or _first_descriptor(context.get("primary_candidates")))
        score = 89 if has_visual else 80
        rule = "legacy_mail_prompt_plan_primary"
        reasoning = reasoning or "Legacy-Mail-Fallback nutzt weiter den mail-dominanten Fall."
    return (
        build_prompt_plan(
            prompt_class=profile_name,
            prompt_score=score,
            reasoning=reasoning,
            module_hint=module_hint,
            extras={
                "source_channel": str(context.get("source_channel", "mail") or "mail"),
                "primary_type": primary_type,
                "secondary_type": secondary_type,
                "visible_context_hints": list(context.get("visible_context_hints", []) or []),
                "context_flags": dict(context.get("context_flags", {}) or {}),
                "planner_origin": "legacy_fallback",
            },
        ),
        rule,
    )


def _build_contextual_fallback_prompt_plan(
    context: Mapping[str, Any],
    scan_mode: str,
    module_hint: str,
    fallback_prompt_plan: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], str]:
    if isinstance(fallback_prompt_plan, Mapping) and dict(fallback_prompt_plan or {}):
        return dict(fallback_prompt_plan or {}), "explicit_legacy_fallback"
    channel = str(context.get("source_channel", "") or "").strip().lower()
    if channel == "order_entry":
        return _build_order_entry_legacy_fallback_plan(context, scan_mode, module_hint)
    if channel == "mail":
        return _build_mail_legacy_fallback_plan(context, module_hint)
    fallback_profile = "discord_ticket_sales" if str(scan_mode or "").strip().lower() == "verkauf" else "purchase_document_generic"
    return (
        build_prompt_plan(
            prompt_class=fallback_profile,
            prompt_score=_profile_score(fallback_profile, None),
            reasoning="Zentraler Planner konnte den Context noch nicht vollstaendig einordnen; generischer Legacy-Fallback wurde genutzt.",
            module_hint=module_hint,
            extras={"planner_origin": "legacy_fallback", "source_channel": channel},
        ),
        "generic_legacy_fallback",
    )


def plan_scan_from_context(
    scan_context: Mapping[str, Any] | None,
    *,
    scan_mode: str = "einkauf",
    module_hint: str = "",
    fallback_prompt_plan: Mapping[str, Any] | None = None,
    fallback_source_plan: Mapping[str, Any] | None = None,
    fallback_primary_visual_source: Mapping[str, Any] | None = None,
    fallback_secondary_context_source: Mapping[str, Any] | None = None,
    fallback_should_allow_second_pass: bool = False,
) -> ScanPlannerResult:
    context = dict(scan_context or {})
    channel = str(context.get("source_channel", "") or "").strip().lower()

    if channel == "order_entry":
        result = _resolve_order_entry_plan(context, scan_mode, module_hint, fallback_prompt_plan=fallback_prompt_plan)
        if result is not None:
            return result
    elif channel == "mail":
        result = _resolve_mail_plan(context, scan_mode, module_hint, fallback_prompt_plan=fallback_prompt_plan)
        if result is not None:
            return result

    resolved_fallback_plan, fallback_rule = _build_contextual_fallback_prompt_plan(
        context,
        scan_mode,
        module_hint,
        fallback_prompt_plan=fallback_prompt_plan,
    )
    decision = build_scan_decision_from_existing(
        prompt_plan=resolved_fallback_plan,
        scan_mode=scan_mode,
        source_plan=dict(fallback_source_plan or context.get("source_plan", {}) or {}),
        primary_visual_source=dict(fallback_primary_visual_source or context.get("primary_candidate", {}) or (context.get("source_plan", {}) or {}).get("primary_visual_source", {}) or {}),
        secondary_context_source=dict(fallback_secondary_context_source or (context.get("source_plan", {}) or {}).get("secondary_context_source", {}) or {}),
        should_allow_second_pass=bool(fallback_should_allow_second_pass),
    )
    return ScanPlannerResult(
        prompt_plan=resolved_fallback_plan,
        decision=decision,
        used_fallback=True,
        planner_rule=fallback_rule,
        context_channel=channel,
        reasoning_summary=str(decision.source_reasoning_summary or ""),
    )

