from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from module.gemini_prompt_renderer import (
    render_gemini_prompt_from_decision,
    resolve_prompt_profile_from_request,
    resolve_scan_decision_from_request,
)
from module.scan_output_contract import get_scan_output_schema_json


@dataclass
class ScanPrompt:
    prompt: str
    resolved_profile: str = ""
    decision: Dict[str, Any] = field(default_factory=dict)
    renderer_name: str = "neutral_scan_prompt"
    used_fallback: bool = False

    def to_meta(self) -> Dict[str, Any]:
        return {
            "resolved_profile": str(self.resolved_profile or ""),
            "renderer_name": str(self.renderer_name or "neutral_scan_prompt"),
            "used_fallback": bool(self.used_fallback),
        }


def build_scan_prompt(
    scan_mode: str,
    custom_text: str = "",
    prompt_profile: str = "",
    prompt_plan: Optional[Dict[str, Any]] = None,
    scan_decision: Optional[Dict[str, Any]] = None,
) -> ScanPrompt:
    mode = str(scan_mode or "einkauf").strip().lower()
    try:
        rendered = render_gemini_prompt_from_decision(
            scan_mode=mode,
            custom_text=custom_text,
            prompt_profile=prompt_profile,
            prompt_plan=prompt_plan if isinstance(prompt_plan, dict) else None,
            scan_decision=scan_decision if isinstance(scan_decision, dict) else None,
            schema_text=get_scan_output_schema_json(mode),
        )
        return ScanPrompt(
            prompt=str(rendered.prompt or "").strip(),
            resolved_profile=str(rendered.resolved_profile or ""),
            decision=dict(rendered.decision or {}),
            renderer_name="shared_scan_prompt_renderer",
            used_fallback=bool(rendered.used_fallback),
        )
    except Exception:
        resolved_profile = resolve_prompt_profile_from_request(
            mode,
            prompt_profile,
            prompt_plan if isinstance(prompt_plan, dict) else None,
            scan_decision if isinstance(scan_decision, dict) else None,
        )
        resolved_decision = resolve_scan_decision_from_request(
            mode,
            prompt_profile,
            prompt_plan if isinstance(prompt_plan, dict) else None,
            scan_decision if isinstance(scan_decision, dict) else None,
        )
        return ScanPrompt(
            prompt=_build_legacy_fallback_prompt(mode, custom_text),
            resolved_profile=resolved_profile,
            decision=dict(resolved_decision or {}),
            renderer_name="shared_scan_prompt_fallback",
            used_fallback=True,
        )


def _build_legacy_fallback_prompt(scan_mode: str, custom_text: str = "") -> str:
    mode = str(scan_mode or "einkauf").strip().lower()
    schema_json = get_scan_output_schema_json(mode)
    mode_hint = "Bestell- oder Rechnungsbeleg" if mode == "einkauf" else "Verkaufsbeleg"
    prompt_parts = [
        f"Analysiere den vorliegenden {mode_hint}.",
        "Antworte ausschliesslich als JSON-Objekt ohne Markdown, ohne Codeblock und ohne Freitext.",
        "Halte dich exakt an dieses Schema:",
        schema_json,
    ]
    extra = str(custom_text or "").strip()
    if extra:
        prompt_parts.append("Zusaetzlicher Kontext:")
        prompt_parts.append(extra)
    return "\n\n".join(part for part in prompt_parts if str(part or "").strip())
