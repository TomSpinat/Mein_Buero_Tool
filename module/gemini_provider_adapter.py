"""
Gemini-Spezialisierung fuer den neutralen KI-Kern.

Dieses Modul kapselt ausschliesslich Gemini-spezifisches Verhalten:
- Request-Aufbau inkl. response_mime_type/response_schema/system_instruction
- Modell-Capabilities
- Safety-Handling
- Usage-/Token-Metadaten
- Response-Normalisierung in den neutralen ProviderResult
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import google.generativeai as genai

from module.ai_provider_core import (
    ProviderCapabilityProfile,
    ProviderRequest,
    ProviderResponseFormatError,
    ProviderResult,
    StructuredAiProvider,
    parse_strict_json_object,
)
from module.gemini_prompt_renderer import (
    GeminiPromptRenderResult,
    render_gemini_prompt_from_decision,
    resolve_prompt_profile_from_request,
    resolve_scan_decision_from_request,
)
from module.scan_output_contract import get_scan_output_schema_json


_DEFAULT_MODEL = "gemini-2.5-flash"
_GEMINI_CAPABILITIES = {
    "gemini-2.5-flash": ProviderCapabilityProfile(
        provider_name="gemini",
        model_name="gemini-2.5-flash",
        supports_response_mime_type=True,
        supports_response_schema=True,
        supports_system_instruction=True,
        supports_tools=True,
        supports_count_tokens=True,
        supports_openai_compatible_transport=False,
    ),
    "gemini-2.5-pro": ProviderCapabilityProfile(
        provider_name="gemini",
        model_name="gemini-2.5-pro",
        supports_response_mime_type=True,
        supports_response_schema=True,
        supports_system_instruction=True,
        supports_tools=True,
        supports_count_tokens=True,
        supports_openai_compatible_transport=False,
    ),
}


class GeminiStructuredProvider(StructuredAiProvider):
    provider_name = "gemini"

    def __init__(self, model_name: str = _DEFAULT_MODEL):
        self.model_name = str(model_name or _DEFAULT_MODEL).strip()

    def get_capabilities(self, model_name: Optional[str] = None) -> ProviderCapabilityProfile:
        chosen = str(model_name or self.model_name or _DEFAULT_MODEL).strip()
        return _GEMINI_CAPABILITIES.get(
            chosen,
            ProviderCapabilityProfile(
                provider_name="gemini",
                model_name=chosen,
                supports_response_mime_type=True,
                supports_response_schema=False,
                supports_system_instruction=True,
                supports_tools=False,
                supports_count_tokens=True,
                supports_openai_compatible_transport=False,
            ),
        )

    def analyze_document(self, request: ProviderRequest) -> ProviderResult:
        key = str(request.api_key or "").strip()
        if not key:
            raise ProviderResponseFormatError(
                error_kind="auth",
                user_message="Kein Gemini API-Key vorhanden.",
                technical_message="missing api key",
                provider=self.provider_name,
            )

        transport = str(request.transport or "native").strip().lower()
        if transport == "openai_compatible":
            return self._analyze_via_openai_compatible_transport(request)
        if transport != "native":
            raise ProviderResponseFormatError(
                error_kind="transport_not_available",
                user_message="Der angeforderte Gemini-Transport ist hier nicht verfuegbar.",
                technical_message=f"unsupported transport: {transport}",
                provider=self.provider_name,
            )

        model_name = str(request.model_name or self.model_name or _DEFAULT_MODEL).strip()
        caps = self.get_capabilities(model_name)

        genai.configure(api_key=key)
        model = self._build_model(model_name, request, caps)
        render_result = self._render_prompt_request(request)
        contents = self._build_contents(request, render_result.prompt)
        usage = self._count_tokens(model, contents, caps)
        response = self._generate_structured_response(model, contents, request, caps)

        response_dict = self._response_to_dict(response)
        prompt_feedback = self._extract_prompt_feedback(response_dict)
        finish_reason = self._extract_finish_reason(response_dict)
        safety_ratings = self._extract_safety_ratings(response_dict)
        self._raise_for_safety_block(prompt_feedback, finish_reason, safety_ratings)

        response_text = self._extract_response_text(response, response_dict)
        payload = parse_strict_json_object(response_text, provider=self.provider_name)
        usage_meta = self._merge_usage(usage, self._extract_usage_metadata(response, response_dict))

        return ProviderResult(
            payload=payload,
            provider=self.provider_name,
            raw_text=response_text,
            meta={
                "model_name": model_name,
                "capabilities": {
                    "response_mime_type": caps.supports_response_mime_type,
                    "response_schema": caps.supports_response_schema,
                    "system_instruction": caps.supports_system_instruction,
                    "tools": caps.supports_tools,
                    "count_tokens": caps.supports_count_tokens,
                    "transport": transport,
                },
                "prompt_profile": render_result.resolved_profile,
                "prompt_plan": dict(request.prompt_plan or {}) if isinstance(request.prompt_plan, dict) else {},
                "scan_decision": dict(render_result.decision or {}),
                "prompt_renderer": render_result.to_meta(),
            },
            usage=usage_meta,
            prompt_feedback=prompt_feedback,
            finish_reason=finish_reason,
            safety_ratings=safety_ratings,
        )

    def _analyze_via_openai_compatible_transport(self, request: ProviderRequest) -> ProviderResult:
        raise ProviderResponseFormatError(
            error_kind="transport_not_available",
            user_message="Ein OpenAI-kompatibler Gemini-Transport ist in diesem Projekt noch nicht aktiviert.",
            technical_message=f"transport=openai_compatible requested for scan_mode={request.scan_mode}",
            provider=self.provider_name,
        )

    def _build_model(self, model_name: str, request: ProviderRequest, caps: ProviderCapabilityProfile):
        kwargs: Dict[str, Any] = {}
        system_instruction = str(request.system_instruction or "").strip() or self._default_system_instruction(request.scan_mode)
        if system_instruction and caps.supports_system_instruction:
            kwargs["system_instruction"] = system_instruction

        try:
            return genai.GenerativeModel(model_name, **kwargs)
        except TypeError:
            return genai.GenerativeModel(model_name)

    def _render_prompt_request(self, request: ProviderRequest) -> GeminiPromptRenderResult:
        try:
            return render_gemini_prompt_from_decision(
                scan_mode=request.scan_mode,
                custom_text=request.custom_text,
                prompt_profile=request.prompt_profile,
                prompt_plan=request.prompt_plan if isinstance(request.prompt_plan, dict) else None,
                scan_decision=request.scan_decision if isinstance(request.scan_decision, dict) else None,
                schema_text=get_scan_output_schema_json(request.scan_mode),
            )
        except Exception:
            resolved_profile = resolve_prompt_profile_from_request(
                request.scan_mode,
                request.prompt_profile,
                request.prompt_plan if isinstance(request.prompt_plan, dict) else None,
                request.scan_decision if isinstance(request.scan_decision, dict) else None,
            )
            resolved_decision = resolve_scan_decision_from_request(
                request.scan_mode,
                request.prompt_profile,
                request.prompt_plan if isinstance(request.prompt_plan, dict) else None,
                request.scan_decision if isinstance(request.scan_decision, dict) else None,
            )
            legacy_prompt = self._build_legacy_fallback_prompt(
                request.scan_mode,
                request.custom_text,
            )
            return GeminiPromptRenderResult(
                prompt=legacy_prompt,
                resolved_profile=resolved_profile,
                decision=dict(resolved_decision or {}),
                renderer_name="legacy_gemini_adapter",
                used_fallback=True,
            )

    def _build_contents(self, request: ProviderRequest, rendered_prompt: str = "") -> List[Any]:
        prompt = str(rendered_prompt or "").strip() or self._build_legacy_fallback_prompt(
            request.scan_mode,
            request.custom_text,
        )
        contents: List[Any] = [prompt]

        image_path = str(request.image_path or "").strip()
        if image_path:
            if not os.path.exists(image_path):
                raise ProviderResponseFormatError(
                    error_kind="input_error",
                    user_message="Die uebergebene Datei fuer den KI-Scan wurde nicht gefunden.",
                    technical_message=f"file not found: {image_path}",
                    provider=self.provider_name,
                )
            upload_ref = genai.upload_file(path=image_path)
            contents.append(upload_ref)
        return contents

    def _count_tokens(self, model, contents: List[Any], caps: ProviderCapabilityProfile) -> Dict[str, Any]:
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        if not caps.supports_count_tokens:
            return usage
        try:
            counted = model.count_tokens(contents)
            total = int(getattr(counted, "total_tokens", 0) or 0)
            usage["prompt_tokens"] = total
            usage["total_tokens"] = total
            return usage
        except Exception:
            estimated = max(1, len(str(contents)) // 4)
            usage["prompt_tokens"] = estimated
            usage["total_tokens"] = estimated
            return usage

    def _generate_structured_response(self, model, contents: List[Any], request: ProviderRequest, caps: ProviderCapabilityProfile):
        generation_config: Dict[str, Any] = {"temperature": 0}
        if caps.supports_response_mime_type and request.response_mime_type:
            generation_config["response_mime_type"] = str(request.response_mime_type)
        if caps.supports_response_schema and request.response_schema:
            generation_config["response_schema"] = request.response_schema

        kwargs: Dict[str, Any] = {"generation_config": generation_config}
        tools = request.tools or []
        if tools and caps.supports_tools:
            kwargs["tools"] = tools

        try:
            return model.generate_content(contents, **kwargs)
        except TypeError:
            fallback_kwargs: Dict[str, Any] = {"generation_config": {"temperature": 0}}
            if generation_config.get("response_mime_type"):
                fallback_kwargs["generation_config"]["response_mime_type"] = generation_config["response_mime_type"]
            try:
                return model.generate_content(contents, **fallback_kwargs)
            except TypeError:
                return model.generate_content(contents)

    def _response_to_dict(self, response) -> Dict[str, Any]:
        if hasattr(response, "to_dict"):
            try:
                payload = response.to_dict() or {}
                if isinstance(payload, dict):
                    return payload
            except Exception:
                pass
        return {}

    def _extract_prompt_feedback(self, response_dict: Dict[str, Any]) -> Dict[str, Any]:
        feedback = response_dict.get("promptFeedback", {})
        return feedback if isinstance(feedback, dict) else {}

    def _extract_finish_reason(self, response_dict: Dict[str, Any]) -> str:
        candidates = response_dict.get("candidates", []) if isinstance(response_dict, dict) else []
        if isinstance(candidates, list) and candidates:
            first = candidates[0] if isinstance(candidates[0], dict) else {}
            return str(first.get("finishReason", "") or "").strip()
        return ""

    def _extract_safety_ratings(self, response_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        candidates = response_dict.get("candidates", []) if isinstance(response_dict, dict) else []
        ratings: List[Dict[str, Any]] = []
        if isinstance(candidates, list) and candidates:
            first = candidates[0] if isinstance(candidates[0], dict) else {}
            raw = first.get("safetyRatings", [])
            if isinstance(raw, list):
                ratings = [item for item in raw if isinstance(item, dict)]
        return ratings

    def _raise_for_safety_block(self, prompt_feedback: Dict[str, Any], finish_reason: str, safety_ratings: List[Dict[str, Any]]):
        block_reason = str(prompt_feedback.get("blockReason", "") or "").strip()
        finish = str(finish_reason or "").strip().upper()
        blocked_ratings = []
        for item in safety_ratings:
            if str(item.get("blocked", "")).strip().lower() == "true" or item.get("blocked") is True:
                blocked_ratings.append(str(item.get("category", "") or "unknown"))

        if block_reason or finish in {"SAFETY", "BLOCKED", "PROHIBITED_CONTENT"} or blocked_ratings:
            detail = []
            if block_reason:
                detail.append(f"blockReason={block_reason}")
            if finish:
                detail.append(f"finishReason={finish}")
            if blocked_ratings:
                detail.append("categories=" + ",".join(blocked_ratings))
            raise ProviderResponseFormatError(
                error_kind="safety_blocked",
                user_message="Gemini hat die Antwort aus Sicherheitsgruenden blockiert.",
                technical_message="; ".join(detail) or "safety block without detail",
                provider=self.provider_name,
            )

    def _extract_response_text(self, response, response_dict: Dict[str, Any]) -> str:
        direct = str(getattr(response, "text", "") or "").strip()
        if direct:
            return direct

        candidates = response_dict.get("candidates", []) if isinstance(response_dict, dict) else []
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            content = candidate.get("content", {})
            if not isinstance(content, dict):
                continue
            parts = content.get("parts", [])
            if not isinstance(parts, list):
                continue
            for part in parts:
                if not isinstance(part, dict):
                    continue
                text = str(part.get("text", "") or "").strip()
                if text:
                    return text

        raise ProviderResponseFormatError(
            error_kind="empty_response",
            user_message="Gemini hat keine auswertbare strukturierte Antwort geliefert.",
            technical_message="missing response text and candidate parts",
            provider=self.provider_name,
        )

    def _extract_usage_metadata(self, response, response_dict: Dict[str, Any]) -> Dict[str, Any]:
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

        usage_meta = getattr(response, "usage_metadata", None)
        if usage_meta is not None:
            usage["prompt_tokens"] = int(getattr(usage_meta, "prompt_token_count", 0) or 0)
            usage["completion_tokens"] = int(getattr(usage_meta, "candidates_token_count", 0) or 0)
            usage["total_tokens"] = int(getattr(usage_meta, "total_token_count", 0) or 0)
            return usage

        raw = response_dict.get("usageMetadata", {}) if isinstance(response_dict, dict) else {}
        if isinstance(raw, dict):
            usage["prompt_tokens"] = int(raw.get("promptTokenCount", 0) or 0)
            usage["completion_tokens"] = int(raw.get("candidatesTokenCount", 0) or 0)
            usage["total_tokens"] = int(raw.get("totalTokenCount", 0) or 0)
        return usage

    def _merge_usage(self, counted_usage: Dict[str, Any], response_usage: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(counted_usage or {})
        for key, value in (response_usage or {}).items():
            if int(value or 0) > 0:
                merged[key] = int(value or 0)
        if int(merged.get("total_tokens", 0) or 0) <= 0:
            merged["total_tokens"] = int(merged.get("prompt_tokens", 0) or 0) + int(merged.get("completion_tokens", 0) or 0)
        return merged

    def _default_system_instruction(self, scan_mode: str) -> str:
        mode = str(scan_mode or "einkauf").strip().lower()
        if mode == "verkauf":
            return (
                "Du bist ein praeziser Extraktionsdienst fuer Verkaufstickets und Chat-Screenshots. "
                "Antworte ausschliesslich strukturiert, ohne Freitext, ohne Erfindungen."
            )
        return (
            "Du bist ein praeziser Extraktionsdienst fuer Einkaufsbelege, Bestelluebersichten und Bestellmails. "
            "Arbeite quelltreu: primaere visuelle Quelle zuerst, Zusatzquelle nur ergaenzend. "
            "Antworte ausschliesslich strukturiert, ohne Freitext, ohne Erfindungen."
        )

    # Legacy-Fallback: nur noch ein kleiner generischer Notfall-Renderer, falls der zentrale Renderer unerwartet scheitert.
    def _build_legacy_fallback_prompt(self, scan_mode: str, custom_text: str = "") -> str:
        mode = str(scan_mode or "einkauf").strip().lower()
        schema_text = get_scan_output_schema_json(mode)
        custom = str(custom_text or "").strip()
        if mode == "verkauf":
            return f"""
Du analysierst eine Verkaufsquelle im Notfall-Fallback.

Regeln:
1) Keine erfundenen Werte.
2) Antworte nur als gueltiges JSON-Objekt ohne Markdown.
3) Nutze exakt die Schluessel aus dem Schema.
4) Produkte, Mengen und Preise nur uebernehmen, wenn sie klar sichtbar oder klar genannt sind.
5) Fehlende Werte leer lassen.

Zusatztext vom Nutzer:
{custom}

Interner Ausgabevertrag (JSON-Schema-Hinweis):
{schema_text}
""".strip()
        return f"""
Du analysierst eine Einkaufsquelle im Notfall-Fallback.

Regeln:
1) Keine erfundenen Werte.
2) Antworte nur als gueltiges JSON-Objekt ohne Markdown.
3) Nutze exakt die Schluessel aus dem Schema.
4) Nutze den Zusatztext als wichtigste Kontextquelle fuer Primaer-/Sekundaerquelle, Tracking und Dokumenthinweise.
5) Produkte, Bilder und Tracking nur uebernehmen, wenn sie klar sichtbar oder klar genannt sind.
6) Fehlende Werte leer lassen.

Zusatztext vom Nutzer:
{custom}

Interner Ausgabevertrag (JSON-Schema-Hinweis):
{schema_text}
""".strip()

