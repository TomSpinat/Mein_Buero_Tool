"""
Gemini-Adapter fuer den neutralen KI-Kern.

Dieser Adapter bleibt provider-spezifisch und kennt nur:
- Gemini Request-Aufbau
- Prompt-Rendering fuer Gemini
- Gemini-spezifische Antwort-/Safety-Metadaten
"""

from __future__ import annotations

import socket
import urllib.error
from typing import Any, Dict, List, Optional

import google.generativeai as genai

from module.ai.files import load_local_input_asset
from module.ai.provider import AiProvider
from module.ai.prompting import ScanPrompt, build_scan_prompt
from module.ai.types import (
    AiProviderError,
    ProviderCapabilities,
    ProviderProfile,
    QuotaStatus,
    ScanRequest,
    ScanResult,
    parse_strict_json_object,
)


_DEFAULT_MODEL = "gemini-2.5-flash"
_GEMINI_CAPABILITIES = {
    "gemini-2.5-flash": ProviderCapabilities(
        provider_name="gemini",
        model_name="gemini-2.5-flash",
        supports_image_input=True,
        supports_pdf_input=True,
        supports_file_input=True,
        supports_response_mime_type=True,
        supports_response_schema=True,
        supports_system_instruction=True,
        supports_tools=True,
        supports_count_tokens=True,
        supported_transports=("native", "openai_compatible"),
    ),
    "gemini-2.5-pro": ProviderCapabilities(
        provider_name="gemini",
        model_name="gemini-2.5-pro",
        supports_image_input=True,
        supports_pdf_input=True,
        supports_file_input=True,
        supports_response_mime_type=True,
        supports_response_schema=True,
        supports_system_instruction=True,
        supports_tools=True,
        supports_count_tokens=True,
        supported_transports=("native", "openai_compatible"),
    ),
}


class GeminiAiProvider(AiProvider):
    provider_name = "gemini"

    def __init__(self, model_name: str = _DEFAULT_MODEL):
        self.model_name = str(model_name or _DEFAULT_MODEL).strip()

    def get_capabilities(
        self,
        profile: Optional[ProviderProfile] = None,
        model_name: Optional[str] = None,
    ) -> ProviderCapabilities:
        chosen = str(
            model_name
            or (profile.model_name if isinstance(profile, ProviderProfile) else "")
            or self.model_name
            or _DEFAULT_MODEL
        ).strip()
        return _GEMINI_CAPABILITIES.get(
            chosen,
            ProviderCapabilities(
                provider_name="gemini",
                model_name=chosen,
                supports_image_input=True,
                supports_pdf_input=True,
                supports_file_input=True,
                supports_response_mime_type=True,
                supports_response_schema=False,
                supports_system_instruction=True,
                supports_tools=False,
                supports_count_tokens=True,
                supported_transports=("native", "openai_compatible"),
            ),
        )

    def analyze_scan(self, request: ScanRequest) -> ScanResult:
        try:
            key = str(request.api_key or "").strip()
            if not key:
                raise AiProviderError(
                    error_kind="auth",
                    user_message="Kein Gemini API-Key vorhanden.",
                    technical_message="missing api key",
                    provider_name=self.provider_name,
                    service=self.provider_name,
                )

            profile = request.profile if isinstance(request.profile, ProviderProfile) else None
            transport = str(
                request.transport or (profile.transport if profile else "") or "native"
            ).strip().lower()
            if transport == "openai_compatible":
                return self._analyze_via_openai_compatible_transport(request)
            if transport != "native":
                raise AiProviderError(
                    error_kind="transport_not_available",
                    user_message="Der angeforderte Gemini-Transport ist hier nicht verfuegbar.",
                    technical_message=f"unsupported transport: {transport}",
                    provider_name=self.provider_name,
                    service=self.provider_name,
                )

            model_name = str(
                request.model_name
                or (profile.model_name if profile else "")
                or self.model_name
                or _DEFAULT_MODEL
            ).strip()
            caps = self.get_capabilities(profile=profile, model_name=model_name)

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
            payload = parse_strict_json_object(response_text, provider_name=self.provider_name)
            usage_meta = self._merge_usage(usage, self._extract_usage_metadata(response, response_dict))

            return ScanResult(
                payload=payload,
                provider_name=self.provider_name,
                profile_name=str((profile.profile_name if profile else "") or request.prompt_profile or ""),
                model_name=model_name,
                raw_text=response_text,
                meta={
                    "model_name": model_name,
                    "capabilities": {
                        "image_input": caps.supports_image_input,
                        "pdf_input": caps.supports_pdf_input,
                        "file_input": caps.supports_file_input,
                        "response_mime_type": caps.supports_response_mime_type,
                        "response_schema": caps.supports_response_schema,
                        "system_instruction": caps.supports_system_instruction,
                        "tools": caps.supports_tools,
                        "count_tokens": caps.supports_count_tokens,
                        "transports": list(caps.supported_transports),
                        "transport": transport,
                    },
                    "provider_profile": {
                        "provider_name": self.provider_name,
                        "profile_name": str((profile.profile_name if profile else "") or ""),
                        "model_name": model_name,
                        "transport": transport,
                    },
                    "effective_profile": profile.to_meta_dict() if isinstance(profile, ProviderProfile) else {},
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
        except AiProviderError:
            raise
        except Exception as exc:
            raise self._normalize_exception(exc, phase="request") from exc

    def _analyze_via_openai_compatible_transport(self, request: ScanRequest) -> ScanResult:
        raise AiProviderError(
            error_kind="transport_not_available",
            user_message="Ein OpenAI-kompatibler Gemini-Transport ist in diesem Projekt noch nicht aktiviert.",
            technical_message=f"transport=openai_compatible requested for scan_mode={request.scan_mode}",
            provider_name=self.provider_name,
            service=self.provider_name,
        )

    def _build_model(self, model_name: str, request: ScanRequest, caps: ProviderCapabilities):
        kwargs: Dict[str, Any] = {}
        system_instruction = str(request.system_instruction or "").strip() or self._default_system_instruction(request.scan_mode)
        if system_instruction and caps.supports_system_instruction:
            kwargs["system_instruction"] = system_instruction

        try:
            return genai.GenerativeModel(model_name, **kwargs)
        except TypeError:
            return genai.GenerativeModel(model_name)

    def _render_prompt_request(self, request: ScanRequest) -> ScanPrompt:
        return build_scan_prompt(
            scan_mode=request.scan_mode,
            custom_text=request.custom_text,
            prompt_profile=request.prompt_profile,
            prompt_plan=request.prompt_plan if isinstance(request.prompt_plan, dict) else None,
            scan_decision=request.scan_decision if isinstance(request.scan_decision, dict) else None,
        )

    def _build_contents(self, request: ScanRequest, rendered_prompt: str = "") -> List[Any]:
        prompt = str(rendered_prompt or "").strip()
        if not prompt:
            prompt = str(request.custom_text or "").strip() or "Analysiere das Dokument und antworte nur mit JSON."
        contents: List[Any] = [prompt]

        image_path = str(request.image_path or "").strip()
        if image_path:
            asset = load_local_input_asset(image_path, provider_name=self.provider_name)
            try:
                upload_ref = genai.upload_file(path=asset.path)
            except Exception as exc:
                raise self._normalize_exception(exc, phase="upload") from exc
            contents.append(upload_ref)
        return contents

    def _count_tokens(self, model, contents: List[Any], caps: ProviderCapabilities) -> Dict[str, Any]:
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

    def _generate_structured_response(self, model, contents: List[Any], request: ScanRequest, caps: ProviderCapabilities):
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
                try:
                    return model.generate_content(contents)
                except Exception as exc:
                    raise self._normalize_exception(exc, phase="generate") from exc
            except Exception as exc:
                raise self._normalize_exception(exc, phase="generate") from exc
        except Exception as exc:
            raise self._normalize_exception(exc, phase="generate") from exc

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
            raise AiProviderError(
                error_kind="safety_blocked",
                user_message="Gemini hat die Antwort aus Sicherheitsgruenden blockiert.",
                technical_message="; ".join(detail) or "safety block without detail",
                provider_name=self.provider_name,
                service=self.provider_name,
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

        raise AiProviderError(
            error_kind="empty_response",
            user_message="Gemini hat keine auswertbare strukturierte Antwort geliefert.",
            technical_message="missing response text and candidate parts",
            provider_name=self.provider_name,
            service=self.provider_name,
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

    def _merge_usage(self, counted: Dict[str, Any], actual: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(counted or {})
        for key, value in dict(actual or {}).items():
            if value not in (None, "", 0):
                merged[key] = value
        if not merged.get("total_tokens"):
            merged["total_tokens"] = int(merged.get("prompt_tokens", 0) or 0) + int(merged.get("completion_tokens", 0) or 0)
        return merged

    def _default_system_instruction(self, scan_mode: str) -> str:
        mode = str(scan_mode or "einkauf").strip().lower()
        if mode == "verkauf":
            return "Antworte nur mit gueltigem JSON passend zum Verkaufsschema."
        return "Antworte nur mit gueltigem JSON passend zum Einkaufsschema."

    def _normalize_exception(self, exc: Exception, phase: str = "request") -> AiProviderError:
        if isinstance(exc, AiProviderError):
            return exc

        text = str(exc or "").strip()
        lowered = text.lower()
        status_code = self._extract_status_code(exc)

        if self._contains_any(lowered, ("daily limit", "daily quota", "per day", "quota exhausted", "insufficient quota")):
            retry_after = self._extract_retry_after_seconds(text)
            return AiProviderError(
                error_kind="quota_exhausted",
                user_message="Gemini-Kontingent ist aktuell erschoepft.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=status_code or 429,
                retryable=False,
                quota_status=QuotaStatus(
                    status="exhausted",
                    limit_name="quota",
                    retry_after_sec=retry_after,
                    scope="provider",
                ),
            )

        if status_code in (401, 403) or self._contains_any(lowered, ("api key", "unauthorized", "forbidden", "permission denied", "authentication")):
            return AiProviderError(
                error_kind="auth",
                user_message="Gemini hat den Zugriff abgelehnt. Bitte API-Key pruefen.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=status_code,
            )

        if status_code == 429 or self._contains_any(lowered, ("quota", "rate limit", "resource exhausted", "too many requests")):
            retry_after = self._extract_retry_after_seconds(text)
            quota_status = QuotaStatus(
                status="limited" if retry_after <= 0 else "waiting_reset",
                limit_name="requests",
                retry_after_sec=retry_after,
                scope="provider",
            )
            return AiProviderError(
                error_kind="rate_limit",
                user_message="Gemini hat das Anfrage-Limit erreicht. Bitte spaeter erneut versuchen.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=status_code or 429,
                retryable=True,
                quota_status=quota_status,
            )

        if status_code == 413 or self._contains_any(lowered, ("request too large", "payload too large", "too many tokens", "context length", "prompt too long")):
            return AiProviderError(
                error_kind="request_too_large",
                user_message="Die Anfrage an Gemini ist zu gross fuer dieses Modell.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=status_code or 413,
            )

        if phase == "upload" or self._contains_any(lowered, ("file", "upload", "mime", "pdf", "image")):
            return AiProviderError(
                error_kind="upload_error",
                user_message="Die Datei konnte nicht sauber an Gemini uebergeben werden.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=status_code,
                retryable=bool(status_code and status_code >= 500),
            )

        if status_code is not None and status_code >= 500:
            return AiProviderError(
                error_kind="service_unavailable",
                user_message="Gemini ist aktuell nicht erreichbar. Bitte spaeter erneut versuchen.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=status_code,
                retryable=True,
            )

        if isinstance(exc, (socket.timeout, TimeoutError)):
            return AiProviderError(
                error_kind="timeout",
                user_message="Die Anfrage an Gemini hat zu lange gedauert.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                retryable=True,
            )

        if isinstance(exc, urllib.error.URLError):
            reason = getattr(exc, "reason", None)
            if isinstance(reason, (socket.timeout, TimeoutError)):
                return AiProviderError(
                    error_kind="timeout",
                    user_message="Die Anfrage an Gemini hat zu lange gedauert.",
                    technical_message=text,
                    provider_name=self.provider_name,
                    service=self.provider_name,
                    phase=phase,
                    retryable=True,
                )
            return AiProviderError(
                error_kind="network",
                user_message="Gemini ist ueber das Netzwerk nicht erreichbar.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                retryable=True,
            )

        if self._contains_any(lowered, ("timeout", "timed out", "deadline exceeded")):
            return AiProviderError(
                error_kind="timeout",
                user_message="Die Anfrage an Gemini hat zu lange gedauert.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                retryable=True,
            )

        if self._contains_any(lowered, ("connection", "dns", "network", "unavailable", "reset by peer")):
            return AiProviderError(
                error_kind="network",
                user_message="Gemini ist ueber das Netzwerk nicht erreichbar.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                retryable=True,
            )

        if self._contains_any(lowered, ("json", "schema", "parse", "format")):
            return AiProviderError(
                error_kind="invalid_response",
                user_message="Gemini hat keine gueltige strukturierte Antwort geliefert.",
                technical_message=text,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                retryable=False,
            )

        return AiProviderError(
            error_kind="unknown",
            user_message="Gemini konnte die Anfrage nicht erfolgreich verarbeiten.",
            technical_message=text,
            provider_name=self.provider_name,
            service=self.provider_name,
            phase=phase,
            retryable=False,
            status_code=status_code,
        )

    def _extract_status_code(self, exc: Exception) -> Optional[int]:
        raw_code = getattr(exc, "code", None)
        if raw_code not in (None, ""):
            try:
                return int(raw_code)
            except Exception:
                pass

        raw_status = getattr(exc, "status_code", None)
        if raw_status not in (None, ""):
            try:
                return int(raw_status)
            except Exception:
                pass

        for token in str(exc or "").replace("(", " ").replace(")", " ").split():
            if token.isdigit():
                try:
                    value = int(token)
                except Exception:
                    continue
                if 400 <= value <= 599:
                    return value
        return None

    def _extract_retry_after_seconds(self, text: str) -> int:
        digits = ""
        for token in str(text or "").replace("=", " ").replace(":", " ").split():
            cleaned = "".join(ch for ch in token if ch.isdigit())
            if cleaned:
                digits = cleaned
                break
        try:
            return max(0, int(digits or 0))
        except Exception:
            return 0

    def _contains_any(self, text: str, needles) -> bool:
        value = str(text or "").lower()
        return any(str(item).lower() in value for item in needles)


GeminiStructuredProvider = GeminiAiProvider
