from __future__ import annotations

import socket
import urllib.error
from typing import Any, Dict, List, Optional

from module.ai.files import LocalInputAsset, load_local_input_asset
from module.ai.http import HttpJsonRequestError, post_json
from module.ai.prompting import ScanPrompt, build_scan_prompt
from module.ai.provider import AiProvider
from module.ai.types import (
    AiProviderError,
    ProviderCapabilities,
    ProviderProfile,
    QuotaStatus,
    ScanRequest,
    ScanResult,
    parse_strict_json_object,
)


_DEFAULT_MODEL = "gpt-5-mini"
_DEFAULT_TIMEOUT_SEC = 90


class OpenAIAiProvider(AiProvider):
    provider_name = "openai"

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
        return ProviderCapabilities(
            provider_name=self.provider_name,
            model_name=chosen,
            supports_image_input=True,
            supports_pdf_input=True,
            supports_file_input=True,
            supports_response_mime_type=False,
            supports_response_schema=True,
            supports_system_instruction=True,
            supports_tools=True,
            supports_count_tokens=False,
            supported_transports=("native",),
        )

    def analyze_scan(self, request: ScanRequest) -> ScanResult:
        try:
            key = str(request.api_key or "").strip()
            if not key:
                raise AiProviderError(
                    error_kind="auth",
                    user_message="Kein OpenAI API-Key vorhanden.",
                    technical_message="missing api key",
                    provider_name=self.provider_name,
                    service=self.provider_name,
                )

            profile = request.profile if isinstance(request.profile, ProviderProfile) else None
            model_name = str(
                request.model_name
                or (profile.model_name if profile else "")
                or self.model_name
                or _DEFAULT_MODEL
            ).strip()
            caps = self.get_capabilities(profile=profile, model_name=model_name)
            prompt = self._build_prompt(request)
            asset = self._load_asset(request)
            endpoint = self._resolve_endpoint(request, profile)
            timeout_sec = self._resolve_timeout_sec(request, profile)
            payload = self._build_payload(request, prompt, asset, model_name, caps)
            headers = self._build_headers(key, request, profile)

            response = post_json(endpoint, payload=payload, headers=headers, timeout_sec=timeout_sec)
            raw_text = self._extract_response_text(response.payload)
            normalized_payload = parse_strict_json_object(raw_text, provider_name=self.provider_name)
            usage = self._extract_usage(response.payload)
            finish_reason = self._extract_finish_reason(response.payload)

            return ScanResult(
                payload=normalized_payload,
                provider_name=self.provider_name,
                profile_name=str((profile.profile_name if profile else "") or request.prompt_profile or ""),
                model_name=str(response.payload.get("model", "") or model_name),
                raw_text=raw_text,
                meta={
                    "endpoint": endpoint,
                    "response_id": str(response.payload.get("id", "") or ""),
                    "status": str(response.payload.get("status", "") or ""),
                    "incomplete_details": dict(response.payload.get("incomplete_details", {}) or {}),
                    "provider_profile": {
                        "provider_name": self.provider_name,
                        "profile_name": str((profile.profile_name if profile else "") or ""),
                        "model_name": model_name,
                        "transport": "native",
                    },
                    "effective_profile": profile.to_meta_dict() if isinstance(profile, ProviderProfile) else {},
                    "capabilities": {
                        "image_input": caps.supports_image_input,
                        "pdf_input": caps.supports_pdf_input,
                        "file_input": caps.supports_file_input,
                        "response_schema": caps.supports_response_schema,
                        "system_instruction": caps.supports_system_instruction,
                        "tools": caps.supports_tools,
                    },
                    "prompt_profile": prompt.resolved_profile,
                    "prompt_plan": dict(request.prompt_plan or {}) if isinstance(request.prompt_plan, dict) else {},
                    "scan_decision": dict(prompt.decision or {}),
                    "prompt_renderer": prompt.to_meta(),
                },
                usage=usage,
                prompt_feedback={},
                finish_reason=finish_reason,
                safety_ratings=[],
            )
        except AiProviderError:
            raise
        except Exception as exc:
            raise self._normalize_exception(exc, phase="request") from exc

    def _build_prompt(self, request: ScanRequest) -> ScanPrompt:
        return build_scan_prompt(
            scan_mode=request.scan_mode,
            custom_text=request.custom_text,
            prompt_profile=request.prompt_profile,
            prompt_plan=request.prompt_plan if isinstance(request.prompt_plan, dict) else None,
            scan_decision=request.scan_decision if isinstance(request.scan_decision, dict) else None,
        )

    def _load_asset(self, request: ScanRequest) -> Optional[LocalInputAsset]:
        image_path = str(request.image_path or "").strip()
        if not image_path:
            return None
        return load_local_input_asset(image_path, provider_name=self.provider_name)

    def _build_payload(
        self,
        request: ScanRequest,
        prompt: ScanPrompt,
        asset: Optional[LocalInputAsset],
        model_name: str,
        caps: ProviderCapabilities,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "model": model_name,
            "input": [
                {
                    "role": "user",
                    "content": self._build_user_content(prompt.prompt, asset),
                }
            ],
        }

        system_instruction = str(request.system_instruction or "").strip() or self._default_system_instruction(request.scan_mode)
        if system_instruction and caps.supports_system_instruction:
            payload["instructions"] = system_instruction

        if request.response_schema and caps.supports_response_schema:
            payload["text"] = {
                "format": {
                    "type": "json_schema",
                    "name": "mail_scan_result",
                    "schema": dict(request.response_schema or {}),
                    "strict": True,
                }
            }

        tools = request.tools or []
        if tools and caps.supports_tools:
            payload["tools"] = list(tools)

        max_output_tokens = self._resolve_numeric_metadata(request, "max_output_tokens", 4096)
        if max_output_tokens > 0:
            payload["max_output_tokens"] = max_output_tokens
        return payload

    def _build_user_content(self, prompt_text: str, asset: Optional[LocalInputAsset]) -> List[Dict[str, Any]]:
        blocks: List[Dict[str, Any]] = []
        if asset is not None:
            if asset.is_image:
                blocks.append(
                    {
                        "type": "input_image",
                        "image_url": asset.data_url,
                    }
                )
            else:
                blocks.append(
                    {
                        "type": "input_file",
                        "filename": asset.filename,
                        "file_data": asset.data_url,
                    }
                )
        blocks.append({"type": "input_text", "text": str(prompt_text or "").strip()})
        return blocks

    def _extract_response_text(self, payload: Dict[str, Any]) -> str:
        direct = str(payload.get("output_text", "") or "").strip()
        if direct:
            return direct

        text_parts: List[str] = []
        refusal_parts: List[str] = []
        for item in payload.get("output", []) if isinstance(payload.get("output", []), list) else []:
            if not isinstance(item, dict):
                continue
            for block in item.get("content", []) if isinstance(item.get("content", []), list) else []:
                if not isinstance(block, dict):
                    continue
                block_type = str(block.get("type", "") or "").strip().lower()
                if block_type in {"output_text", "text"}:
                    text = str(block.get("text", "") or "").strip()
                    if text:
                        text_parts.append(text)
                elif block_type == "refusal":
                    refusal = str(block.get("refusal", "") or block.get("text", "") or "").strip()
                    if refusal:
                        refusal_parts.append(refusal)

        joined = "\n".join(part for part in text_parts if part).strip()
        if joined:
            return joined

        if refusal_parts:
            raise AiProviderError(
                error_kind="safety_blocked",
                user_message="OpenAI hat die Antwort aus Sicherheitsgruenden verweigert.",
                technical_message=" | ".join(refusal_parts),
                provider_name=self.provider_name,
                service=self.provider_name,
                phase="response",
            )

        raise AiProviderError(
            error_kind="empty_response",
            user_message="OpenAI hat keine auswertbare strukturierte Antwort geliefert.",
            technical_message="missing output_text and assistant content",
            provider_name=self.provider_name,
            service=self.provider_name,
            phase="response",
        )

    def _extract_usage(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        usage_raw = payload.get("usage", {}) if isinstance(payload, dict) else {}
        if not isinstance(usage_raw, dict):
            return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

        prompt_tokens = self._safe_int(usage_raw.get("input_tokens", usage_raw.get("prompt_tokens", 0)))
        completion_tokens = self._safe_int(usage_raw.get("output_tokens", usage_raw.get("completion_tokens", 0)))
        total_tokens = self._safe_int(usage_raw.get("total_tokens", prompt_tokens + completion_tokens))
        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }

    def _extract_finish_reason(self, payload: Dict[str, Any]) -> str:
        incomplete = payload.get("incomplete_details", {}) if isinstance(payload, dict) else {}
        if isinstance(incomplete, dict):
            reason = str(incomplete.get("reason", "") or "").strip()
            if reason:
                return reason
        return str(payload.get("status", "") or "").strip()

    def _build_headers(
        self,
        api_key: str,
        request: ScanRequest,
        profile: Optional[ProviderProfile],
    ) -> Dict[str, str]:
        headers = {
            "Authorization": f"Bearer {api_key}",
        }
        organization = self._resolve_text_metadata(request, profile, "organization")
        if organization:
            headers["OpenAI-Organization"] = organization
        project = self._resolve_text_metadata(request, profile, "project")
        if project:
            headers["OpenAI-Project"] = project
        extra_headers = self._resolve_dict_metadata(request, profile, "default_headers")
        for key, value in extra_headers.items():
            headers[str(key)] = str(value)
        return headers

    def _resolve_endpoint(self, request: ScanRequest, profile: Optional[ProviderProfile]) -> str:
        explicit = self._resolve_text_metadata(request, profile, "endpoint")
        if explicit:
            return explicit
        base_url = self._resolve_text_metadata(request, profile, "base_url") or "https://api.openai.com"
        return base_url.rstrip("/") + "/v1/responses"

    def _resolve_timeout_sec(self, request: ScanRequest, profile: Optional[ProviderProfile]) -> int:
        request_value = request.metadata.get("timeout_sec") if isinstance(request.metadata, dict) else None
        if request_value not in (None, ""):
            try:
                return max(1, int(request_value))
            except Exception:
                return _DEFAULT_TIMEOUT_SEC
        if isinstance(profile, ProviderProfile) and isinstance(profile.metadata, dict):
            profile_value = profile.metadata.get("timeout_sec")
            try:
                return max(1, int(profile_value))
            except Exception:
                return _DEFAULT_TIMEOUT_SEC
        return _DEFAULT_TIMEOUT_SEC

    def _resolve_text_metadata(self, request: ScanRequest, profile: Optional[ProviderProfile], key: str) -> str:
        request_value = request.metadata.get(key) if isinstance(request.metadata, dict) else None
        if request_value not in (None, ""):
            return str(request_value).strip()
        if isinstance(profile, ProviderProfile) and isinstance(profile.metadata, dict):
            profile_value = profile.metadata.get(key)
            if profile_value not in (None, ""):
                return str(profile_value).strip()
        return ""

    def _resolve_dict_metadata(self, request: ScanRequest, profile: Optional[ProviderProfile], key: str) -> Dict[str, Any]:
        request_value = request.metadata.get(key) if isinstance(request.metadata, dict) else None
        if isinstance(request_value, dict):
            return dict(request_value)
        if isinstance(profile, ProviderProfile) and isinstance(profile.metadata, dict):
            profile_value = profile.metadata.get(key)
            if isinstance(profile_value, dict):
                return dict(profile_value)
        return {}

    def _resolve_numeric_metadata(self, request: ScanRequest, key: str, default: int) -> int:
        value = request.metadata.get(key) if isinstance(request.metadata, dict) else None
        if value not in (None, ""):
            try:
                return max(1, int(value))
            except Exception:
                return int(default)
        return int(default)

    def _normalize_exception(self, exc: Exception, phase: str = "request") -> AiProviderError:
        if isinstance(exc, AiProviderError):
            return exc
        if isinstance(exc, HttpJsonRequestError):
            return self._normalize_http_error(exc, phase=phase)
        if isinstance(exc, (socket.timeout, TimeoutError)):
            return AiProviderError(
                error_kind="timeout",
                user_message="Die Anfrage an OpenAI hat zu lange gedauert.",
                technical_message=str(exc),
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
                    user_message="Die Anfrage an OpenAI hat zu lange gedauert.",
                    technical_message=str(exc),
                    provider_name=self.provider_name,
                    service=self.provider_name,
                    phase=phase,
                    retryable=True,
                )
            return AiProviderError(
                error_kind="network",
                user_message="OpenAI ist ueber das Netzwerk nicht erreichbar.",
                technical_message=str(exc),
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                retryable=True,
            )
        return AiProviderError(
            error_kind="unknown",
            user_message="OpenAI konnte die Anfrage nicht erfolgreich verarbeiten.",
            technical_message=str(exc),
            provider_name=self.provider_name,
            service=self.provider_name,
            phase=phase,
            retryable=False,
        )

    def _normalize_http_error(self, exc: HttpJsonRequestError, phase: str = "request") -> AiProviderError:
        error_payload = exc.payload.get("error", {}) if isinstance(exc.payload, dict) else {}
        message = str(error_payload.get("message", "") or exc.body_text or exc).strip()
        lowered = message.lower()
        error_type = str(error_payload.get("type", "") or "").strip().lower()
        error_code = str(error_payload.get("code", "") or "").strip().lower()
        retry_after = self._retry_after_seconds(exc.headers)
        reset_hint = self._extract_reset_hint(exc.headers)

        if exc.status_code in (401, 403) or error_type in {"authentication_error", "invalid_api_key"}:
            return AiProviderError(
                error_kind="auth",
                user_message="OpenAI hat den Zugriff abgelehnt. Bitte API-Key pruefen.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
            )

        if exc.status_code == 429 and (
            error_code in {"insufficient_quota", "billing_hard_limit_reached"}
            or "insufficient quota" in lowered
            or "billing hard limit" in lowered
            or "exceeded your current quota" in lowered
        ):
            return AiProviderError(
                error_kind="quota_exhausted",
                user_message="OpenAI-Kontingent ist aktuell erschoepft.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
                retryable=False,
                quota_status=QuotaStatus(
                    status="exhausted",
                    limit_name=error_code or "quota",
                    retry_after_sec=retry_after,
                    reset_at=reset_hint,
                    scope="provider",
                ),
            )

        if exc.status_code == 429:
            return AiProviderError(
                error_kind="rate_limit",
                user_message="OpenAI hat das Anfrage-Limit erreicht. Bitte spaeter erneut versuchen.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
                retryable=True,
                quota_status=QuotaStatus(
                    status="limited" if retry_after <= 0 else "waiting_reset",
                    limit_name="requests",
                    retry_after_sec=retry_after,
                    reset_at=reset_hint,
                    scope="provider",
                ),
            )

        if exc.status_code in (408, 504) or "timeout" in lowered:
            return AiProviderError(
                error_kind="timeout",
                user_message="Die Anfrage an OpenAI hat zu lange gedauert.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
                retryable=True,
            )

        if exc.status_code == 413 or any(fragment in lowered for fragment in ("request too large", "payload too large")):
            return AiProviderError(
                error_kind="request_too_large",
                user_message="Die Anfrage an OpenAI ist zu gross.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code or 413,
            )

        if any(fragment in lowered for fragment in ("context length", "maximum context length", "too many tokens", "prompt is too long", "context window")):
            return AiProviderError(
                error_kind="context_too_large",
                user_message="Die Eingabe ist zu gross fuer das gewaehlte OpenAI-Modell.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
            )

        if any(fragment in lowered for fragment in ("image", "pdf", "file", "base64", "mime", "unsupported file")) or error_code in {"invalid_image", "invalid_file"}:
            return AiProviderError(
                error_kind="upload_error",
                user_message="Die Datei konnte nicht sauber an OpenAI uebergeben werden.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
            )

        if exc.status_code >= 500:
            return AiProviderError(
                error_kind="service_unavailable",
                user_message="OpenAI ist aktuell nicht erreichbar. Bitte spaeter erneut versuchen.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
                retryable=True,
            )

        if any(fragment in lowered for fragment in ("json schema", "response_format", "invalid schema")):
            return AiProviderError(
                error_kind="input_error",
                user_message="Die strukturierte OpenAI-Anfrage ist ungueltig aufgebaut.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
            )

        return AiProviderError(
            error_kind="input_error",
            user_message="OpenAI konnte die Anfrage in dieser Form nicht verarbeiten.",
            technical_message=message,
            provider_name=self.provider_name,
            service=self.provider_name,
            phase=phase,
            status_code=exc.status_code,
            retryable=False,
        )

    def _retry_after_seconds(self, headers: Dict[str, str]) -> int:
        value = self._header_value(headers, "retry-after")
        return self._parse_retry_seconds(value)

    def _extract_reset_hint(self, headers: Dict[str, str]) -> str:
        return self._header_value(
            headers,
            "x-ratelimit-reset-requests",
            "x-ratelimit-reset-tokens",
            "x-ratelimit-reset",
        )

    def _header_value(self, headers: Dict[str, str], *names: str) -> str:
        wanted = {str(name or "").strip().lower() for name in names if str(name or "").strip()}
        for key, value in dict(headers or {}).items():
            if str(key).strip().lower() in wanted:
                return str(value or "").strip()
        return ""

    def _parse_retry_seconds(self, raw_value: str) -> int:
        text = str(raw_value or "").strip().lower()
        if not text:
            return 0
        try:
            return max(0, int(float(text)))
        except Exception:
            pass
        digits = "".join(ch for ch in text if ch.isdigit())
        if not digits:
            return 0
        try:
            return max(0, int(digits))
        except Exception:
            return 0

    def _default_system_instruction(self, scan_mode: str) -> str:
        mode = str(scan_mode or "einkauf").strip().lower()
        if mode == "verkauf":
            return "Antworte nur mit gueltigem JSON passend zum Verkaufsschema."
        return "Antworte nur mit gueltigem JSON passend zum Einkaufsschema."

    def _safe_int(self, value: Any) -> int:
        try:
            return int(value or 0)
        except Exception:
            return 0
