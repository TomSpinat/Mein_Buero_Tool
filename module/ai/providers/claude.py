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


_DEFAULT_MODEL = "claude-sonnet-4-20250514"
_DEFAULT_TIMEOUT_SEC = 90
_DEFAULT_ANTHROPIC_VERSION = "2023-06-01"


class ClaudeAiProvider(AiProvider):
    provider_name = "claude"

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
            supports_file_input=False,
            supports_response_mime_type=False,
            supports_response_schema=False,
            supports_system_instruction=True,
            supports_tools=False,
            supports_count_tokens=False,
            supported_transports=("native",),
        )

    def analyze_scan(self, request: ScanRequest) -> ScanResult:
        try:
            key = str(request.api_key or "").strip()
            if not key:
                raise AiProviderError(
                    error_kind="auth",
                    user_message="Kein Claude API-Key vorhanden.",
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
            payload = self._build_payload(request, prompt, asset, model_name)
            headers = self._build_headers(key, request, profile)

            response = post_json(endpoint, payload=payload, headers=headers, timeout_sec=timeout_sec)
            raw_text = self._extract_response_text(response.payload)
            normalized_payload = parse_strict_json_object(raw_text, provider_name=self.provider_name)
            usage = self._extract_usage(response.payload)
            finish_reason = str(response.payload.get("stop_reason", "") or response.payload.get("stop_sequence", "") or "").strip()

            return ScanResult(
                payload=normalized_payload,
                provider_name=self.provider_name,
                profile_name=str((profile.profile_name if profile else "") or request.prompt_profile or ""),
                model_name=str(response.payload.get("model", "") or model_name),
                raw_text=raw_text,
                meta={
                    "endpoint": endpoint,
                    "response_id": str(response.payload.get("id", "") or ""),
                    "type": str(response.payload.get("type", "") or ""),
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
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "model": model_name,
            "max_tokens": self._resolve_numeric_metadata(request, "max_output_tokens", 4096),
            "messages": [
                {
                    "role": "user",
                    "content": self._build_user_content(prompt.prompt, asset),
                }
            ],
        }

        system_instruction = str(request.system_instruction or "").strip() or self._default_system_instruction(request.scan_mode)
        if system_instruction:
            payload["system"] = system_instruction
        return payload

    def _build_user_content(self, prompt_text: str, asset: Optional[LocalInputAsset]) -> List[Dict[str, Any]]:
        blocks: List[Dict[str, Any]] = []
        if asset is not None:
            if asset.is_image:
                blocks.append(
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": asset.mime_type,
                            "data": asset.base64_data,
                        },
                    }
                )
            elif asset.is_pdf:
                blocks.append(
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": asset.base64_data,
                        },
                    }
                )
            else:
                raise AiProviderError(
                    error_kind="upload_error",
                    user_message="Claude kann in diesem Schritt nur Bilder oder PDFs fuer den Scan verarbeiten.",
                    technical_message=f"unsupported file type: {asset.mime_type}",
                    provider_name=self.provider_name,
                    service=self.provider_name,
                    phase="input",
                )
        blocks.append({"type": "text", "text": str(prompt_text or "").strip()})
        return blocks

    def _extract_response_text(self, payload: Dict[str, Any]) -> str:
        text_parts: List[str] = []
        for block in payload.get("content", []) if isinstance(payload.get("content", []), list) else []:
            if not isinstance(block, dict):
                continue
            if str(block.get("type", "") or "").strip().lower() != "text":
                continue
            text = str(block.get("text", "") or "").strip()
            if text:
                text_parts.append(text)

        joined = "\n".join(part for part in text_parts if part).strip()
        if joined:
            return joined

        raise AiProviderError(
            error_kind="empty_response",
            user_message="Claude hat keine auswertbare strukturierte Antwort geliefert.",
            technical_message="missing text content blocks",
            provider_name=self.provider_name,
            service=self.provider_name,
            phase="response",
        )

    def _extract_usage(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        usage_raw = payload.get("usage", {}) if isinstance(payload, dict) else {}
        if not isinstance(usage_raw, dict):
            return {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        prompt_tokens = self._safe_int(usage_raw.get("input_tokens", 0))
        completion_tokens = self._safe_int(usage_raw.get("output_tokens", 0))
        return {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        }

    def _build_headers(
        self,
        api_key: str,
        request: ScanRequest,
        profile: Optional[ProviderProfile],
    ) -> Dict[str, str]:
        headers = {
            "x-api-key": api_key,
            "anthropic-version": self._resolve_text_metadata(request, profile, "anthropic_version") or _DEFAULT_ANTHROPIC_VERSION,
        }
        beta_value = self._resolve_beta_header(request, profile)
        if beta_value:
            headers["anthropic-beta"] = beta_value
        extra_headers = self._resolve_dict_metadata(request, profile, "default_headers")
        for key, value in extra_headers.items():
            headers[str(key)] = str(value)
        return headers

    def _resolve_beta_header(self, request: ScanRequest, profile: Optional[ProviderProfile]) -> str:
        raw = request.metadata.get("anthropic_beta") if isinstance(request.metadata, dict) else None
        if raw in (None, "") and isinstance(profile, ProviderProfile) and isinstance(profile.metadata, dict):
            raw = profile.metadata.get("anthropic_beta")
        if isinstance(raw, (list, tuple)):
            cleaned = [str(item).strip() for item in raw if str(item).strip()]
            return ",".join(cleaned)
        return str(raw or "").strip()

    def _resolve_endpoint(self, request: ScanRequest, profile: Optional[ProviderProfile]) -> str:
        explicit = self._resolve_text_metadata(request, profile, "endpoint")
        if explicit:
            return explicit
        base_url = self._resolve_text_metadata(request, profile, "base_url") or "https://api.anthropic.com"
        return base_url.rstrip("/") + "/v1/messages"

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
                user_message="Die Anfrage an Claude hat zu lange gedauert.",
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
                    user_message="Die Anfrage an Claude hat zu lange gedauert.",
                    technical_message=str(exc),
                    provider_name=self.provider_name,
                    service=self.provider_name,
                    phase=phase,
                    retryable=True,
                )
            return AiProviderError(
                error_kind="network",
                user_message="Claude ist ueber das Netzwerk nicht erreichbar.",
                technical_message=str(exc),
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                retryable=True,
            )
        return AiProviderError(
            error_kind="unknown",
            user_message="Claude konnte die Anfrage nicht erfolgreich verarbeiten.",
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
        retry_after = self._retry_after_seconds(exc.headers)
        reset_hint = self._extract_reset_hint(exc.headers)

        if exc.status_code in (401, 403) or error_type in {"authentication_error", "permission_error"}:
            return AiProviderError(
                error_kind="auth",
                user_message="Claude hat den Zugriff abgelehnt. Bitte API-Key pruefen.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
            )

        if (exc.status_code == 429 or error_type == "rate_limit_error") and (
            "credit balance" in lowered
            or "quota" in lowered
            or "usage limit" in lowered
            or "billing" in lowered
        ):
            return AiProviderError(
                error_kind="quota_exhausted",
                user_message="Claude-Kontingent ist aktuell erschoepft.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code or 429,
                retryable=False,
                quota_status=QuotaStatus(
                    status="exhausted",
                    limit_name="quota",
                    retry_after_sec=retry_after,
                    reset_at=reset_hint,
                    scope="provider",
                ),
            )

        if exc.status_code == 429 or error_type == "rate_limit_error":
            return AiProviderError(
                error_kind="rate_limit",
                user_message="Claude hat das Anfrage-Limit erreicht. Bitte spaeter erneut versuchen.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code or 429,
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
                user_message="Die Anfrage an Claude hat zu lange gedauert.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
                retryable=True,
            )

        if exc.status_code == 413 or error_type == "request_too_large":
            return AiProviderError(
                error_kind="request_too_large",
                user_message="Die Anfrage an Claude ist zu gross.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code or 413,
            )

        if any(fragment in lowered for fragment in ("context window", "prompt is too long", "too many tokens", "context length")):
            return AiProviderError(
                error_kind="context_too_large",
                user_message="Die Eingabe ist zu gross fuer das gewaehlte Claude-Modell.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
            )

        if any(fragment in lowered for fragment in ("pdf", "document", "image", "base64", "media_type", "file")):
            return AiProviderError(
                error_kind="upload_error",
                user_message="Die Datei konnte nicht sauber an Claude uebergeben werden.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
            )

        if exc.status_code >= 500 or error_type in {"api_error", "overloaded_error"}:
            return AiProviderError(
                error_kind="service_unavailable",
                user_message="Claude ist aktuell nicht erreichbar. Bitte spaeter erneut versuchen.",
                technical_message=message,
                provider_name=self.provider_name,
                service=self.provider_name,
                phase=phase,
                status_code=exc.status_code,
                retryable=True,
            )

        return AiProviderError(
            error_kind="input_error",
            user_message="Claude konnte die Anfrage in dieser Form nicht verarbeiten.",
            technical_message=message,
            provider_name=self.provider_name,
            service=self.provider_name,
            phase=phase,
            status_code=exc.status_code,
            retryable=False,
        )

    def _retry_after_seconds(self, headers: Dict[str, str]) -> int:
        return self._parse_retry_seconds(self._header_value(headers, "retry-after"))

    def _extract_reset_hint(self, headers: Dict[str, str]) -> str:
        return self._header_value(
            headers,
            "anthropic-ratelimit-requests-reset",
            "anthropic-ratelimit-tokens-reset",
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
