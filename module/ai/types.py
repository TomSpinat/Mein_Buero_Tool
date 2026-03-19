from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class QuotaStatus:
    status: str = "unknown"
    limit_name: str = ""
    retry_after_sec: int = 0
    remaining: Optional[int] = None
    reset_at: str = ""
    scope: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


@dataclass
class ProviderCapabilities:
    provider_name: str
    model_name: str = ""
    supports_image_input: bool = True
    supports_pdf_input: bool = False
    supports_file_input: bool = False
    supports_response_mime_type: bool = False
    supports_response_schema: bool = False
    supports_system_instruction: bool = False
    supports_tools: bool = False
    supports_count_tokens: bool = False
    supported_transports: Tuple[str, ...] = ("native",)

    @property
    def supports_openai_compatible_transport(self) -> bool:
        return "openai_compatible" in tuple(self.supported_transports or ())

    def to_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


@dataclass(frozen=True)
class ExecutionPolicy:
    max_parallel_requests: int = 1
    serialize_requests: bool = True
    initial_delay_sec: float = 0.0
    request_spacing_sec: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    retry_on_rate_limit: bool = True
    retry_on_timeout: bool = True
    retry_on_network: bool = True
    retry_on_service_unavailable: bool = True
    retry_on_invalid_response: bool = False
    allow_same_input_repeat: bool = False
    max_same_input_repeats: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


@dataclass(frozen=True)
class BackoffPolicy:
    strategy: str = "fixed"
    initial_delay_sec: float = 0.0
    multiplier: float = 1.0
    max_delay_sec: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


@dataclass(frozen=True)
class SecondPassPolicy:
    mode: str = "forbidden"
    max_passes: int = 0
    require_secondary_source: bool = True
    require_missing_fields: bool = True
    allowed_missing_fields: Tuple[str, ...] = ()
    allowed_source_types: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


@dataclass(frozen=True)
class InputPolicy:
    preferred_input_strategy: str = "auto"
    screenshot_aggressiveness: str = "balanced"
    text_only_fallback: str = "when_no_file"
    upload_conservatism: str = "balanced"
    allow_additional_upload_pass: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


@dataclass(frozen=True)
class CostPolicy:
    expensive_repeat_mode: str = "avoid"
    max_extra_calls_per_item: int = 0
    max_upload_calls_per_item: int = 1
    prefer_single_pass: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


@dataclass(frozen=True)
class ProviderBehaviorPolicy:
    execution: ExecutionPolicy = field(default_factory=ExecutionPolicy)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    second_pass: SecondPassPolicy = field(default_factory=SecondPassPolicy)
    input: InputPolicy = field(default_factory=InputPolicy)
    cost: CostPolicy = field(default_factory=CostPolicy)

    def to_dict(self) -> Dict[str, Any]:
        return dict(asdict(self))


@dataclass
class ProviderProfile:
    provider_name: str
    profile_name: str = ""
    display_name: str = ""
    description: str = ""
    model_name: str = ""
    transport: str = "native"
    capabilities: Optional[ProviderCapabilities] = None
    policy: ProviderBehaviorPolicy = field(default_factory=ProviderBehaviorPolicy)
    status_hints: Dict[str, str] = field(default_factory=dict)
    base_profile_name: str = ""
    override_values: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_meta_dict(self) -> Dict[str, Any]:
        return {
            "provider_name": str(self.provider_name or ""),
            "profile_name": str(self.profile_name or ""),
            "display_name": str(self.display_name or ""),
            "description": str(self.description or ""),
            "model_name": str(self.model_name or ""),
            "transport": str(self.transport or ""),
            "capabilities": self.capabilities.to_dict() if isinstance(self.capabilities, ProviderCapabilities) else {},
            "policy": self.policy.to_dict() if isinstance(self.policy, ProviderBehaviorPolicy) else {},
            "status_hints": dict(self.status_hints or {}),
            "base_profile_name": str(self.base_profile_name or self.profile_name or ""),
            "override_values": dict(self.override_values or {}),
            "metadata": dict(self.metadata or {}),
        }


@dataclass
class ProviderErrorInfo:
    provider_name: str = ""
    error_kind: str = "invalid_response"
    user_message: str = "Antwort des KI-Dienstes ist ungueltig."
    technical_message: str = ""
    retryable: bool = False
    status_code: Optional[int] = None
    service: str = ""
    phase: str = ""
    field_name: str = ""
    quota_status: Optional[QuotaStatus] = None
    meta: Dict[str, Any] = field(default_factory=dict)


class AiProviderError(RuntimeError):
    """Neutrale Provider-Ausnahme mit schlanker Fehlerstruktur."""

    def __init__(
        self,
        error_kind: str,
        user_message: str,
        technical_message: str = "",
        provider_name: str = "",
        retryable: bool = False,
        status_code: Optional[int] = None,
        service: str = "",
        phase: str = "",
        field_name: str = "",
        quota_status: Optional[QuotaStatus] = None,
        meta: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(user_message)
        self.error_info = ProviderErrorInfo(
            provider_name=str(provider_name or ""),
            error_kind=str(error_kind or "invalid_response"),
            user_message=str(user_message or "Antwort des KI-Dienstes ist ungueltig."),
            technical_message=str(technical_message or ""),
            retryable=bool(retryable),
            status_code=status_code,
            service=str(service or provider_name or ""),
            phase=str(phase or ""),
            field_name=str(field_name or ""),
            quota_status=quota_status,
            meta=dict(meta or {}),
        )
        self.error_kind = self.error_info.error_kind
        self.user_message = self.error_info.user_message
        self.technical_message = self.error_info.technical_message
        self.provider = self.error_info.provider_name
        self.provider_name = self.error_info.provider_name
        self.retryable = self.error_info.retryable
        self.status_code = self.error_info.status_code
        self.service = self.error_info.service
        self.phase = self.error_info.phase
        self.field_name = self.error_info.field_name
        self.quota_status = self.error_info.quota_status


@dataclass
class ScanRequest:
    api_key: str
    image_path: Optional[str] = None
    custom_text: str = ""
    scan_mode: str = "einkauf"
    prompt_profile: str = ""
    prompt_plan: Optional[Dict[str, Any]] = None
    scan_decision: Optional[Dict[str, Any]] = None
    response_mime_type: str = "application/json"
    response_schema: Optional[Dict[str, Any]] = None
    system_instruction: str = ""
    tools: Optional[List[Dict[str, Any]]] = None
    transport: str = "native"
    model_name: Optional[str] = None
    profile: Optional[ProviderProfile] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def provider_name(self) -> str:
        if isinstance(self.profile, ProviderProfile):
            return str(self.profile.provider_name or "").strip()
        return ""


@dataclass
class ScanResult:
    payload: Dict[str, Any]
    provider_name: str = ""
    profile_name: str = ""
    model_name: str = ""
    raw_text: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    usage: Dict[str, Any] = field(default_factory=dict)
    prompt_feedback: Dict[str, Any] = field(default_factory=dict)
    finish_reason: str = ""
    safety_ratings: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[ProviderErrorInfo] = None

    @property
    def provider(self) -> str:
        return str(self.provider_name or "")

    @property
    def token_count(self) -> int:
        total = self.usage.get("total_tokens", 0)
        try:
            return int(total or 0)
        except Exception:
            return 0


def parse_strict_json_object(raw_text: str, provider_name: str = "", provider: str = "") -> Dict[str, Any]:
    """
    Strikter JSON-Parser ohne Markdown-Heuristiken.
    """
    resolved_provider = str(provider_name or provider or "").strip()
    text = str(raw_text or "").strip()
    if not text:
        raise AiProviderError(
            error_kind="empty_response",
            user_message="Die Modellantwort war leer.",
            technical_message="empty response text",
            provider_name=resolved_provider,
            service=resolved_provider,
        )

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AiProviderError(
            error_kind="invalid_response",
            user_message="Die Modellantwort war kein gueltiges JSON.",
            technical_message=f"json decode failed: {exc}",
            provider_name=resolved_provider,
            service=resolved_provider,
        ) from exc

    if not isinstance(parsed, dict):
        raise AiProviderError(
            error_kind="schema_violation",
            user_message="Die Modellantwort muss ein JSON-Objekt sein.",
            technical_message=f"parsed type: {type(parsed).__name__}",
            provider_name=resolved_provider,
            service=resolved_provider,
        )

    return parsed
