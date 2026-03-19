"""
Schlanker, provider-unabhaengiger KI-Kern.

Dieser Bereich enthaelt nur die gemeinsame Trennschicht:
- neutrale Typen fuer Scan-Requests und -Ergebnisse
- Basisschnittstelle fuer Provider
- Resolver/Factory fuer interne Provider-Aufloesung

Businesslogik und UI bleiben bewusst ausserhalb.
"""

from module.ai.provider import AiProvider
from module.ai.profiles import (
    apply_provider_profile_overrides,
    evaluate_second_pass_policy,
    get_default_profile_name,
    get_profile_status_hint,
    get_provider_profile_definition,
    list_provider_profiles,
    resolve_provider_profile,
    validate_provider_profile,
)
from module.ai.resolver import (
    build_provider_profile,
    get_known_provider_names,
    resolve_ai_provider,
)
from module.ai.types import (
    AiProviderError,
    BackoffPolicy,
    CostPolicy,
    ExecutionPolicy,
    InputPolicy,
    ProviderCapabilities,
    ProviderBehaviorPolicy,
    ProviderErrorInfo,
    ProviderProfile,
    QuotaStatus,
    RetryPolicy,
    ScanRequest,
    ScanResult,
    SecondPassPolicy,
    parse_strict_json_object,
)

__all__ = [
    "AiProvider",
    "AiProviderError",
    "apply_provider_profile_overrides",
    "BackoffPolicy",
    "ProviderCapabilities",
    "CostPolicy",
    "evaluate_second_pass_policy",
    "ExecutionPolicy",
    "get_default_profile_name",
    "ProviderErrorInfo",
    "ProviderProfile",
    "ProviderBehaviorPolicy",
    "get_profile_status_hint",
    "get_provider_profile_definition",
    "InputPolicy",
    "list_provider_profiles",
    "QuotaStatus",
    "resolve_provider_profile",
    "RetryPolicy",
    "ScanRequest",
    "ScanResult",
    "SecondPassPolicy",
    "validate_provider_profile",
    "build_provider_profile",
    "get_known_provider_names",
    "parse_strict_json_object",
    "resolve_ai_provider",
]
