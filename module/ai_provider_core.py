"""
Kompatibilitaets-Wrapper fuer den neuen neutralen KI-Kern.

Der eigentliche provider-unabhaengige Kern liegt jetzt unter `module.ai`.
Diese Datei bleibt nur fuer bestehende Importe erhalten.
"""

from module.ai import (
    AiProvider,
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


ProviderResponseFormatError = AiProviderError
ProviderCapabilityProfile = ProviderCapabilities
ProviderRequest = ScanRequest
ProviderResult = ScanResult
StructuredAiProvider = AiProvider

__all__ = [
    "AiProvider",
    "AiProviderError",
    "BackoffPolicy",
    "CostPolicy",
    "ExecutionPolicy",
    "InputPolicy",
    "ProviderCapabilities",
    "ProviderCapabilityProfile",
    "ProviderBehaviorPolicy",
    "ProviderErrorInfo",
    "ProviderProfile",
    "ProviderRequest",
    "ProviderResponseFormatError",
    "ProviderResult",
    "QuotaStatus",
    "RetryPolicy",
    "ScanRequest",
    "ScanResult",
    "SecondPassPolicy",
    "StructuredAiProvider",
    "parse_strict_json_object",
]
