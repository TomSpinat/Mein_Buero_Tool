"""Kompatibilitaets-Wrapper fuer den neutralen Provider-Kern."""

from module.ai_provider_core import (
    ProviderCapabilityProfile,
    ProviderRequest,
    ProviderResponseFormatError,
    ProviderResult,
    StructuredAiProvider,
    parse_strict_json_object,
)


StructuredLlmProvider = StructuredAiProvider
