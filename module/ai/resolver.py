from __future__ import annotations

from typing import Dict, Tuple

from module.ai.profiles import (
    get_default_profile_name,
    resolve_provider_profile,
    validate_provider_profile,
)
from module.ai.types import AiProviderError, ProviderCapabilities, ProviderProfile


_KNOWN_PROVIDER_NAMES: Tuple[str, ...] = ("gemini", "openai", "claude")
_DEFAULT_MODELS: Dict[str, str] = {
    "gemini": "gemini-2.5-flash",
    "openai": "gpt-5-mini",
    "claude": "claude-sonnet-4-20250514",
}


def normalize_provider_name(provider_name: str = "") -> str:
    normalized = str(provider_name or "").strip().lower()
    return normalized or "gemini"


def get_known_provider_names() -> Tuple[str, ...]:
    return _KNOWN_PROVIDER_NAMES


def build_provider_profile(
    provider_name: str = "gemini",
    profile_name: str = "",
    model_name: str = "",
    transport: str = "",
    overrides: Dict[str, object] | None = None,
    metadata: Dict[str, object] | None = None,
) -> ProviderProfile:
    normalized_provider = normalize_provider_name(provider_name)
    resolved_profile_name = str(profile_name or get_default_profile_name(normalized_provider) or f"{normalized_provider}_default").strip()
    resolved_profile = resolve_provider_profile(
        provider_name=normalized_provider,
        profile_name=resolved_profile_name,
        model_name=model_name,
        transport=transport,
        overrides=overrides,
        metadata=metadata,
    )
    resolved_model = str(resolved_profile.model_name or _DEFAULT_MODELS.get(normalized_provider, "")).strip()
    resolved_profile.model_name = resolved_model
    resolved_profile.capabilities = _build_default_capabilities(normalized_provider, resolved_model)
    validate_provider_profile(resolved_profile, expected_provider_name=normalized_provider)
    return resolved_profile


def _build_default_capabilities(provider_name: str, model_name: str) -> ProviderCapabilities:
    normalized_provider = normalize_provider_name(provider_name)
    if normalized_provider == "gemini":
        return ProviderCapabilities(
            provider_name=normalized_provider,
            model_name=model_name,
            supports_image_input=True,
            supports_pdf_input=True,
            supports_file_input=True,
            supports_response_mime_type=True,
            supports_response_schema=True,
            supports_system_instruction=True,
            supports_tools=True,
            supports_count_tokens=True,
            supported_transports=("native", "openai_compatible"),
        )
    if normalized_provider == "openai":
        return ProviderCapabilities(
            provider_name=normalized_provider,
            model_name=model_name,
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
    if normalized_provider == "claude":
        return ProviderCapabilities(
            provider_name=normalized_provider,
            model_name=model_name,
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
    return ProviderCapabilities(
        provider_name=normalized_provider,
        model_name=model_name,
        supports_image_input=True,
        supports_pdf_input=False,
        supports_file_input=False,
        supports_response_mime_type=True,
        supports_response_schema=True,
        supports_system_instruction=True,
        supports_tools=True,
        supports_count_tokens=False,
        supported_transports=("native",),
    )


def resolve_ai_provider(provider_name: str = "", profile: ProviderProfile | None = None):
    normalized_provider = normalize_provider_name(
        profile.provider_name if isinstance(profile, ProviderProfile) else provider_name
    )
    if isinstance(profile, ProviderProfile):
        validate_provider_profile(profile, expected_provider_name=normalized_provider)
    if normalized_provider == "gemini":
        from module.ai.providers.gemini import GeminiAiProvider

        model_name = ""
        if isinstance(profile, ProviderProfile):
            model_name = str(profile.model_name or "").strip()
        return GeminiAiProvider(model_name=model_name or _DEFAULT_MODELS["gemini"])

    if normalized_provider == "openai":
        from module.ai.providers.openai import OpenAIAiProvider

        model_name = ""
        if isinstance(profile, ProviderProfile):
            model_name = str(profile.model_name or "").strip()
        return OpenAIAiProvider(model_name=model_name or _DEFAULT_MODELS["openai"])

    if normalized_provider == "claude":
        from module.ai.providers.claude import ClaudeAiProvider

        model_name = ""
        if isinstance(profile, ProviderProfile):
            model_name = str(profile.model_name or "").strip()
        return ClaudeAiProvider(model_name=model_name or _DEFAULT_MODELS["claude"])

    raise AiProviderError(
        error_kind="provider_not_available",
        user_message=f"Der Provider '{normalized_provider}' ist nicht bekannt.",
        technical_message=f"unknown provider name: {normalized_provider}",
        provider_name=normalized_provider,
        service=normalized_provider,
        retryable=False,
    )
