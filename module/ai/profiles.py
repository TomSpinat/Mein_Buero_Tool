from __future__ import annotations

from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
from typing import Any, Dict, Mapping

from module.ai.types import (
    AiProviderError,
    BackoffPolicy,
    CostPolicy,
    ExecutionPolicy,
    InputPolicy,
    ProviderBehaviorPolicy,
    ProviderProfile,
    RetryPolicy,
    SecondPassPolicy,
)


_DEFAULT_PROFILE_BY_PROVIDER = {
    "gemini": "gemini_paid_standard",
    "openai": "openai_standard",
    "claude": "claude_document_first",
}

_PROFILE_DEFINITIONS: Dict[str, ProviderProfile] = {
    "gemini_free_conservative": ProviderProfile(
        provider_name="gemini",
        profile_name="gemini_free_conservative",
        display_name="Gemini Free konservativ",
        description="Vorsichtiges Gemini-Profil fuer knappe Limits und moeglichst wenige Zusatzaufrufe.",
        model_name="gemini-2.5-flash",
        transport="native",
        policy=ProviderBehaviorPolicy(
            execution=ExecutionPolicy(
                max_parallel_requests=1,
                serialize_requests=True,
                initial_delay_sec=2.0,
                request_spacing_sec=3.0,
            ),
            retry=RetryPolicy(
                max_attempts=2,
                retry_on_rate_limit=True,
                retry_on_timeout=True,
                retry_on_network=True,
                retry_on_service_unavailable=True,
                retry_on_invalid_response=False,
                allow_same_input_repeat=False,
                max_same_input_repeats=0,
            ),
            backoff=BackoffPolicy(
                strategy="exponential",
                initial_delay_sec=6.0,
                multiplier=2.0,
                max_delay_sec=45.0,
            ),
            second_pass=SecondPassPolicy(
                mode="forbidden",
                max_passes=0,
                require_secondary_source=True,
                require_missing_fields=True,
                allowed_missing_fields=(),
                allowed_source_types=(),
            ),
            input=InputPolicy(
                preferred_input_strategy="pdf_first",
                screenshot_aggressiveness="low",
                text_only_fallback="when_no_file",
                upload_conservatism="conservative",
                allow_additional_upload_pass=False,
            ),
            cost=CostPolicy(
                expensive_repeat_mode="avoid",
                max_extra_calls_per_item=0,
                max_upload_calls_per_item=1,
                prefer_single_pass=True,
            ),
        ),
        status_hints={
            "second_pass_forbidden": "Aktives Free-Profil unterdrueckt den zweiten KI-Pass.",
            "parallelism_reduced": "Konservatives Gemini-Free-Profil reduziert Parallelitaet und Zusatzaufrufe.",
            "pdf_preferred": "Konservatives Free-Profil bevorzugt PDFs vor Screenshots.",
            "text_fallback": "Text-only-Fallback wird nur ohne nutzbare Datei erlaubt.",
        },
        metadata={
            "profile_family": "starter",
            "provider_tier": "free",
            "gemini_media_resolution": "medium",
            "supports_future_overrides": True,
            "safe_adjustments": {
                "second_pass_mode": ("off", "missing_required"),
                "retry_level": ("low", "normal"),
                "parallel_level": ("conservative", "normal"),
                "screenshot_level": ("sparse", "auto"),
                "document_focus": ("prefer_pdf", "strong_pdf"),
                "limit_behavior": ("reduce", "normal"),
            },
        },
    ),
    "gemini_paid_standard": ProviderProfile(
        provider_name="gemini",
        profile_name="gemini_paid_standard",
        display_name="Gemini Paid Standard",
        description="Ausgewogenes Gemini-Profil fuer normalen Mail-Scan mit genau einem moeglichen Ergaenzungspass.",
        model_name="gemini-2.5-flash",
        transport="native",
        policy=ProviderBehaviorPolicy(
            execution=ExecutionPolicy(
                max_parallel_requests=2,
                serialize_requests=False,
                initial_delay_sec=1.0,
                request_spacing_sec=1.0,
            ),
            retry=RetryPolicy(
                max_attempts=2,
                retry_on_rate_limit=True,
                retry_on_timeout=True,
                retry_on_network=True,
                retry_on_service_unavailable=True,
                retry_on_invalid_response=False,
                allow_same_input_repeat=False,
                max_same_input_repeats=0,
            ),
            backoff=BackoffPolicy(
                strategy="exponential",
                initial_delay_sec=3.0,
                multiplier=2.0,
                max_delay_sec=20.0,
            ),
            second_pass=SecondPassPolicy(
                mode="conditional",
                max_passes=1,
                require_secondary_source=True,
                require_missing_fields=True,
                allowed_missing_fields=("waren", "waren_unvollstaendig", "bestellnummer", "preise"),
                allowed_source_types=("mail_attachment", "email_message"),
            ),
            input=InputPolicy(
                preferred_input_strategy="auto",
                screenshot_aggressiveness="balanced",
                text_only_fallback="when_no_file",
                upload_conservatism="balanced",
                allow_additional_upload_pass=True,
            ),
            cost=CostPolicy(
                expensive_repeat_mode="standard",
                max_extra_calls_per_item=1,
                max_upload_calls_per_item=2,
                prefer_single_pass=False,
            ),
        ),
        status_hints={
            "second_pass_conditional": "Aktives Gemini-Profil erlaubt nur einen knappen Ergaenzungspass bei klar passenden Luecken.",
            "parallelism_reduced": "Standardprofil haelt die Parallelitaet moderat, um Aussetzer zu vermeiden.",
            "text_fallback": "Text-only-Fallback bleibt ein Rueckfall, nicht der Hauptweg.",
            "second_pass_saved": "Zweiter Pass wurde eingespart, weil Pass 1 schon genug Material hatte.",
        },
        metadata={
            "profile_family": "starter",
            "provider_tier": "paid",
            "gemini_media_resolution": "medium",
            "supports_future_overrides": True,
            "safe_adjustments": {
                "second_pass_mode": ("off", "missing_required", "expanded"),
                "retry_level": ("low", "normal", "high"),
                "parallel_level": ("conservative", "normal", "increased"),
                "screenshot_level": ("sparse", "auto", "expanded"),
                "document_focus": ("standard", "prefer_pdf", "strong_pdf"),
                "limit_behavior": ("reduce", "normal", "exhaust"),
            },
        },
    ),
    "openai_standard": ProviderProfile(
        provider_name="openai",
        profile_name="openai_standard",
        display_name="OpenAI Standard",
        description="Ausgewogenes OpenAI-Profil fuer gemischte Mail-, Bild- und PDF-Faelle ohne aggressive Wiederholungen.",
        model_name="gpt-5-mini",
        transport="native",
        policy=ProviderBehaviorPolicy(
            execution=ExecutionPolicy(
                max_parallel_requests=2,
                serialize_requests=False,
                initial_delay_sec=0.5,
                request_spacing_sec=0.75,
            ),
            retry=RetryPolicy(
                max_attempts=2,
                retry_on_rate_limit=True,
                retry_on_timeout=True,
                retry_on_network=True,
                retry_on_service_unavailable=True,
                retry_on_invalid_response=False,
                allow_same_input_repeat=False,
                max_same_input_repeats=0,
            ),
            backoff=BackoffPolicy(
                strategy="exponential",
                initial_delay_sec=2.0,
                multiplier=2.0,
                max_delay_sec=18.0,
            ),
            second_pass=SecondPassPolicy(
                mode="conditional",
                max_passes=1,
                require_secondary_source=True,
                require_missing_fields=True,
                allowed_missing_fields=("waren", "waren_unvollstaendig", "bestellnummer", "preise"),
                allowed_source_types=("mail_attachment", "mail_render_screenshot"),
            ),
            input=InputPolicy(
                preferred_input_strategy="auto",
                screenshot_aggressiveness="balanced",
                text_only_fallback="when_no_file",
                upload_conservatism="balanced",
                allow_additional_upload_pass=True,
            ),
            cost=CostPolicy(
                expensive_repeat_mode="standard",
                max_extra_calls_per_item=1,
                max_upload_calls_per_item=2,
                prefer_single_pass=False,
            ),
        ),
        status_hints={
            "second_pass_conditional": "OpenAI-Standard erlaubt einen gezielten Zusatzpass nur bei passenden Luecken.",
            "parallelism_reduced": "Parallelitaet bleibt begrenzt, um Lastspitzen klein zu halten.",
            "text_fallback": "Text-only-Fallback bleibt nur ein Rueckfall.",
        },
        metadata={
            "profile_family": "starter",
            "provider_tier": "standard",
            "supports_future_overrides": True,
            "safe_adjustments": {
                "second_pass_mode": ("off", "missing_required", "expanded"),
                "retry_level": ("low", "normal", "high"),
                "parallel_level": ("conservative", "normal", "increased"),
                "screenshot_level": ("sparse", "auto", "expanded"),
                "document_focus": ("standard", "prefer_pdf"),
                "limit_behavior": ("reduce", "normal", "exhaust"),
            },
        },
    ),
    "claude_document_first": ProviderProfile(
        provider_name="claude",
        profile_name="claude_document_first",
        display_name="Claude Document First",
        description="Dokumentenorientiertes Claude-Profil mit PDF-Vorrang und bewusst gebremsten Zusatzaufrufen.",
        model_name="claude-sonnet-4-20250514",
        transport="native",
        policy=ProviderBehaviorPolicy(
            execution=ExecutionPolicy(
                max_parallel_requests=1,
                serialize_requests=True,
                initial_delay_sec=1.5,
                request_spacing_sec=1.5,
            ),
            retry=RetryPolicy(
                max_attempts=2,
                retry_on_rate_limit=True,
                retry_on_timeout=True,
                retry_on_network=True,
                retry_on_service_unavailable=True,
                retry_on_invalid_response=False,
                allow_same_input_repeat=False,
                max_same_input_repeats=0,
            ),
            backoff=BackoffPolicy(
                strategy="exponential",
                initial_delay_sec=4.0,
                multiplier=2.0,
                max_delay_sec=30.0,
            ),
            second_pass=SecondPassPolicy(
                mode="conditional",
                max_passes=1,
                require_secondary_source=True,
                require_missing_fields=True,
                allowed_missing_fields=("waren", "waren_unvollstaendig", "bestellnummer", "preise"),
                allowed_source_types=("mail_attachment",),
            ),
            input=InputPolicy(
                preferred_input_strategy="pdf_first",
                screenshot_aggressiveness="low",
                text_only_fallback="when_no_file",
                upload_conservatism="document_first",
                allow_additional_upload_pass=False,
            ),
            cost=CostPolicy(
                expensive_repeat_mode="avoid",
                max_extra_calls_per_item=1,
                max_upload_calls_per_item=1,
                prefer_single_pass=True,
            ),
        ),
        status_hints={
            "second_pass_conditional": "Claude-Dokumentprofil erlaubt einen Zusatzpass nur fuer passende PDFs.",
            "parallelism_reduced": "Dokumentenprofil verarbeitet Anfragen bewusst eher seriell.",
            "pdf_preferred": "Dokumentenorientiertes Profil bevorzugt PDF als Hauptquelle.",
            "text_fallback": "Text-only-Fallback bleibt ein Rueckfall, wenn kein brauchbares Dokument vorliegt.",
        },
        metadata={
            "profile_family": "starter",
            "provider_tier": "document_first",
            "supports_future_overrides": True,
            "safe_adjustments": {
                "second_pass_mode": ("off", "missing_required"),
                "retry_level": ("low", "normal", "high"),
                "parallel_level": ("conservative", "normal"),
                "screenshot_level": ("sparse", "auto"),
                "document_focus": ("prefer_pdf", "strong_pdf"),
                "limit_behavior": ("reduce", "normal"),
            },
        },
    ),
}


def list_provider_profiles(provider_name: str = "") -> list[ProviderProfile]:
    normalized_provider = str(provider_name or "").strip().lower()
    profiles = []
    for profile in _PROFILE_DEFINITIONS.values():
        if normalized_provider and profile.provider_name != normalized_provider:
            continue
        profiles.append(deepcopy(profile))
    return profiles


def get_default_profile_name(provider_name: str) -> str:
    normalized_provider = str(provider_name or "").strip().lower()
    return str(_DEFAULT_PROFILE_BY_PROVIDER.get(normalized_provider, "")).strip()


def get_provider_profile_definition(profile_name: str, provider_name: str = "") -> ProviderProfile:
    resolved_name = str(profile_name or "").strip()
    if not resolved_name:
        resolved_name = get_default_profile_name(provider_name)
    if resolved_name not in _PROFILE_DEFINITIONS:
        raise AiProviderError(
            error_kind="invalid_profile",
            user_message=f"Unbekanntes Providerprofil: {resolved_name or '<leer>'}.",
            technical_message=f"unknown profile name: {resolved_name}",
            provider_name=str(provider_name or ""),
            service=str(provider_name or ""),
        )
    profile = deepcopy(_PROFILE_DEFINITIONS[resolved_name])
    expected_provider = str(provider_name or "").strip().lower()
    if expected_provider and profile.provider_name != expected_provider:
        raise AiProviderError(
            error_kind="invalid_profile",
            user_message=f"Das Profil '{resolved_name}' passt nicht zum Provider '{expected_provider}'.",
            technical_message=f"profile provider mismatch: profile={profile.provider_name}, expected={expected_provider}",
            provider_name=expected_provider,
            service=expected_provider,
        )
    return profile


def resolve_provider_profile(
    provider_name: str = "",
    profile_name: str = "",
    model_name: str = "",
    transport: str = "",
    overrides: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> ProviderProfile:
    expected_provider = str(provider_name or "").strip().lower()
    profile = get_provider_profile_definition(profile_name, provider_name=expected_provider)
    profile.base_profile_name = str(profile.profile_name or "")

    resolved_model_name = str(model_name or "").strip()
    if resolved_model_name:
        profile.model_name = resolved_model_name
    resolved_transport = str(transport or "").strip()
    if resolved_transport:
        profile.transport = resolved_transport
    if isinstance(metadata, Mapping) and metadata:
        profile.metadata = _merge_dicts(profile.metadata, metadata)
    if isinstance(overrides, Mapping) and overrides:
        profile = apply_provider_profile_overrides(profile, overrides)

    validate_provider_profile(profile, expected_provider_name=expected_provider or profile.provider_name)
    return profile


def apply_provider_profile_overrides(profile: ProviderProfile, overrides: Mapping[str, Any] | None = None) -> ProviderProfile:
    cloned = deepcopy(profile)
    if not isinstance(overrides, Mapping) or not overrides:
        return cloned

    cloned.override_values = _merge_dicts(cloned.override_values, overrides)
    top_level = dict(overrides)
    for field_name in ("profile_name", "display_name", "description", "model_name", "transport"):
        if field_name not in top_level:
            continue
        setattr(cloned, field_name, str(top_level.get(field_name) or "").strip())

    if isinstance(top_level.get("metadata"), Mapping):
        cloned.metadata = _merge_dicts(cloned.metadata, top_level["metadata"])
    if isinstance(top_level.get("status_hints"), Mapping):
        cloned.status_hints = _merge_dicts(cloned.status_hints, top_level["status_hints"])
    if isinstance(top_level.get("policy"), Mapping):
        cloned.policy = _apply_dataclass_overrides(cloned.policy, top_level["policy"])
    if isinstance(top_level.get("capabilities"), Mapping) and cloned.capabilities is not None:
        cloned.capabilities = _apply_dataclass_overrides(cloned.capabilities, top_level["capabilities"])
    validate_provider_profile(cloned, expected_provider_name=cloned.provider_name)
    return cloned


def validate_provider_profile(profile: ProviderProfile, expected_provider_name: str = "") -> ProviderProfile:
    if not isinstance(profile, ProviderProfile):
        raise AiProviderError(
            error_kind="invalid_profile",
            user_message="Interner Profilfehler: Profilobjekt ist ungueltig.",
            technical_message=f"unexpected profile type: {type(profile).__name__}",
        )

    resolved_provider = str(profile.provider_name or "").strip().lower()
    expected_provider = str(expected_provider_name or "").strip().lower()
    if expected_provider and resolved_provider != expected_provider:
        raise AiProviderError(
            error_kind="invalid_profile",
            user_message=f"Das Profil '{profile.profile_name}' passt nicht zum Provider '{expected_provider}'.",
            technical_message=f"profile provider mismatch: profile={resolved_provider}, expected={expected_provider}",
            provider_name=expected_provider,
            service=expected_provider,
        )

    if int(profile.policy.execution.max_parallel_requests or 0) < 1:
        raise _invalid_profile_error(profile, "execution.max_parallel_requests must be >= 1")
    if int(profile.policy.execution.max_parallel_requests or 0) > 3:
        raise _invalid_profile_error(profile, "execution.max_parallel_requests must stay between 1 and 3")
    if int(profile.policy.retry.max_attempts or 0) < 1:
        raise _invalid_profile_error(profile, "retry.max_attempts must be >= 1")
    if int(profile.policy.retry.max_attempts or 0) > 3:
        raise _invalid_profile_error(profile, "retry.max_attempts must stay between 1 and 3")
    if int(profile.policy.retry.max_same_input_repeats or 0) < 0:
        raise _invalid_profile_error(profile, "retry.max_same_input_repeats must be >= 0")
    if int(profile.policy.retry.max_same_input_repeats or 0) > 1:
        raise _invalid_profile_error(profile, "retry.max_same_input_repeats must stay between 0 and 1")
    if int(profile.policy.second_pass.max_passes or 0) < 0 or int(profile.policy.second_pass.max_passes or 0) > 1:
        raise _invalid_profile_error(profile, "second_pass.max_passes must stay between 0 and 1")
    if int(profile.policy.cost.max_extra_calls_per_item or 0) < 0:
        raise _invalid_profile_error(profile, "cost.max_extra_calls_per_item must be >= 0")
    if int(profile.policy.cost.max_extra_calls_per_item or 0) > 2:
        raise _invalid_profile_error(profile, "cost.max_extra_calls_per_item must stay between 0 and 2")
    if int(profile.policy.cost.max_upload_calls_per_item or 0) < 0:
        raise _invalid_profile_error(profile, "cost.max_upload_calls_per_item must be >= 0")
    if int(profile.policy.cost.max_upload_calls_per_item or 0) > 3:
        raise _invalid_profile_error(profile, "cost.max_upload_calls_per_item must stay between 0 and 3")
    if profile.capabilities is not None:
        capability_provider = str(profile.capabilities.provider_name or "").strip().lower()
        if capability_provider and capability_provider != resolved_provider:
            raise _invalid_profile_error(
                profile,
                f"capabilities provider mismatch: capabilities={capability_provider}, profile={resolved_provider}",
            )
    return profile


def get_profile_status_hint(profile: ProviderProfile, hint_key: str, fallback: str = "") -> str:
    return str((profile.status_hints or {}).get(str(hint_key or "").strip(), fallback) or fallback).strip()


def evaluate_second_pass_policy(
    profile: ProviderProfile,
    missing_fields: list[str] | tuple[str, ...],
    secondary_source_type: str = "",
) -> tuple[bool, str]:
    validate_provider_profile(profile, expected_provider_name=profile.provider_name)
    policy = profile.policy.second_pass
    source_type = str(secondary_source_type or "").strip()
    relevant_missing = {str(item).strip() for item in list(missing_fields or []) if str(item).strip()}

    if policy.mode == "forbidden" or int(policy.max_passes or 0) <= 0:
        return False, get_profile_status_hint(profile, "second_pass_forbidden", "Aktives Profil unterdrueckt den zweiten KI-Pass.")

    if policy.require_secondary_source and not source_type:
        return False, "Kein Zweitpass moeglich, weil keine Sekundaerquelle vorliegt."

    if policy.require_missing_fields and not relevant_missing:
        return False, ""

    allowed_sources = {str(item).strip() for item in tuple(policy.allowed_source_types or ()) if str(item).strip()}
    if allowed_sources and source_type and source_type not in allowed_sources:
        return False, get_profile_status_hint(profile, "second_pass_conditional", "Aktives Profil erlaubt den zweiten KI-Pass nur fuer passende Quellen.")

    allowed_missing = {str(item).strip() for item in tuple(policy.allowed_missing_fields or ()) if str(item).strip()}
    if policy.mode == "conditional" and allowed_missing and not allowed_missing.intersection(relevant_missing):
        return False, get_profile_status_hint(profile, "second_pass_conditional", "Aktives Profil erlaubt den zweiten KI-Pass nur bei passenden Luecken.")

    return True, ""


def _invalid_profile_error(profile: ProviderProfile, detail: str) -> AiProviderError:
    return AiProviderError(
        error_kind="invalid_profile",
        user_message=f"Das Profil '{profile.profile_name}' ist ungueltig konfiguriert.",
        technical_message=str(detail or "invalid profile"),
        provider_name=str(profile.provider_name or ""),
        service=str(profile.provider_name or ""),
    )


def _merge_dicts(base: Mapping[str, Any] | None, updates: Mapping[str, Any] | None) -> Dict[str, Any]:
    merged = dict(base or {})
    for key, value in dict(updates or {}).items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[key] = _merge_dicts(existing, value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _apply_dataclass_overrides(base_obj, overrides: Mapping[str, Any]):
    if not is_dataclass(base_obj):
        return deepcopy(overrides)

    update_values = {}
    for field_def in fields(base_obj):
        field_name = str(field_def.name)
        if field_name not in overrides:
            continue
        current_value = getattr(base_obj, field_name)
        override_value = overrides[field_name]
        if is_dataclass(current_value) and isinstance(override_value, Mapping):
            update_values[field_name] = _apply_dataclass_overrides(current_value, override_value)
        elif isinstance(current_value, Mapping) and isinstance(override_value, Mapping):
            update_values[field_name] = _merge_dicts(current_value, override_value)
        elif isinstance(current_value, tuple) and isinstance(override_value, (list, tuple)):
            update_values[field_name] = tuple(override_value)
        else:
            update_values[field_name] = deepcopy(override_value)
    return replace(base_obj, **update_values)
