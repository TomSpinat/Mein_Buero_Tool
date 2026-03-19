from __future__ import annotations

from copy import deepcopy
import json
import socket
import urllib.error
import urllib.request
from typing import Any, Dict, Tuple

from module.ai.profiles import (
    apply_provider_profile_overrides,
    get_default_profile_name,
    get_provider_profile_definition,
)
from module.ai.types import ProviderProfile
from module.crash_logger import AppError, error_to_payload


_PROVIDER_OPTIONS: Tuple[Tuple[str, str], ...] = (
    ("Gemini", "gemini"),
    ("OpenAI", "openai"),
    ("Claude", "claude"),
)

_PROVIDER_META: Dict[str, Dict[str, str]] = {
    "gemini": {
        "label": "Gemini",
        "secret_key": "gemini_api_key",
        "key_label": "Gemini API Key:",
        "key_placeholder": "Gemini API-Key eingeben",
        "missing_key": "Kein Gemini API-Key eingetragen.",
        "delete_success": "Der gespeicherte Gemini API-Key wurde geloescht.",
        "provider_hint": "Gemini ist ein flotter Allrounder. Das konkrete Modell waehlt spaeter das jeweilige Modul automatisch.",
    },
    "openai": {
        "label": "OpenAI",
        "secret_key": "openai_api_key",
        "key_label": "OpenAI API Key:",
        "key_placeholder": "OpenAI API-Key eingeben",
        "missing_key": "Kein OpenAI API-Key eingetragen.",
        "delete_success": "Der gespeicherte OpenAI API-Key wurde geloescht.",
        "provider_hint": "OpenAI ist fuer Text- und Bildfaelle vorbereitet. Das konkrete Modell wird spaeter pro Aufgabe gesetzt.",
    },
    "claude": {
        "label": "Claude",
        "secret_key": "claude_api_key",
        "key_label": "Claude API Key:",
        "key_placeholder": "Claude API-Key eingeben",
        "missing_key": "Kein Claude API-Key eingetragen.",
        "delete_success": "Der gespeicherte Claude API-Key wurde geloescht.",
        "provider_hint": "Claude ist hier besonders dokumentenorientiert vorbereitet. Das konkrete Modell bleibt intern gesteuert.",
    },
}

_PROFILE_OPTION_FIELDS: Tuple[Dict[str, str], ...] = (
    {
        "id": "second_pass_mode",
        "label": "Zweiter KI-Blick",
        "help": "Legt fest, ob die KI bei fehlenden Informationen noch einen zweiten vorsichtigen Nachschlag machen darf.",
    },
    {
        "id": "retry_level",
        "label": "Wiederholungen bei kurzen Stoerungen",
        "help": "Bestimmt, wie vorsichtig oder hartnaeckig bei Timeout- oder Limitproblemen erneut versucht wird.",
    },
    {
        "id": "parallel_level",
        "label": "Gleichzeitige Bearbeitung",
        "help": "Steuert, wie viele Mails parallel an den KI-Dienst gehen duerfen.",
    },
    {
        "id": "screenshot_level",
        "label": "Screenshot-Nutzung",
        "help": "Steuert, wie schnell zusaetzliche Screenshots eingesetzt werden.",
    },
    {
        "id": "document_focus",
        "label": "Dokumentfokus",
        "help": "Legt fest, wie stark PDF-Dateien gegenueber Mailtext bevorzugt werden.",
    },
    {
        "id": "limit_behavior",
        "label": "Zusatzaufrufe bei knappen Limits",
        "help": "Regelt, ob bei spuerbarem Limitdruck eher gespart oder mehr ausgeschoepft wird.",
    },
)

_PROFILE_OPTION_VALUES: Dict[str, Dict[str, Dict[str, str]]] = {
    "second_pass_mode": {
        "off": {
            "label": "Aus",
            "description": "Kein zweiter KI-Durchgang.",
        },
        "missing_required": {
            "label": "Nur bei Luecken",
            "description": "Nur wenn wichtige Felder fehlen.",
        },
        "expanded": {
            "label": "Erweitert",
            "description": "Etwas grosszuegiger bei fehlenden Angaben.",
        },
    },
    "retry_level": {
        "low": {
            "label": "Niedrig",
            "description": "Wenig Wiederholungen, eher vorsichtig.",
        },
        "normal": {
            "label": "Normal",
            "description": "Ausgewogener Standard fuer den Alltag.",
        },
        "high": {
            "label": "Hoch",
            "description": "Mehr Geduld bei kurzen Provider-Aussetzern.",
        },
    },
    "parallel_level": {
        "conservative": {
            "label": "Konservativ",
            "description": "Weniger Gleichzeitigkeit, dafuer stabiler.",
        },
        "normal": {
            "label": "Normal",
            "description": "Ausgewogene Parallelitaet.",
        },
        "increased": {
            "label": "Erhoeht",
            "description": "Etwas mehr Tempo, wenn der Provider mitspielt.",
        },
    },
    "screenshot_level": {
        "sparse": {
            "label": "Sparsam",
            "description": "Screenshots nur sehr zurueckhaltend nutzen.",
        },
        "auto": {
            "label": "Automatisch",
            "description": "Normaler Mittelweg.",
        },
        "expanded": {
            "label": "Erweitert",
            "description": "Screenshots grosszuegiger zulassen.",
        },
    },
    "document_focus": {
        "standard": {
            "label": "Standard",
            "description": "Keine besondere PDF-Betonung.",
        },
        "prefer_pdf": {
            "label": "PDF bevorzugen",
            "description": "PDF wenn moeglich vorziehen.",
        },
        "strong_pdf": {
            "label": "Stark PDF-orientiert",
            "description": "Dokumente klar vor Mailtext stellen.",
        },
    },
    "limit_behavior": {
        "reduce": {
            "label": "Stark reduzieren",
            "description": "Zusatzaufwand bei knappen Limits schnell bremsen.",
        },
        "normal": {
            "label": "Normal",
            "description": "Ausgewogener Alltag.",
        },
        "exhaust": {
            "label": "Moeglichst ausschoepfen",
            "description": "Mehr ausreizen, aber weiter begrenzt und sicher.",
        },
    },
}

_PROFILE_ALLOWED_OPTION_VALUES: Dict[str, Dict[str, Tuple[str, ...]]] = {
    "gemini_free_conservative": {
        "second_pass_mode": ("off", "missing_required"),
        "retry_level": ("low", "normal"),
        "parallel_level": ("conservative", "normal"),
        "screenshot_level": ("sparse", "auto"),
        "document_focus": ("prefer_pdf", "strong_pdf"),
        "limit_behavior": ("reduce", "normal"),
    },
    "gemini_paid_standard": {
        "second_pass_mode": ("off", "missing_required", "expanded"),
        "retry_level": ("low", "normal", "high"),
        "parallel_level": ("conservative", "normal", "increased"),
        "screenshot_level": ("sparse", "auto", "expanded"),
        "document_focus": ("standard", "prefer_pdf", "strong_pdf"),
        "limit_behavior": ("reduce", "normal", "exhaust"),
    },
    "openai_standard": {
        "second_pass_mode": ("off", "missing_required", "expanded"),
        "retry_level": ("low", "normal", "high"),
        "parallel_level": ("conservative", "normal", "increased"),
        "screenshot_level": ("sparse", "auto", "expanded"),
        "document_focus": ("standard", "prefer_pdf"),
        "limit_behavior": ("reduce", "normal", "exhaust"),
    },
    "claude_document_first": {
        "second_pass_mode": ("off", "missing_required"),
        "retry_level": ("low", "normal", "high"),
        "parallel_level": ("conservative", "normal"),
        "screenshot_level": ("sparse", "auto"),
        "document_focus": ("prefer_pdf", "strong_pdf"),
        "limit_behavior": ("reduce", "normal"),
    },
}

_SECOND_PASS_PRESETS: Dict[str, Dict[str, Dict[str, Tuple[str, ...]]]] = {
    "gemini": {
        "missing_required": {
            "fields": ("waren", "waren_unvollstaendig", "bestellnummer", "preise"),
            "sources": ("mail_attachment", "mail_render_screenshot"),
        },
        "expanded": {
            "fields": ("waren", "waren_unvollstaendig", "bestellnummer", "preise", "tracking"),
            "sources": ("mail_attachment", "email_message", "mail_render_screenshot"),
        },
    },
    "openai": {
        "missing_required": {
            "fields": ("waren", "waren_unvollstaendig", "bestellnummer", "preise"),
            "sources": ("mail_attachment", "mail_render_screenshot"),
        },
        "expanded": {
            "fields": ("waren", "waren_unvollstaendig", "bestellnummer", "preise", "tracking"),
            "sources": ("mail_attachment", "mail_render_screenshot"),
        },
    },
    "claude": {
        "missing_required": {
            "fields": ("waren", "waren_unvollstaendig", "bestellnummer", "preise"),
            "sources": ("mail_attachment",),
        },
        "expanded": {
            "fields": ("waren", "waren_unvollstaendig", "bestellnummer", "preise", "tracking"),
            "sources": ("mail_attachment",),
        },
    },
}


def list_ai_provider_options() -> Tuple[Tuple[str, str], ...]:
    return _PROVIDER_OPTIONS


def normalize_ai_provider_name(provider_name: str = "") -> str:
    normalized = str(provider_name or "").strip().lower()
    if normalized in _PROVIDER_META:
        return normalized
    return "gemini"


def validate_ai_provider_name(provider_name: str = "") -> str:
    raw_value = str(provider_name or "").strip().lower()
    if not raw_value:
        return "gemini"
    if raw_value not in _PROVIDER_META:
        raise AppError(
            category="input_error",
            user_message=f"Unbekannter KI-Provider: {provider_name}",
            technical_message=f"unknown ai provider: {provider_name}",
            service="settings",
            retryable=False,
        )
    return raw_value


def get_ai_provider_label(provider_name: str = "") -> str:
    normalized = normalize_ai_provider_name(provider_name)
    return str(_PROVIDER_META[normalized]["label"])


def get_ai_provider_secret_key(provider_name: str = "") -> str:
    normalized = normalize_ai_provider_name(provider_name)
    return str(_PROVIDER_META[normalized]["secret_key"])


def get_ai_provider_key_label(provider_name: str = "") -> str:
    normalized = normalize_ai_provider_name(provider_name)
    return str(_PROVIDER_META[normalized]["key_label"])


def get_ai_provider_key_placeholder(provider_name: str = "") -> str:
    normalized = normalize_ai_provider_name(provider_name)
    return str(_PROVIDER_META[normalized]["key_placeholder"])


def get_ai_provider_missing_key_message(provider_name: str = "") -> str:
    normalized = normalize_ai_provider_name(provider_name)
    return str(_PROVIDER_META[normalized]["missing_key"])


def get_ai_provider_delete_success_message(provider_name: str = "") -> str:
    normalized = normalize_ai_provider_name(provider_name)
    return str(_PROVIDER_META[normalized]["delete_success"])


def get_ai_provider_hint_text(provider_name: str = "", profile_name: str = "", raw_overrides: Dict[str, Any] | None = None) -> str:
    normalized = normalize_ai_provider_name(provider_name)
    resolved_profile_name = str(profile_name or get_default_profile_name(normalized) or "").strip()
    profile = get_provider_profile_definition(resolved_profile_name, provider_name=normalized)
    display_name = str(profile.display_name or profile.profile_name or "").strip()
    provider_hint = str(_PROVIDER_META[normalized]["provider_hint"])
    summary_text = describe_ai_profile_adjustments(normalized, resolved_profile_name, raw_overrides)
    return (
        f"{provider_hint} Aktives Startprofil: {display_name}. "
        f"{summary_text} "
        "Feinere Anpassungen laufen ueber 'Profil anpassen'."
    )


def build_default_ai_profile_name_map() -> Dict[str, str]:
    return {
        provider_name: get_default_profile_name(provider_name)
        for _label, provider_name in _PROVIDER_OPTIONS
    }


def build_default_ai_profile_override_map() -> Dict[str, Dict[str, Any]]:
    return {
        provider_name: {}
        for _label, provider_name in _PROVIDER_OPTIONS
    }


def get_ai_profile_adjustment_schema(
    provider_name: str,
    profile_name: str = "",
    raw_overrides: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_provider = normalize_ai_provider_name(provider_name)
    profile = get_provider_profile_definition(profile_name, provider_name=normalized_provider)
    default_options = _derive_profile_ui_defaults(profile)
    allowed_values = _allowed_profile_option_values(profile)
    current_options = _resolve_profile_ui_options(profile, raw_overrides)
    persisted_overrides = _build_persisted_profile_override_entry(profile, current_options)
    effective_profile = _build_effective_profile(profile, persisted_overrides)
    summary_text = _build_profile_adjustment_summary(current_options)

    fields = []
    for field_meta in _PROFILE_OPTION_FIELDS:
        field_id = str(field_meta["id"])
        option_entries = []
        for value in allowed_values[field_id]:
            option_meta = _PROFILE_OPTION_VALUES[field_id][value]
            option_entries.append(
                {
                    "value": value,
                    "label": str(option_meta.get("label", value)),
                    "description": str(option_meta.get("description", "")),
                    "is_default": value == default_options[field_id],
                }
            )
        fields.append(
            {
                "id": field_id,
                "label": str(field_meta["label"]),
                "help": str(field_meta["help"]),
                "current_value": current_options[field_id],
                "default_value": default_options[field_id],
                "options": option_entries,
            }
        )

    return {
        "provider_name": normalized_provider,
        "provider_label": get_ai_provider_label(normalized_provider),
        "profile_name": str(profile.profile_name or ""),
        "profile_display_name": str(profile.display_name or profile.profile_name or ""),
        "profile_description": str(profile.description or ""),
        "fields": fields,
        "current_ui_options": dict(current_options),
        "default_ui_options": dict(default_options),
        "persisted_overrides": dict(persisted_overrides),
        "effective_profile": effective_profile.to_meta_dict() if isinstance(effective_profile, ProviderProfile) else {},
        "summary_text": summary_text,
        "summary_lines": _build_profile_adjustment_summary_lines(current_options),
    }


def normalize_ai_profile_settings(
    provider_name: str,
    profile_name_map: Dict[str, Any] | None,
    override_map: Dict[str, Any] | None,
) -> Dict[str, Any]:
    normalized_provider = normalize_ai_provider_name(provider_name)
    normalized_names = build_default_ai_profile_name_map()
    raw_names = dict(profile_name_map or {})
    raw_overrides = dict(override_map or {})
    normalized_overrides = build_default_ai_profile_override_map()

    for _label, current_provider in _PROVIDER_OPTIONS:
        candidate_name = str(raw_names.get(current_provider, "") or "").strip()
        try:
            profile = get_provider_profile_definition(candidate_name, provider_name=current_provider)
            normalized_names[current_provider] = str(profile.profile_name or normalized_names[current_provider])
        except Exception:
            fallback_name = get_default_profile_name(current_provider)
            profile = get_provider_profile_definition(fallback_name, provider_name=current_provider)
            normalized_names[current_provider] = fallback_name

        candidate_override = raw_overrides.get(current_provider, {})
        normalized_overrides[current_provider] = normalize_ai_profile_override_entry(
            current_provider,
            normalized_names[current_provider],
            candidate_override,
        )

    return {
        "ai_provider": normalized_provider,
        "ai_profile_name_by_provider": normalized_names,
        "ai_profile_overrides_by_provider": normalized_overrides,
    }


def normalize_ai_profile_override_entry(
    provider_name: str,
    profile_name: str = "",
    raw_override: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_provider = normalize_ai_provider_name(provider_name)
    profile = get_provider_profile_definition(profile_name, provider_name=normalized_provider)
    resolved_ui = _resolve_profile_ui_options(profile, raw_override)
    return _build_persisted_profile_override_entry(profile, resolved_ui)


def resolve_ai_profile_overrides(
    provider_name: str,
    profile_name: str = "",
    raw_override: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_provider = normalize_ai_provider_name(provider_name)
    profile = get_provider_profile_definition(profile_name, provider_name=normalized_provider)
    default_ui = _derive_profile_ui_defaults(profile)
    resolved_ui = _resolve_profile_ui_options(profile, raw_override)
    changed_fields = [field_id for field_id, value in resolved_ui.items() if value != default_ui[field_id]]
    if not changed_fields:
        return {}

    policy_overrides: Dict[str, Any] = {}
    status_hints: Dict[str, str] = {}
    metadata: Dict[str, Any] = {
        "safe_profile_customization": True,
        "profile_ui_options": dict(resolved_ui),
        "profile_ui_changes": {field_id: resolved_ui[field_id] for field_id in changed_fields},
        "profile_adjustment_summary": _build_profile_adjustment_summary(resolved_ui),
    }
    for field_id in changed_fields:
        _apply_profile_ui_override(
            profile,
            policy_overrides,
            status_hints,
            field_id,
            resolved_ui[field_id],
        )

    resolved_override = {"policy": policy_overrides, "metadata": metadata}
    if status_hints:
        resolved_override["status_hints"] = status_hints
    return resolved_override


def describe_ai_profile_adjustments(
    provider_name: str,
    profile_name: str = "",
    raw_overrides: Dict[str, Any] | None = None,
) -> str:
    schema = get_ai_profile_adjustment_schema(provider_name, profile_name, raw_overrides)
    return str(schema.get("summary_text", "") or "").strip()


def get_ai_provider_profile_shell_text(provider_name: str = "", profile_name: str = "", raw_overrides: Dict[str, Any] | None = None) -> str:
    schema = get_ai_profile_adjustment_schema(provider_name, profile_name, raw_overrides)
    summary_text = str(schema.get("summary_text", "") or "").strip()
    if summary_text:
        return summary_text
    return (
        "Hier werden nur sichere, feste Profiloptionen angeboten. "
        "Freie Modellwahl oder rohe Technikwerte bleiben bewusst ausgeblendet."
    )


def _derive_profile_ui_defaults(profile: ProviderProfile) -> Dict[str, str]:
    execution = profile.policy.execution
    retry = profile.policy.retry
    backoff = profile.policy.backoff
    second_pass = profile.policy.second_pass
    input_policy = profile.policy.input
    cost = profile.policy.cost

    if str(second_pass.mode or "").strip().lower() == "forbidden" or int(second_pass.max_passes or 0) <= 0:
        second_pass_mode = "off"
    elif "tracking" in tuple(second_pass.allowed_missing_fields or ()):
        second_pass_mode = "expanded"
    else:
        second_pass_mode = "missing_required"

    if int(retry.max_attempts or 1) >= 3:
        retry_level = "high"
    elif int(retry.max_attempts or 1) <= 2 and float(backoff.initial_delay_sec or 0.0) >= 4.0:
        retry_level = "low"
    else:
        retry_level = "normal"

    if bool(execution.serialize_requests) or int(execution.max_parallel_requests or 1) <= 1:
        parallel_level = "conservative"
    elif int(execution.max_parallel_requests or 1) >= 3:
        parallel_level = "increased"
    else:
        parallel_level = "normal"

    screenshot_raw = str(input_policy.screenshot_aggressiveness or "").strip().lower()
    screenshot_level = {
        "low": "sparse",
        "balanced": "auto",
        "high": "expanded",
    }.get(screenshot_raw, "auto")

    if str(input_policy.preferred_input_strategy or "").strip().lower() == "pdf_first":
        if str(input_policy.upload_conservatism or "").strip().lower() == "document_first":
            document_focus = "strong_pdf"
        else:
            document_focus = "prefer_pdf"
    else:
        document_focus = "standard"

    if bool(cost.prefer_single_pass) or str(cost.expensive_repeat_mode or "").strip().lower() == "avoid":
        limit_behavior = "reduce"
    elif int(cost.max_extra_calls_per_item or 0) >= 2 or int(cost.max_upload_calls_per_item or 0) >= 3:
        limit_behavior = "exhaust"
    else:
        limit_behavior = "normal"

    return {
        "second_pass_mode": second_pass_mode,
        "retry_level": retry_level,
        "parallel_level": parallel_level,
        "screenshot_level": screenshot_level,
        "document_focus": document_focus,
        "limit_behavior": limit_behavior,
    }


def _allowed_profile_option_values(profile: ProviderProfile) -> Dict[str, Tuple[str, ...]]:
    allowed = {}
    profile_specific = _PROFILE_ALLOWED_OPTION_VALUES.get(str(profile.profile_name or "").strip(), {})
    metadata_allowed = (
        dict((profile.metadata or {}).get("safe_adjustments", {}) or {})
        if isinstance(getattr(profile, "metadata", {}), dict)
        else {}
    )
    for field_meta in _PROFILE_OPTION_FIELDS:
        field_id = str(field_meta["id"])
        explicit = metadata_allowed.get(field_id, profile_specific.get(field_id))
        if isinstance(explicit, (list, tuple)) and explicit:
            values = tuple(str(item).strip() for item in explicit if str(item).strip())
        else:
            values = tuple(_PROFILE_OPTION_VALUES[field_id].keys())
        allowed[field_id] = values
    return allowed


def _extract_profile_ui_options(raw_override: Dict[str, Any] | None) -> Dict[str, str]:
    if not isinstance(raw_override, dict):
        return {}
    candidate = raw_override.get("ui_options", raw_override)
    if not isinstance(candidate, dict):
        return {}
    return {
        str(key).strip(): str(value).strip()
        for key, value in candidate.items()
        if str(key).strip() and str(value).strip()
    }


def _resolve_profile_ui_options(profile: ProviderProfile, raw_override: Dict[str, Any] | None) -> Dict[str, str]:
    defaults = _derive_profile_ui_defaults(profile)
    allowed_values = _allowed_profile_option_values(profile)
    incoming = _extract_profile_ui_options(raw_override)
    resolved = {}
    for field_id, default_value in defaults.items():
        candidate = str(incoming.get(field_id, default_value) or "").strip()
        allowed = allowed_values.get(field_id, ())
        resolved[field_id] = candidate if candidate in allowed else default_value
    return resolved


def _build_persisted_profile_override_entry(profile: ProviderProfile, ui_options: Dict[str, str]) -> Dict[str, Any]:
    defaults = _derive_profile_ui_defaults(profile)
    changes = {
        field_id: value
        for field_id, value in dict(ui_options or {}).items()
        if defaults.get(field_id) != value
    }
    if not changes:
        return {}
    return {
        "base_profile_name": str(profile.profile_name or ""),
        "ui_options": dict(changes),
    }


def _build_effective_profile(profile: ProviderProfile, raw_override: Dict[str, Any] | None) -> ProviderProfile:
    resolved_override = resolve_ai_profile_overrides(
        profile.provider_name,
        profile.profile_name,
        raw_override,
    )
    if not resolved_override:
        return deepcopy(profile)
    return apply_provider_profile_overrides(profile, resolved_override)


def _build_profile_adjustment_summary(ui_options: Dict[str, str]) -> str:
    return "Aktive Anpassung: " + "; ".join(_build_profile_adjustment_summary_lines(ui_options)) + "."


def _build_profile_adjustment_summary_lines(ui_options: Dict[str, str]) -> list[str]:
    values = dict(ui_options or {})
    return [
        _summary_label("Zweiter Pass", "second_pass_mode", values),
        _summary_label("Retry", "retry_level", values),
        _summary_label("Parallelitaet", "parallel_level", values),
        _summary_label("Screenshots", "screenshot_level", values),
        _summary_label("Dokumentfokus", "document_focus", values),
        _summary_label("Limitverhalten", "limit_behavior", values),
    ]


def _summary_label(prefix: str, field_id: str, values: Dict[str, str]) -> str:
    value = str(values.get(field_id, "") or "").strip()
    label = str(_PROFILE_OPTION_VALUES.get(field_id, {}).get(value, {}).get("label", value) or value)
    return f"{prefix}: {label}"


def _apply_profile_ui_override(
    profile: ProviderProfile,
    policy_overrides: Dict[str, Any],
    status_hints: Dict[str, str],
    field_id: str,
    value: str,
):
    value = str(value or "").strip()
    provider_name = str(profile.provider_name or "").strip().lower()
    policy_bucket = policy_overrides

    if field_id == "second_pass_mode":
        preset = _SECOND_PASS_PRESETS.get(provider_name, {})
        if value == "off":
            policy_bucket["second_pass"] = {
                "mode": "forbidden",
                "max_passes": 0,
                "require_secondary_source": True,
                "require_missing_fields": True,
                "allowed_missing_fields": (),
                "allowed_source_types": (),
            }
            status_hints["second_pass_forbidden"] = "Der zweite KI-Pass wurde in den Einstellungen ausgeschaltet."
            return
        preset_key = "expanded" if value == "expanded" else "missing_required"
        preset_values = preset.get(preset_key, {})
        policy_bucket["second_pass"] = {
            "mode": "conditional",
            "max_passes": 1,
            "require_secondary_source": True,
            "require_missing_fields": True,
            "allowed_missing_fields": tuple(preset_values.get("fields", ()) or ()),
            "allowed_source_types": tuple(preset_values.get("sources", ()) or ()),
        }
        if value == "expanded":
            status_hints["second_pass_conditional"] = "Der zweite KI-Pass wurde erweitert, bleibt aber auf einen Zusatzdurchgang begrenzt."
        else:
            status_hints["second_pass_conditional"] = "Der zweite KI-Pass laeuft nur bei fehlenden Pflichtfeldern."
        return

    if field_id == "retry_level":
        if value == "low":
            policy_bucket["retry"] = {
                "max_attempts": 2,
                "allow_same_input_repeat": False,
                "max_same_input_repeats": 0,
            }
            policy_bucket["backoff"] = {
                "initial_delay_sec": 4.0,
                "multiplier": 2.0,
                "max_delay_sec": 32.0,
            }
        elif value == "high":
            policy_bucket["retry"] = {
                "max_attempts": 3,
                "allow_same_input_repeat": False,
                "max_same_input_repeats": 0,
            }
            policy_bucket["backoff"] = {
                "initial_delay_sec": 1.5,
                "multiplier": 1.8,
                "max_delay_sec": 18.0,
            }
        else:
            policy_bucket["retry"] = {
                "max_attempts": 2,
                "allow_same_input_repeat": False,
                "max_same_input_repeats": 0,
            }
            policy_bucket["backoff"] = {
                "initial_delay_sec": 2.5,
                "multiplier": 2.0,
                "max_delay_sec": 24.0,
            }
        return

    if field_id == "parallel_level":
        if value == "conservative":
            policy_bucket["execution"] = {
                "max_parallel_requests": 1,
                "serialize_requests": True,
                "request_spacing_sec": 1.5,
            }
            status_hints["parallelism_reduced"] = "Die gleichzeitige Bearbeitung wurde vorsichtiger gestellt."
        elif value == "increased":
            policy_bucket["execution"] = {
                "max_parallel_requests": 3,
                "serialize_requests": False,
                "request_spacing_sec": 0.35,
            }
            status_hints["parallelism_reduced"] = "Die gleichzeitige Bearbeitung wurde leicht erhoeht."
        else:
            policy_bucket["execution"] = {
                "max_parallel_requests": 2,
                "serialize_requests": False,
                "request_spacing_sec": 0.8,
            }
        return

    if field_id == "screenshot_level":
        if value == "sparse":
            policy_bucket["input"] = {
                "screenshot_aggressiveness": "low",
                "allow_additional_upload_pass": False,
            }
        elif value == "expanded":
            policy_bucket["input"] = {
                "screenshot_aggressiveness": "high",
                "allow_additional_upload_pass": True,
            }
        else:
            policy_bucket["input"] = {
                "screenshot_aggressiveness": "balanced",
                "allow_additional_upload_pass": True,
            }
        return

    if field_id == "document_focus":
        if value == "strong_pdf":
            policy_bucket["input"] = {
                "preferred_input_strategy": "pdf_first",
                "upload_conservatism": "document_first",
                "text_only_fallback": "when_no_file",
            }
            status_hints["pdf_preferred"] = "Das Profil wurde staerker auf PDF-Dokumente ausgerichtet."
        elif value == "prefer_pdf":
            policy_bucket["input"] = {
                "preferred_input_strategy": "pdf_first",
                "upload_conservatism": "balanced",
                "text_only_fallback": "when_no_file",
            }
            status_hints["pdf_preferred"] = "PDF-Dateien werden bevorzugt, wenn sie brauchbar sind."
        else:
            policy_bucket["input"] = {
                "preferred_input_strategy": "auto",
                "upload_conservatism": "balanced",
                "text_only_fallback": "when_no_file",
            }
        return

    if field_id == "limit_behavior":
        if value == "reduce":
            policy_bucket["cost"] = {
                "expensive_repeat_mode": "avoid",
                "max_extra_calls_per_item": 0,
                "max_upload_calls_per_item": 1,
                "prefer_single_pass": True,
            }
        elif value == "exhaust":
            policy_bucket["cost"] = {
                "expensive_repeat_mode": "standard",
                "max_extra_calls_per_item": 2,
                "max_upload_calls_per_item": 3,
                "prefer_single_pass": False,
            }
        else:
            policy_bucket["cost"] = {
                "expensive_repeat_mode": "standard",
                "max_extra_calls_per_item": 1,
                "max_upload_calls_per_item": 2,
                "prefer_single_pass": False,
            }


def test_provider_api_key_detailed(provider_name: str, api_key: str) -> Dict[str, Any]:
    normalized_provider = normalize_ai_provider_name(provider_name)
    key = str(api_key or "").strip()
    if not key:
        return {
            "ok": False,
            "error": error_to_payload(
                AppError(
                    category="auth",
                    user_message=get_ai_provider_missing_key_message(normalized_provider),
                    technical_message="missing api key",
                    service=normalized_provider,
                    retryable=False,
                )
            ),
        }

    if normalized_provider == "gemini":
        from module.gemini_api import test_api_key_detailed

        return test_api_key_detailed(key, provider_name="gemini")

    if normalized_provider == "openai":
        return _test_openai_key(key)

    if normalized_provider == "claude":
        return _test_claude_key(key)

    return {
        "ok": False,
        "error": error_to_payload(
            AppError(
                category="input_error",
                user_message=f"Unbekannter KI-Provider: {provider_name}",
                technical_message=f"unknown provider {provider_name}",
                service="settings",
                retryable=False,
            )
        ),
    }


def _test_openai_key(api_key: str) -> Dict[str, Any]:
    try:
        response = _request_json(
            "GET",
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        if isinstance(response, dict):
            return {"ok": True, "error": None}
    except Exception as exc:
        return {"ok": False, "error": error_to_payload(_classify_provider_http_error(exc, "openai", "OpenAI"))}
    return {
        "ok": False,
        "error": error_to_payload(
            AppError(
                category="unknown",
                user_message="OpenAI antwortet nicht wie erwartet.",
                technical_message="unexpected openai test response",
                service="openai",
                retryable=True,
            )
        ),
    }


def _test_claude_key(api_key: str) -> Dict[str, Any]:
    try:
        response = _request_json(
            "GET",
            "https://api.anthropic.com/v1/models",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
        )
        if isinstance(response, dict):
            return {"ok": True, "error": None}
    except Exception as exc:
        return {"ok": False, "error": error_to_payload(_classify_provider_http_error(exc, "claude", "Claude"))}
    return {
        "ok": False,
        "error": error_to_payload(
            AppError(
                category="unknown",
                user_message="Claude antwortet nicht wie erwartet.",
                technical_message="unexpected claude test response",
                service="claude",
                retryable=True,
            )
        ),
    }


def _request_json(method: str, url: str, headers: Dict[str, str] | None = None) -> Dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers=dict(headers or {}),
        method=str(method or "GET").upper(),
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        raw_text = response.read().decode("utf-8", errors="replace")
        parsed = json.loads(raw_text or "{}")
        return parsed if isinstance(parsed, dict) else {}


def _classify_provider_http_error(exc: Exception, provider_name: str, provider_label: str) -> AppError:
    text = str(exc or "")
    if isinstance(exc, urllib.error.HTTPError):
        status_code = int(getattr(exc, "code", 0) or 0)
        if status_code in (401, 403):
            return AppError(
                category="auth",
                user_message=f"{provider_label}: Zugriff verweigert. Bitte API-Key pruefen.",
                technical_message=text,
                status_code=status_code,
                service=provider_name,
                retryable=False,
            )
        if status_code == 429:
            return AppError(
                category="rate_limit",
                user_message=f"{provider_label}: Anfrage-Limit erreicht. Bitte spaeter erneut versuchen.",
                technical_message=text,
                status_code=status_code,
                service=provider_name,
                retryable=True,
            )
        if status_code >= 500:
            return AppError(
                category="service_unavailable",
                user_message=f"{provider_label}: Dienst aktuell nicht erreichbar. Bitte spaeter erneut versuchen.",
                technical_message=text,
                status_code=status_code,
                service=provider_name,
                retryable=True,
            )
        return AppError(
            category="http_error",
            user_message=f"{provider_label}: Anfrage wurde abgelehnt.",
            technical_message=text,
            status_code=status_code,
            service=provider_name,
            retryable=False,
        )

    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, (socket.timeout, TimeoutError)):
            return AppError(
                category="timeout",
                user_message=f"{provider_label}: Anfrage hat zu lange gedauert.",
                technical_message=text,
                service=provider_name,
                retryable=True,
            )
        return AppError(
            category="network",
            user_message=f"{provider_label}: Netzwerkfehler bei der Verbindung.",
            technical_message=text,
            service=provider_name,
            retryable=True,
        )

    if isinstance(exc, (socket.timeout, TimeoutError)):
        return AppError(
            category="timeout",
            user_message=f"{provider_label}: Anfrage hat zu lange gedauert.",
            technical_message=text,
            service=provider_name,
            retryable=True,
        )

    return AppError(
        category="unknown",
        user_message=f"{provider_label}: Verbindungstest fehlgeschlagen.",
        technical_message=text,
        service=provider_name,
        retryable=False,
    )
