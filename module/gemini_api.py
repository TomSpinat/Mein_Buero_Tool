"""
gemini_api.py
Kompatibilitaets-Fassade fuer den Rest der App.

Die aufrufenden Module behalten process_receipt_with_gemini(...),
aber intern laeuft jetzt:
- provider-spezifischer Adapter (Gemini)
- striktes JSON ohne Fence/Freitext-Hacks
- zentral validierter, interner Output-Vertrag
"""

from __future__ import annotations

import google.generativeai as genai

from module.ai import ScanRequest, build_provider_profile, resolve_ai_provider
from module.ai.provider_settings import get_ai_provider_label, normalize_ai_provider_name
from module.crash_logger import (
    AppError,
    classify_gemini_error,
    error_to_payload,
    log_classified_error,
)
from module.scan_output_contract import get_scan_output_schema, validate_and_normalize_output
from module.secret_store import sanitize_text


def test_api_key_detailed(api_key, provider_name="gemini"):
    """Prueft den API-Key und liefert ein strukturiertes Ergebnis statt nur True/False."""
    key = str(api_key or "").strip()
    if not key:
        err = AppError(
            category="auth",
            user_message="Kein API-Key eingetragen.",
            technical_message="missing api key",
            service="gemini",
            retryable=False,
        )
        return {"ok": False, "error": error_to_payload(err)}

    try:
        genai.configure(api_key=key)
        models = list(genai.list_models())
        has_generate = any(
            "generateContent" in getattr(model, "supported_generation_methods", [])
            for model in models
        )
        if has_generate:
            return {"ok": True, "error": None}

        err = AppError(
            category="not_found",
            user_message="Gemini ist erreichbar, aber es wurde kein passendes Modell gefunden.",
            technical_message="no model with generateContent",
            service="gemini",
            retryable=False,
        )
        log_classified_error(
            f"{__name__}.test_api_key_detailed",
            err.category,
            err.user_message,
            service=err.service,
            extra={"model_count": len(models)},
        )
        return {"ok": False, "error": error_to_payload(err)}
    except Exception as exc:
        app_error = classify_gemini_error(exc, phase="auth_check")
        log_classified_error(
            f"{__name__}.test_api_key_detailed",
            app_error.category,
            app_error.user_message,
            status_code=app_error.status_code,
            service=app_error.service,
            exc=exc,
        )
        return {"ok": False, "error": error_to_payload(app_error)}

def test_api_key(api_key):
    """Rueckwaertskompatibel: liefert weiterhin bool."""
    return bool(test_api_key_detailed(api_key).get("ok"))


def classify_ai_provider_error(exc, provider_name="gemini", phase="request"):
    normalized_provider = normalize_ai_provider_name(provider_name)
    if normalized_provider == "gemini":
        return classify_gemini_error(exc, phase=phase)
    if isinstance(exc, AppError):
        return exc

    provider_label = get_ai_provider_label(normalized_provider)
    error_kind = str(getattr(exc, "error_kind", "") or "").strip().lower()
    technical = str(getattr(exc, "technical_message", "") or exc or "")
    status_code = getattr(exc, "status_code", None)
    retryable = bool(getattr(exc, "retryable", False))
    quota_status = getattr(exc, "quota_status", None)
    quota_payload = quota_status.to_dict() if hasattr(quota_status, "to_dict") else {}
    error_phase = str(getattr(exc, "phase", "") or phase or "").strip()
    provider_meta = {
        "provider_name": normalized_provider,
        "provider_error_kind": error_kind,
        "provider_phase": error_phase,
        "quota_status": quota_payload,
    }

    mapping = {
        "auth": ("auth", f"{provider_label}: Zugriff fehlgeschlagen. Bitte API-Key pruefen.", False),
        "rate_limit": ("rate_limit", f"{provider_label}: Anfrage-Limit erreicht. Bitte spaeter erneut versuchen.", True),
        "quota_exhausted": ("quota_exhausted", f"{provider_label}: Kontingent ist aktuell erschoepft.", False),
        "timeout": ("timeout", f"{provider_label}: Anfrage hat zu lange gedauert.", True),
        "network": ("network", f"{provider_label}: Netzwerkfehler bei der Verbindung.", True),
        "request_too_large": ("input_error", f"{provider_label}: Anfrage ist zu gross fuer den Dienst.", False),
        "context_too_large": ("input_error", f"{provider_label}: Eingabe ist zu gross fuer das gewaehlte Modell.", False),
        "upload_error": ("input_error", f"{provider_label}: Datei konnte nicht sauber uebergeben werden.", False),
        "service_unavailable": ("service_unavailable", f"{provider_label}: Dienst ist aktuell nicht erreichbar.", True),
        "empty_response": ("empty_response", f"{provider_label}: Keine auswertbare Antwort erhalten.", True),
        "invalid_response": ("invalid_response", f"{provider_label}: Antwort war nicht im erwarteten Format.", False),
        "schema_violation": ("schema_violation", f"{provider_label}: Antwortdaten hatten ein ungueltiges Format.", False),
        "safety_blocked": ("safety_blocked", f"{provider_label}: Antwort wurde aus Sicherheitsgruenden blockiert.", False),
        "invalid_profile": ("input_error", f"{provider_label}: Profil-Konfiguration ist ungueltig.", False),
    }
    category, user_message, retryable_default = mapping.get(
        error_kind,
        ("unknown", f"{provider_label}: Verarbeitung fehlgeschlagen.", retryable),
    )
    return AppError(
        category=category,
        user_message=user_message,
        technical_message=technical,
        status_code=status_code,
        service=normalized_provider,
        retryable=retryable if error_kind not in mapping else retryable_default,
        meta=provider_meta,
    )


def process_receipt_with_gemini(
    api_key,
    image_path=None,
    custom_text="",
    scan_mode="einkauf",
    prompt_profile="",
    prompt_plan=None,
    scan_decision=None,
    provider_name="gemini",
    provider_profile_name="",
    provider_profile_overrides=None,
):
    """
    Kompatibler Haupteinstieg fuer Modul 1 und Mail-Scraper.

    Rueckgabe:
    - validiertes Dictionary im bekannten App-Format
    - plus _token_count
    """
    key = str(api_key or "").strip()
    normalized_provider = normalize_ai_provider_name(provider_name)
    provider_label = get_ai_provider_label(normalized_provider)
    if not key:
        raise AppError(
            category="auth",
            user_message=f"Kein {provider_label} API Key hinterlegt. Bitte in den Einstellungen eintragen.",
            technical_message="missing api key",
            service=normalized_provider,
            retryable=False,
        )

    mode = str(scan_mode or "einkauf").strip().lower()
    provider_profile = build_provider_profile(
        provider_name=normalized_provider,
        profile_name=str(provider_profile_name or ""),
        transport="native",
        overrides=dict(provider_profile_overrides or {}) if isinstance(provider_profile_overrides, dict) else None,
    )
    provider = resolve_ai_provider(profile=provider_profile)

    provider_result = None
    try:
        provider_result = provider.analyze_scan(
            ScanRequest(
                api_key=key,
                image_path=image_path,
                custom_text=custom_text,
                scan_mode=mode,
                prompt_profile=str(prompt_profile or ""),
                prompt_plan=dict(prompt_plan or {}) if isinstance(prompt_plan, dict) else None,
                scan_decision=dict(scan_decision or {}) if isinstance(scan_decision, dict) else None,
                response_mime_type="application/json",
                response_schema=get_scan_output_schema(mode),
                system_instruction="",
                tools=None,
                transport=provider_profile.transport,
                model_name=provider_profile.model_name,
                profile=provider_profile,
                metadata={
                    "media_resolution": str(provider_profile.metadata.get("gemini_media_resolution", "") or "").strip(),
                    "input_category": str((scan_decision or {}).get("input_category", "") or ""),
                    "prompt_class": str(prompt_profile or ""),
                },
            )
        )
    except Exception as exc:
        app_error = classify_ai_provider_error(exc, provider_name=normalized_provider, phase="provider_request")
        log_classified_error(
            f"{__name__}.process_receipt_with_gemini",
            app_error.category,
            app_error.user_message,
            status_code=app_error.status_code,
            service=app_error.service,
            exc=exc,
            extra={
                "phase": "provider_request",
                "scan_mode": mode,
                "prompt_profile": str(prompt_profile or ""),
                "image_path": sanitize_text(str(image_path or "")),
                "error_kind": str(getattr(exc, "error_kind", "") or ""),
                "provider": normalized_provider,
            },
        )
        raise app_error

    try:
        validated = validate_and_normalize_output(mode, provider_result.payload if provider_result else None)
    except Exception as exc:
        app_error = classify_ai_provider_error(exc, provider_name=normalized_provider, phase="schema_validate")
        log_classified_error(
            f"{__name__}.process_receipt_with_gemini",
            app_error.category,
            app_error.user_message,
            status_code=app_error.status_code,
            service=app_error.service,
            exc=exc,
            extra={
                "phase": "schema_validate",
                "scan_mode": mode,
                "provider": normalized_provider,
                "error_kind": str(getattr(exc, "error_kind", "") or ""),
                "field_name": str(getattr(exc, "field_name", "") or ""),
                "technical": sanitize_text(str(getattr(exc, "technical_message", "") or ""))[:240],
            },
        )
        raise app_error

    result_dict = validated.to_dict()
    result_dict["_token_count"] = int(getattr(provider_result, "token_count", 0) or 0)
    result_dict["_provider_meta"] = {
        "provider": getattr(provider_result, "provider_name", "gemini"),
        "profile_name": str(getattr(provider_result, "profile_name", "") or ""),
        "model_name": str(getattr(provider_result, "model_name", "") or ""),
        "finish_reason": str(getattr(provider_result, "finish_reason", "") or ""),
        "usage": dict(getattr(provider_result, "usage", {}) or {}),
        "prompt_feedback": dict(getattr(provider_result, "prompt_feedback", {}) or {}),
        "safety_ratings": list(getattr(provider_result, "safety_ratings", []) or []),
        "meta": dict(getattr(provider_result, "meta", {}) or {}),
    }
    return result_dict


