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

from module.ai_provider_core import ProviderRequest
from module.crash_logger import (
    AppError,
    classify_gemini_error,
    error_to_payload,
    log_classified_error,
)
from module.gemini_provider_adapter import GeminiStructuredProvider
from module.scan_output_contract import get_scan_output_schema, validate_and_normalize_output
from module.secret_store import sanitize_text


def test_api_key_detailed(api_key):
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


def process_receipt_with_gemini(api_key, image_path=None, custom_text="", scan_mode="einkauf", prompt_profile="", prompt_plan=None, scan_decision=None):
    """
    Kompatibler Haupteinstieg fuer Modul 1 und Mail-Scraper.

    Rueckgabe:
    - validiertes Dictionary im bekannten App-Format
    - plus _token_count
    """
    key = str(api_key or "").strip()
    if not key:
        raise AppError(
            category="auth",
            user_message="Kein API Key hinterlegt. Bitte in den Einstellungen eintragen.",
            technical_message="missing api key",
            service="gemini",
            retryable=False,
        )

    mode = str(scan_mode or "einkauf").strip().lower()
    provider = GeminiStructuredProvider(model_name="gemini-2.5-flash")

    provider_result = None
    try:
        provider_result = provider.analyze_document(
            ProviderRequest(
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
                transport="native",
                model_name="gemini-2.5-flash",
            )
        )
    except Exception as exc:
        app_error = classify_gemini_error(exc, phase="provider_request")
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
                "provider": "gemini",
            },
        )
        raise app_error

    try:
        validated = validate_and_normalize_output(mode, provider_result.payload if provider_result else None)
    except Exception as exc:
        app_error = classify_gemini_error(exc, phase="schema_validate")
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
                "provider": "gemini",
                "error_kind": str(getattr(exc, "error_kind", "") or ""),
                "field_name": str(getattr(exc, "field_name", "") or ""),
                "technical": sanitize_text(str(getattr(exc, "technical_message", "") or ""))[:240],
            },
        )
        raise app_error

    result_dict = validated.to_dict()
    result_dict["_token_count"] = int(getattr(provider_result, "token_count", 0) or 0)
    result_dict["_provider_meta"] = {
        "provider": getattr(provider_result, "provider", "gemini"),
        "finish_reason": str(getattr(provider_result, "finish_reason", "") or ""),
        "usage": dict(getattr(provider_result, "usage", {}) or {}),
        "prompt_feedback": dict(getattr(provider_result, "prompt_feedback", {}) or {}),
        "safety_ratings": list(getattr(provider_result, "safety_ratings", []) or []),
        "meta": dict(getattr(provider_result, "meta", {}) or {}),
    }
    return result_dict


