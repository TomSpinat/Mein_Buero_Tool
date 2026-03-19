"""
Zentrales Crash- und Fehler-Logging fuer das gesamte Projekt.

- schreibt Events in data/crash_logs/crash_YYYY-MM-DD.log
- globale Hooks fuer unbehandelte Fehler
- optional faulthandler fuer harte/native Crashes
- zentrale Fehlerklassifizierung + nutzerfreundliche Kurzmeldungen
"""

from __future__ import annotations

import datetime
import faulthandler
import json
import os
import re
import socket
import sys
import threading
import traceback
import urllib.error
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from module.secret_store import sanitize_text
from storage_paths import storage_path


_FAULTHANDLER_FILE = None
_HOOKS_INSTALLED = False
_MAX_LOG_RETENTION_DAYS = 14
_MAX_DAILY_LOG_BYTES = 5 * 1024 * 1024

_ERROR_PRIORITY = {
    "auth": 10,
    "quota_exhausted": 12,
    "input_error": 15,
    "not_found": 20,
    "transport_not_available": 25,
    "rate_limit": 30,
    "timeout": 40,
    "network": 50,
    "safety_blocked": 52,
    "incomplete_response": 55,
    "schema_violation": 58,
    "empty_response": 59,
    "invalid_response": 60,
    "service_unavailable": 70,
    "http_error": 80,
    "unknown": 100,
}


@dataclass
class AppError(Exception):
    category: str
    user_message: str
    technical_message: str = ""
    status_code: Optional[int] = None
    service: str = ""
    retryable: bool = False
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        safe_user = sanitize_text(self.user_message or "").strip()
        safe_tech = sanitize_text(self.technical_message or "").strip()
        object.__setattr__(self, "user_message", safe_user)
        object.__setattr__(self, "technical_message", safe_tech)
        Exception.__init__(self, safe_user or safe_tech or self.category or "unknown_error")

    def to_payload(self):
        return {
            "category": str(self.category or "unknown"),
            "user_message": str(self.user_message or "Unbekannter Fehler."),
            "technical_message": str(self.technical_message or ""),
            "status_code": self.status_code,
            "service": str(self.service or ""),
            "retryable": bool(self.retryable),
            "meta": dict(self.meta or {}),
        }


def error_to_payload(error: Any):
    if isinstance(error, AppError):
        return error.to_payload()
    text = _safe_text(error)
    return {
        "category": "unknown",
        "user_message": text or "Unbekannter Fehler.",
        "technical_message": text,
        "status_code": None,
        "service": "",
        "retryable": False,
        "meta": {},
    }


def user_message_from_error(error: Any, fallback="Es ist ein Fehler aufgetreten."):
    if isinstance(error, AppError):
        return error.user_message or fallback
    txt = _safe_text(error)
    return txt or fallback


def error_category_priority(category: str):
    return _ERROR_PRIORITY.get(str(category or "").strip().lower(), 999)


def _project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


def _log_dir():
    path = os.fspath(storage_path("data", "crash_logs"))
    os.makedirs(path, exist_ok=True)
    return path


def _log_path(prefix="crash"):
    day = datetime.datetime.now().strftime("%Y-%m-%d")
    return os.path.join(_log_dir(), f"{prefix}_{day}.log")


def _safe_text(value):
    try:
        return sanitize_text(str(value))
    except Exception:
        return "<unprintable>"


def _format_extra(extra):
    if extra is None:
        return ""
    try:
        if isinstance(extra, (dict, list, tuple)):
            return _safe_text(json.dumps(extra, ensure_ascii=False, default=str))
        return _safe_text(extra)
    except Exception:
        return _safe_text(extra)


def _cleanup_old_logs(prefix="crash"):
    log_dir = _log_dir()
    cutoff = datetime.datetime.now() - datetime.timedelta(days=_MAX_LOG_RETENTION_DAYS)
    file_prefix = f"{prefix}_"

    try:
        file_names = os.listdir(log_dir)
    except OSError:
        return

    for file_name in file_names:
        if not file_name.startswith(file_prefix) or not file_name.endswith(".log"):
            continue

        file_path = os.path.join(log_dir, file_name)
        try:
            modified_at = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        except OSError:
            continue

        if modified_at >= cutoff:
            continue

        try:
            os.remove(file_path)
        except OSError:
            pass


def _rotate_large_log_file(path, prefix="crash"):
    try:
        if not os.path.exists(path):
            return
        if os.path.getsize(path) < _MAX_DAILY_LOG_BYTES:
            return

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        archive_path = os.path.join(_log_dir(), f"{prefix}_{timestamp}.log")
        os.replace(path, archive_path)
    except OSError:
        pass


def _write_block(title, lines, prefix="crash"):
    path = _log_path(prefix=prefix)
    _cleanup_old_logs(prefix=prefix)
    _rotate_large_log_file(path, prefix=prefix)
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open(path, "a", encoding="utf-8") as file_handle:
        file_handle.write("\n" + "=" * 88 + "\n")
        file_handle.write(f"[{ts}] {title}\n")
        file_handle.write("-" * 88 + "\n")
        for line in lines:
            safe_line = _safe_text(line)
            file_handle.write(safe_line)
            if not safe_line.endswith("\n"):
                file_handle.write("\n")


def _extract_status_code(exc: Exception, fallback: Optional[int] = None):
    if fallback is not None:
        try:
            return int(fallback)
        except Exception:
            pass

    if isinstance(exc, urllib.error.HTTPError):
        try:
            return int(exc.code)
        except Exception:
            return None

    text = str(exc or "")
    match = re.search(r"\b([45]\d{2})\b", text)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            return None
    return None


def _http_error(service_label: str, status_code: int):
    status = int(status_code)
    if status in (401, 403):
        return ("auth", f"{service_label}: Zugriff verweigert. Bitte Zugangsdaten oder API-Key pruefen.", False)
    if status == 404:
        return ("not_found", f"{service_label}: Endpunkt oder Ressource nicht gefunden. Bitte Konfiguration pruefen.", False)
    if status == 429:
        return ("rate_limit", f"{service_label}: Anfrage-Limit erreicht. Bitte spaeter erneut versuchen.", True)
    if status >= 500:
        return ("service_unavailable", f"{service_label}: Dienst aktuell nicht erreichbar. Bitte spaeter erneut versuchen.", True)
    return ("http_error", f"{service_label}: Unerwarteter HTTP-Fehler ({status}).", False)


def _contains_any(text: str, keywords):
    value = str(text or "").lower()
    return any(keyword in value for keyword in keywords)


def classify_upcitemdb_error(exc: Exception, query_text="", status_code: Optional[int] = None):
    service = "upcitemdb"
    status = _extract_status_code(exc, fallback=status_code)
    technical = _safe_text(exc)

    if status is not None:
        category, user_message, retryable = _http_error("UPCitemdb", status)
        return AppError(category=category, user_message=user_message, technical_message=technical, status_code=status, service=service, retryable=retryable)

    if isinstance(exc, json.JSONDecodeError):
        return AppError(
            category="invalid_response",
            user_message="UPCitemdb hat keine gueltige Antwort geliefert. Bitte spaeter erneut versuchen.",
            technical_message=technical,
            service=service,
            retryable=True,
        )

    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, (socket.timeout, TimeoutError)):
            return AppError(
                category="timeout",
                user_message="UPCitemdb Anfrage ist in ein Timeout gelaufen. Bitte Netzwerk pruefen.",
                technical_message=technical,
                service=service,
                retryable=True,
            )
        return AppError(
            category="network",
            user_message="UPCitemdb ist ueber das Netzwerk nicht erreichbar. Bitte Verbindung pruefen.",
            technical_message=technical,
            service=service,
            retryable=True,
        )

    if isinstance(exc, (socket.timeout, TimeoutError)):
        return AppError(
            category="timeout",
            user_message="UPCitemdb Anfrage hat zu lange gedauert. Bitte spaeter erneut versuchen.",
            technical_message=technical,
            service=service,
            retryable=True,
        )

    if isinstance(exc, ValueError):
        return AppError(
            category="invalid_response",
            user_message="UPCitemdb Antwort war ungueltig oder unvollstaendig.",
            technical_message=technical,
            service=service,
            retryable=False,
        )

    query_info = f" query='{_safe_text(query_text)[:120]}'" if query_text else ""
    return AppError(
        category="unknown",
        user_message="UPCitemdb Suche ist fehlgeschlagen. Bitte spaeter erneut versuchen.",
        technical_message=f"{technical}{query_info}",
        service=service,
        retryable=False,
    )


def classify_gemini_error(exc: Exception, phase="request", status_code: Optional[int] = None):
    if isinstance(exc, AppError):
        return exc

    service = "gemini"
    text = _safe_text(exc)
    low = text.lower()
    status = _extract_status_code(exc, fallback=status_code)

    error_kind = str(getattr(exc, "error_kind", "") or "").strip().lower()
    quota_status = getattr(exc, "quota_status", None)
    quota_payload = quota_status.to_dict() if hasattr(quota_status, "to_dict") else {}
    meta = {
        "provider_name": service,
        "provider_phase": str(getattr(exc, "phase", "") or phase or "").strip(),
        "provider_error_kind": error_kind,
        "quota_status": quota_payload,
    }
    if error_kind:
        technical = _safe_text(getattr(exc, "technical_message", "") or text)
        user_hint = _safe_text(getattr(exc, "user_message", "") or "")
        field_name = str(getattr(exc, "field_name", "") or "").strip()
        scan_mode = str(getattr(exc, "scan_mode", "") or "").strip()
        detail_parts = []
        if scan_mode:
            detail_parts.append(f"scan_mode={scan_mode}")
        if field_name:
            detail_parts.append(f"field={field_name}")
        detail_suffix = f" ({', '.join(detail_parts)})" if detail_parts else ""

        if error_kind == "empty_response":
            return AppError(
                category="empty_response",
                user_message=user_hint or "Gemini hat keine strukturierte Antwort geliefert. Bitte erneut versuchen.",
                technical_message=technical,
                service=service,
                retryable=True,
                meta=meta,
            )

        if error_kind in ("incomplete_response", "missing_required_field"):
            return AppError(
                category="incomplete_response",
                user_message=user_hint or f"Die Modellantwort war unvollstaendig{detail_suffix}.",
                technical_message=technical,
                service=service,
                retryable=False,
                meta=meta,
            )

        if error_kind == "schema_violation":
            return AppError(
                category="schema_violation",
                user_message=user_hint or f"Die Modellantwort hatte ein ungueltiges Datenformat{detail_suffix}.",
                technical_message=technical,
                service=service,
                retryable=False,
                meta=meta,
            )

        if error_kind in ("invalid_response", "invalid_json"):
            return AppError(
                category="invalid_response",
                user_message=user_hint or "Die KI-Antwort war nicht im erwarteten Format. Bitte erneut versuchen.",
                technical_message=technical,
                service=service,
                retryable=False,
                meta=meta,
            )

        if error_kind == "auth":
            return AppError(
                category="auth",
                user_message=user_hint or "Gemini Zugriff fehlgeschlagen. Bitte API-Key pruefen.",
                technical_message=technical,
                service=service,
                retryable=False,
                meta=meta,
            )

        if error_kind == "quota_exhausted":
            return AppError(
                category="quota_exhausted",
                user_message=user_hint or "Gemini Kontingent ist aktuell erschoepft.",
                technical_message=technical,
                service=service,
                retryable=False,
                meta=meta,
            )

        if error_kind == "input_error":
            return AppError(
                category="input_error",
                user_message=user_hint or "Die an Gemini uebergebene Eingabe ist ungueltig.",
                technical_message=technical,
                service=service,
                retryable=False,
                meta=meta,
            )

        if error_kind == "transport_not_available":
            return AppError(
                category="transport_not_available",
                user_message=user_hint or "Die angeforderte Gemini-Anbindung ist hier nicht verfuegbar.",
                technical_message=technical,
                service=service,
                retryable=False,
                meta=meta,
            )

        if error_kind == "safety_blocked":
            return AppError(
                category="safety_blocked",
                user_message=user_hint or "Gemini hat die Antwort aus Sicherheitsgruenden blockiert.",
                technical_message=technical,
                service=service,
                retryable=False,
                meta=meta,
            )

    if status is not None:
        category, user_message, retryable = _http_error("Gemini", status)
        return AppError(category=category, user_message=user_message, technical_message=text, status_code=status, service=service, retryable=retryable, meta=meta)

    if isinstance(exc, json.JSONDecodeError) or phase == "json_parse":
        return AppError(
            category="invalid_response",
            user_message="Die KI-Antwort war nicht im erwarteten Format. Bitte erneut versuchen.",
            technical_message=text,
            service=service,
            retryable=False,
            meta=meta,
        )

    if _contains_any(low, ["daily", "per day", "day limit", "daily limit", "daily quota", "insufficient quota", "quota exhausted"]):
        return AppError(
            category="quota_exhausted",
            user_message="Gemini Tages- oder Kontingentgrenze ist aktuell erreicht.",
            technical_message=text,
            service=service,
            retryable=False,
            meta=meta,
        )

    if _contains_any(low, ["quota", "rate limit", "resource exhausted", "too many requests"]):
        return AppError(
            category="rate_limit",
            user_message="Gemini-Limit erreicht. Bitte kurz warten und erneut versuchen.",
            technical_message=text,
            service=service,
            retryable=True,
            meta=meta,
        )

    if _contains_any(low, ["api key", "permission denied", "unauthorized", "forbidden", "authentication"]):
        return AppError(
            category="auth",
            user_message="Gemini Zugriff fehlgeschlagen. Bitte API-Key in den Einstellungen pruefen.",
            technical_message=text,
            service=service,
            retryable=False,
            meta=meta,
        )

    if _contains_any(low, ["not found", "resource not found", "model not found"]):
        return AppError(
            category="not_found",
            user_message="Gemini Modell oder Ressource wurde nicht gefunden.",
            technical_message=text,
            service=service,
            retryable=False,
            meta=meta,
        )

    if isinstance(exc, (socket.timeout, TimeoutError)) or _contains_any(low, ["timeout", "timed out", "deadline exceeded"]):
        return AppError(
            category="timeout",
            user_message="Gemini Anfrage hat zu lange gedauert. Bitte spaeter erneut versuchen.",
            technical_message=text,
            service=service,
            retryable=True,
            meta=meta,
        )

    if _contains_any(low, ["connection", "dns", "network", "unavailable", "reset by peer"]):
        return AppError(
            category="network",
            user_message="Gemini ist ueber das Netzwerk nicht erreichbar. Bitte Verbindung pruefen.",
            technical_message=text,
            service=service,
            retryable=True,
            meta=meta,
        )

    return AppError(
        category="unknown",
        user_message="Gemini Kommunikation ist fehlgeschlagen. Bitte spaeter erneut versuchen.",
        technical_message=text,
        service=service,
        retryable=False,
        meta=meta,
    )


def log_exception(context, exc=None, extra=None, category="", service="", status_code=None, user_message=""):
    """
    Kontext + Exception + Traceback in Crash-Log schreiben.
    Kann direkt in except-Bloecken aufgerufen werden.
    """
    try:
        if exc is None:
            formatted = traceback.format_exc()
            exc_text = "<no-explicit-exception>"
        else:
            formatted = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            exc_text = _safe_text(exc)

        lines = [
            f"context: {context}",
            f"thread: {threading.current_thread().name}",
            f"category: {_safe_text(category)}",
            f"service: {_safe_text(service)}",
            f"status_code: {_safe_text(status_code)}",
            f"user_message: {_safe_text(user_message)}",
            f"exception: {exc_text}",
            "traceback:",
            _safe_text(formatted),
        ]

        if extra is not None:
            lines.append("extra:")
            lines.append(_format_extra(extra))

        _write_block("PYTHON_EXCEPTION", lines, prefix="crash")
    except Exception:
        pass


def log_message(context, message, extra=None):
    try:
        lines = [
            f"context: {context}",
            f"thread: {threading.current_thread().name}",
            f"message: {_safe_text(message)}",
        ]
        if extra is not None:
            lines.append("extra:")
            lines.append(_format_extra(extra))
        _write_block("INFO", lines, prefix="crash")
    except Exception:
        pass


def log_mail_scan_trace(context, message, extra=None):
    try:
        lines = [
            f"context: {context}",
            f"thread: {threading.current_thread().name}",
            f"message: {_safe_text(message)}",
        ]
        if extra is not None:
            lines.append("extra:")
            lines.append(_format_extra(extra))
        _write_block("MAIL_SCAN_TRACE", lines, prefix="mailscan")
    except Exception:
        pass


def log_classified_error(context, category, user_message, status_code=None, service="", exc=None, extra=None):
    """
    Schreibt klassifizierte Fehler zentral ins Crash-Log.
    """
    try:
        lines = [
            f"context: {context}",
            f"thread: {threading.current_thread().name}",
            f"category: {_safe_text(category)}",
            f"service: {_safe_text(service)}",
            f"status_code: {_safe_text(status_code)}",
            f"user_message: {_safe_text(user_message)}",
        ]

        if exc is not None:
            lines.append(f"exception: {_safe_text(exc)}")

        if extra is not None:
            lines.append("extra:")
            lines.append(_format_extra(extra))

        _write_block("CLASSIFIED_ERROR", lines, prefix="crash")
    except Exception:
        pass


def install_global_exception_hooks():
    """
    Einmalig globale Hooks aktivieren:
    - sys.excepthook
    - threading.excepthook
    - faulthandler fuer native/harte Abstuerze
    """
    global _HOOKS_INSTALLED
    global _FAULTHANDLER_FILE

    if _HOOKS_INSTALLED:
        return
    _HOOKS_INSTALLED = True

    old_sys_hook = sys.excepthook

    def _sys_hook(exc_type, exc_value, exc_tb):
        try:
            tb = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            _write_block(
                "UNHANDLED_SYS_EXCEPTION",
                [
                    f"thread: {threading.current_thread().name}",
                    f"exception: {_safe_text(exc_value)}",
                    "traceback:",
                    _safe_text(tb),
                ],
                prefix="crash",
            )
        except Exception:
            pass
        old_sys_hook(exc_type, exc_value, exc_tb)

    sys.excepthook = _sys_hook

    if hasattr(threading, "excepthook"):
        old_thread_hook = threading.excepthook

        def _thread_hook(args):
            try:
                tb = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
                _write_block(
                    "UNHANDLED_THREAD_EXCEPTION",
                    [
                        f"thread: {getattr(args.thread, 'name', '?')}",
                        f"exception: {_safe_text(args.exc_value)}",
                        "traceback:",
                        _safe_text(tb),
                    ],
                    prefix="crash",
                )
            except Exception:
                pass
            old_thread_hook(args)

        threading.excepthook = _thread_hook

    try:
        fatal_path = _log_path(prefix="fatal_native")
        _FAULTHANDLER_FILE = open(fatal_path, "a", encoding="utf-8")
        faulthandler.enable(_FAULTHANDLER_FILE, all_threads=True)
        log_message("crash_logger.install_global_exception_hooks", "faulthandler enabled", extra=fatal_path)
    except Exception as exc:
        log_exception("crash_logger.install_global_exception_hooks", exc)
