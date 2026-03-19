from __future__ import annotations

from dataclasses import dataclass, field
import random
import threading
import time
from typing import Any, Callable

from PyQt6.QtCore import QObject, Qt, pyqtSignal, pyqtSlot

from module.ai import build_provider_profile
from module.ai.types import ProviderProfile
from module.crash_logger import AppError, log_mail_scan_trace


def _safe_text(value) -> str:
    return str(value or "").strip()


def _safe_float(value, default=0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return float(default or 0.0)


def _safe_int(value, default=0) -> int:
    try:
        return int(value or 0)
    except Exception:
        return int(default or 0)


@dataclass
class GovernorSnapshot:
    provider_name: str = ""
    profile_name: str = ""
    current_concurrency: int = 1
    max_concurrency: int = 1
    active_workers: int = 0
    queued_workers: int = 0
    cooldown_active: bool = False
    cooldown_until_epoch: float = 0.0
    hard_quota_active: bool = False
    hard_quota_reset_at: str = ""
    waiting_reason: str = ""
    stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider_name": self.provider_name,
            "profile_name": self.profile_name,
            "current_concurrency": int(self.current_concurrency or 1),
            "max_concurrency": int(self.max_concurrency or 1),
            "active_workers": int(self.active_workers or 0),
            "queued_workers": int(self.queued_workers or 0),
            "cooldown_active": bool(self.cooldown_active),
            "cooldown_until_epoch": float(self.cooldown_until_epoch or 0.0),
            "hard_quota_active": bool(self.hard_quota_active),
            "hard_quota_reset_at": _safe_text(self.hard_quota_reset_at),
            "waiting_reason": _safe_text(self.waiting_reason),
            "stats": dict(self.stats or {}),
        }


class MailQuotaGovernor(QObject):
    status_signal = pyqtSignal(object)
    metrics_signal = pyqtSignal(object)
    _status_dispatch_signal = pyqtSignal(object)
    _metrics_dispatch_signal = pyqtSignal(object)

    def __init__(self, session_id, provider_name="", profile_name="", profile_overrides=None, parent=None):
        super().__init__(parent)
        self.session_id = int(session_id or 0)
        self.provider_name = _safe_text(provider_name).lower() or "gemini"
        self.profile_name = _safe_text(profile_name)
        self.profile_overrides = dict(profile_overrides or {}) if isinstance(profile_overrides, dict) else {}
        self.profile = build_provider_profile(
            provider_name=self.provider_name,
            profile_name=self.profile_name,
            transport="native",
            overrides=self.profile_overrides or None,
        )
        if not isinstance(self.profile, ProviderProfile):
            raise RuntimeError("Providerprofil konnte nicht aufgebaut werden.")

        execution = self.profile.policy.execution
        retry_policy = self.profile.policy.retry
        backoff_policy = self.profile.policy.backoff
        cost_policy = self.profile.policy.cost
        second_pass_policy = self.profile.policy.second_pass

        self._lock = threading.RLock()
        self._schedule_lock = threading.Lock()
        self._cancelled = False
        self._initial_delay_sec = max(0.0, _safe_float(getattr(execution, "initial_delay_sec", 0.0)))
        self._initial_ready_at = time.monotonic() + self._initial_delay_sec if self._initial_delay_sec > 0 else 0.0
        self._request_spacing_sec = max(0.0, _safe_float(getattr(execution, "request_spacing_sec", 0.0)))
        self._max_attempts = max(1, _safe_int(getattr(retry_policy, "max_attempts", 1), 1))
        self._backoff_initial_sec = max(0.0, _safe_float(getattr(backoff_policy, "initial_delay_sec", 0.0)))
        self._backoff_multiplier = max(1.0, _safe_float(getattr(backoff_policy, "multiplier", 1.0), 1.0))
        self._backoff_max_sec = max(self._backoff_initial_sec, _safe_float(getattr(backoff_policy, "max_delay_sec", 0.0)))
        self._serialize_requests = bool(getattr(execution, "serialize_requests", True))
        raw_max_parallel = max(1, _safe_int(getattr(execution, "max_parallel_requests", 1), 1))
        self._max_concurrency = 1 if self._serialize_requests else raw_max_parallel
        self._min_concurrency = 1
        self._current_concurrency = 1
        self._success_streak = 0
        self._instability_score = 0
        self._last_request_started_at = 0.0
        self._requests_started = 0
        self._cooldown_until = 0.0
        self._cooldown_reason = ""
        self._hard_quota_active = False
        self._hard_quota_reason = ""
        self._hard_quota_reset_at = ""
        self._active_workers: set[str] = set()
        self._wait_notified_keys: set[tuple[str, str]] = set()
        self._allow_second_pass = int(getattr(second_pass_policy, "max_passes", 0) or 0) > 0
        self._prefer_single_pass = bool(getattr(cost_policy, "prefer_single_pass", True))
        self._max_extra_calls = max(0, _safe_int(getattr(cost_policy, "max_extra_calls_per_item", 0), 0))
        self._last_pressure_at = 0.0
        self._stats = {
            "requests_started": 0,
            "primary_requests_started": 0,
            "second_pass_requests_started": 0,
            "completed_items": 0,
            "successful_items": 0,
            "empty_items": 0,
            "failed_items": 0,
            "retry_count": 0,
            "rate_limit_events": 0,
            "quota_exhausted_events": 0,
            "timeout_events": 0,
            "network_events": 0,
            "service_unavailable_events": 0,
            "cooldown_events": 0,
            "retry_after_events": 0,
            "minutes_limit_waits": 0,
            "reset_waits": 0,
            "second_pass_suppressed": 0,
            "second_pass_delayed": 0,
            "second_pass_started": 0,
            "user_aborts": 0,
            "peak_concurrency": 0,
            "adaptive_increase_events": 0,
            "adaptive_decrease_events": 0,
            "last_error_category": "",
        }
        self._status_dispatch_signal.connect(self._forward_status_signal, Qt.ConnectionType.QueuedConnection)
        self._metrics_dispatch_signal.connect(self._forward_metrics_signal, Qt.ConnectionType.QueuedConnection)

    def _trace(self, message: str, extra=None):
        payload = dict(extra or {}) if isinstance(extra, dict) else {"detail": str(extra or "")}
        payload.setdefault("session_id", int(self.session_id or 0))
        payload.setdefault("provider_name", self.provider_name)
        payload.setdefault("profile_name", _safe_text(getattr(self.profile, "profile_name", "") or self.profile_name))
        log_mail_scan_trace("mail_quota_governor.MailQuotaGovernor", message, extra=payload)

    @pyqtSlot(object)
    def _forward_status_signal(self, payload):
        self.status_signal.emit(dict(payload or {}))

    @pyqtSlot(object)
    def _forward_metrics_signal(self, payload):
        self.metrics_signal.emit(dict(payload or {}))

    def cancel(self, count_as_user_abort=False):
        with self._lock:
            self._cancelled = True
            if count_as_user_abort:
                self._stats["user_aborts"] = int(self._stats.get("user_aborts", 0) or 0) + 1
                self._emit_status_locked(
                    {
                        "phase": "aborted",
                        "status_text": "Scan wurde vom Nutzer abgebrochen.",
                        "category": "aborted",
                    }
                )
            self._emit_metrics_locked()

    def max_attempts(self) -> int:
        return int(self._max_attempts or 1)

    def dispatch_slots_available(self, active_count=0) -> int:
        with self._lock:
            if self._cancelled or self._hard_quota_active:
                return 0
            if self._cooldown_until > time.monotonic():
                return 0
            return max(0, int(self._current_concurrency or 1) - max(0, int(active_count or 0)))

    def register_worker_started(self, mail_key: str):
        with self._lock:
            key = _safe_text(mail_key)
            if key:
                self._active_workers.add(key)
            active_count = len(self._active_workers)
            self._stats["peak_concurrency"] = max(int(self._stats.get("peak_concurrency", 0) or 0), active_count)
            self._emit_metrics_locked()

    def register_worker_finished(self, mail_key: str, success=False, empty=False, error_category=""):
        with self._lock:
            key = _safe_text(mail_key)
            if key and key in self._active_workers:
                self._active_workers.remove(key)
            self._stats["completed_items"] = int(self._stats.get("completed_items", 0) or 0) + 1
            if success:
                self._stats["successful_items"] = int(self._stats.get("successful_items", 0) or 0) + 1
                self._note_success_locked()
            elif empty:
                self._stats["empty_items"] = int(self._stats.get("empty_items", 0) or 0) + 1
                self._note_success_locked()
            else:
                self._stats["failed_items"] = int(self._stats.get("failed_items", 0) or 0) + 1
                if error_category:
                    self._stats["last_error_category"] = _safe_text(error_category).lower()
            self._emit_metrics_locked()

    def before_request(self, mail_key: str, request_kind="primary", is_cancelled: Callable[[], bool] | None = None) -> dict[str, Any]:
        self._trace("before_request_enter", {"mail_key": _safe_text(mail_key), "request_kind": _safe_text(request_kind)})
        with self._schedule_lock:
            while True:
                with self._lock:
                    cancelled = self._cancelled or bool(callable(is_cancelled) and is_cancelled())
                    if cancelled:
                        self._trace("before_request_aborted", {"mail_key": _safe_text(mail_key), "request_kind": _safe_text(request_kind)})
                        return {
                            "action": "aborted",
                            "phase": "aborted",
                            "status_text": "Scan wurde abgebrochen.",
                        }

                    if self._hard_quota_active:
                        retry_after = self._retry_after_from_reset_locked()
                        self._trace("before_request_hard_quota", {"mail_key": _safe_text(mail_key), "request_kind": _safe_text(request_kind), "retry_after": int(retry_after or 0)})
                        return {
                            "action": "quota_exhausted",
                            "phase": "quota_exhausted",
                            "status_text": self._hard_quota_status_text_locked(),
                            "wait_seconds": retry_after,
                            "reset_at": _safe_text(self._hard_quota_reset_at),
                        }

                    now = time.monotonic()
                    spacing_wait = 0.0
                    cooldown_wait = max(0.0, self._cooldown_until - now)
                    if self._requests_started == 0 and self._initial_ready_at > 0 and self._last_request_started_at <= 0:
                        spacing_wait = max(0.0, self._initial_ready_at - now)
                    elif self._request_spacing_sec > 0 and self._last_request_started_at > 0:
                        spacing_wait = max(0.0, (self._last_request_started_at + self._request_spacing_sec) - now)
                    wait_seconds = max(spacing_wait, cooldown_wait)
                    if wait_seconds <= 0:
                        self._initial_ready_at = 0.0
                        self._last_request_started_at = now
                        self._requests_started += 1
                        self._stats["requests_started"] = int(self._stats.get("requests_started", 0) or 0) + 1
                        if str(request_kind or "") == "second_pass":
                            self._stats["second_pass_requests_started"] = int(self._stats.get("second_pass_requests_started", 0) or 0) + 1
                            self._stats["second_pass_started"] = int(self._stats.get("second_pass_started", 0) or 0) + 1
                        else:
                            self._stats["primary_requests_started"] = int(self._stats.get("primary_requests_started", 0) or 0) + 1
                        self._wait_notified_keys.discard((_safe_text(mail_key), _safe_text(request_kind)))
                        self._emit_metrics_locked()
                        self._trace("before_request_ready", {"mail_key": _safe_text(mail_key), "request_kind": _safe_text(request_kind)})
                        return {
                            "action": "ready",
                            "phase": "ready",
                            "status_text": "",
                            "wait_seconds": 0,
                        }

                    reason = "cooldown" if cooldown_wait >= spacing_wait else "spacing"
                    status_text = self._build_wait_status_locked(reason, wait_seconds)
                    wait_key = (_safe_text(mail_key), _safe_text(request_kind))
                    if wait_key not in self._wait_notified_keys:
                        self._wait_notified_keys.add(wait_key)
                        if reason == "cooldown":
                            self._stats["cooldown_events"] = int(self._stats.get("cooldown_events", 0) or 0) + 1
                            if self._cooldown_reason == "rate_limit":
                                self._stats["reset_waits"] = int(self._stats.get("reset_waits", 0) or 0) + 1
                        self._emit_status_locked(
                            {
                                "mail_key": _safe_text(mail_key),
                                "phase": "waiting_reset" if reason == "cooldown" and wait_seconds >= 20 else "waiting_retry",
                                "category": "rate_limit" if reason == "cooldown" else "spacing",
                                "status_text": status_text,
                                "wait_seconds": int(wait_seconds),
                            }
                        )
                        self._trace(
                            "before_request_waiting",
                            {
                                "mail_key": _safe_text(mail_key),
                                "request_kind": _safe_text(request_kind),
                                "reason": reason,
                                "wait_seconds": int(wait_seconds),
                            },
                        )
                time.sleep(min(1.0, max(0.2, wait_seconds)))

    def should_retry(self, app_error: AppError, attempt_number: int) -> bool:
        policy = self.profile.policy.retry
        category = _safe_text(getattr(app_error, "category", "")).lower()
        if attempt_number >= self.max_attempts():
            return False
        if category == "quota_exhausted":
            return False
        if category == "rate_limit":
            return bool(getattr(policy, "retry_on_rate_limit", True))
        if category == "timeout":
            return bool(getattr(policy, "retry_on_timeout", True))
        if category == "network":
            return bool(getattr(policy, "retry_on_network", True))
        if category == "service_unavailable":
            return bool(getattr(policy, "retry_on_service_unavailable", True))
        if category in {"invalid_response", "empty_response"}:
            return bool(getattr(policy, "retry_on_invalid_response", False))
        return False

    def register_retryable_error(self, mail_key: str, app_error: AppError, attempt_number: int) -> dict[str, Any]:
        category = _safe_text(getattr(app_error, "category", "")).lower()
        quota_status = self._quota_status_payload(app_error)
        retry_after = _safe_int(quota_status.get("retry_after_sec", 0), 0)
        with self._lock:
            self._success_streak = 0
            self._instability_score = min(6, int(self._instability_score or 0) + 1)
            self._stats["retry_count"] = int(self._stats.get("retry_count", 0) or 0) + 1
            self._stats["last_error_category"] = category
            if category == "rate_limit":
                self._stats["rate_limit_events"] = int(self._stats.get("rate_limit_events", 0) or 0) + 1
                if retry_after > 0:
                    self._stats["retry_after_events"] = int(self._stats.get("retry_after_events", 0) or 0) + 1
                self._stats["minutes_limit_waits"] = int(self._stats.get("minutes_limit_waits", 0) or 0) + 1
            elif category == "timeout":
                self._stats["timeout_events"] = int(self._stats.get("timeout_events", 0) or 0) + 1
            elif category == "network":
                self._stats["network_events"] = int(self._stats.get("network_events", 0) or 0) + 1
            elif category == "service_unavailable":
                self._stats["service_unavailable_events"] = int(self._stats.get("service_unavailable_events", 0) or 0) + 1

            wait_seconds = self._choose_retry_wait_locked(category, attempt_number, retry_after)
            if category == "rate_limit":
                self._cooldown_until = max(self._cooldown_until, time.monotonic() + wait_seconds)
                self._cooldown_reason = "rate_limit"
                self._reduce_concurrency_locked("limitdruck")
            elif category in {"timeout", "network", "service_unavailable"}:
                self._cooldown_until = max(self._cooldown_until, time.monotonic() + wait_seconds)
                self._cooldown_reason = category
                if self._instability_score >= 2:
                    self._reduce_concurrency_locked("instabile Providerlage")

            status_text = self._retry_status_text_locked(category, wait_seconds, attempt_number)
            self._emit_status_locked(
                {
                    "mail_key": _safe_text(mail_key),
                    "phase": "retrying",
                    "category": category,
                    "status_text": status_text,
                    "wait_seconds": int(wait_seconds),
                    "attempt": int(attempt_number),
                }
            )
            self._emit_metrics_locked()
            return {
                "wait_seconds": int(wait_seconds),
                "status_text": status_text,
                "category": category,
            }

    def register_hard_quota_error(self, mail_key: str, app_error: AppError) -> dict[str, Any]:
        quota_status = self._quota_status_payload(app_error)
        with self._lock:
            self._hard_quota_active = True
            self._hard_quota_reason = _safe_text(app_error.user_message) or "Kontingent erschoepft."
            self._hard_quota_reset_at = _safe_text(quota_status.get("reset_at", ""))
            self._stats["quota_exhausted_events"] = int(self._stats.get("quota_exhausted_events", 0) or 0) + 1
            self._reduce_concurrency_locked("Kontingent erschopft")
            payload = {
                "mail_key": _safe_text(mail_key),
                "phase": "quota_exhausted",
                "category": "quota_exhausted",
                "status_text": self._hard_quota_status_text_locked(),
                "reset_at": self._hard_quota_reset_at,
            }
            self._emit_status_locked(payload)
            self._emit_metrics_locked()
            return payload

    def allow_second_pass(self, mail_key: str, missing_fields=None, source_type="") -> tuple[bool, str]:
        missing_fields = [item for item in list(missing_fields or []) if _safe_text(item)]
        with self._lock:
            if not self._allow_second_pass or self._max_extra_calls <= 0:
                self._stats["second_pass_suppressed"] = int(self._stats.get("second_pass_suppressed", 0) or 0) + 1
                return False, "Zweiter Pass laut Profil nicht vorgesehen."
            if self._prefer_single_pass and self._instability_score > 0:
                self._stats["second_pass_suppressed"] = int(self._stats.get("second_pass_suppressed", 0) or 0) + 1
                return False, "Zweiter Pass wegen vorsichtigem Profil aktuell unterdrueckt."
            if self._hard_quota_active:
                self._stats["second_pass_suppressed"] = int(self._stats.get("second_pass_suppressed", 0) or 0) + 1
                return False, "Zweiter Pass wegen erreichtem Kontingent unterdrueckt."
            cooldown_left = max(0.0, self._cooldown_until - time.monotonic())
            if cooldown_left >= 10:
                self._stats["second_pass_suppressed"] = int(self._stats.get("second_pass_suppressed", 0) or 0) + 1
                self._stats["second_pass_delayed"] = int(self._stats.get("second_pass_delayed", 0) or 0) + 1
                return False, "Zweiter Pass wegen aktuellem Limitdruck unterdrueckt."
            if self._current_concurrency <= 1 and self._last_pressure_at > 0 and (time.monotonic() - self._last_pressure_at) < 90:
                self._stats["second_pass_suppressed"] = int(self._stats.get("second_pass_suppressed", 0) or 0) + 1
                return False, "Zweiter Pass wegen frischem Limitdruck unterdrueckt."
            if _safe_text(source_type) == "mail_render_screenshot" and self.profile.provider_name == "claude":
                self._stats["second_pass_suppressed"] = int(self._stats.get("second_pass_suppressed", 0) or 0) + 1
                return False, "Dokumentenprofil spart den zweiten Screenshot-Pass."
            return True, ""

    def snapshot(self, queued_workers=0) -> dict[str, Any]:
        with self._lock:
            return GovernorSnapshot(
                provider_name=self.provider_name,
                profile_name=_safe_text(getattr(self.profile, "profile_name", "") or self.profile_name),
                current_concurrency=int(self._current_concurrency or 1),
                max_concurrency=int(self._max_concurrency or 1),
                active_workers=len(self._active_workers),
                queued_workers=max(0, int(queued_workers or 0)),
                cooldown_active=bool(self._cooldown_until > time.monotonic()),
                cooldown_until_epoch=time.time() + max(0.0, self._cooldown_until - time.monotonic()),
                hard_quota_active=bool(self._hard_quota_active),
                hard_quota_reset_at=_safe_text(self._hard_quota_reset_at),
                waiting_reason=_safe_text(self._cooldown_reason or self._hard_quota_reason),
                stats=dict(self._stats or {}),
            ).to_dict()

    def _quota_status_payload(self, app_error: AppError) -> dict[str, Any]:
        meta = dict(getattr(app_error, "meta", {}) or {})
        quota_status = meta.get("quota_status", {}) if isinstance(meta.get("quota_status", {}), dict) else {}
        return dict(quota_status or {})

    def _choose_retry_wait_locked(self, category: str, attempt_number: int, retry_after_sec: int) -> float:
        if category == "rate_limit" and retry_after_sec > 0:
            return float(retry_after_sec)
        base = self._backoff_initial_sec if self._backoff_initial_sec > 0 else (2.0 if category == "rate_limit" else 1.5)
        multiplier = max(1.0, float(self._backoff_multiplier or 1.0))
        wait_value = base * (multiplier ** max(0, int(attempt_number or 1) - 1))
        max_delay = self._backoff_max_sec if self._backoff_max_sec > 0 else max(base, 30.0)
        bounded = min(wait_value, max_delay)
        jitter = min(2.0, max(0.1, bounded * 0.15))
        return max(0.5, bounded + random.uniform(0.0, jitter))

    def _note_success_locked(self):
        self._success_streak = int(self._success_streak or 0) + 1
        self._instability_score = max(0, int(self._instability_score or 0) - 1)
        if self._cooldown_until <= time.monotonic():
            self._cooldown_reason = ""
        threshold = max(2, int(self._current_concurrency or 1) * 2)
        if self._current_concurrency < self._max_concurrency and self._success_streak >= threshold and self._instability_score <= 0:
            self._current_concurrency += 1
            self._success_streak = 0
            self._stats["adaptive_increase_events"] = int(self._stats.get("adaptive_increase_events", 0) or 0) + 1
            self._emit_status_locked(
                {
                    "phase": "concurrency_update",
                    "category": "adaptive",
                    "status_text": f"Provider stabil, Parallelitaet vorsichtig auf {self._current_concurrency} erhoeht.",
                }
            )

    def _reduce_concurrency_locked(self, reason: str):
        self._last_pressure_at = time.monotonic()
        if self._current_concurrency <= self._min_concurrency:
            return
        self._current_concurrency = max(self._min_concurrency, int(self._current_concurrency or 1) - 1)
        self._success_streak = 0
        self._stats["adaptive_decrease_events"] = int(self._stats.get("adaptive_decrease_events", 0) or 0) + 1
        self._emit_status_locked(
            {
                "phase": "concurrency_update",
                "category": "adaptive",
                "status_text": f"Parallelitaet wird wegen {reason} auf {self._current_concurrency} reduziert.",
            }
        )

    def _retry_status_text_locked(self, category: str, wait_seconds: float, attempt_number: int) -> str:
        wait_int = max(1, int(round(wait_seconds or 0)))
        if category == "rate_limit":
            return f"Wartet auf Minutenlimit oder Reset ({wait_int}s), danach erneuter Versuch {attempt_number + 1}/{self.max_attempts()}."
        if category == "timeout":
            return f"Temporarer Timeout, erneuter Versuch in {wait_int}s."
        if category == "network":
            return f"Temporarer Netzwerkfehler, erneuter Versuch in {wait_int}s."
        if category == "service_unavailable":
            return f"Provider temporaer nicht verfuegbar, erneuter Versuch in {wait_int}s."
        return f"Temporarer Providerfehler, erneuter Versuch in {wait_int}s."

    def _build_wait_status_locked(self, reason: str, wait_seconds: float) -> str:
        wait_int = max(1, int(round(wait_seconds or 0)))
        if reason == "cooldown":
            if self._cooldown_reason == "rate_limit":
                return f"Wartet auf Minutenlimit oder Retry-After ({wait_int}s)."
            return f"Temporarer Providerfehler, Wartezeit laeuft noch {wait_int}s."
        return f"Konservativer Startabstand aktiv, naechster Request in {wait_int}s."

    def _retry_after_from_reset_locked(self) -> int:
        return max(0, int(round(max(0.0, self._cooldown_until - time.monotonic()))))

    def _hard_quota_status_text_locked(self) -> str:
        if self._hard_quota_reset_at:
            return f"Tageskontingent erreicht. Warte auf Reset: {self._hard_quota_reset_at}."
        return self._hard_quota_reason or "Kontingent erschopft. Weitere Retries werden vermieden."

    def _emit_status_locked(self, payload: dict[str, Any]):
        data = dict(payload or {})
        data.setdefault("session_id", self.session_id)
        data.setdefault("provider_name", self.provider_name)
        data.setdefault("profile_name", _safe_text(getattr(self.profile, "profile_name", "") or self.profile_name))
        data.setdefault("current_concurrency", int(self._current_concurrency or 1))
        data.setdefault("max_concurrency", int(self._max_concurrency or 1))
        data.setdefault("active_workers", len(self._active_workers))
        self._status_dispatch_signal.emit(data)

    def _emit_metrics_locked(self):
        self._metrics_dispatch_signal.emit(self.snapshot())
