from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

from PyQt6.QtCore import QObject, QTimer, pyqtSignal

from module.crash_logger import log_mail_scan_trace


def _safe_text(value):
    return str(value or "").strip()


def mail_scan_item_key(raw_email):
    raw_email = raw_email if isinstance(raw_email, dict) else {}
    key = _safe_text(raw_email.get("_pipeline_card_key", ""))
    if key:
        return key
    return "|".join(
        part
        for part in (
            _safe_text(raw_email.get("sender", "")),
            _safe_text(raw_email.get("subject", "")),
            _safe_text(raw_email.get("date", "")),
        )
        if part
    )


def _mail_preplan(raw_email):
    raw_email = raw_email if isinstance(raw_email, dict) else {}
    preplan = raw_email.get("_mail_scan_preplan")
    return dict(preplan or {}) if isinstance(preplan, dict) else {}


@dataclass
class MailScanPipelineItem:
    mail_key: str
    raw_email: dict[str, Any]
    order_index: int = 0
    screenshot_path: str = ""
    phase: str = "detected"
    queued_for_provider: bool = False
    finished: bool = False
    error_message: str = ""
    result: dict[str, Any] | None = None


class MailScanCoordinator(QObject):
    log_signal = pyqtSignal(str)
    item_status_signal = pyqtSignal(object)
    result_signal = pyqtSignal(list, int, int)

    def __init__(self, session_id, worker_factory, governor=None, parent=None):
        super().__init__(parent)
        self.session_id = session_id
        self._worker_factory = worker_factory
        self._governor = governor
        self._items: dict[str, MailScanPipelineItem] = {}
        self._pending_keys = deque()
        self._render_phase_finished = False
        self._cancelled = False
        self._finished_emitted = False
        self._active_workers: dict[str, Any] = {}
        self._expected_total = 0
        self._completed_count = 0
        self._success_count = 0
        self._error_count = 0
        self._results_by_key: dict[str, dict[str, Any]] = {}

    def _trace(self, message, extra=None):
        payload = dict(extra or {}) if isinstance(extra, dict) else {"detail": str(extra or "")}
        payload.setdefault("session_id", int(self.session_id or 0))
        payload.setdefault("pending_count", len(self._pending_keys))
        payload.setdefault("active_count", len(self._active_workers))
        payload.setdefault("completed_count", int(self._completed_count or 0))
        log_mail_scan_trace("mail_scan_coordinator.MailScanCoordinator", message, extra=payload)

    def set_expected_total(self, total):
        try:
            self._expected_total = max(self._expected_total, int(total or 0))
        except Exception:
            pass

    def register_detected_mail(self, raw_email, current=0, total=0):
        item = self._ensure_item(raw_email, current=current)
        self._trace(
            "mail_detected",
            {
                "mail_key": item.mail_key,
                "current": int(current or 0),
                "total": int(total or 0),
            },
        )
        if total:
            self.set_expected_total(total)
        self._set_phase(item, "detected")

    def mark_rendering_started(self, raw_email):
        item = self._ensure_item(raw_email)
        self._set_phase(item, "rendering_required")

    def submit_render_result(self, raw_email, screenshot_path=""):
        item = self._ensure_item(raw_email)
        item.screenshot_path = str(screenshot_path or "")
        self._trace(
            "render_result_submitted",
            {
                "mail_key": item.mail_key,
                "has_screenshot": bool(item.screenshot_path),
                "screenshot_path": item.screenshot_path,
            },
        )
        if item.screenshot_path:
            self._set_phase(item, "prepared", screenshot_path=item.screenshot_path)
        else:
            self._set_phase(item, "rendering_skipped", screenshot_path="")
        self._set_phase(item, "scan_ready", screenshot_path=item.screenshot_path)
        if not item.queued_for_provider:
            item.queued_for_provider = True
            self._pending_keys.append(item.mail_key)
            self._set_phase(item, "provider_queued", screenshot_path=item.screenshot_path)
            self._start_next_worker_if_possible()

    def mark_render_phase_finished(self):
        self._render_phase_finished = True
        self._trace("render_phase_marked_finished")
        self._start_next_worker_if_possible()
        self._try_finish_run()

    def cancel(self):
        self._cancelled = True
        self._pending_keys.clear()
        if self._governor is not None:
            try:
                self._governor.cancel(count_as_user_abort=False)
            except Exception:
                pass
        for worker in list(self._active_workers.values()):
            if worker is not None:
                try:
                    worker.requestInterruption()
                except Exception:
                    pass

    def _ensure_item(self, raw_email, current=0):
        raw_email = dict(raw_email or {})
        mail_key = mail_scan_item_key(raw_email)
        item = self._items.get(mail_key)
        if item is None:
            order_index = max(0, int(current or 0) - 1) if current else len(self._items)
            item = MailScanPipelineItem(mail_key=mail_key, raw_email=raw_email, order_index=order_index)
            self._items[mail_key] = item
        else:
            item.raw_email = raw_email
            if current:
                item.order_index = min(item.order_index, max(0, int(current) - 1))
        return item

    def _emit_item_status(self, item, phase, **extra):
        preplan = _mail_preplan(item.raw_email)
        preplan_text = _safe_text(preplan.get("status_text", ""))
        status_text = {
            "detected": "Neue Mail erkannt.",
            "rendering_required": "Mail wird fuer den Scan vorbereitet.",
            "rendering_skipped": preplan_text or "Screenshot uebersprungen, Text-Fallback aktiv.",
            "prepared": "Mail ist vorbereitet.",
            "scan_ready": preplan_text or "Mail ist scanbereit.",
            "provider_queued": preplan_text or "Mail wartet auf den Provider-Scan.",
            "scanning": preplan_text or "Mail wird von der KI analysiert.",
            "waiting_retry": "Mail wartet auf einen erneuten Versuch.",
            "waiting_reset": "Mail wartet auf Provider-Reset oder Cooldown.",
            "retrying": "Mail wird nach einer Wartezeit erneut versucht.",
            "quota_exhausted": "Provider-Kontingent ist erschopft.",
            "aborted": "Mail-Scan wurde abgebrochen.",
            "finished": "Mail-Scan abgeschlossen.",
            "error": "Mail-Scan fehlgeschlagen.",
        }.get(str(phase or ""), "")
        payload = {
            "session_id": self.session_id,
            "phase": str(phase or ""),
            "state": str(phase or ""),
            "mail_key": item.mail_key,
            "raw_email": dict(item.raw_email or {}),
            "subject": _safe_text((item.raw_email or {}).get("subject", "")),
            "sender": _safe_text((item.raw_email or {}).get("sender", "")),
            "completed": int(self._completed_count),
            "success_count": int(self._success_count),
            "error_count": int(self._error_count),
            "queued": int(len(self._pending_keys)),
            "total": int(self._expected_total or len(self._items)),
            "screenshot_path": item.screenshot_path,
            "render_finished": bool(self._render_phase_finished),
            "status_text": status_text,
            "active_workers": int(len(self._active_workers)),
        }
        if self._governor is not None:
            try:
                payload["governor"] = self._governor.snapshot(queued_workers=len(self._pending_keys))
            except Exception:
                payload["governor"] = {}
        payload.update(extra)
        self.item_status_signal.emit(payload)

    def _set_phase(self, item, phase, **extra):
        item.phase = str(phase or "")
        self._emit_item_status(item, item.phase, **extra)

    def _start_next_worker_if_possible(self):
        if self._cancelled or not self._pending_keys:
            return
        while self._pending_keys:
            active_count = len(self._active_workers)
            if self._governor is not None:
                try:
                    available = self._governor.dispatch_slots_available(active_count=active_count)
                    if available <= 0:
                        snapshot = self._governor.snapshot(queued_workers=len(self._pending_keys))
                        if bool(snapshot.get("hard_quota_active", False)):
                            self._mark_pending_as_blocked(
                                phase="quota_exhausted",
                                status_text=_safe_text(snapshot.get("waiting_reason", "")) or "Provider-Kontingent ist erschopft.",
                            )
                            self._try_finish_run()
                        return
                except Exception:
                    pass
            elif active_count > 0:
                return

            mail_key = self._pending_keys.popleft()
            item = self._items.get(mail_key)
            if item is None or item.finished:
                continue
            self._set_phase(item, "scanning")
            self._trace("worker_started", {"mail_key": item.mail_key, "order_index": int(item.order_index or 0)})
            worker = self._worker_factory(item)
            worker.log_signal.connect(self.log_signal.emit)
            if hasattr(worker, "status_signal"):
                worker.status_signal.connect(self._on_worker_status)
            worker.finished.connect(worker.deleteLater)
            worker.finished.connect(lambda mail_key=item.mail_key: QTimer.singleShot(0, lambda: self._on_worker_thread_finished(mail_key)))
            worker.finished_signal.connect(self._on_worker_finished)
            self._active_workers[item.mail_key] = worker
            if self._governor is not None:
                try:
                    self._governor.register_worker_started(item.mail_key)
                except Exception:
                    pass
            worker.start()

    def _on_worker_finished(self, payload):
        payload = payload if isinstance(payload, dict) else {}
        if self._cancelled:
            self._active_workers.clear()
            return
        mail_key = _safe_text(payload.get("mail_key", ""))
        item = self._items.get(mail_key)
        if item is None:
            item = self._ensure_item(payload.get("raw_email", {}))
        item.finished = True
        self._completed_count += 1
        if mail_key in self._active_workers:
            self._active_workers.pop(mail_key, None)

        result = payload.get("result") if isinstance(payload.get("result"), dict) else None
        error_message = _safe_text(payload.get("error_message", ""))
        empty = bool(payload.get("empty", False))
        error_category = _safe_text(payload.get("error_category", "")).lower()

        if error_message:
            self._error_count += 1
            item.error_message = error_message
            self._trace("worker_finished_error", {"mail_key": item.mail_key, "error_message": error_message, "error_category": error_category})
            self._set_phase(item, "error", success=False, error_message=error_message)
        elif result:
            self._success_count += 1
            item.result = dict(result)
            self._results_by_key[item.mail_key] = dict(result)
            self._trace("worker_finished_success", {"mail_key": item.mail_key})
            self._set_phase(item, "finished", success=True)
        else:
            self._trace("worker_finished_empty", {"mail_key": item.mail_key, "empty": bool(empty)})
            self._set_phase(item, "finished", success=False, empty=empty)

        if self._governor is not None:
            try:
                self._governor.register_worker_finished(
                    item.mail_key,
                    success=bool(result),
                    empty=bool(empty and not result and not error_message),
                    error_category=error_category,
                )
            except Exception:
                pass

        self._start_next_worker_if_possible()
        self._try_finish_run()

    def _on_worker_thread_finished(self, mail_key):
        mail_key = _safe_text(mail_key)
        if not mail_key or self._cancelled:
            return
        worker = self._active_workers.get(mail_key)
        item = self._items.get(mail_key)
        if worker is None or item is None or item.finished:
            return

        self._active_workers.pop(mail_key, None)
        item.finished = True
        self._completed_count += 1
        self._error_count += 1
        fallback_message = "Mail-Scan wurde unerwartet beendet. Diese Mail wird uebersprungen, der Rest laeuft weiter."
        item.error_message = fallback_message
        self._trace("worker_thread_finished_without_payload", {"mail_key": item.mail_key, "error_message": fallback_message})
        self._set_phase(item, "error", success=False, error_message=fallback_message)
        self.log_signal.emit(f" {fallback_message}")

        if self._governor is not None:
            try:
                self._governor.register_worker_finished(
                    item.mail_key,
                    success=False,
                    empty=False,
                    error_category="worker_ended",
                )
            except Exception:
                pass

        self._start_next_worker_if_possible()
        self._try_finish_run()

    def _on_worker_status(self, payload):
        payload = payload if isinstance(payload, dict) else {}
        mail_key = _safe_text(payload.get("mail_key", ""))
        item = self._items.get(mail_key)
        if item is None:
            raw_email = payload.get("raw_email") if isinstance(payload.get("raw_email"), dict) else {}
            item = self._ensure_item(raw_email)
        phase = _safe_text(payload.get("phase", "")) or "scanning"
        self._trace("worker_status", {"mail_key": item.mail_key, "phase": phase, "status_text": _safe_text(payload.get("status_text", ""))})
        extra = dict(payload)
        extra.pop("phase", None)
        extra.pop("mail_key", None)
        extra.pop("raw_email", None)
        self._set_phase(item, phase, **extra)

    def _try_finish_run(self):
        if self._finished_emitted or self._cancelled or not self._render_phase_finished:
            return
        if self._active_workers or self._pending_keys:
            return
        ordered_results = []
        for item in sorted(self._items.values(), key=lambda row: int(row.order_index or 0)):
            if item.mail_key in self._results_by_key:
                ordered_results.append(dict(self._results_by_key[item.mail_key]))
        self._finished_emitted = True
        self._trace(
            "run_finished",
            {
                "result_count": len(ordered_results),
                "success_count": int(self._success_count or 0),
                "error_count": int(self._error_count or 0),
            },
        )
        self.result_signal.emit(ordered_results, -1, -1)

    def _mark_pending_as_blocked(self, phase="quota_exhausted", status_text=""):
        blocked_keys = list(self._pending_keys)
        self._pending_keys.clear()
        for mail_key in blocked_keys:
            item = self._items.get(mail_key)
            if item is None or item.finished:
                continue
            item.finished = True
            item.error_message = _safe_text(status_text) or "Provider-Kontingent ist erschopft."
            self._completed_count += 1
            self._error_count += 1
            self._trace("pending_mail_blocked", {"mail_key": item.mail_key, "phase": str(phase or "quota_exhausted"), "status_text": item.error_message})
            self._set_phase(item, str(phase or "quota_exhausted"), success=False, error_message=item.error_message)
