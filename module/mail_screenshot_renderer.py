"""
Asynchrones Screenshot-Rendering fuer HTML-Mails.

Wichtig:
QWebEngine-Rendering muss im GUI-Kontext bleiben, darf aber die UI nicht durch
manuelle EventLoops blockieren. Diese Klasse rendert daher Schritt fuer Schritt
asynchron und meldet Fortschritt per Signal zurueck.
"""

from __future__ import annotations

import os
import tempfile

from PyQt6.QtCore import QObject, QTimer, pyqtSignal
from PyQt6.QtWebEngineWidgets import QWebEngineView

from module.crash_logger import log_exception
from module.database_manager import DatabaseManager
from module.media.media_service import MediaService
from module.safe_mail_renderer import SafeMailRenderer


class MailScreenshotRenderJob(QObject):
    progress_signal = pyqtSignal(object, object)
    finished_signal = pyqtSignal(object, object)
    error_signal = pyqtSignal(object, str)

    def __init__(self, session_id, raw_emails, settings_manager, parent=None):
        super().__init__(parent)
        self.session_id = session_id
        self.raw_emails = list(raw_emails or [])
        self.settings_manager = settings_manager
        self._results = []
        self._index = 0
        self._view = None
        self._cancelled = False
        self._current_raw = None
        self._media = None

    def start(self):
        try:
            self._ensure_view()
            self._render_next()
        except Exception as exc:
            self._handle_error(exc)

    def cancel(self):
        self._cancelled = True
        self._cleanup_tempfiles()
        self._cleanup_view()

    def _ensure_view(self):
        if self._view is not None:
            return
        host = self.parent()
        self._view = QWebEngineView(host)
        self._view.setFixedSize(900, 2000)
        self._view.move(-2000, -2000)
        self._view.show()

    def _media_service(self):
        if self._media is None:
            self._media = MediaService(DatabaseManager(self.settings_manager))
        return self._media

    def _register_current_screenshot(self, screenshot_path):
        raw = dict(self._current_raw or {})
        mail_key = str(raw.get("_pipeline_card_key", "") or f"mail-{self.session_id}-{self._index + 1}")
        subject = str(raw.get("subject", "") or "mail-screenshot").strip()
        sender = str(raw.get("sender", "") or "").strip()
        date_value = str(raw.get("date", "") or "").strip()
        registration = self._media_service().register_screenshot(
            source_path=screenshot_path,
            preferred_name=subject or "mail-screenshot",
            source_module="modul_mail_scraper",
            source_kind="mail_screenshot",
            source_ref=mail_key,
            context_key=mail_key,
            metadata={
                "mail_key": mail_key,
                "session_id": str(self.session_id),
                "subject": subject,
                "sender": sender,
                "date": date_value,
                "mail_index": int(self._index + 1),
            },
        )
        asset = dict(registration.get("asset") or {})
        raw["_registered_screenshot_asset_id"] = asset.get("id")
        raw["_registered_screenshot_media_key"] = str(asset.get("media_key", "") or "")
        raw["_registered_screenshot_path"] = str(registration.get("path", "") or "")
        raw["_registered_screenshot_temp_file"] = False
        return registration, raw

    def _render_next(self):
        if self._cancelled:
            return

        total = len(self.raw_emails)
        if self._index >= total:
            results = list(self._results)
            self._cleanup_view()
            self.finished_signal.emit(self.session_id, results)
            return

        self._current_raw = dict(self.raw_emails[self._index] or {})
        current_human = self._index + 1
        subject = str(self._current_raw.get("subject", ""))[:40]
        self.progress_signal.emit(
            self.session_id,
            {
                "current": current_human - 1,
                "total": total,
                "status_text": f"E-Mails werden fuer die KI vorbereitet... ({current_human}/{total})",
                "log_message": f" Screenshot {current_human}/{total}: {subject}...",
                "raw_email": dict(self._current_raw or {}),
                "mail_key": str((self._current_raw or {}).get("_pipeline_card_key", "") or ""),
            },
        )

        html = self._current_raw.get("body_html", "") or self._current_raw.get("body_text", "")
        render_result = SafeMailRenderer.prepare_html(
            html,
            text_fallback=self._current_raw.get("body_text", ""),
            sender_text=self._current_raw.get("sender", ""),
            settings_manager=self.settings_manager,
            inline_cid_map=self._current_raw.get("cid_map", {}),
            allow_external=False,
        )

        try:
            self._view.loadFinished.disconnect(self._on_load_finished)
        except Exception:
            pass
        self._view.loadFinished.connect(self._on_load_finished)
        SafeMailRenderer.apply_to_view(self._view, render_result)

    def _on_load_finished(self, _ok):
        try:
            self._view.loadFinished.disconnect(self._on_load_finished)
        except Exception:
            pass

        if self._cancelled:
            return

        QTimer.singleShot(350, self._capture_current)

    def _capture_current(self):
        if self._cancelled or self._view is None:
            return

        try:
            tmp_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False, prefix="mailshot_")
            tmp_path = tmp_file.name
            tmp_file.close()

            pixmap = self._view.grab()
            if not pixmap.isNull():
                pixmap.save(tmp_path, "PNG")
                fsize = os.path.getsize(tmp_path)
                final_path = tmp_path
                result_raw = dict(self._current_raw or {})
                result_raw["_registered_screenshot_temp_file"] = True
                try:
                    registration, registered_raw = self._register_current_screenshot(tmp_path)
                    registered_path = str(registration.get("path", "") or "")
                    if registered_path:
                        final_path = registered_path
                        result_raw = registered_raw
                        try:
                            if os.path.exists(tmp_path) and os.path.abspath(tmp_path) != os.path.abspath(final_path):
                                os.remove(tmp_path)
                        except Exception:
                            pass
                except Exception as reg_exc:
                    log_exception(__name__, reg_exc, extra={"tmp_path": tmp_path, "session_id": self.session_id})
                    result_raw["_registered_screenshot_temp_file"] = True

                self._results.append((final_path, result_raw))
                self.progress_signal.emit(
                    self.session_id,
                    {
                        "current": self._index + 1,
                        "total": len(self.raw_emails),
                        "log_message": f" Screenshot gespeichert: {pixmap.width()}x{pixmap.height()}px, {fsize // 1024}KB -> {os.path.basename(final_path)}",
                        "raw_email": dict(result_raw or {}),
                        "mail_key": str((result_raw or {}).get("_pipeline_card_key", "") or ""),
                        "screenshot_path": final_path,
                        "screenshot_asset_id": (result_raw or {}).get("_registered_screenshot_asset_id"),
                    },
                )
            else:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                self._results.append((None, self._current_raw))
                self.progress_signal.emit(
                    self.session_id,
                    {
                        "current": self._index + 1,
                        "total": len(self.raw_emails),
                        "log_message": " Screenshot fehlgeschlagen, Text-Fallback wird verwendet.",
                        "raw_email": dict(self._current_raw or {}),
                        "mail_key": str((self._current_raw or {}).get("_pipeline_card_key", "") or ""),
                        "screenshot_path": "",
                    },
                )

            self._index += 1
            QTimer.singleShot(0, self._render_next)
        except Exception as exc:
            self._handle_error(exc)

    def _handle_error(self, exc):
        log_exception(__name__, exc, extra={"session_id": self.session_id, "index": self._index})
        self._cleanup_tempfiles()
        self._cleanup_view()
        if not self._cancelled:
            self.error_signal.emit(self.session_id, str(exc))

    def _cleanup_tempfiles(self):
        for path, raw in self._results:
            try:
                is_temp = bool((raw or {}).get("_registered_screenshot_temp_file", False))
                if is_temp and path and os.path.exists(path):
                    os.remove(path)
            except Exception as exc:
                log_exception(__name__, exc)
        self._results = []

    def _cleanup_view(self):
        if self._view is None:
            return
        try:
            self._view.hide()
            self._view.deleteLater()
        except Exception as exc:
            log_exception(__name__, exc)
        self._view = None
