"""
mail_preview_helper.py
Separater Prozess fuer die E-Mail-Vorschau (QWebEngine).
Wenn Chromium hier crasht, bleibt die Haupt-App stabil.
"""

import argparse
import json
import os
import sys

from PyQt6.QtWidgets import QApplication, QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QFrame
from PyQt6.QtWebEngineWidgets import QWebEngineView

from config import SettingsManager
from module.crash_logger import install_global_exception_hooks, log_exception
from module.safe_mail_renderer import SafeMailRenderer


def _load_payload(path):
    try:
        with open(path, "r", encoding="utf-8-sig") as file_handle:
            return json.load(file_handle)
    except Exception as exc:
        log_exception(__name__, exc, extra={"payload_path": path})
        raise


class MailPreviewDialog(QDialog):
    def __init__(self, payload, payload_path, parent=None):
        super().__init__(parent)
        self.payload_path = payload_path
        self.settings_manager = SettingsManager()
        self.payload = dict(payload or {})
        self.allow_external_once = bool(self.payload.get("allow_external_once", False))

        shop_name = str(self.payload.get("shop_name", "Unbekannt"))
        email_sender = str(self.payload.get("email_sender", "Unbekannt"))
        email_date = str(self.payload.get("email_date", ""))

        self.setWindowTitle(f"E-Mail Vorschau: {shop_name}")
        self.resize(1080, 820)
        self.setStyleSheet("background-color: #1a1b26; color: #DADADA;")

        layout = QVBoxLayout(self)

        header_frame = QFrame()
        header_frame.setStyleSheet("background-color: #24283b; border-radius: 4px; padding: 4px 8px;")
        header_layout = QHBoxLayout(header_frame)
        header_layout.setContentsMargins(8, 2, 8, 2)
        header_layout.addWidget(QLabel(f"Von: {email_sender}"))
        header_layout.addStretch()
        header_layout.addWidget(QLabel(email_date))
        layout.addWidget(header_frame)

        self.lbl_notice = QLabel("")
        self.lbl_notice.setWordWrap(True)
        self.lbl_notice.setStyleSheet("font-size: 12px; color: #a9b1d6; padding: 4px 2px;")
        layout.addWidget(self.lbl_notice)

        action_row = QHBoxLayout()
        self.btn_load_external = QPushButton("Bilder fuer diese Mail laden")
        self.btn_load_external.clicked.connect(self._load_external_once)
        self.btn_trust_sender = QPushButton("Absender vertrauen")
        self.btn_trust_sender.clicked.connect(self._trust_sender)
        self.btn_trust_domain = QPushButton("Domain vertrauen")
        self.btn_trust_domain.clicked.connect(self._trust_domain)
        action_row.addWidget(self.btn_load_external)
        action_row.addWidget(self.btn_trust_sender)
        action_row.addWidget(self.btn_trust_domain)
        action_row.addStretch()
        layout.addLayout(action_row)

        self.browser = QWebEngineView(self)
        layout.addWidget(self.browser, 1)

        btn_close = QPushButton("Schliessen")
        btn_close.clicked.connect(self.accept)
        layout.addWidget(btn_close)

        self._render_mail()

    def _render_mail(self):
        result = SafeMailRenderer.prepare_html(
            self.payload.get("email_html", ""),
            text_fallback=self.payload.get("email_text", ""),
            sender_text=self.payload.get("email_sender", ""),
            settings_manager=self.settings_manager,
            inline_cid_map=self.payload.get("cid_map", {}),
            allow_external=self.allow_external_once,
        )
        self._render_result = result
        SafeMailRenderer.apply_to_view(self.browser, result)
        self.lbl_notice.setText(SafeMailRenderer.build_notice_text(result))
        self.btn_load_external.setVisible(result.blocked_remote_images > 0 or result.blocked_remote_links > 0)
        self.btn_trust_sender.setVisible(result.can_trust_sender)
        self.btn_trust_domain.setVisible(result.can_trust_domain)

    def _load_external_once(self):
        self.allow_external_once = True
        self._render_mail()

    def _trust_sender(self):
        if SafeMailRenderer.trust_sender(self.settings_manager, self.payload.get("email_sender", "")):
            self.allow_external_once = True
        self._render_mail()

    def _trust_domain(self):
        if SafeMailRenderer.trust_domain(self.settings_manager, self.payload.get("email_sender", "")):
            self.allow_external_once = True
        self._render_mail()

    def closeEvent(self, event):
        try:
            self.browser.setHtml("<html><body></body></html>")
        except Exception as exc:
            log_exception(__name__, exc)

        try:
            if self.payload_path and os.path.exists(self.payload_path):
                os.remove(self.payload_path)
        except Exception as exc:
            log_exception(__name__, exc, extra={"payload_path": self.payload_path})

        super().closeEvent(event)


def main():
    install_global_exception_hooks()
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--payload", required=True, help="Pfad zur JSON-Payload")
        args = parser.parse_args()

        payload = _load_payload(args.payload)

        app = QApplication(sys.argv)
        dlg = MailPreviewDialog(payload, args.payload)
        dlg.exec()
    except Exception as exc:
        log_exception(__name__, exc)
        raise


if __name__ == "__main__":
    main()

