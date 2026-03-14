"""
mail_preview_helper.py
Separater Prozess fuer die E-Mail- oder PDF-Vorschau.
Wenn Chromium hier crasht, bleibt die Haupt-App stabil.
"""

import argparse
import json
import os
import sys

from PyQt6.QtCore import QUrl
from PyQt6.QtWebEngineCore import QWebEngineSettings
from PyQt6.QtWidgets import QApplication, QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QFrame
from PyQt6.QtWebEngineWidgets import QWebEngineView

try:
    from PyQt6.QtPdf import QPdfDocument
    from PyQt6.QtPdfWidgets import QPdfView
    QT_PDF_AVAILABLE = True
except Exception:
    QPdfDocument = None
    QPdfView = None
    QT_PDF_AVAILABLE = False

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
        self.preview_kind = str(self.payload.get("preview_kind", "mail") or "mail").strip().lower()
        self.pdf_document = None

        shop_name = str(self.payload.get("shop_name", "Unbekannt"))
        email_sender = str(self.payload.get("email_sender", "Unbekannt"))
        email_date = str(self.payload.get("email_date", ""))
        attachment_name = str(self.payload.get("attachment_name", "PDF-Anhang"))

        self.setWindowTitle(f"PDF Vorschau: {attachment_name}" if self.preview_kind == "pdf" else f"E-Mail Vorschau: {shop_name}")
        self.resize(1080, 820)
        self.setStyleSheet("background-color: #1a1b26; color: #DADADA;")

        layout = QVBoxLayout(self)

        header_frame = QFrame()
        header_frame.setStyleSheet("background-color: #24283b; border-radius: 4px; padding: 4px 8px;")
        header_layout = QHBoxLayout(header_frame)
        header_layout.setContentsMargins(8, 2, 8, 2)
        header_layout.addWidget(QLabel(f"Von: {email_sender}"))
        header_layout.addStretch()
        header_layout.addWidget(QLabel(attachment_name if self.preview_kind == "pdf" else email_date))
        layout.addWidget(header_frame)

        self.lbl_notice = QLabel("")
        self.lbl_notice.setWordWrap(True)
        self.lbl_notice.setStyleSheet("font-size: 12px; color: #a9b1d6; padding: 4px 2px;")
        layout.addWidget(self.lbl_notice)

        if self.preview_kind == "pdf" and QT_PDF_AVAILABLE:
            self.browser = None
            self.pdf_document = QPdfDocument(self)
            try:
                self.pdf_document.statusChanged.connect(self._on_pdf_status_changed)
            except Exception:
                pass
            self.pdf_view = QPdfView(self)
            self.pdf_view.setDocument(self.pdf_document)
            try:
                self.pdf_view.setZoomMode(QPdfView.ZoomMode.FitToWidth)
            except Exception:
                pass
            try:
                self.pdf_view.setPageMode(QPdfView.PageMode.MultiPage)
            except Exception:
                pass
            layout.addWidget(self.pdf_view, 1)
        else:
            self.pdf_view = None
            self.browser = QWebEngineView(self)
            if self.preview_kind == "pdf":
                try:
                    settings = self.browser.settings()
                    settings.setAttribute(QWebEngineSettings.WebAttribute.JavascriptEnabled, False)
                    settings.setAttribute(QWebEngineSettings.WebAttribute.PluginsEnabled, False)
                    settings.setAttribute(QWebEngineSettings.WebAttribute.LocalStorageEnabled, False)
                    settings.setAttribute(QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, False)
                    settings.setAttribute(QWebEngineSettings.WebAttribute.HyperlinkAuditingEnabled, False)
                    settings.setAttribute(QWebEngineSettings.WebAttribute.ErrorPageEnabled, False)
                    settings.setAttribute(QWebEngineSettings.WebAttribute.PdfViewerEnabled, True)
                except Exception as exc:
                    log_exception(__name__, exc)
            layout.addWidget(self.browser, 1)

        if self.preview_kind == "mail":
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
            layout.insertLayout(2, action_row)
            self._render_mail()
        else:
            self.btn_load_external = None
            self.btn_trust_sender = None
            self.btn_trust_domain = None
            self._render_pdf()

        btn_close = QPushButton("Schliessen")
        btn_close.clicked.connect(self.accept)
        layout.addWidget(btn_close)

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

    def _on_pdf_status_changed(self, status):
        if self.pdf_document is None:
            return
        try:
            if status == QPdfDocument.Status.Error:
                self.lbl_notice.setText("PDF wurde erkannt, konnte aber von Qt nicht geladen werden.")
            elif status == QPdfDocument.Status.Ready:
                page_count = self.pdf_document.pageCount()
                if page_count <= 0:
                    self.lbl_notice.setText("PDF wurde erkannt, aber Qt hat keine Seiten gefunden.")
                else:
                    self.lbl_notice.setText(f"Lokale PDF-Vorschau des Anhangs ({page_count} Seite(n)).")
        except Exception as exc:
            log_exception(__name__, exc)

    def _render_pdf(self):
        pdf_path = str(self.payload.get("pdf_path", "") or "").strip()
        if not pdf_path or not os.path.exists(pdf_path):
            self.lbl_notice.setText("Der PDF-Anhang ist nicht mehr verfuegbar.")
            if self.browser is not None:
                self.browser.setHtml("<html><body style='background-color:#10111a;color:#a9b1d6;font-family:Segoe UI,sans-serif;'><div style='padding:18px;'>Keine PDF-Vorschau verfuegbar.</div></body></html>")
            return

        self.lbl_notice.setText("Lade lokalen PDF-Anhang...")
        if self.pdf_document is not None:
            try:
                self.pdf_document.close()
            except Exception:
                pass
            try:
                status = self.pdf_document.load(pdf_path)
                if status == QPdfDocument.Status.Error:
                    self.lbl_notice.setText("PDF konnte nicht geladen werden.")
            except Exception as exc:
                log_exception(__name__, exc, extra={"pdf_path": pdf_path})
                self.lbl_notice.setText("PDF konnte nicht geladen werden.")
        elif self.browser is not None:
            self.lbl_notice.setText("Lokale PDF-Vorschau des Anhangs.")
            self.browser.setUrl(QUrl.fromLocalFile(pdf_path))

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
            if self.browser is not None:
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
