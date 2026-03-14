from __future__ import annotations

import os
import re

from PyQt6.QtCore import QEasingCurve, QLineF, QPropertyAnimation, QRectF, Qt, QVariantAnimation
from PyQt6.QtGui import QColor, QFont, QIcon, QLinearGradient, QPainter, QPainterPath, QPen, QPixmap
from PyQt6.QtWidgets import (
    QFrame,
    QGraphicsOpacityEffect,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)


PIPELINE_STAGES = ("scan", "screenshots", "cloudscan")


def _safe_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def mail_card_key(payload) -> str:
    payload = payload if isinstance(payload, dict) else {}
    key = _safe_text(payload.get("_pipeline_card_key"))
    if key:
        return key
    sender = _safe_text(payload.get("sender") or payload.get("_email_sender"))
    subject = _safe_text(payload.get("subject") or payload.get("betreff"))
    email_date = _safe_text(payload.get("date") or payload.get("_email_date"))
    raw_key = "|".join(part for part in (sender, subject, email_date) if part)
    if raw_key:
        return raw_key
    return f"mail-{id(payload)}"


def sender_domain(payload) -> str:
    payload = payload if isinstance(payload, dict) else {}
    sender = _safe_text(payload.get("sender") or payload.get("_email_sender"))
    match = re.search(r"@([A-Za-z0-9.-]+\.[A-Za-z]{2,})", sender)
    if match:
        return match.group(1).lower()
    return sender or "unbekannte-domain"


def subject_preview(payload, limit=54) -> str:
    payload = payload if isinstance(payload, dict) else {}
    subject = _safe_text(payload.get("subject") or payload.get("betreff"))
    if not subject:
        return "Betreff noch ohne Vorschau"
    if len(subject) <= limit:
        return subject
    return subject[: limit - 1].rstrip() + "..."


def _load_icon_pixmap(path: str, size: int) -> QPixmap:
    if path and os.path.exists(path):
        icon = QIcon(path)
        pixmap = icon.pixmap(size, size)
        if not pixmap.isNull():
            return pixmap
        pixmap = QPixmap(path)
        if not pixmap.isNull():
            return pixmap.scaled(
                size,
                size,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
    return QPixmap()


class PipelineConnectorWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._progress = 0.0
        self._active = False
        self.setMinimumHeight(30)
        self.setMinimumWidth(96)

    def set_progress(self, progress: float, active: bool):
        self._progress = max(0.0, min(1.0, float(progress or 0.0)))
        self._active = bool(active)
        self.update()

    def paintEvent(self, event):
        del event
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)

        line_rect = QRectF(8, (self.height() / 2.0) - 4.0, max(8.0, self.width() - 28.0), 8.0)
        center_y = line_rect.center().y()

        base_pen = QPen(QColor("#2D3554"))
        base_pen.setWidthF(8.0)
        base_pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        painter.setPen(base_pen)
        painter.drawLine(QLineF(line_rect.left(), center_y, line_rect.right(), center_y))

        if self._progress > 0.0:
            grad = QLinearGradient(line_rect.left(), line_rect.top(), line_rect.right(), line_rect.bottom())
            grad.setColorAt(0.0, QColor("#4FD1FF"))
            grad.setColorAt(1.0, QColor("#F7A34B"))
            active_pen = QPen(grad, 8.0, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap)
            painter.setPen(active_pen)
            fill_x = line_rect.left() + (line_rect.width() * self._progress)
            painter.drawLine(QLineF(line_rect.left(), center_y, fill_x, center_y))

            arrow_path = QPainterPath()
            arrow_x = min(self.width() - 8.0, fill_x + 10.0)
            arrow_y = center_y
            arrow_path.moveTo(arrow_x - 8.0, arrow_y - 8.0)
            arrow_path.lineTo(arrow_x + 6.0, arrow_y)
            arrow_path.lineTo(arrow_x - 8.0, arrow_y + 8.0)
            arrow_path.closeSubpath()
            painter.fillPath(arrow_path, QColor("#F7A34B" if self._active else "#4FD1FF"))


class PipelineStepWidget(QWidget):
    def __init__(self, title: str, icon_path: str, parent=None):
        super().__init__(parent)
        self.title = title
        self.icon_path = icon_path
        self.state = "upcoming"
        self._pulse = 0.0
        self._pulse_anim = QVariantAnimation(self)
        self._pulse_anim.setDuration(1700)
        self._pulse_anim.setStartValue(0.22)
        self._pulse_anim.setEndValue(1.0)
        self._pulse_anim.setLoopCount(-1)
        self._pulse_anim.setEasingCurve(QEasingCurve.Type.InOutSine)
        self._pulse_anim.valueChanged.connect(self._on_pulse_value)
        self.setMinimumSize(168, 144)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

    def _on_pulse_value(self, value):
        self._pulse = float(value or 0.0)
        self.update()

    def set_state(self, state: str):
        state = str(state or "upcoming")
        if self.state == state:
            return
        self.state = state
        if self.state == "active":
            if self._pulse_anim.state() != QVariantAnimation.State.Running:
                self._pulse_anim.start()
        else:
            self._pulse_anim.stop()
            self._pulse = 0.0
        self.update()

    def paintEvent(self, event):
        del event
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
        rect = QRectF(6, 6, self.width() - 12, self.height() - 12)

        if self.state == "done":
            bg_grad = QLinearGradient(rect.left(), rect.top(), rect.right(), rect.bottom())
            bg_grad.setColorAt(0.0, QColor("#1E3248"))
            bg_grad.setColorAt(1.0, QColor("#24304E"))
            border = QColor("#4FD1FF")
            glow = QColor(79, 209, 255, 42)
        elif self.state == "active":
            bg_grad = QLinearGradient(rect.left(), rect.top(), rect.right(), rect.bottom())
            bg_grad.setColorAt(0.0, QColor("#202A45"))
            bg_grad.setColorAt(1.0, QColor("#2A2348"))
            border = QColor("#F7A34B")
            glow = QColor(247, 163, 75, int(36 + (46 * self._pulse)))
        else:
            bg_grad = QLinearGradient(rect.left(), rect.top(), rect.right(), rect.bottom())
            bg_grad.setColorAt(0.0, QColor("#181D30"))
            bg_grad.setColorAt(1.0, QColor("#1E2440"))
            border = QColor("#36405F")
            glow = QColor(0, 0, 0, 0)

        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(glow)
        painter.drawRoundedRect(rect.adjusted(-2, -2, 2, 2), 22, 22)

        painter.setBrush(bg_grad)
        painter.setPen(QPen(border, 2.0))
        painter.drawRoundedRect(rect, 20, 20)

        icon_pixmap = _load_icon_pixmap(self.icon_path, 64)
        if not icon_pixmap.isNull():
            icon_x = int((self.width() - icon_pixmap.width()) / 2)
            painter.drawPixmap(icon_x, 28, icon_pixmap)

        title_rect = QRectF(rect.left() + 12, rect.bottom() - 48, rect.width() - 24, 28)
        painter.setPen(QColor("#E5E9F5" if self.state != "upcoming" else "#9AA4C5"))
        font = QFont()
        font.setPointSize(11)
        font.setBold(True)
        painter.setFont(font)
        painter.drawText(title_rect, int(Qt.AlignmentFlag.AlignCenter), self.title)

        if self.state == "done":
            badge_rect = QRectF(rect.right() - 28, rect.top() + 10, 18, 18)
            painter.setBrush(QColor("#4FD1FF"))
            painter.setPen(Qt.PenStyle.NoPen)
            painter.drawEllipse(badge_rect)
            painter.setPen(QPen(QColor("#0F172A"), 2.0))
            painter.drawLine(QLineF(
                badge_rect.left() + 5,
                badge_rect.center().y(),
                badge_rect.left() + 8,
                badge_rect.bottom() - 5,
            ))
            painter.drawLine(QLineF(
                badge_rect.left() + 8,
                badge_rect.bottom() - 5,
                badge_rect.right() - 4,
                badge_rect.top() + 5,
            ))


class MailPipelineHeaderWidget(QFrame):
    def __init__(self, icon_paths: dict[str, str], parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.NoFrame)
        self._current_stage = None
        self._progress = 0.0
        self._finished = False
        self._steps = []
        self._connectors = []

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        titles = {
            "scan": "Scan",
            "screenshots": "Screenshots",
            "cloudscan": "Cloudscan",
        }
        for index, stage_key in enumerate(PIPELINE_STAGES):
            step_widget = PipelineStepWidget(titles[stage_key], icon_paths.get(stage_key, ""), self)
            self._steps.append(step_widget)
            layout.addWidget(step_widget, 1)
            if index < len(PIPELINE_STAGES) - 1:
                connector = PipelineConnectorWidget(self)
                self._connectors.append(connector)
                layout.addWidget(connector)
        self.reset()

    def reset(self):
        self._current_stage = None
        self._progress = 0.0
        self._finished = False
        self._apply_visual_state()

    def set_stage(self, stage_key: str, progress: float = 0.0):
        self._current_stage = str(stage_key or "")
        self._progress = max(0.0, min(1.0, float(progress or 0.0)))
        self._finished = False
        self._apply_visual_state()

    def finish_all(self):
        self._current_stage = PIPELINE_STAGES[-1]
        self._progress = 1.0
        self._finished = True
        self._apply_visual_state()

    def _apply_visual_state(self):
        if self._current_stage in PIPELINE_STAGES:
            current_index = PIPELINE_STAGES.index(self._current_stage)
        else:
            current_index = -1

        for index, widget in enumerate(self._steps):
            if current_index < 0:
                state = "upcoming"
            elif index < current_index:
                state = "done"
            elif index == current_index and self._finished:
                state = "done"
            elif index == current_index:
                state = "active"
            else:
                state = "upcoming"
            widget.set_state(state)

        for index, connector in enumerate(self._connectors):
            if current_index < 0:
                progress = 0.0
                active = False
            elif index < current_index:
                progress = 1.0
                active = False
            elif index == current_index:
                progress = 1.0 if self._finished else self._progress
                active = not self._finished
            else:
                progress = 0.0
                active = False
            connector.set_progress(progress, active)


class MailPreviewCardWidget(QFrame):
    STATUS_STYLES = {
        "scanned": {"border": "#3C4D73", "badge_bg": "#25334F", "badge_fg": "#9AA4C5", "label": "Erkannt"},
        "rendering": {"border": "#8B5CF6", "badge_bg": "#2D2250", "badge_fg": "#D3BCFF", "label": "Rendering"},
        "rendered": {"border": "#4FD1FF", "badge_bg": "#1D3340", "badge_fg": "#9BE7FF", "label": "Screenshot"},
        "cloudscan": {"border": "#F7A34B", "badge_bg": "#3B2B19", "badge_fg": "#FFD19C", "label": "Cloudscan"},
        "done": {"border": "#34D399", "badge_bg": "#193629", "badge_fg": "#A7F3D0", "label": "Fertig"},
        "error": {"border": "#F87171", "badge_bg": "#42202A", "badge_fg": "#FECACA", "label": "Fehler"},
    }

    def __init__(self, icon_paths: dict[str, str], parent=None):
        super().__init__(parent)
        self._icon_paths = dict(icon_paths or {})
        self._card_key = ""
        self._preview_effect = None
        self._fade_anim = None
        self._card_effect = None
        self._card_fade_anim = None
        self._build_ui()
        self.set_state("scanned")

    def _build_ui(self):
        self.setObjectName("mailPreviewCard")
        self.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.setFixedSize(236, 274)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(12)

        self.preview_label = QLabel(self)
        self.preview_label.setFixedHeight(158)
        self.preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.preview_label.setStyleSheet("background-color: #131A2C; border-radius: 16px; border: 1px solid #2D3554;")
        layout.addWidget(self.preview_label)

        self.lbl_subject = QLabel("Betreff noch ohne Vorschau")
        self.lbl_subject.setWordWrap(True)
        self.lbl_subject.setStyleSheet("font-size: 13px; font-weight: bold; color: #E5E9F5;")
        layout.addWidget(self.lbl_subject)

        meta_row = QHBoxLayout()
        meta_row.setContentsMargins(0, 0, 0, 0)
        meta_row.setSpacing(8)

        self.lbl_badge = QLabel("")
        self.lbl_badge.setStyleSheet("font-size: 11px; font-weight: bold; padding: 4px 8px; border-radius: 10px;")
        meta_row.addWidget(self.lbl_badge, 0)

        self.lbl_domain = QLabel("unbekannte-domain")
        self.lbl_domain.setStyleSheet("font-size: 11px; color: #9AA4C5;")
        self.lbl_domain.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        meta_row.addWidget(self.lbl_domain, 1)
        layout.addLayout(meta_row)

    def animate_appearance(self):
        if self._card_effect is None:
            self._card_effect = QGraphicsOpacityEffect(self)
            self.setGraphicsEffect(self._card_effect)
        if self._card_fade_anim is not None:
            self._card_fade_anim.stop()
        self._card_effect.setOpacity(0.0)
        self._card_fade_anim = QPropertyAnimation(self._card_effect, b"opacity", self)
        self._card_fade_anim.setDuration(280)
        self._card_fade_anim.setStartValue(0.0)
        self._card_fade_anim.setEndValue(1.0)
        self._card_fade_anim.setEasingCurve(QEasingCurve.Type.OutCubic)
        self._card_fade_anim.start()

    def set_mail(self, payload):
        payload = payload if isinstance(payload, dict) else {}
        self._card_key = mail_card_key(payload)
        full_subject = _safe_text(payload.get("subject") or payload.get("betreff"))
        preview_text = subject_preview(payload)
        self.lbl_subject.setText(preview_text)
        self.lbl_subject.setToolTip(full_subject or preview_text)
        self.lbl_domain.setText(sender_domain(payload))
        if self.preview_label.pixmap() is None:
            self.set_placeholder_preview(payload)

    def set_placeholder_preview(self, payload):
        payload = payload if isinstance(payload, dict) else {}
        pixmap = QPixmap(420, 280)
        pixmap.fill(QColor("#111827"))
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)

        outer = QRectF(6, 6, pixmap.width() - 12, pixmap.height() - 12)
        grad = QLinearGradient(outer.left(), outer.top(), outer.right(), outer.bottom())
        grad.setColorAt(0.0, QColor("#182033"))
        grad.setColorAt(1.0, QColor("#101726"))
        painter.setBrush(grad)
        painter.setPen(QPen(QColor("#25304D"), 2.0))
        painter.drawRoundedRect(outer, 20, 20)

        icon = _load_icon_pixmap(self._icon_paths.get("scan", ""), 64)
        if not icon.isNull():
            painter.drawPixmap(24, 22, icon)

        painter.setPen(QColor("#E5E9F5"))
        title_font = QFont()
        title_font.setPointSize(15)
        title_font.setBold(True)
        painter.setFont(title_font)
        painter.drawText(QRectF(108, 28, 280, 30), "Neue Mail erkannt")

        painter.setPen(QColor("#8CA0C8"))
        info_font = QFont()
        info_font.setPointSize(10)
        painter.setFont(info_font)
        painter.drawText(QRectF(108, 58, 280, 22), sender_domain(payload))

        line_pen = QPen(QColor("#334266"))
        line_pen.setWidth(10)
        line_pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        painter.setPen(line_pen)
        for offset in (118, 146, 174):
            painter.drawLine(28, offset, pixmap.width() - 32, offset)

        painter.end()
        self._set_preview_pixmap(pixmap, animate=False)

    def set_screenshot_preview(self, path: str):
        pixmap = QPixmap(path)
        if pixmap.isNull():
            return
        scaled = pixmap.scaled(
            420,
            280,
            Qt.AspectRatioMode.KeepAspectRatioByExpanding,
            Qt.TransformationMode.SmoothTransformation,
        )
        self._set_preview_pixmap(scaled, animate=True)

    def _set_preview_pixmap(self, pixmap: QPixmap, animate: bool):
        if pixmap.isNull():
            return
        scaled = pixmap.scaled(
            self.preview_label.width() - 4,
            self.preview_label.height() - 4,
            Qt.AspectRatioMode.KeepAspectRatioByExpanding,
            Qt.TransformationMode.SmoothTransformation,
        )
        self.preview_label.setPixmap(scaled)
        if animate:
            if self._preview_effect is None:
                self._preview_effect = QGraphicsOpacityEffect(self.preview_label)
                self.preview_label.setGraphicsEffect(self._preview_effect)
            if self._fade_anim is not None:
                self._fade_anim.stop()
            self._preview_effect.setOpacity(0.0)
            self._fade_anim = QPropertyAnimation(self._preview_effect, b"opacity", self)
            self._fade_anim.setDuration(260)
            self._fade_anim.setStartValue(0.0)
            self._fade_anim.setEndValue(1.0)
            self._fade_anim.setEasingCurve(QEasingCurve.Type.InOutQuad)
            self._fade_anim.start()

    def set_state(self, state_key: str):
        style = self.STATUS_STYLES.get(state_key, self.STATUS_STYLES["scanned"])
        self.setStyleSheet(
            "QFrame#mailPreviewCard {"
            "background-color: #171F33;"
            f"border: 2px solid {style['border']};"
            "border-radius: 22px;"
            "}"
        )
        self.lbl_badge.setText(style["label"])
        self.lbl_badge.setStyleSheet(
            f"font-size: 11px; font-weight: bold; padding: 4px 8px; border-radius: 10px; background-color: {style['badge_bg']}; color: {style['badge_fg']};"
        )


class MailPipelineDashboardWidget(QFrame):
    def __init__(self, icon_paths: dict[str, str], parent=None):
        super().__init__(parent)
        self._icon_paths = dict(icon_paths or {})
        self._cards = {}
        self._card_order = []
        self._build_ui()

    def _build_ui(self):
        self.setObjectName("mailPipelineDashboard")
        self.setStyleSheet(
            "QFrame#mailPipelineDashboard {"
            "background-color: #141B2E;"
            "border: 1px solid #27304A;"
            "border-radius: 24px;"
            "}"
        )
        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(16)

        title = QLabel("Visuelle Scan-Pipeline")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #E5E9F5;")
        layout.addWidget(title)

        self.lbl_status = QLabel("Bereit fuer einen neuen Scan.")
        self.lbl_status.setWordWrap(True)
        self.lbl_status.setStyleSheet("font-size: 12px; color: #8CA0C8;")
        layout.addWidget(self.lbl_status)

        self.header = MailPipelineHeaderWidget(self._icon_paths, self)
        layout.addWidget(self.header)

        self.cards_scroll = QScrollArea(self)
        self.cards_scroll.setWidgetResizable(True)
        self.cards_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.cards_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.cards_scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")

        self.cards_host = QWidget(self.cards_scroll)
        self.cards_layout = QGridLayout(self.cards_host)
        self.cards_layout.setContentsMargins(0, 8, 0, 0)
        self.cards_layout.setHorizontalSpacing(16)
        self.cards_layout.setVerticalSpacing(16)
        self.cards_layout.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        self.cards_scroll.setWidget(self.cards_host)
        layout.addWidget(self.cards_scroll, 1)

        self.lbl_empty = QLabel("Sobald Mails erkannt werden, entstehen hier die Vorschaukarten direkt im Ablauf.")
        self.lbl_empty.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_empty.setWordWrap(True)
        self.lbl_empty.setStyleSheet("font-size: 13px; color: #7B88A8; padding: 24px;")
        self.cards_layout.addWidget(self.lbl_empty, 0, 0)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._relayout_cards()

    def reset(self, message="Bereit fuer einen neuen Scan."):
        self.header.reset()
        self.set_status_text(message)
        for key in list(self._card_order):
            widget = self._cards.pop(key, None)
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()
        self._card_order = []
        self.lbl_empty.setVisible(True)
        self._relayout_cards()

    def set_status_text(self, text: str):
        self.lbl_status.setText(_safe_text(text) or "Bereit fuer einen neuen Scan.")

    def set_stage(self, stage_key: str, progress: float = 0.0):
        self.header.set_stage(stage_key, progress)

    def finish_all(self):
        self.header.finish_all()

    def upsert_mail(self, payload, state_key="scanned"):
        key = mail_card_key(payload)
        if key not in self._cards:
            card = MailPreviewCardWidget(self._icon_paths, self.cards_host)
            card.set_mail(payload)
            card.animate_appearance()
            self._cards[key] = card
            self._card_order.append(key)
        card = self._cards[key]
        card.set_mail(payload)
        card.set_state(state_key)
        self.lbl_empty.setVisible(False)
        self._relayout_cards()
        return card

    def set_screenshot(self, payload, screenshot_path: str):
        card = self.upsert_mail(payload, state_key="rendered")
        if screenshot_path:
            card.set_screenshot_preview(screenshot_path)

    def set_mail_state(self, payload, state_key: str):
        self.upsert_mail(payload, state_key=state_key)

    def _relayout_cards(self):
        while self.cards_layout.count():
            item = self.cards_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(None)

        if not self._card_order:
            self.cards_layout.addWidget(self.lbl_empty, 0, 0)
            self.lbl_empty.setVisible(True)
            return

        self.lbl_empty.setVisible(False)
        width = max(1, self.cards_scroll.viewport().width())
        columns = max(1, width // 260)

        for index, key in enumerate(self._card_order):
            widget = self._cards.get(key)
            if widget is None:
                continue
            row = index // columns
            col = index % columns
            self.cards_layout.addWidget(widget, row, col)








