from __future__ import annotations

import logging

from PyQt6.QtCore import QRectF, Qt
from PyQt6.QtGui import QColor, QFont, QPainter, QPainterPath, QPixmap


def create_placeholder_pixmap(label, size, background="#24283b", foreground="#7aa2f7", radius=8):
    text = str(label or "").strip().upper()[:2] or "?"
    pixmap = QPixmap(int(size), int(size))
    pixmap.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    painter.setPen(Qt.PenStyle.NoPen)
    painter.setBrush(QColor(background))
    painter.drawRoundedRect(QRectF(0, 0, int(size), int(size)), float(radius), float(radius))
    painter.setPen(QColor(foreground))
    font = QFont()
    font.setBold(True)
    font.setPointSize(max(7, int(size * 0.24)))
    painter.setFont(font)
    painter.drawText(QRectF(0, 0, int(size), int(size)), int(Qt.AlignmentFlag.AlignCenter), text)
    painter.end()
    return pixmap


def render_preview_pixmap(source_pixmap, size, background="#ffffff", radius=8, inset=2):
    source_pixmap = source_pixmap if isinstance(source_pixmap, QPixmap) else QPixmap()
    if source_pixmap.isNull():
        return QPixmap()

    if source_pixmap.hasAlphaChannel():
        logging.debug("Transparentes Bild wird fuer Vorschau auf weissen Hintergrund gelegt")

    canvas = QPixmap(int(size), int(size))
    canvas.fill(Qt.GlobalColor.transparent)
    painter = QPainter(canvas)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)

    clip_path = QPainterPath()
    clip_path.addRoundedRect(QRectF(0, 0, int(size), int(size)), float(radius), float(radius))
    painter.setClipPath(clip_path)
    painter.fillRect(QRectF(0, 0, int(size), int(size)), QColor(background))

    target = source_pixmap.scaled(
        max(1, int(size) - int(inset) * 2),
        max(1, int(size) - int(inset) * 2),
        Qt.AspectRatioMode.KeepAspectRatio,
        Qt.TransformationMode.SmoothTransformation,
    )
    x_pos = int((int(size) - target.width()) / 2)
    y_pos = int((int(size) - target.height()) / 2)
    painter.drawPixmap(x_pos, y_pos, target)
    painter.end()
    return canvas
