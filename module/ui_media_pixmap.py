from __future__ import annotations

import logging

from PyQt6.QtCore import QRectF, Qt
from PyQt6.QtGui import QColor, QFont, QPainter, QPainterPath, QPixmap, QPen


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


def render_card_visual_pixmap(
    logo_pixmap: QPixmap | None,
    item_pixmap: QPixmap | None,
    total_menge: int = 1,
    logo_size: int = 72,
    item_size: int = 36,
) -> QPixmap:
    """Logo groß + Produkt-Thumbnail überlappt unten-rechts + Mengen-Badge.

    Gesamtgröße: (logo_size + item_size // 2) × (logo_size + item_size // 2)
    Das Thumbnail überragt das Logo nach rechts-unten um item_size // 2.
    """
    total = logo_size + item_size // 2
    canvas = QPixmap(total, total)
    canvas.fill(Qt.GlobalColor.transparent)
    painter = QPainter(canvas)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)

    # --- Logo (oben-links, logo_size × logo_size) ---
    logo_r = float(logo_size)
    logo_radius = 10.0
    clip_logo = QPainterPath()
    clip_logo.addRoundedRect(QRectF(0, 0, logo_r, logo_r), logo_radius, logo_radius)
    painter.save()
    painter.setClipPath(clip_logo)
    painter.fillRect(QRectF(0, 0, logo_r, logo_r), QColor("#ffffff"))
    if logo_pixmap and not logo_pixmap.isNull():
        scaled = logo_pixmap.scaled(
            int(logo_r) - 4, int(logo_r) - 4,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        ox = int((logo_r - scaled.width()) / 2)
        oy = int((logo_r - scaled.height()) / 2)
        painter.drawPixmap(ox, oy, scaled)
    painter.restore()

    # Dünner Border um Logo
    painter.save()
    painter.setPen(QPen(QColor("#414868"), 1.5))
    painter.setBrush(Qt.BrushStyle.NoBrush)
    painter.drawRoundedRect(QRectF(0.75, 0.75, logo_r - 1.5, logo_r - 1.5), logo_radius, logo_radius)
    painter.restore()

    # --- Thumbnail (unten-rechts, überlappt Logo um item_size//2) ---
    thumb_x = logo_size - item_size // 2
    thumb_y = logo_size - item_size // 2
    thumb_r = float(item_size)
    thumb_radius = 6.0

    clip_thumb = QPainterPath()
    clip_thumb.addRoundedRect(QRectF(thumb_x, thumb_y, thumb_r, thumb_r), thumb_radius, thumb_radius)
    painter.save()
    painter.setClipPath(clip_thumb)
    painter.fillRect(QRectF(thumb_x, thumb_y, thumb_r, thumb_r), QColor("#202233"))
    if item_pixmap and not item_pixmap.isNull():
        scaled_t = item_pixmap.scaled(
            item_size - 2, item_size - 2,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        tx = thumb_x + int((item_size - scaled_t.width()) / 2)
        ty = thumb_y + int((item_size - scaled_t.height()) / 2)
        painter.drawPixmap(tx, ty, scaled_t)
    painter.restore()

    # Border um Thumbnail
    painter.save()
    painter.setPen(QPen(QColor("#1a1b26"), 2.0))
    painter.setBrush(Qt.BrushStyle.NoBrush)
    painter.drawRoundedRect(QRectF(thumb_x + 1, thumb_y + 1, thumb_r - 2, thumb_r - 2), thumb_radius, thumb_radius)
    painter.restore()

    # --- Mengen-Badge oben-rechts auf Thumbnail (ab Menge > 1) ---
    if total_menge > 1:
        badge_text = f"×{total_menge}"
        badge_h = 14
        font = QFont()
        font.setBold(True)
        font.setPointSize(6)
        painter.setFont(font)
        fm = painter.fontMetrics()
        badge_w = max(18, fm.horizontalAdvance(badge_text) + 6)
        bx = float(thumb_x + item_size - badge_w + 1)
        by = float(thumb_y - badge_h // 2)
        badge_path = QPainterPath()
        badge_path.addRoundedRect(QRectF(bx, by, badge_w, badge_h), badge_h / 2, badge_h / 2)
        painter.save()
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor("#ff9900"))
        painter.drawPath(badge_path)
        painter.setPen(QColor("#1a1b26"))
        painter.drawText(QRectF(bx, by, badge_w, badge_h), Qt.AlignmentFlag.AlignCenter, badge_text)
        painter.restore()

    painter.end()
    return canvas
