"""Lokale Crop-Hilfslogik fuer bereits gespeicherte Screenshots."""

from __future__ import annotations

import logging
import os

from PyQt6.QtGui import QImage

from module.crash_logger import log_exception


class MediaCropper:
    @staticmethod
    def _to_int(value, field_name):
        try:
            return int(round(float(value)))
        except (TypeError, ValueError):
            raise ValueError(f"Ungueltiger Zahlenwert fuer {field_name}.")

    @staticmethod
    def _to_float(value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @classmethod
    def image_dimensions(cls, screenshot_path):
        image = QImage(str(screenshot_path))
        if image.isNull():
            raise ValueError(f"Screenshot konnte nicht geladen werden: {screenshot_path}")
        return int(image.width()), int(image.height())

    @classmethod
    def normalize_detection(cls, detection):
        if not isinstance(detection, dict):
            raise ValueError("Detektion muss als Dictionary uebergeben werden.")

        normalized = {
            "produkt_name_hint": str(
                detection.get("produkt_name_hint")
                or detection.get("product_name_hint")
                or detection.get("label")
                or ""
            ).strip(),
            "product_key": str(detection.get("product_key", "") or "").strip(),
            "ean": str(detection.get("ean", "") or "").strip(),
            "variant_text": str(detection.get("variant_text") or detection.get("varianten_info") or "").strip(),
            "x": cls._to_int(detection.get("x"), "x"),
            "y": cls._to_int(detection.get("y"), "y"),
            "width": cls._to_int(detection.get("width"), "width"),
            "height": cls._to_int(detection.get("height"), "height"),
            "confidence": cls._to_float(detection.get("confidence"), 0.0),
        }
        coord_units = str(detection.get("coord_units", "") or detection.get("units", "") or "").strip().lower()
        coord_origin = str(detection.get("coord_origin", "") or detection.get("origin", "") or "").strip().lower()
        if coord_units:
            normalized["coord_units"] = coord_units
        if coord_origin:
            normalized["coord_origin"] = coord_origin
        ware_index_value = detection.get("ware_index", detection.get("waren_index", ""))
        ware_index_text = str(ware_index_value or "").strip()
        if ware_index_text:
            normalized["ware_index"] = ware_index_text
        for width_key in ("source_image_width", "detection_image_width", "render_width", "image_width", "viewport_width"):
            if detection.get(width_key) not in (None, ""):
                normalized["source_image_width"] = cls._to_int(detection.get(width_key), width_key)
                break
        for height_key in ("source_image_height", "detection_image_height", "render_height", "image_height", "viewport_height"):
            if detection.get(height_key) not in (None, ""):
                normalized["source_image_height"] = cls._to_int(detection.get(height_key), height_key)
                break
        return normalized

    @classmethod
    def _normalize_detection_to_image(cls, detection, image_width, image_height):
        image_width = cls._to_int(image_width, "image_width")
        image_height = cls._to_int(image_height, "image_height")
        coord_units = str(detection.get("coord_units", "") or "").strip().lower()
        source_width = detection.get("source_image_width")
        source_height = detection.get("source_image_height")
        x = float(detection.get("x", 0))
        y = float(detection.get("y", 0))
        width = float(detection.get("width", 0))
        height = float(detection.get("height", 0))
        normalization_mode = "none"
        was_rescaled = False

        if coord_units in {"relative", "ratio", "normalized"} or (
            coord_units in {"", "px"}
            and all(0.0 <= value <= 1.0 for value in (x, y, width, height))
        ):
            normalization_mode = "relative_to_image"
            was_rescaled = True
            x *= image_width
            y *= image_height
            width *= image_width
            height *= image_height
        elif coord_units in {"percent", "percentage"}:
            normalization_mode = "percent_to_image"
            was_rescaled = True
            x = (x / 100.0) * image_width
            y = (y / 100.0) * image_height
            width = (width / 100.0) * image_width
            height = (height / 100.0) * image_height
        elif coord_units in {"relative_1000", "normalized_1000", "scale_1000", "thousand"}:
            normalization_mode = "relative_1000_to_image"
            was_rescaled = True
            x = (x / 1000.0) * image_width
            y = (y / 1000.0) * image_height
            width = (width / 1000.0) * image_width
            height = (height / 1000.0) * image_height
            if source_width in (None, ""):
                source_width = 1000
            if source_height in (None, ""):
                source_height = 1000
        elif source_width not in (None, "") and source_height not in (None, ""):
            source_width = cls._to_int(source_width, "source_image_width")
            source_height = cls._to_int(source_height, "source_image_height")
            if source_width > 0 and source_height > 0 and (source_width != image_width or source_height != image_height):
                normalization_mode = "scaled_from_source_image"
                was_rescaled = True
                x = x * (float(image_width) / float(source_width))
                y = y * (float(image_height) / float(source_height))
                width = width * (float(image_width) / float(source_width))
                height = height * (float(image_height) / float(source_height))

        return {
            "x": cls._to_int(x, "x"),
            "y": cls._to_int(y, "y"),
            "width": cls._to_int(width, "width"),
            "height": cls._to_int(height, "height"),
            "coord_units": "px",
            "coord_origin": str(detection.get("coord_origin", "") or "top_left").strip().lower() or "top_left",
            "source_image_width": cls._to_int(source_width, "source_image_width") if source_width not in (None, "") else None,
            "source_image_height": cls._to_int(source_height, "source_image_height") if source_height not in (None, "") else None,
            "normalization_mode": normalization_mode,
            "was_rescaled": was_rescaled,
        }

    @classmethod
    def _should_try_relative_1000_fallback(cls, normalized, scaled, image_width, image_height):
        coord_units = str(normalized.get("coord_units", "") or "").strip().lower()
        if coord_units and coord_units not in {"px"}:
            return False
        try:
            x = cls._to_int(normalized.get("x"), "x")
            y = cls._to_int(normalized.get("y"), "y")
            width = cls._to_int(normalized.get("width"), "width")
            height = cls._to_int(normalized.get("height"), "height")
        except Exception:
            return False
        if width <= 0 or height <= 0:
            return False
        max_value = max(abs(x), abs(y), abs(width), abs(height))
        if max_value > 1000:
            return False
        try:
            scaled_x = cls._to_int(scaled.get("x"), "x")
            scaled_y = cls._to_int(scaled.get("y"), "y")
            scaled_width = cls._to_int(scaled.get("width"), "width")
            scaled_height = cls._to_int(scaled.get("height"), "height")
        except Exception:
            return False
        return bool(
            scaled_x < 0
            or scaled_y < 0
            or scaled_x >= image_width
            or scaled_y >= image_height
            or (scaled_x + scaled_width) > image_width
            or (scaled_y + scaled_height) > image_height
        )
    @classmethod
    def validate_region(cls, image_width, image_height, x, y, width, height, clamp=True):
        x = cls._to_int(x, "x")
        y = cls._to_int(y, "y")
        width = cls._to_int(width, "width")
        height = cls._to_int(height, "height")
        image_width = cls._to_int(image_width, "image_width")
        image_height = cls._to_int(image_height, "image_height")

        if image_width <= 0 or image_height <= 0:
            raise ValueError("Ungueltige Bildgroesse fuer Crop.")
        if width <= 0 or height <= 0:
            raise ValueError("Crop-Breite und Crop-Hoehe muessen groesser als 0 sein.")

        if not clamp:
            if x < 0 or y < 0:
                raise ValueError("Crop-Koordinaten muessen innerhalb des Bildes starten.")
            if x + width > image_width or y + height > image_height:
                raise ValueError("Crop-Bereich liegt ausserhalb des Screenshot-Bildes.")
            return {
                "x": x,
                "y": y,
                "width": width,
                "height": height,
                "coord_origin": "top_left",
                "coord_units": "px",
                "was_clamped": False,
            }

        start_x = max(0, x)
        start_y = max(0, y)
        end_x = min(image_width, x + width)
        end_y = min(image_height, y + height)
        if start_x >= image_width or start_y >= image_height:
            raise ValueError("Crop-Koordinaten starten ausserhalb des Screenshot-Bildes.")
        if end_x <= start_x or end_y <= start_y:
            raise ValueError("Crop-Bereich ist leer oder komplett ausserhalb des Screenshot-Bildes.")

        return {
            "x": int(start_x),
            "y": int(start_y),
            "width": int(end_x - start_x),
            "height": int(end_y - start_y),
            "coord_origin": "top_left",
            "coord_units": "px",
            "was_clamped": bool(start_x != x or start_y != y or (end_x - start_x) != width or (end_y - start_y) != height),
        }

    @classmethod
    def validate_detection_box(cls, screenshot_path, detection, clamp=True):
        image_width, image_height = cls.image_dimensions(screenshot_path)
        normalized = cls.normalize_detection(detection)
        scaled = cls._normalize_detection_to_image(normalized, image_width=image_width, image_height=image_height)
        logging.info(
            "Detektions-Koordinaten vorbereitet: screenshot=%s, image=%sx%s, source=%sx%s, mode=%s, input=(%s,%s,%s,%s), scaled=(%s,%s,%s,%s)",
            os.path.basename(str(screenshot_path or "")),
            image_width,
            image_height,
            scaled.get("source_image_width"),
            scaled.get("source_image_height"),
            scaled.get("normalization_mode", "none"),
            normalized.get("x"),
            normalized.get("y"),
            normalized.get("width"),
            normalized.get("height"),
            scaled.get("x"),
            scaled.get("y"),
            scaled.get("width"),
            scaled.get("height"),
        )
        try:
            region = cls.validate_region(
                image_width=image_width,
                image_height=image_height,
                x=scaled["x"],
                y=scaled["y"],
                width=scaled["width"],
                height=scaled["height"],
                clamp=clamp,
            )
        except Exception as exc:
            recovered_with_relative_1000 = False
            if cls._should_try_relative_1000_fallback(normalized, scaled, image_width, image_height):
                retry_detection = dict(normalized or {})
                retry_detection["coord_units"] = "relative_1000"
                retry_detection.setdefault("source_image_width", 1000)
                retry_detection.setdefault("source_image_height", 1000)
                scaled_retry = cls._normalize_detection_to_image(
                    retry_detection,
                    image_width=image_width,
                    image_height=image_height,
                )
                try:
                    region = cls.validate_region(
                        image_width=image_width,
                        image_height=image_height,
                        x=scaled_retry["x"],
                        y=scaled_retry["y"],
                        width=scaled_retry["width"],
                        height=scaled_retry["height"],
                        clamp=clamp,
                    )
                    scaled = scaled_retry
                    recovered_with_relative_1000 = True
                    logging.warning(
                        "Detektions-Box via relative_1000-Fallback normalisiert: screenshot=%s, input=(%s,%s,%s,%s), fallback_scaled=(%s,%s,%s,%s)",
                        os.path.basename(str(screenshot_path or "")),
                        normalized.get("x"),
                        normalized.get("y"),
                        normalized.get("width"),
                        normalized.get("height"),
                        scaled.get("x"),
                        scaled.get("y"),
                        scaled.get("width"),
                        scaled.get("height"),
                    )
                except Exception:
                    recovered_with_relative_1000 = False
            if not recovered_with_relative_1000:
                logging.warning(
                    "Detektions-Box ausserhalb des echten Screenshots: screenshot=%s, image=%sx%s, source=%sx%s, scaled=(%s,%s,%s,%s), reason=%s",
                    os.path.basename(str(screenshot_path or "")),
                    image_width,
                    image_height,
                    scaled.get("source_image_width"),
                    scaled.get("source_image_height"),
                    scaled.get("x"),
                    scaled.get("y"),
                    scaled.get("width"),
                    scaled.get("height"),
                    str(exc),
                )
                raise
        logging.info(
            "Crop-Region validiert: screenshot=%s, before_clamp=(%s,%s,%s,%s), after_clamp=(%s,%s,%s,%s), clamped=%s",
            os.path.basename(str(screenshot_path or "")),
            scaled.get("x"),
            scaled.get("y"),
            scaled.get("width"),
            scaled.get("height"),
            region.get("x"),
            region.get("y"),
            region.get("width"),
            region.get("height"),
            region.get("was_clamped"),
        )
        return {
            **normalized,
            **scaled,
            **region,
            "image_width": image_width,
            "image_height": image_height,
        }

    @classmethod
    def crop_image(cls, screenshot_path, output_path, x, y, width, height, image_format="PNG", clamp=True):
        try:
            image = QImage(str(screenshot_path))
            if image.isNull():
                raise ValueError(f"Screenshot konnte nicht geladen werden: {screenshot_path}")

            region = cls.validate_region(image.width(), image.height(), x, y, width, height, clamp=clamp)
            cropped = image.copy(region["x"], region["y"], region["width"], region["height"])
            if cropped.isNull():
                raise ValueError("Crop konnte nicht aus dem Screenshot erstellt werden.")

            out_dir = os.path.dirname(os.path.abspath(str(output_path)))
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            if not cropped.save(str(output_path), str(image_format or "PNG").upper()):
                raise ValueError(f"Crop konnte nicht gespeichert werden: {output_path}")

            return {
                "output_path": os.path.abspath(str(output_path)),
                "width_px": int(cropped.width()),
                "height_px": int(cropped.height()),
                "region": region,
            }
        except Exception as exc:
            log_exception(__name__, exc, extra={"screenshot_path": screenshot_path, "output_path": output_path})
            logging.error(f"Fehler bei crop_image: {exc}")
            raise



