"""Dateisystemnahe Medienablage ohne UI-Abhaengigkeiten."""

from __future__ import annotations

import hashlib
import mimetypes
import os
import shutil
from pathlib import Path

from PyQt6.QtGui import QImage


class LocalMediaStore:
    def __init__(self, base_dir=None):
        root_dir = base_dir or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.base_dir = os.path.abspath(root_dir)
        self.media_root = os.path.join(self.base_dir, "data", "media")

    def ensure_structure(self):
        for rel_path in ("shops", "products", "screenshots", os.path.join("screenshots", "crops")):
            os.makedirs(os.path.join(self.media_root, rel_path), exist_ok=True)
        return self.media_root

    def bucket_path(self, bucket):
        bucket_text = str(bucket or "").strip().replace("\\", "/").strip("/")
        if not bucket_text:
            bucket_text = "misc"
        abs_path = os.path.join(self.media_root, *bucket_text.split("/"))
        os.makedirs(abs_path, exist_ok=True)
        return abs_path

    def sanitize_filename(self, name, fallback="asset"):
        stem = Path(str(name or "")).stem.strip()
        if not stem:
            stem = fallback
        safe = []
        for char in stem:
            if char.isalnum():
                safe.append(char.lower())
            elif char in ("-", "_"):
                safe.append(char)
            else:
                safe.append("-")
        cleaned = "".join(safe).strip("-_")
        while "--" in cleaned:
            cleaned = cleaned.replace("--", "-")
        return cleaned or fallback

    def compute_sha256(self, file_path):
        digest = hashlib.sha256()
        with open(file_path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                if chunk:
                    digest.update(chunk)
        return digest.hexdigest()

    def _probe_image_dimensions(self, file_path):
        image = QImage(str(file_path))
        if image.isNull():
            return None, None
        return int(image.width()), int(image.height())

    def inspect_file(self, file_path):
        abs_path = os.path.abspath(str(file_path))
        if not os.path.exists(abs_path):
            raise FileNotFoundError(abs_path)

        mime_type, _ = mimetypes.guess_type(abs_path)
        ext = Path(abs_path).suffix.lower()
        width_px, height_px = self._probe_image_dimensions(abs_path)
        return {
            "absolute_path": abs_path,
            "relative_path": self.to_relative_path(abs_path),
            "original_name": os.path.basename(abs_path),
            "mime_type": mime_type or "",
            "file_ext": ext,
            "file_size_bytes": int(os.path.getsize(abs_path)),
            "sha256": self.compute_sha256(abs_path),
            "width_px": width_px,
            "height_px": height_px,
        }

    def to_relative_path(self, file_path):
        abs_path = os.path.abspath(str(file_path))
        try:
            return os.path.relpath(abs_path, self.base_dir)
        except Exception:
            return abs_path

    def resolve_path(self, file_path):
        text = str(file_path or "").strip()
        if not text:
            return ""
        if os.path.isabs(text):
            return text
        return os.path.abspath(os.path.join(self.base_dir, text))

    def build_managed_filename(self, source_name, sha256, extension="", fallback="asset"):
        safe_name = self.sanitize_filename(source_name, fallback=fallback)
        ext = str(extension or "").strip().lower()
        if ext and not ext.startswith("."):
            ext = f".{ext}"
        return f"{sha256[:12]}_{safe_name}{ext}"

    def ingest_file(self, source_path, bucket, preferred_name=""):
        self.ensure_structure()
        info = self.inspect_file(source_path)
        target_dir = self.bucket_path(bucket)
        target_name = self.build_managed_filename(
            preferred_name or info["original_name"],
            info["sha256"],
            extension=info["file_ext"],
            fallback="asset",
        )
        target_abs = os.path.join(target_dir, target_name)
        if os.path.abspath(info["absolute_path"]) != os.path.abspath(target_abs):
            if not os.path.exists(target_abs):
                shutil.copy2(info["absolute_path"], target_abs)
        stored = self.inspect_file(target_abs)
        stored["bucket"] = str(bucket or "").strip()
        return stored

    def build_generated_path(self, bucket, preferred_name="", extension=".png", token="generated"):
        self.ensure_structure()
        target_dir = self.bucket_path(bucket)
        safe_name = self.sanitize_filename(preferred_name or token, fallback=token)
        ext = str(extension or ".png").strip().lower()
        if not ext.startswith("."):
            ext = f".{ext}"
        candidate = os.path.join(target_dir, f"{safe_name}{ext}")
        counter = 1
        while os.path.exists(candidate):
            candidate = os.path.join(target_dir, f"{safe_name}_{counter}{ext}")
            counter += 1
        return {
            "absolute_path": candidate,
            "relative_path": self.to_relative_path(candidate),
        }
