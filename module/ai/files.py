from __future__ import annotations

from dataclasses import dataclass
import base64
import mimetypes
import os

from module.ai.types import AiProviderError


@dataclass(frozen=True)
class LocalInputAsset:
    path: str
    filename: str
    mime_type: str
    kind: str
    size_bytes: int
    base64_data: str

    @property
    def is_image(self) -> bool:
        return self.kind == "image"

    @property
    def is_pdf(self) -> bool:
        return self.kind == "pdf"

    @property
    def data_url(self) -> str:
        return f"data:{self.mime_type};base64,{self.base64_data}"


def load_local_input_asset(path: str, provider_name: str = "") -> LocalInputAsset:
    normalized_path = str(path or "").strip()
    if not normalized_path:
        raise AiProviderError(
            error_kind="input_error",
            user_message="Es wurde keine Datei fuer den KI-Scan uebergeben.",
            technical_message="empty input path",
            provider_name=provider_name,
            service=provider_name,
        )
    if not os.path.exists(normalized_path):
        raise AiProviderError(
            error_kind="upload_error",
            user_message="Die uebergebene Datei fuer den KI-Scan wurde nicht gefunden.",
            technical_message=f"file not found: {normalized_path}",
            provider_name=provider_name,
            service=provider_name,
        )

    with open(normalized_path, "rb") as handle:
        binary = handle.read()

    mime_type, _encoding = mimetypes.guess_type(normalized_path)
    mime = str(mime_type or "").strip().lower()
    if not mime:
        lower = normalized_path.lower()
        if lower.endswith(".pdf"):
            mime = "application/pdf"
        elif lower.endswith(".png"):
            mime = "image/png"
        elif lower.endswith(".jpg") or lower.endswith(".jpeg"):
            mime = "image/jpeg"
        elif lower.endswith(".webp"):
            mime = "image/webp"
        else:
            mime = "application/octet-stream"

    if mime.startswith("image/"):
        kind = "image"
    elif mime == "application/pdf" or normalized_path.lower().endswith(".pdf"):
        kind = "pdf"
    else:
        kind = "file"

    return LocalInputAsset(
        path=normalized_path,
        filename=os.path.basename(normalized_path),
        mime_type=mime,
        kind=kind,
        size_bytes=len(binary),
        base64_data=base64.b64encode(binary).decode("ascii"),
    )
