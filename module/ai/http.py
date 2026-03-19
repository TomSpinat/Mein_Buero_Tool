from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any, Dict
import urllib.error
import urllib.request


@dataclass
class HttpJsonResponse:
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    payload: Dict[str, Any] = field(default_factory=dict)
    raw_text: str = ""


class HttpJsonRequestError(RuntimeError):
    def __init__(
        self,
        status_code: int,
        message: str,
        body_text: str = "",
        headers: Dict[str, str] | None = None,
        payload: Dict[str, Any] | None = None,
        url: str = "",
    ):
        super().__init__(message)
        self.status_code = int(status_code or 0)
        self.body_text = str(body_text or "")
        self.headers = dict(headers or {})
        self.payload = dict(payload or {})
        self.url = str(url or "")


def post_json(url: str, payload: Dict[str, Any], headers: Dict[str, str] | None = None, timeout_sec: int = 60) -> HttpJsonResponse:
    body = json.dumps(payload or {}).encode("utf-8")
    request = urllib.request.Request(
        str(url or "").strip(),
        data=body,
        headers={"Content-Type": "application/json", **dict(headers or {})},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=max(5, int(timeout_sec or 60))) as response:
            raw = response.read().decode("utf-8", errors="replace")
            parsed = _parse_json_body(raw)
            return HttpJsonResponse(
                status_code=int(getattr(response, "status", 200) or 200),
                headers=_headers_to_dict(getattr(response, "headers", None)),
                payload=parsed,
                raw_text=raw,
            )
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise HttpJsonRequestError(
            status_code=int(getattr(exc, "code", 0) or 0),
            message=str(exc),
            body_text=raw,
            headers=_headers_to_dict(getattr(exc, "headers", None)),
            payload=_parse_json_body(raw),
            url=str(url or ""),
        ) from exc


def _headers_to_dict(headers_obj) -> Dict[str, str]:
    if headers_obj is None:
        return {}
    try:
        return {str(key): str(value) for key, value in headers_obj.items()}
    except Exception:
        return {}


def _parse_json_body(raw_text: str) -> Dict[str, Any]:
    text = str(raw_text or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}
