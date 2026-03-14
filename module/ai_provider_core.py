"""
Provider-neutraler Kern fuer strukturierte KI-Ausgaben.

Dieses Modul kennt:
- den neutralen Request/Response-Rahmen
- provider-nahe Fehlerobjekte
- optionale Modell-Capabilities

Es kennt bewusst keine Businesslogik und kein app-internes Fachmodell.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import json
from typing import Any, Dict, List, Optional


class ProviderResponseFormatError(RuntimeError):
    """
    Fehler fuer provider-nahe Strukturprobleme.
    error_kind wird spaeter zentral auf AppError gemappt.
    """

    def __init__(
        self,
        error_kind: str,
        user_message: str,
        technical_message: str = "",
        provider: str = "",
    ):
        super().__init__(user_message)
        self.error_kind = str(error_kind or "invalid_response")
        self.user_message = str(user_message or "Antwort des KI-Dienstes ist ungueltig.")
        self.technical_message = str(technical_message or "")
        self.provider = str(provider or "")


@dataclass(frozen=True)
class ProviderCapabilityProfile:
    provider_name: str
    model_name: str
    supports_response_mime_type: bool = False
    supports_response_schema: bool = False
    supports_system_instruction: bool = False
    supports_tools: bool = False
    supports_count_tokens: bool = False
    supports_openai_compatible_transport: bool = False


@dataclass
class ProviderRequest:
    api_key: str
    image_path: Optional[str] = None
    custom_text: str = ""
    scan_mode: str = "einkauf"
    prompt_profile: str = ""
    prompt_plan: Optional[Dict[str, Any]] = None
    scan_decision: Optional[Dict[str, Any]] = None
    response_mime_type: str = "application/json"
    response_schema: Optional[Dict[str, Any]] = None
    system_instruction: str = ""
    tools: Optional[List[Dict[str, Any]]] = None
    transport: str = "native"
    model_name: Optional[str] = None


@dataclass
class ProviderResult:
    payload: Dict[str, Any]
    provider: str = ""
    raw_text: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    usage: Dict[str, Any] = field(default_factory=dict)
    prompt_feedback: Dict[str, Any] = field(default_factory=dict)
    finish_reason: str = ""
    safety_ratings: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def token_count(self) -> int:
        total = self.usage.get("total_tokens", 0)
        try:
            return int(total or 0)
        except Exception:
            return 0


class StructuredAiProvider(ABC):
    provider_name = "unknown"

    @abstractmethod
    def analyze_document(self, request: ProviderRequest) -> ProviderResult:
        raise NotImplementedError

    @abstractmethod
    def get_capabilities(self, model_name: Optional[str] = None) -> ProviderCapabilityProfile:
        raise NotImplementedError


def parse_strict_json_object(raw_text: str, provider: str = "") -> Dict[str, Any]:
    """
    Strikter JSON-Parser ohne Markdown/Fence/Freitext-Heuristik.
    """
    text = str(raw_text or "").strip()
    if not text:
        raise ProviderResponseFormatError(
            error_kind="empty_response",
            user_message="Die Modellantwort war leer.",
            technical_message="empty response text",
            provider=provider,
        )

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ProviderResponseFormatError(
            error_kind="invalid_response",
            user_message="Die Modellantwort war kein gueltiges JSON.",
            technical_message=f"json decode failed: {exc}",
            provider=provider,
        ) from exc

    if not isinstance(parsed, dict):
        raise ProviderResponseFormatError(
            error_kind="schema_violation",
            user_message="Die Modellantwort muss ein JSON-Objekt sein.",
            technical_message=f"parsed type: {type(parsed).__name__}",
            provider=provider,
        )

    return parsed

