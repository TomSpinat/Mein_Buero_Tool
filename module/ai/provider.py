from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from module.ai.types import ProviderCapabilities, ProviderProfile, ScanRequest, ScanResult


class AiProvider(ABC):
    """
    Schlanke Basisschnittstelle fuer KI-Provider im aktuellen Scan-Kontext.
    """

    provider_name = "unknown"

    @abstractmethod
    def analyze_scan(self, request: ScanRequest) -> ScanResult:
        raise NotImplementedError

    def analyze_document(self, request: ScanRequest) -> ScanResult:
        """
        Rueckwaertskompatibler Alias fuer den bestehenden Codepfad.
        """
        return self.analyze_scan(request)

    @abstractmethod
    def get_capabilities(
        self,
        profile: Optional[ProviderProfile] = None,
        model_name: Optional[str] = None,
    ) -> ProviderCapabilities:
        raise NotImplementedError
