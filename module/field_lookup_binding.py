"""UI-Binding-Layer: verbindet QLineEdit-Widgets mit dem LookupService.

Einmal konfiguriert, laeuft die Lookup-Logik in Modul 1, Modul 2 und
dem Mail-Wizard identisch. Dieses Modul enthaelt die einzige Stelle, an
der `returnPressed` / `editingFinished` mit Lookups verknuepft wird.

Typische Nutzung:
    service = LookupService(db)
    binding = FieldLookupBinding(
        widget=self.inputs["shop_name"],
        field_type=FieldType.SHOP_NAME,
        lookup_service=service,
        on_result=self._handle_shop_result,
        parent_widget=self,
    )
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Optional

from PyQt6.QtCore import QObject, QRunnable, QThreadPool, Qt, pyqtSignal, pyqtSlot

from module.lookup_results import (
    FieldState,
    FieldType,
    LookupResult,
    LookupSource,
)
from module.einkauf_ui import set_field_state
from module.crash_logger import log_exception

if TYPE_CHECKING:
    from PyQt6.QtWidgets import QLineEdit, QWidget
    from module.lookup_service import LookupService

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
#  Async Worker: fuehrt den Lookup im Threadpool aus
# ---------------------------------------------------------------------------

class _LookupSignals(QObject):
    """Qt-Signale fuer den async Lookup-Worker."""
    finished = pyqtSignal(object)  # LookupResult
    error = pyqtSignal(str)


class _LookupWorker(QRunnable):
    """Fuehrt einen LookupService-Aufruf im Threadpool aus."""

    def __init__(
        self,
        lookup_service: "LookupService",
        field_type: FieldType,
        value: str,
        sender_domain: str = "",
        varianten_info: str = "",
    ):
        super().__init__()
        self.signals = _LookupSignals()
        self._service = lookup_service
        self._field_type = field_type
        self._value = value
        self._sender_domain = sender_domain
        self._varianten_info = varianten_info
        self.setAutoDelete(True)

    @pyqtSlot()
    def run(self):
        try:
            result = self._service.lookup(
                field_type=self._field_type,
                value=self._value,
                sender_domain=self._sender_domain,
                varianten_info=self._varianten_info,
            )
            self.signals.finished.emit(result)
        except Exception as exc:
            log_exception(__name__, exc)
            self.signals.error.emit(str(exc))


# ---------------------------------------------------------------------------
#  FieldLookupBinding: zentrales Binding-Objekt pro Feld
# ---------------------------------------------------------------------------

class FieldLookupBinding(QObject):
    """Bindet ein QLineEdit an den LookupService.

    Verhalten bei Enter/EditingFinished:
        1. Feld wird sofort USER_CONFIRMED (gruen) gesetzt.
        2. Async Lookup-Worker wird gestartet (LOOKUP_RUNNING).
        3. Ergebnis kommt zurueck → Farbe und Callback werden gesetzt.

    Args:
        widget:          Das QLineEdit-Widget.
        field_type:      Welcher Lookup-Typ (SHOP_NAME, EAN, …).
        lookup_service:  Die LookupService-Instanz.
        on_result:       Callback(LookupResult) – wird im Main-Thread aufgerufen.
        parent_widget:   Optionales Parent-Widget fuer Dialoge.
        sender_domain:   Optionale E-Mail-Domain (nur fuer Shop-Lookup).
    """

    # Signal das andere Widgets verbinden koennen
    result_ready = pyqtSignal(object)  # LookupResult

    def __init__(
        self,
        widget: "QLineEdit",
        field_type: FieldType,
        lookup_service: "LookupService",
        on_result: Optional[Callable[["LookupResult"], None]] = None,
        parent_widget: Optional["QWidget"] = None,
        sender_domain: str = "",
    ):
        super().__init__(parent=widget)
        self.widget = widget
        self.field_type = field_type
        self.lookup_service = lookup_service
        self._on_result = on_result
        self._parent_widget = parent_widget
        self._sender_domain = sender_domain
        self._last_lookup_text = ""
        self._current_state = FieldState.EMPTY

        # Signal-Verbindungen
        self.widget.returnPressed.connect(self._on_enter_pressed)

    # -- Public API --

    @property
    def current_state(self) -> FieldState:
        return self._current_state

    def set_sender_domain(self, domain: str):
        """Aktualisiert die sender_domain (z.B. wenn Mail wechselt)."""
        self._sender_domain = str(domain or "").strip()

    def trigger_lookup(self, text: str = ""):
        """Lookup manuell ausloesen (z.B. nach AI-Fill)."""
        value = text or str(self.widget.text()).strip()
        if value:
            self._start_lookup(value)

    def set_state(self, state: FieldState):
        """Setzt den visuellen State manuell (z.B. nach AI-Fill)."""
        self._current_state = state
        set_field_state(self.widget, state)

    def reset(self):
        """Setzt das Binding in den Ausgangszustand zurueck."""
        self._last_lookup_text = ""
        self._current_state = FieldState.EMPTY
        set_field_state(self.widget, FieldState.EMPTY)

    # -- Private --

    def _on_enter_pressed(self):
        """Wird aufgerufen wenn User Enter drueckt."""
        text = str(self.widget.text()).strip()
        if not text:
            self.set_state(FieldState.EMPTY)
            return

        # Sofort gruen setzen (User hat aktiv bestaetigt)
        self.set_state(FieldState.USER_CONFIRMED)

        # Lookup starten (auch wenn Text gleich geblieben ist – User will explizit)
        self._start_lookup(text)

    def _start_lookup(self, text: str):
        """Startet den async Lookup-Worker."""
        self._last_lookup_text = text
        self.set_state(FieldState.LOOKUP_RUNNING)

        worker = _LookupWorker(
            lookup_service=self.lookup_service,
            field_type=self.field_type,
            value=text,
            sender_domain=self._sender_domain,
        )
        worker.signals.finished.connect(self._on_lookup_finished)
        worker.signals.error.connect(self._on_lookup_error)

        QThreadPool.globalInstance().start(worker)

    def _on_lookup_finished(self, result: LookupResult):
        """Wird im Main-Thread aufgerufen wenn der Lookup fertig ist."""
        # Pruefen ob der Text sich seitdem geaendert hat
        current_text = str(self.widget.text()).strip()
        if current_text != self._last_lookup_text:
            # Text hat sich geaendert waehrend Lookup lief – Ergebnis verwerfen
            return

        # State setzen basierend auf Ergebnis
        self._current_state = result.state
        set_field_state(self.widget, result.state)

        # Signal emittieren
        self.result_ready.emit(result)

        # Callback aufrufen
        if self._on_result:
            try:
                self._on_result(result)
            except Exception as exc:
                log_exception(__name__, exc)

    def _on_lookup_error(self, error_msg: str):
        """Wird bei Lookup-Fehler aufgerufen."""
        logger.error("Lookup-Fehler fuer %s: %s", self.field_type.value, error_msg)
        self._current_state = FieldState.NOT_FOUND
        set_field_state(self.widget, FieldState.NOT_FOUND)

        if self._on_result:
            try:
                self._on_result(LookupResult(
                    state=FieldState.NOT_FOUND,
                    source=LookupSource.NONE,
                    field_type=self.field_type,
                    error=error_msg,
                ))
            except Exception as exc:
                log_exception(__name__, exc)


# ---------------------------------------------------------------------------
#  Helper: Mehrere Bindings auf einmal erstellen
# ---------------------------------------------------------------------------

def create_bindings(
    widgets: dict[str, "QLineEdit"],
    lookup_service: "LookupService",
    result_handler: Callable[["LookupResult"], None],
    parent_widget: Optional["QWidget"] = None,
    sender_domain: str = "",
) -> dict[str, FieldLookupBinding]:
    """Erstellt FieldLookupBindings fuer alle bekannten Feld-Typen.

    Args:
        widgets:         Dict mit key → QLineEdit (z.B. self.inputs).
        lookup_service:  LookupService-Instanz.
        result_handler:  Gemeinsamer Callback fuer alle Lookups.
        parent_widget:   Parent fuer Dialoge.
        sender_domain:   E-Mail-Domain (fuer Shop-Lookup).

    Returns:
        Dict mit key → FieldLookupBinding (nur fuer bekannte Felder).
    """
    field_type_map = {
        "shop_name": FieldType.SHOP_NAME,
        "zahlungsart": FieldType.ZAHLUNGSART,
        "bestellnummer": FieldType.BESTELLNUMMER,
        "kaufdatum": FieldType.KAUFDATUM,
    }

    bindings: dict[str, FieldLookupBinding] = {}

    for key, field_type in field_type_map.items():
        widget = widgets.get(key)
        if widget is None:
            continue
        binding = FieldLookupBinding(
            widget=widget,
            field_type=field_type,
            lookup_service=lookup_service,
            on_result=result_handler,
            parent_widget=parent_widget,
            sender_domain=sender_domain,
        )
        bindings[key] = binding

    return bindings
