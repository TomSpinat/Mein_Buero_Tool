"""
shared_search_workflows.py
Gemeinsame Such-Workflows (Logo, spaeter EAN), die von Modul 1 und Modul 2 genutzt werden.
Extrahiert aus modul_order_entry.py und modul_mail_scraper.py, um Duplikate zu vermeiden.
"""

import os

from module.shop_logo_search_worker import ShopLogoSearchWorker
from module.media.media_grid_selection_dialog import MediaGridSelectionDialog
from module.media.media_service import MediaService
from module.media.media_store import LocalMediaStore
from module.database_manager import DatabaseManager
from module.custom_msgbox import CustomMsgBox
from module.crash_logger import log_exception


def create_logo_search_worker(
    *,
    parent_widget,
    settings_manager,
    shop_name,
    sender_domain,
    current_worker,
    logo_button,
    on_finished_callback,
    on_error_callback,
):
    """
    Erstellt und startet einen ShopLogoSearchWorker.

    Prueft zuerst, ob *current_worker* noch laeuft, und ob *shop_name* gesetzt ist.
    Setzt *logo_button* auf Busy-State.

    Gibt den gestarteten Worker zurueck, oder None wenn die Suche abgelehnt wurde.
    Der Aufrufer ist dafuer verantwortlich, den Worker in seinem eigenen Attribut zu speichern.
    """
    if current_worker is not None and current_worker.isRunning():
        CustomMsgBox.information(
            parent_widget, "Logo-Suche",
            "Es laeuft bereits eine Logo-Suche im Hintergrund.",
        )
        return None

    if not shop_name:
        CustomMsgBox.information(
            parent_widget, "Logo-Suche",
            "Bitte zuerst einen Shop-Namen eintragen.",
        )
        return None

    logo_button.setEnabled(False)
    logo_button.setText("Logo suchen...")

    worker = ShopLogoSearchWorker(
        settings_manager,
        canonical_shop_name=shop_name,
        sender_domain=sender_domain,
        limit=6,
    )
    worker.result_signal.connect(on_finished_callback)
    worker.error_signal.connect(on_error_callback)
    worker.start()
    return worker


def reset_logo_search_button(logo_button):
    """
    Setzt den Logo-Such-Button in den Grundzustand zurueck.
    """
    if logo_button is not None:
        logo_button.setEnabled(True)
        logo_button.setText("Logo suchen")


def handle_logo_search_result(
    *,
    parent_widget,
    settings_manager,
    result_dict,
    shop_name,
    source_module,
    form_widget=None,
    on_complete=None,
):
    """
    Verarbeitet das Ergebnis einer Logo-Suche:
    Kandidatenauswahl -> MediaService-Speicherung -> Logo-Vorschau -> optionaler Callback.

    Parameter:
        parent_widget:    QWidget-Eltern fuer Dialoge
        settings_manager: SettingsManager-Instanz
        result_dict:      Ergebnis vom ShopLogoSearchWorker
        shop_name:        Kanonischer Shop-Name
        source_module:    z.B. "modul_order_entry" oder "modul_mail_scraper"
        form_widget:      EinkaufHeadFormWidget mit set_shop_logo_path() (optional)
        on_complete:      Callable(shop_name) – wird nach erfolgreichem Speichern aufgerufen (optional)
    """
    candidates = result_dict.get("candidates", []) if isinstance(result_dict, dict) else []

    if not candidates:
        CustomMsgBox.information(
            parent_widget, "Logo-Suche",
            "Es wurden keine passenden Logos gefunden.",
        )
        return

    selected = MediaGridSelectionDialog.choose(
        shop_name or "Shop-Logo",
        candidates,
        search_type="Logo",
        parent=parent_widget,
    )
    if not selected:
        return

    image_url = str(
        selected.get("image_url", "") or selected.get("thumbnail_url", "") or ""
    ).strip()
    if not image_url:
        CustomMsgBox.warning(
            parent_widget, "Logo-Suche",
            "Der gewaehlte Eintrag hat keine gueltige Bild-URL.",
        )
        return

    try:
        db = DatabaseManager(settings_manager)
        media_service = MediaService(db)
        result = media_service.register_remote_shop_logo(
            shop_name=shop_name,
            image_url=image_url,
            source_module=source_module,
            source_kind="manual_web_selection",
            source_ref=str(selected.get("source_page_url", "") or "").strip(),
        )
        logo_path = ""
        if isinstance(result, dict):
            asset = result.get("asset") or {}
            logo_path = str(asset.get("file_path", "") or "").strip()
        if logo_path and form_widget is not None:
            abs_logo_path = (
                LocalMediaStore().resolve_path(logo_path)
                if not os.path.isabs(logo_path)
                else logo_path
            )
            if abs_logo_path and os.path.exists(abs_logo_path):
                logo_path = abs_logo_path
            form_widget.set_shop_logo_path(logo_path)
        CustomMsgBox.information(
            parent_widget, "Logo-Suche",
            f"Logo fuer '{shop_name}' wurde gespeichert.",
        )
        if on_complete is not None:
            on_complete(shop_name)
    except Exception as exc:
        log_exception(__name__, exc)
        CustomMsgBox.warning(
            parent_widget, "Logo-Suche",
            f"Das Logo konnte nicht gespeichert werden:\n{exc}",
        )


def handle_logo_search_error(*, parent_widget, err_msg):
    """
    Zeigt eine Fehlermeldung fuer eine fehlgeschlagene Logo-Suche.
    """
    text = str(err_msg or "").strip() or "Unbekannter Fehler bei der Logo-Suche."
    CustomMsgBox.warning(
        parent_widget, "Logo-Suche",
        f"Die Logo-Suche ist fehlgeschlagen:\n{text}",
    )
