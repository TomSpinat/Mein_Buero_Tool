"""
settings_dialog.py
Fenster fuer die allgemeinen Einstellungen.
Normale Werte liegen in settings.json, geheime Werte im Windows-Anmeldespeicher.
"""

from PyQt6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QRadioButton,
    QVBoxLayout,
)
from PyQt6.QtCore import Qt

from module.ai.provider_settings import (
    describe_ai_profile_adjustments,
    get_ai_provider_delete_success_message,
    get_ai_provider_hint_text,
    get_ai_provider_key_label,
    get_ai_provider_key_placeholder,
    get_ai_provider_label,
    get_ai_profile_adjustment_schema,
    get_ai_provider_profile_shell_text,
    get_ai_provider_secret_key,
    list_ai_provider_options,
    normalize_ai_profile_override_entry,
    normalize_ai_profile_settings,
    normalize_ai_provider_name,
    test_provider_api_key_detailed,
    validate_ai_provider_name,
)
from module.ai.profiles import get_provider_profile_definition
from config import SettingsManager
from module.background_tasks import BackgroundTask
from module.crash_logger import error_to_payload, log_exception
from module.database_manager import DatabaseManager
from module.secret_store import sanitize_text
from storage_paths import default_external_storage_dir



def _payload_user_message(payload, fallback):
    data = payload if isinstance(payload, dict) else {}
    message = str(data.get("user_message", "")).strip()
    return message or fallback


def _run_settings_connection_checks(provider_name, api_key, db_settings):
    api_result = test_provider_api_key_detailed(provider_name, api_key)
    if not isinstance(api_result, dict):
        api_result = {"ok": False, "error": None}

    db_result = {"ok": False, "error": None}
    conn = None
    try:
        db_manager = DatabaseManager(db_settings)
        conn = db_manager._get_connection(include_db=False)
        db_result["ok"] = bool(conn and conn.is_connected())
        if not db_result["ok"]:
            db_result["error"] = {
                "category": "unknown",
                "user_message": "MySQL antwortet nicht wie erwartet.",
                "technical_message": "connection not connected",
                "status_code": None,
                "service": "mysql",
                "retryable": True,
            }
    except Exception as exc:
        log_exception(__name__, exc, extra={"phase": "settings_test_db"})
        db_result["error"] = error_to_payload(exc)
        db_result["ok"] = False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return {
        "api": api_result,
        "db": db_result,
    }


class ProviderProfileShellDialog(QDialog):
    def __init__(self, provider_name, profile_name, raw_overrides, parent=None):
        super().__init__(parent)
        self.provider_name = normalize_ai_provider_name(provider_name)
        self.profile_name = str(profile_name or "").strip()
        self._raw_overrides = dict(raw_overrides or {}) if isinstance(raw_overrides, dict) else {}
        self._combo_boxes = {}
        self._result_overrides = dict(self._raw_overrides)
        self.setWindowTitle("Profil anpassen")
        self.setFixedSize(560, 560)

        self._schema = get_ai_profile_adjustment_schema(
            self.provider_name,
            self.profile_name,
            self._raw_overrides,
        )

        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        lbl_title = QLabel(f"Aktiver Provider: {get_ai_provider_label(self.provider_name)}")
        lbl_title.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(lbl_title)

        lbl_profile = QLabel(
            f"Aktuelles Startprofil: {self._schema.get('profile_display_name') or self._schema.get('profile_name')}"
        )
        lbl_profile.setWordWrap(True)
        layout.addWidget(lbl_profile)

        lbl_body = QLabel(
            "Hier kannst du nur sichere, feste Verhaltensstufen anpassen. "
            "Freie Modellwahl oder rohe Technikwerte bleiben bewusst ausgeblendet."
        )
        lbl_body.setWordWrap(True)
        lbl_body.setStyleSheet("color: #9a9aaa; font-size: 12px;")
        layout.addWidget(lbl_body)

        self.lbl_summary = QLabel("")
        self.lbl_summary.setWordWrap(True)
        self.lbl_summary.setStyleSheet("color: #7aa2f7; font-size: 12px;")
        layout.addWidget(self.lbl_summary)

        self.grp_adjustments = QGroupBox("Profil-Anpassung")
        form_layout = QFormLayout()
        form_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        form_layout.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        form_layout.setHorizontalSpacing(18)
        form_layout.setVerticalSpacing(12)

        for field in list(self._schema.get("fields", [])):
            field_id = str(field.get("id", "") or "").strip()
            combo = QComboBox()
            for option in list(field.get("options", [])):
                label = str(option.get("label", option.get("value", "")) or "")
                if bool(option.get("is_default")):
                    label += " (Standard)"
                combo.addItem(label, str(option.get("value", "") or ""))
            current_index = combo.findData(str(field.get("current_value", "") or ""))
            if current_index >= 0:
                combo.setCurrentIndex(current_index)
            combo.currentIndexChanged.connect(self._update_summary)
            self._combo_boxes[field_id] = combo

            label_widget = QLabel(str(field.get("label", field_id)))
            help_text = str(field.get("help", "") or "")
            if help_text:
                label_widget.setToolTip(help_text)
                combo.setToolTip(help_text)
            form_layout.addRow(label_widget, combo)

        self.grp_adjustments.setLayout(form_layout)
        layout.addWidget(self.grp_adjustments)

        lbl_model_hint = QLabel(
            "Das konkrete Modell wird hier bewusst noch nicht frei waehlbar gemacht. "
            "Es wird spaeter vom jeweiligen Modul passend gesetzt."
        )
        lbl_model_hint.setWordWrap(True)
        lbl_model_hint.setStyleSheet("color: #9a9aaa; font-size: 12px;")
        layout.addWidget(lbl_model_hint)

        layout.addStretch()

        button_row = QHBoxLayout()
        btn_reset = QPushButton("Standard")
        btn_reset.clicked.connect(self._reset_to_defaults)
        button_row.addWidget(btn_reset)
        button_row.addStretch()

        btn_save = QPushButton("Uebernehmen")
        btn_save.clicked.connect(self._accept_with_values)
        button_row.addWidget(btn_save)

        btn_close = QPushButton("Abbrechen")
        btn_close.clicked.connect(self.reject)
        button_row.addWidget(btn_close)
        layout.addLayout(button_row)
        self._update_summary()

    def _current_ui_options(self):
        current_values = {}
        for field_id, combo in self._combo_boxes.items():
            current_values[field_id] = str(combo.currentData() or "")
        return current_values

    def _update_summary(self):
        summary_text = get_ai_provider_profile_shell_text(
            self.provider_name,
            self.profile_name,
            {"ui_options": self._current_ui_options()},
        )
        self.lbl_summary.setText(summary_text)

    def _reset_to_defaults(self):
        default_map = dict(self._schema.get("default_ui_options", {}) or {})
        for field_id, combo in self._combo_boxes.items():
            target_index = combo.findData(str(default_map.get(field_id, "") or ""))
            if target_index >= 0:
                combo.setCurrentIndex(target_index)

    def _accept_with_values(self):
        self._result_overrides = normalize_ai_profile_override_entry(
            self.provider_name,
            self.profile_name,
            {"ui_options": self._current_ui_options()},
        )
        self.accept()

    def result_overrides(self):
        return dict(self._result_overrides or {})


class SettingsDialog(QDialog):
    """Kleines Einstellungsfenster fuer allgemeine App-Werte und Secrets."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Einstellungen")
        self.setFixedSize(620, 980)

        self.settings_manager = SettingsManager()
        self._connection_test_task = None
        self._connection_test_token = 0
        self._ai_key_drafts = {}
        self._ai_profile_override_drafts = {}
        self._current_ai_provider = normalize_ai_provider_name(self.settings_manager.get_active_ai_provider())

        self._create_widgets()
        self._setup_layout()
        self._load_current_settings()
        self._show_secret_warnings()

    def _create_widgets(self):
        self.lbl_secret_note = QLabel()
        self.lbl_secret_note.setWordWrap(True)

        self.lbl_ai_provider = QLabel("KI-Provider:")
        self.combo_ai_provider = QComboBox()
        for label, provider_name in list_ai_provider_options():
            self.combo_ai_provider.addItem(label, provider_name)
        self.combo_ai_provider.currentIndexChanged.connect(self._on_ai_provider_changed)
        self.btn_ai_profile_adjust = QPushButton("Profil anpassen")
        self.btn_ai_profile_adjust.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_ai_profile_adjust.clicked.connect(self._open_ai_profile_shell)

        self.lbl_ai_provider_hint = QLabel()
        self.lbl_ai_provider_hint.setWordWrap(True)
        self.lbl_ai_provider_hint.setStyleSheet("color: #9a9aaa; font-size: 11px;")

        self.lbl_ai_api_key = QLabel("Gemini API Key:")
        self.entry_ai_api_key = QLineEdit()
        self.entry_ai_api_key.setEchoMode(QLineEdit.EchoMode.PasswordEchoOnEdit)
        self.entry_ai_api_key.setPlaceholderText("API-Key eingeben")
        self.btn_clear_ai_api_key = QPushButton("API-Key loeschen")
        self.btn_clear_ai_api_key.clicked.connect(self._delete_active_ai_api_key)
        self.lbl_ai_secret_state = QLabel()

        self.lbl_image_search_provider = QLabel("Bildsuche Provider:")
        self.combo_image_search_provider = QComboBox()
        self.combo_image_search_provider.addItem("Brave Search (empfohlen)", "brave")
        self.combo_image_search_provider.addItem("Google Custom Search", "google")
        self.combo_image_search_provider.currentIndexChanged.connect(self._on_image_provider_changed)

        self.lbl_product_image_api_key = QLabel("Bildsuche API Key:")
        self.entry_product_image_api_key = QLineEdit()
        self.entry_product_image_api_key.setEchoMode(QLineEdit.EchoMode.PasswordEchoOnEdit)
        self.entry_product_image_api_key.setPlaceholderText("API-Key fuer die Web-Bildsuche eingeben")
        self.btn_clear_product_image_api_key = QPushButton("Bildsuche-Key loeschen")
        self.btn_clear_product_image_api_key.clicked.connect(self._delete_product_image_api_key)
        self.lbl_product_image_api_secret_state = QLabel()

        self.lbl_product_image_search_google_cx = QLabel("Google Custom Search CX:")
        self.entry_product_image_search_google_cx = QLineEdit()
        self.entry_product_image_search_google_cx.setPlaceholderText("Suchmaschinen-ID (CX) fuer Custom Search")

        self.lbl_db_host = QLabel("MySQL Host:")
        self.entry_db_host = QLineEdit()
        self.entry_db_host.setPlaceholderText("z.B. 127.0.0.1")

        self.lbl_db_port = QLabel("MySQL Port:")
        self.entry_db_port = QLineEdit()
        self.entry_db_port.setPlaceholderText("z.B. 3306")

        self.lbl_db_user = QLabel("MySQL Benutzer:")
        self.entry_db_user = QLineEdit()
        self.entry_db_user.setPlaceholderText("z.B. root")

        self.lbl_db_pass = QLabel("MySQL Passwort:")
        self.entry_db_pass = QLineEdit()
        self.entry_db_pass.setEchoMode(QLineEdit.EchoMode.PasswordEchoOnEdit)
        self.entry_db_pass.setPlaceholderText("Neues Passwort eingeben")
        self.btn_clear_db_pass = QPushButton("DB-Passwort loeschen")
        self.btn_clear_db_pass.clicked.connect(self._delete_db_password)
        self.lbl_db_pass_state = QLabel()

        self.lbl_db_name = QLabel("Datenbank Name:")
        self.entry_db_name = QLineEdit()
        self.entry_db_name.setPlaceholderText("buchhaltung")

        self.lbl_external_storage_dir = QLabel("Externer Ablageordner:")
        self.entry_external_storage_dir = QLineEdit()
        self.entry_external_storage_dir.setPlaceholderText(str(default_external_storage_dir()))

        self.lbl_external_storage_hint = QLabel(
            "Hier landen technische Dateien wie Logs, Sicherungen und zwischengespeicherte Bilder. "
            "Wenn das Feld leer ist, bleibt alles im Projektordner."
        )
        self.lbl_external_storage_hint.setWordWrap(True)
        self.lbl_external_storage_hint.setStyleSheet("color: #9a9aaa; font-size: 11px;")

        self.lbl_api_status = QLabel("KI-API Status: ungetestet")
        self.lbl_db_status = QLabel("DB Status: ungetestet")
        self.lbl_test_busy = QLabel("")
        self.lbl_test_busy.setStyleSheet("color: #7aa2f7; font-size: 12px;")
        self.lbl_test_busy.hide()

        self.progress_test = QProgressBar()
        self.progress_test.setVisible(False)
        self.progress_test.setTextVisible(False)
        self.progress_test.setFixedHeight(10)

        # --- Buchhaltungseinstellungen ---
        self.grp_buchhaltung = QGroupBox("Buchhaltungseinstellungen")

        self.radio_kleinunternehmer = QRadioButton(
            "Kleinunternehmer (§19 UStG) – keine Umsatzsteuer"
        )
        self.radio_regelbesteuerung = QRadioButton(
            "Regelbesteuerung – Netto-/Bruttopreise trennen"
        )
        self.radio_kleinunternehmer.setChecked(True)

        self.btn_group_steuer = QButtonGroup(self)
        self.btn_group_steuer.addButton(self.radio_kleinunternehmer)
        self.btn_group_steuer.addButton(self.radio_regelbesteuerung)
        self.btn_group_steuer.buttonClicked.connect(self._on_steuer_modus_changed)

        self.lbl_steuer_info = QLabel()
        self.lbl_steuer_info.setWordWrap(True)
        self.lbl_steuer_info.setStyleSheet("color: #9a9aaa; font-size: 11px; font-style: italic;")

        self.lbl_ust_satz = QLabel("Standard-USt-Satz (%):")
        self.entry_ust_satz = QLineEdit()
        self.entry_ust_satz.setPlaceholderText("19")
        self.entry_ust_satz.setFixedWidth(80)

        self.btn_test = QPushButton("Verbindungen testen")
        self.btn_test.clicked.connect(self.test_connections)

        self.btn_save = QPushButton("Speichern")
        self.btn_save.clicked.connect(self.save_settings)

        self.btn_cancel = QPushButton("Abbrechen")
        self.btn_cancel.clicked.connect(self.reject)

    def _setup_layout(self):
        main_layout = QVBoxLayout()
        main_layout.addWidget(self.lbl_secret_note)
        main_layout.addSpacing(8)

        ai_provider_row = QHBoxLayout()
        ai_provider_row.addWidget(self.lbl_ai_provider)
        ai_provider_row.addWidget(self.combo_ai_provider)
        ai_provider_row.addSpacing(8)
        ai_provider_row.addWidget(self.btn_ai_profile_adjust)
        ai_provider_row.addStretch()
        main_layout.addLayout(ai_provider_row)
        main_layout.addWidget(self.lbl_ai_provider_hint)

        main_layout.addWidget(self.lbl_ai_api_key)
        ai_key_row = QHBoxLayout()
        ai_key_row.addWidget(self.entry_ai_api_key)
        ai_key_row.addWidget(self.btn_clear_ai_api_key)
        main_layout.addLayout(ai_key_row)
        main_layout.addWidget(self.lbl_ai_secret_state)
        main_layout.addSpacing(6)

        provider_row = QHBoxLayout()
        provider_row.addWidget(self.lbl_image_search_provider)
        provider_row.addWidget(self.combo_image_search_provider)
        provider_row.addStretch()
        main_layout.addLayout(provider_row)

        main_layout.addWidget(self.lbl_product_image_api_key)
        image_api_row = QHBoxLayout()
        image_api_row.addWidget(self.entry_product_image_api_key)
        image_api_row.addWidget(self.btn_clear_product_image_api_key)
        main_layout.addLayout(image_api_row)
        main_layout.addWidget(self.lbl_product_image_api_secret_state)

        main_layout.addWidget(self.lbl_product_image_search_google_cx)
        main_layout.addWidget(self.entry_product_image_search_google_cx)

        main_layout.addSpacing(10)

        main_layout.addSpacing(10)

        row1 = QHBoxLayout()
        row1.addWidget(self.lbl_db_host)
        row1.addWidget(self.entry_db_host)
        row1.addWidget(self.lbl_db_port)
        row1.addWidget(self.entry_db_port)
        main_layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.addWidget(self.lbl_db_user)
        row2.addWidget(self.entry_db_user)
        row2.addWidget(self.lbl_db_name)
        row2.addWidget(self.entry_db_name)
        main_layout.addLayout(row2)

        main_layout.addWidget(self.lbl_db_pass)
        db_pass_row = QHBoxLayout()
        db_pass_row.addWidget(self.entry_db_pass)
        db_pass_row.addWidget(self.btn_clear_db_pass)
        main_layout.addLayout(db_pass_row)
        main_layout.addWidget(self.lbl_db_pass_state)

        main_layout.addSpacing(10)

        main_layout.addWidget(self.lbl_external_storage_dir)
        main_layout.addWidget(self.entry_external_storage_dir)
        main_layout.addWidget(self.lbl_external_storage_hint)

        main_layout.addSpacing(10)

        # --- Buchhaltung-GroupBox ---
        buch_layout = QVBoxLayout()
        buch_layout.addWidget(self.radio_kleinunternehmer)
        buch_layout.addWidget(self.radio_regelbesteuerung)
        buch_layout.addWidget(self.lbl_steuer_info)

        ust_row = QHBoxLayout()
        ust_row.addWidget(self.lbl_ust_satz)
        ust_row.addWidget(self.entry_ust_satz)
        ust_row.addStretch()
        buch_layout.addLayout(ust_row)

        self.grp_buchhaltung.setLayout(buch_layout)
        main_layout.addWidget(self.grp_buchhaltung)

        main_layout.addSpacing(10)

        status_layout = QHBoxLayout()
        status_layout.addWidget(self.lbl_api_status)
        status_layout.addSpacing(20)
        status_layout.addWidget(self.lbl_db_status)
        status_layout.addStretch()
        main_layout.addLayout(status_layout)
        main_layout.addWidget(self.lbl_test_busy)
        main_layout.addWidget(self.progress_test)

        main_layout.addStretch()

        button_layout = QHBoxLayout()
        button_layout.setAlignment(Qt.AlignmentFlag.AlignRight)
        button_layout.addWidget(self.btn_test)
        button_layout.addWidget(self.btn_cancel)
        button_layout.addWidget(self.btn_save)
        main_layout.addLayout(button_layout)

        self.setLayout(main_layout)

    def _show_secret_warnings(self):
        warnings = self.settings_manager.consume_secret_warnings()
        if warnings:
            QMessageBox.warning(self, "Secret-Speicher", "\n\n".join(warnings))

    def _refresh_secret_states(self):
        store_ok = self.settings_manager.is_secret_store_available()
        if store_ok:
            self.lbl_secret_note.setText(
                "Geheime Werte werden getrennt im Windows-Anmeldespeicher abgelegt. "
                "Leere Felder bleiben beim Speichern unveraendert."
            )
        else:
            self.lbl_secret_note.setText(
                "Warnung: Der sichere Windows-Anmeldespeicher ist aktuell nicht verfuegbar. "
                "Neue Geheimwerte werden dann nicht dauerhaft gespeichert."
            )

        active_provider = normalize_ai_provider_name(self.combo_ai_provider.currentData() or self.settings_manager.get_active_ai_provider())
        active_secret_key = get_ai_provider_secret_key(active_provider)
        active_api_set = self.settings_manager.has_secret(active_secret_key)
        db_set = self.settings_manager.has_secret("db_pass")
        image_api_set = self.settings_manager.has_secret("product_image_search_api_key")

        self.lbl_ai_secret_state.setText(
            f"{get_ai_provider_label(active_provider)} API Key: gesetzt"
            if active_api_set
            else f"{get_ai_provider_label(active_provider)} API Key: nicht gesetzt"
        )
        self.lbl_db_pass_state.setText("MySQL Passwort: gesetzt" if db_set else "MySQL Passwort: nicht gesetzt")
        self.lbl_product_image_api_secret_state.setText("Bildsuche API Key: gesetzt" if image_api_set else "Bildsuche API Key: nicht gesetzt")

        self.entry_db_pass.clear()
        self.entry_product_image_api_key.clear()
        self.entry_db_pass.setPlaceholderText(
            "Bereits gespeichert - nur fuellen, wenn du ersetzen willst" if db_set else "Neues Passwort eingeben"
        )
        self.entry_product_image_api_key.setPlaceholderText(
            "Bereits gespeichert - nur fuellen, wenn du ersetzen willst" if image_api_set else "API-Key fuer die Web-Bildsuche eingeben"
        )
        self._refresh_ai_provider_ui(reset_entry=False)

    def _load_current_settings(self):
        normalized_ai_settings = normalize_ai_profile_settings(
            self.settings_manager.get_active_ai_provider(),
            self.settings_manager.get("ai_profile_name_by_provider", {}),
            self.settings_manager.get("ai_profile_overrides_by_provider", {}),
        )
        current_ai_provider = normalize_ai_provider_name(self.settings_manager.get_active_ai_provider())
        ai_idx = self.combo_ai_provider.findData(current_ai_provider)
        if ai_idx >= 0:
            self.combo_ai_provider.setCurrentIndex(ai_idx)
        self._current_ai_provider = current_ai_provider
        self._ai_profile_override_drafts = dict(normalized_ai_settings.get("ai_profile_overrides_by_provider", {}) or {})

        self.entry_db_host.setText(self.settings_manager.get("db_host", "127.0.0.1"))
        self.entry_db_port.setText(self.settings_manager.get("db_port", "3306"))
        self.entry_db_user.setText(self.settings_manager.get("db_user", "root"))
        self.entry_db_name.setText(self.settings_manager.get("db_name", "buchhaltung"))
        self.entry_external_storage_dir.setText(self.settings_manager.get("external_storage_dir", ""))
        
        self.entry_product_image_search_google_cx.setText(self.settings_manager.get("product_image_search_google_cx", ""))

        # Provider-Dropdown setzen
        current_provider = self.settings_manager.get("product_image_search_provider", "brave")
        idx = self.combo_image_search_provider.findData(current_provider)
        if idx >= 0:
            self.combo_image_search_provider.setCurrentIndex(idx)
        self._on_image_provider_changed()

        # Buchhaltungseinstellungen laden
        steuer_modus = self.settings_manager.get("steuer_modus", "kleinunternehmer")
        if steuer_modus == "regelbesteuerung":
            self.radio_regelbesteuerung.setChecked(True)
        else:
            self.radio_kleinunternehmer.setChecked(True)

        ust_satz = self.settings_manager.get("default_ust_satz", 19.0)
        self.entry_ust_satz.setText(str(ust_satz))

        self._on_steuer_modus_changed()
        self._refresh_secret_states()
        self._refresh_ai_provider_ui(reset_entry=True)

    def _on_steuer_modus_changed(self, _btn=None):
        is_regel = self.radio_regelbesteuerung.isChecked()
        if is_regel:
            self.lbl_steuer_info.setText(
                "Nettowerte werden aus Brutto \u00f7 (1 + USt) berechnet und separat gespeichert."
            )
            self.lbl_ust_satz.setEnabled(True)
            self.entry_ust_satz.setEnabled(True)
        else:
            self.lbl_steuer_info.setText(
                "Preise enthalten keine MwSt. Formeln arbeiten nur mit Bruttobetragen."
            )
            self.lbl_ust_satz.setEnabled(False)
            self.entry_ust_satz.setEnabled(False)

    def _active_ai_provider(self):
        return normalize_ai_provider_name(self.combo_ai_provider.currentData() or self.settings_manager.get_active_ai_provider())

    def _store_current_ai_key_draft(self):
        provider_name = normalize_ai_provider_name(self._current_ai_provider)
        self._ai_key_drafts[provider_name] = self.entry_ai_api_key.text()

    def _refresh_ai_provider_ui(self, reset_entry=False):
        provider_name = self._active_ai_provider()
        self._current_ai_provider = provider_name
        profile_override = self._ai_profile_override_drafts.get(provider_name, {})
        self.lbl_ai_api_key.setText(get_ai_provider_key_label(provider_name))
        self.lbl_ai_provider_hint.setText(
            get_ai_provider_hint_text(provider_name, self.settings_manager.get_ai_profile_name(provider_name), profile_override)
        )

        active_profile_name = self.settings_manager.get_ai_profile_name(provider_name)
        active_profile = get_provider_profile_definition(active_profile_name, provider_name=provider_name)
        adjustment_summary = describe_ai_profile_adjustments(provider_name, active_profile_name, profile_override)
        self.btn_ai_profile_adjust.setToolTip(
            f"Aktuelles Startprofil: {active_profile.display_name or active_profile.profile_name}\n{adjustment_summary}"
        )

        has_secret = self.settings_manager.has_secret(get_ai_provider_secret_key(provider_name))
        self.lbl_ai_secret_state.setText(
            f"{get_ai_provider_label(provider_name)} API Key: gesetzt"
            if has_secret
            else f"{get_ai_provider_label(provider_name)} API Key: nicht gesetzt"
        )
        self.entry_ai_api_key.setPlaceholderText(
            "Bereits gespeichert - nur fuellen, wenn du ersetzen willst"
            if has_secret
            else get_ai_provider_key_placeholder(provider_name)
        )

        if reset_entry:
            current_text = self._ai_key_drafts.get(provider_name, "")
            self.entry_ai_api_key.setText(current_text)

        self.lbl_api_status.setText(
            "KI-API Status: Key gesetzt" if has_secret else "KI-API Status: Key fehlt"
        )

    def _on_ai_provider_changed(self):
        previous_provider = normalize_ai_provider_name(self._current_ai_provider)
        self._ai_key_drafts[previous_provider] = self.entry_ai_api_key.text()
        self._refresh_ai_provider_ui(reset_entry=True)

    def _open_ai_profile_shell(self):
        provider_name = self._active_ai_provider()
        profile_name = self.settings_manager.get_ai_profile_name(provider_name)
        dialog = ProviderProfileShellDialog(
            provider_name,
            profile_name,
            self._ai_profile_override_drafts.get(provider_name, {}),
            parent=self,
        )
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self._ai_profile_override_drafts[provider_name] = dialog.result_overrides()
            self._refresh_ai_provider_ui(reset_entry=False)

    def _delete_active_ai_api_key(self):
        provider_name = self._active_ai_provider()
        self.settings_manager.delete_secret(get_ai_provider_secret_key(provider_name))
        self._ai_key_drafts[provider_name] = ""
        self.entry_ai_api_key.clear()
        self._refresh_secret_states()
        QMessageBox.information(self, "Entfernt", get_ai_provider_delete_success_message(provider_name))

    def _on_image_provider_changed(self):
        is_google = self.combo_image_search_provider.currentData() == "google"
        self.lbl_product_image_search_google_cx.setVisible(is_google)
        self.entry_product_image_search_google_cx.setVisible(is_google)

    def _delete_product_image_api_key(self):
        self.settings_manager.delete_secret("product_image_search_api_key")
        self._refresh_secret_states()
        QMessageBox.information(self, "Entfernt", "Der gespeicherte Bildsuch-API-Key wurde geloescht.")

    def _delete_db_password(self):
        self.settings_manager.delete_secret("db_pass")
        self._refresh_secret_states()
        QMessageBox.information(self, "Entfernt", "Das gespeicherte MySQL Passwort wurde geloescht.")

    def _set_test_busy(self, busy, text=""):
        self.btn_test.setEnabled(not busy)
        self.btn_save.setEnabled(not busy)
        self.btn_cancel.setEnabled(not busy)
        self.combo_ai_provider.setEnabled(not busy)
        self.entry_ai_api_key.setEnabled(not busy)
        self.btn_ai_profile_adjust.setEnabled(not busy)
        self.entry_product_image_api_key.setEnabled(not busy)
        self.entry_db_host.setEnabled(not busy)
        self.entry_db_port.setEnabled(not busy)
        self.entry_db_user.setEnabled(not busy)
        self.entry_db_pass.setEnabled(not busy)
        self.entry_db_name.setEnabled(not busy)
        self.entry_external_storage_dir.setEnabled(not busy)
        self.btn_clear_ai_api_key.setEnabled(not busy)
        self.btn_clear_product_image_api_key.setEnabled(not busy)
        self.btn_clear_db_pass.setEnabled(not busy)
        self.btn_test.setText("Teste..." if busy else "Verbindungen testen")
        self.progress_test.setVisible(busy)
        self.lbl_test_busy.setVisible(bool(text))
        self.lbl_test_busy.setText(text)
        if busy:
            self.progress_test.setRange(0, 0)
        else:
            self.progress_test.setRange(0, 1)
            self.progress_test.setValue(0)

    def test_connections(self):
        if self._connection_test_task is not None and self._connection_test_task.isRunning():
            return

        active_provider = self._active_ai_provider()
        api_key = self.entry_ai_api_key.text().strip() or self.settings_manager.get_ai_api_key(active_provider)
        temp_settings = {
            "db_host": self.entry_db_host.text().strip(),
            "db_port": self.entry_db_port.text().strip() or "3306",
            "db_user": self.entry_db_user.text().strip(),
            "db_pass": self.entry_db_pass.text().strip() or self.settings_manager.get("db_pass", ""),
            "db_name": self.entry_db_name.text().strip(),
        }

        self._connection_test_token += 1
        task_id = self._connection_test_token
        self.lbl_api_status.setText("KI-API Status: teste...")
        self.lbl_db_status.setText("DB Status: teste...")
        self._set_test_busy(True, "Verbindungen werden im Hintergrund geprueft...")

        self._connection_test_task = BackgroundTask(
            _run_settings_connection_checks,
            active_provider,
            api_key,
            temp_settings,
            task_id=task_id,
            parent=self,
        )
        self._connection_test_task.result_signal.connect(self._on_test_connections_result)
        self._connection_test_task.error_signal.connect(self._on_test_connections_error)
        self._connection_test_task.finished_signal.connect(self._on_test_connections_finished)
        self._connection_test_task.start()

    def _on_test_connections_result(self, task_id, result):
        if task_id != self._connection_test_token:
            return

        result = result if isinstance(result, dict) else {}
        api_result = result.get("api", {}) if isinstance(result.get("api", {}), dict) else {}
        db_result = result.get("db", {}) if isinstance(result.get("db", {}), dict) else {}
        active_provider = self._active_ai_provider()
        active_provider_label = get_ai_provider_label(active_provider)

        api_ok = bool(api_result.get("ok"))
        db_ok = bool(db_result.get("ok"))
        self.lbl_api_status.setText("KI-API Status: verbunden" if api_ok else "KI-API Status: Fehler")
        self.lbl_db_status.setText("DB Status: verbunden" if db_ok else "DB Status: Fehler")

        if api_ok and db_ok:
            return

        hint_lines = []
        if not api_ok:
            hint_lines.append(
                f"{active_provider_label}: " + _payload_user_message(
                    api_result.get("error"),
                    "Verbindung fehlgeschlagen. Bitte API-Key pruefen.",
                )
            )
        if not db_ok:
            hint_lines.append(
                "MySQL: " + _payload_user_message(
                    db_result.get("error"),
                    "Verbindung fehlgeschlagen. Bitte Host/Port/Benutzer pruefen.",
                )
            )

        QMessageBox.warning(
            self,
            "Verbindungscheck",
            "\n\n".join(hint_lines) if hint_lines else "Verbindungspruefung fehlgeschlagen.",
        )

    def _on_test_connections_error(self, task_id, err_msg):
        if task_id != self._connection_test_token:
            return

        self.lbl_api_status.setText("KI-API Status: Fehler")
        self.lbl_db_status.setText("DB Status: Fehler")
        QMessageBox.critical(self, "Test fehlgeschlagen", f"Die Verbindungspruefung ist fehlgeschlagen:\n{err_msg}")

    def _on_test_connections_finished(self, task_id):
        if task_id != self._connection_test_token:
            return
        self._connection_test_task = None
        self._set_test_busy(False)

    def save_settings(self):
        self._store_current_ai_key_draft()
        selected_provider = self.combo_image_search_provider.currentData() or "brave"
        selected_ai_provider = validate_ai_provider_name(self.combo_ai_provider.currentData() or "gemini")
        steuer_modus = "regelbesteuerung" if self.radio_regelbesteuerung.isChecked() else "kleinunternehmer"
        try:
            ust_satz_val = float(self.entry_ust_satz.text().replace(",", ".").strip() or "19")
        except ValueError:
            ust_satz_val = 19.0

        normalized_ai_settings = normalize_ai_profile_settings(
            selected_ai_provider,
            self.settings_manager.get("ai_profile_name_by_provider", {}),
            self._ai_profile_override_drafts,
        )

        settings_dict = {
            "ai_provider": selected_ai_provider,
            "ai_profile_name_by_provider": normalized_ai_settings["ai_profile_name_by_provider"],
            "ai_profile_overrides_by_provider": normalized_ai_settings["ai_profile_overrides_by_provider"],
            "db_host": self.entry_db_host.text().strip(),
            "db_port": self.entry_db_port.text().strip() or "3306",
            "db_user": self.entry_db_user.text().strip(),
            "db_name": self.entry_db_name.text().strip(),
            "external_storage_dir": self.entry_external_storage_dir.text().strip(),
            "product_image_search_provider": selected_provider,
            "shop_logo_search_provider": selected_provider,
            "product_image_search_google_cx": self.entry_product_image_search_google_cx.text().strip(),
            "shop_logo_search_google_cx": self.entry_product_image_search_google_cx.text().strip(),
            "steuer_modus": steuer_modus,
            "default_ust_satz": ust_satz_val,
        }

        ai_api_key = self.entry_ai_api_key.text().strip()
        db_pass = self.entry_db_pass.text().strip()
        product_image_api_key = self.entry_product_image_api_key.text().strip()
        if ai_api_key:
            settings_dict[get_ai_provider_secret_key(selected_ai_provider)] = ai_api_key
        if db_pass:
            settings_dict["db_pass"] = db_pass
        if product_image_api_key:
            settings_dict["product_image_search_api_key"] = product_image_api_key

        try:
            self.settings_manager.save_settings(settings_dict)
            self._ai_key_drafts[selected_ai_provider] = ""
            self.entry_ai_api_key.clear()
            self._refresh_secret_states()
            self._show_secret_warnings()
            active_key_present = bool(
                ai_api_key or self.settings_manager.get_ai_api_key(selected_ai_provider)
            )
            success_message = "Einstellungen wurden erfolgreich gespeichert."
            if not active_key_present:
                success_message += (
                    f"\n\nHinweis: Fuer {get_ai_provider_label(selected_ai_provider)} ist noch kein API-Key gespeichert."
                )
            QMessageBox.information(self, "Erfolg", success_message)
            self.accept()
        except Exception as exc:
            log_exception(__name__, exc)
            QMessageBox.critical(
                self,
                "Fehler",
                f"Beim Speichern trat ein Fehler auf:\n{sanitize_text(exc)}",
            )

    def closeEvent(self, event):
        if self._connection_test_task is not None and self._connection_test_task.isRunning():
            self._connection_test_task.cancel()
        super().closeEvent(event)
