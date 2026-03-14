"""
config.py
Hilfsfunktionen fuer Ressourcenpfade und einfache Einstellungen.
"""

import copy
import json
import os
import sys
import uuid

from module.crash_logger import log_exception
from module.secret_store import SecretManager


def resource_path(relative_path):
    """
    Liefert den korrekten Dateipfad sowohl im normalen Python-Start
    als auch in einer gepackten .exe (PyInstaller).
    """
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(os.path.dirname(__file__))

    return os.path.join(base_path, relative_path)


class SettingsManager:
    """
    Liest normale Einstellungen aus settings.json.
    Secrets werden getrennt ueber den Windows Credential Manager verwaltet.
    """

    SECRET_KEYS = {
        "gemini_api_key": "Gemini API Key",
        "db_pass": "MySQL Passwort",
        "email_password": "E-Mail Passwort",
        "imap_pass": "IMAP Passwort",
        "upcitemdb_api_key": "UPCitemdb API Key",
        "product_image_search_api_key": "Produktbildsuche API Key",
    }
    MAIL_ACCOUNT_SECRET_FIELD = "pwd"
    MAIL_ACCOUNT_SECRET_REF_FIELD = "secret_ref"

    def __init__(self):
        if getattr(sys, "frozen", False):
            base_dir = os.path.dirname(sys.executable)
        else:
            base_dir = os.path.abspath(os.path.dirname(__file__))

        self.settings_file = os.path.join(base_dir, "settings.json")
        self.secret_manager = SecretManager("MeinBueroTool")
        self._volatile_secrets = {}
        self._pending_secret_warnings = []
        self.settings = self.load_settings()

    def _default_settings(self):
        return {
            "gemini_api_key": "",
            "db_host": "127.0.0.1",
            "db_port": "3306",
            "db_user": "root",
            "db_pass": "",
            "db_name": "buchhaltung",
            "imap_host": "imap.ionos.de",
            "imap_user": "",
            "imap_pass": "",
            "imap_server": "imap.ionos.de",
            "imap_port": "993",
            "email_user": "",
            "email_password": "",
            "mail_accounts": [],
            "ean_api_enabled": True,
            "upcitemdb_api_url": "https://api.upcitemdb.com/prod/trial/search",
            "upcitemdb_timeout_sec": 8,
            "upcitemdb_max_queries": 5,
            "upcitemdb_min_score": 0.16,
            "upcitemdb_api_key": "",
            "product_image_search_enabled": True,
            "product_image_search_provider": "brave",
            "product_image_search_api_url": "https://api.search.brave.com/res/v1/images/search",
            "product_image_search_api_key": "",
            "product_image_search_timeout_sec": 8,
            "product_image_search_max_results": 3,
            "shop_logo_search_enabled": True,
            "shop_logo_search_provider": "brave",
            "shop_logo_search_api_url": "https://api.search.brave.com/res/v1/images/search",
            "shop_logo_search_timeout_sec": 8,
            "shop_logo_search_max_results": 3,
            "trusted_mail_senders": [],
            "trusted_mail_domains": [],
            "test_wipe_on_start": False,
            "test_last_wipe_at": "",
        }

    def _queue_secret_warning(self, message):
        text = str(message or "").strip()
        if text and text not in self._pending_secret_warnings:
            self._pending_secret_warnings.append(text)

    def consume_secret_warnings(self):
        warnings = list(self._pending_secret_warnings)
        self._pending_secret_warnings.clear()
        return warnings

    def is_secret_store_available(self):
        return self.secret_manager.is_available()

    def _read_plain_settings_file(self):
        if not os.path.exists(self.settings_file):
            return {}

        try:
            with open(self.settings_file, "r", encoding="utf-8-sig") as file_handle:
                data = json.load(file_handle)
                return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}
        except Exception as exc:
            log_exception(__name__, exc)
            return {}

    def _write_plain_settings_file(self, settings_dict):
        with open(self.settings_file, "w", encoding="utf-8") as file_handle:
            json.dump(settings_dict, file_handle, indent=4, ensure_ascii=False)

    def _mail_secret_key(self, secret_ref):
        return f"mail_account:{secret_ref}"

    def _read_secret_value(self, secret_key, default=""):
        if secret_key in self._volatile_secrets:
            return self._volatile_secrets.get(secret_key, default)
        if self.secret_manager.is_available():
            try:
                return self.secret_manager.get_secret(secret_key, default)
            except Exception as exc:
                log_exception(__name__, exc, extra={"secret_key": secret_key, "op": "read"})
        return default

    def _delete_secret_value(self, secret_key):
        self._volatile_secrets.pop(secret_key, None)
        if self.secret_manager.is_available():
            try:
                self.secret_manager.delete_secret(secret_key)
            except Exception as exc:
                log_exception(__name__, exc, extra={"secret_key": secret_key, "op": "delete"})

    def _store_secret_value(self, secret_key, value, label="Secret"):
        secret_text = str(value or "")
        if not secret_text:
            self._delete_secret_value(secret_key)
            return True

        if self.secret_manager.is_available():
            try:
                if self.secret_manager.set_secret(secret_key, secret_text):
                    self._volatile_secrets.pop(secret_key, None)
                    return True
            except Exception as exc:
                log_exception(__name__, exc, extra={"secret_key": secret_key, "op": "write"})

        self._volatile_secrets[secret_key] = secret_text
        self._queue_secret_warning(
            f"{label} konnte nicht sicher im Windows-Anmeldespeicher abgelegt werden. "
            f"Der Wert bleibt nur fuer diese Sitzung verfuegbar und wird nicht erneut im Klartext gespeichert."
        )
        return False

    def _sanitize_plain_settings(self, settings_dict):
        sanitized = copy.deepcopy(settings_dict if isinstance(settings_dict, dict) else {})

        for key in self.SECRET_KEYS:
            sanitized.pop(key, None)

        accounts = []
        for account in sanitized.get("mail_accounts", []) or []:
            if not isinstance(account, dict):
                continue
            clean_account = copy.deepcopy(account)
            clean_account.pop("pwd", None)
            clean_account.pop("password", None)
            accounts.append(clean_account)
        sanitized["mail_accounts"] = accounts
        return sanitized

    def _prepare_mail_accounts_for_save(self, accounts):
        stored_accounts = []
        for account in accounts or []:
            if not isinstance(account, dict):
                continue

            clean_account = copy.deepcopy(account)
            secret_ref = str(clean_account.get(self.MAIL_ACCOUNT_SECRET_REF_FIELD, "")).strip()
            if not secret_ref:
                secret_ref = uuid.uuid4().hex
            clean_account[self.MAIL_ACCOUNT_SECRET_REF_FIELD] = secret_ref

            has_pwd_update = "pwd" in clean_account or "password" in clean_account
            pwd_value = clean_account.get("pwd", clean_account.get("password", ""))
            if has_pwd_update:
                self._store_secret_value(
                    self._mail_secret_key(secret_ref),
                    str(pwd_value or "").strip(),
                    label=f"E-Mail Passwort ({clean_account.get('name') or clean_account.get('user') or 'Konto'})",
                )

            clean_account.pop("pwd", None)
            clean_account.pop("password", None)
            stored_accounts.append(clean_account)
        return stored_accounts

    def _hydrate_mail_accounts(self, accounts):
        runtime_accounts = []
        for account in accounts or []:
            if not isinstance(account, dict):
                continue

            hydrated = copy.deepcopy(account)
            secret_ref = str(hydrated.get(self.MAIL_ACCOUNT_SECRET_REF_FIELD, "")).strip()
            pwd_value = ""
            if secret_ref:
                pwd_value = self._read_secret_value(self._mail_secret_key(secret_ref), "")
            if not pwd_value:
                pwd_value = str(hydrated.get("pwd") or hydrated.get("password") or "")

            hydrated["pwd"] = pwd_value
            hydrated.pop("password", None)
            runtime_accounts.append(hydrated)
        return runtime_accounts

    def _build_runtime_settings(self, persisted_settings):
        runtime = self._default_settings()
        runtime.update(copy.deepcopy(persisted_settings if isinstance(persisted_settings, dict) else {}))

        for key in self.SECRET_KEYS:
            runtime[key] = self._read_secret_value(key, runtime.get(key, ""))

        runtime["mail_accounts"] = self._hydrate_mail_accounts(runtime.get("mail_accounts", []))
        return runtime

    def _migrate_plaintext_secrets(self, persisted_settings):
        changed = False

        for key, label in self.SECRET_KEYS.items():
            raw_value = str(persisted_settings.get(key, "") or "").strip()
            if not raw_value:
                continue
            if self._store_secret_value(key, raw_value, label=label):
                persisted_settings.pop(key, None)
                changed = True

        accounts = persisted_settings.get("mail_accounts", [])
        if isinstance(accounts, list):
            for account in accounts:
                if not isinstance(account, dict):
                    continue
                legacy_pwd = str(account.get("pwd") or account.get("password") or "").strip()
                if not legacy_pwd:
                    continue

                secret_ref = str(account.get(self.MAIL_ACCOUNT_SECRET_REF_FIELD, "")).strip() or uuid.uuid4().hex
                account[self.MAIL_ACCOUNT_SECRET_REF_FIELD] = secret_ref
                label = f"E-Mail Passwort ({account.get('name') or account.get('user') or 'Konto'})"
                if self._store_secret_value(self._mail_secret_key(secret_ref), legacy_pwd, label=label):
                    account.pop("pwd", None)
                    account.pop("password", None)
                    changed = True

        return changed

    def load_settings(self):
        persisted_settings = self._default_settings()
        persisted_settings.update(self._read_plain_settings_file())

        changed = self._migrate_plaintext_secrets(persisted_settings)
        sanitized_persisted = self._sanitize_plain_settings(persisted_settings)
        runtime_settings = self._build_runtime_settings(sanitized_persisted)

        if changed:
            try:
                self._write_plain_settings_file(sanitized_persisted)
            except Exception as exc:
                log_exception(__name__, exc)

        return runtime_settings

    def save_settings(self, settings_dict):
        plain_settings = self._default_settings()
        plain_settings.update(self._read_plain_settings_file())
        updates = copy.deepcopy(settings_dict if isinstance(settings_dict, dict) else {})

        for key, label in self.SECRET_KEYS.items():
            if key in updates:
                self._store_secret_value(key, updates.pop(key), label=label)

        if "mail_accounts" in updates:
            updates["mail_accounts"] = self._prepare_mail_accounts_for_save(updates.get("mail_accounts", []))

        plain_settings.update(updates)
        sanitized = self._sanitize_plain_settings(plain_settings)
        self._write_plain_settings_file(sanitized)
        self.settings = self._build_runtime_settings(sanitized)

    def save_setting(self, key, value):
        self.save_settings({key: value})

    def get(self, key, default=""):
        value = self.settings.get(key, default)
        if isinstance(value, (dict, list)):
            return copy.deepcopy(value)
        return value

    def has_secret(self, key):
        if key not in self.SECRET_KEYS:
            return False
        return bool(self._read_secret_value(key, ""))

    def delete_secret(self, key):
        if key not in self.SECRET_KEYS:
            return
        self._delete_secret_value(key)
        self.settings[key] = ""
        self.save_settings({})





