"""
secret_store.py
Kleine Abstraktion fuer geheime Werte ueber den Windows Credential Manager.

- keine Klartext-Persistenz in settings.json
- sicherer Fallback: wenn kein System-Store verfuegbar ist, werden Secrets nicht dauerhaft gespeichert
- kleine Hilfsfunktion zum Redigieren sensibler Texte fuer Logs/Dialogs
"""

import ctypes
from ctypes import wintypes
import re
import sys


_CRED_TYPE_GENERIC = 1
_CRED_PERSIST_LOCAL_MACHINE = 2
_ERROR_NOT_FOUND = 1168


_SECRET_PATTERNS = [
    re.compile(r'(?i)(authorization\s*:\s*bearer\s+)([^\s,;]+)'),
    re.compile(r'(?i)((?:api[_ -]?key|token|secret|password|passwd|pwd)\s*[:=]\s*)([^\s,;]+)'),
    re.compile(r'(?i)((?:"(?:gemini_api_key|upcitemdb_api_key|db_pass|email_password|imap_pass|pwd|password|token|secret)"\s*:\s*")([^"]*)("))'),
]


def sanitize_text(value):
    """Redigiert typische Secret-Muster in Texten fuer Logs und Dialoge."""
    try:
        text = str(value)
    except Exception:
        return "<unprintable>"

    sanitized = text
    for pattern in _SECRET_PATTERNS:
        sanitized = pattern.sub(_mask_match, sanitized)
    return sanitized


def _mask_match(match):
    if match.lastindex == 3:
        return f"{match.group(1)}[REDACTED]{match.group(3)}"
    return f"{match.group(1)}[REDACTED]"


class SecretManager:
    """Schreibt und liest Secrets ueber den Windows Credential Manager."""

    def __init__(self, app_name="MeinBueroTool"):
        self.app_name = str(app_name or "MeinBueroTool").strip() or "MeinBueroTool"
        self._available = False
        self._advapi32 = None
        self._credential_type = None
        self._lpbyte_type = None
        self._cred_write = None
        self._cred_read = None
        self._cred_delete = None
        self._cred_free = None
        self._init_backend()

    def _init_backend(self):
        if not sys.platform.startswith("win"):
            return

        try:
            class CREDENTIALW(ctypes.Structure):
                _fields_ = [
                    ("Flags", wintypes.DWORD),
                    ("Type", wintypes.DWORD),
                    ("TargetName", wintypes.LPWSTR),
                    ("Comment", wintypes.LPWSTR),
                    ("LastWritten", wintypes.FILETIME),
                    ("CredentialBlobSize", wintypes.DWORD),
                    ("CredentialBlob", ctypes.POINTER(ctypes.c_ubyte)),
                    ("Persist", wintypes.DWORD),
                    ("AttributeCount", wintypes.DWORD),
                    ("Attributes", ctypes.c_void_p),
                    ("TargetAlias", wintypes.LPWSTR),
                    ("UserName", wintypes.LPWSTR),
                ]

            advapi32 = ctypes.WinDLL("Advapi32.dll", use_last_error=True)
            cred_write = advapi32.CredWriteW
            cred_write.argtypes = [ctypes.POINTER(CREDENTIALW), wintypes.DWORD]
            cred_write.restype = wintypes.BOOL

            cred_read = advapi32.CredReadW
            cred_read.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(ctypes.POINTER(CREDENTIALW))]
            cred_read.restype = wintypes.BOOL

            cred_delete = advapi32.CredDeleteW
            cred_delete.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD]
            cred_delete.restype = wintypes.BOOL

            cred_free = advapi32.CredFree
            cred_free.argtypes = [ctypes.c_void_p]
            cred_free.restype = None

            self._advapi32 = advapi32
            self._credential_type = CREDENTIALW
            self._lpbyte_type = ctypes.POINTER(ctypes.c_ubyte)
            self._cred_write = cred_write
            self._cred_read = cred_read
            self._cred_delete = cred_delete
            self._cred_free = cred_free
            self._available = True
        except Exception:
            self._available = False

    def is_available(self):
        return self._available

    def _target(self, key):
        key_text = str(key or "").strip()
        return f"{self.app_name}:{key_text}"

    def set_secret(self, key, value):
        if not self._available:
            return False

        target = self._target(key)
        secret_text = str(value or "")
        blob = secret_text.encode("utf-16-le")
        buffer = (ctypes.c_ubyte * len(blob))(*blob) if blob else None

        credential = self._credential_type()
        credential.Flags = 0
        credential.Type = _CRED_TYPE_GENERIC
        credential.TargetName = target
        credential.Comment = None
        credential.CredentialBlobSize = len(blob)
        credential.CredentialBlob = ctypes.cast(buffer, self._lpbyte_type) if buffer is not None else None
        credential.Persist = _CRED_PERSIST_LOCAL_MACHINE
        credential.AttributeCount = 0
        credential.Attributes = None
        credential.TargetAlias = None
        credential.UserName = self.app_name

        return bool(self._cred_write(ctypes.byref(credential), 0))

    def get_secret(self, key, default=""):
        if not self._available:
            return default

        target = self._target(key)
        cred_ptr = ctypes.POINTER(self._credential_type)()
        ok = self._cred_read(target, _CRED_TYPE_GENERIC, 0, ctypes.byref(cred_ptr))
        if not ok:
            return default

        try:
            credential = cred_ptr.contents
            if not credential.CredentialBlobSize or not credential.CredentialBlob:
                return ""
            raw = ctypes.string_at(credential.CredentialBlob, credential.CredentialBlobSize)
            return raw.decode("utf-16-le")
        finally:
            self._cred_free(cred_ptr)

    def delete_secret(self, key):
        if not self._available:
            return False

        target = self._target(key)
        ok = self._cred_delete(target, _CRED_TYPE_GENERIC, 0)
        if ok:
            return True

        err = ctypes.get_last_error()
        return err == _ERROR_NOT_FOUND

    def has_secret(self, key):
        return bool(self.get_secret(key, ""))
