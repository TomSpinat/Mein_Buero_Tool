"""
Zentrales Crash-Logging fuer das gesamte Projekt.

- schreibt Python-Exceptions in data/crash_logs/crash_YYYY-MM-DD.log
- installiert globale Hooks fuer unbehandelte Fehler
- aktiviert optional faulthandler fuer harte/native Crashes
"""

import datetime
import faulthandler
import os
import sys
import threading
import traceback

from module.secret_store import sanitize_text


_FAULTHANDLER_FILE = None
_HOOKS_INSTALLED = False


def _project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


def _log_dir():
    path = os.path.join(_project_root(), "data", "crash_logs")
    os.makedirs(path, exist_ok=True)
    return path


def _log_path(prefix="crash"):
    day = datetime.datetime.now().strftime("%Y-%m-%d")
    return os.path.join(_log_dir(), f"{prefix}_{day}.log")


def _safe_text(value):
    try:
        return sanitize_text(str(value))
    except Exception:
        return "<unprintable>"


def _write_block(title, lines, prefix="crash"):
    path = _log_path(prefix=prefix)
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open(path, "a", encoding="utf-8") as file_handle:
        file_handle.write("\n" + "=" * 88 + "\n")
        file_handle.write(f"[{ts}] {title}\n")
        file_handle.write("-" * 88 + "\n")
        for line in lines:
            safe_line = _safe_text(line)
            file_handle.write(safe_line)
            if not safe_line.endswith("\n"):
                file_handle.write("\n")


def log_exception(context, exc=None, extra=None):
    """
    Kontext + Exception + Traceback in Crash-Log schreiben.
    Kann direkt in except-Bloecken aufgerufen werden.
    """
    try:
        if exc is None:
            formatted = traceback.format_exc()
            exc_text = "<no-explicit-exception>"
        else:
            formatted = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            exc_text = _safe_text(exc)

        lines = [
            f"context: {context}",
            f"thread: {threading.current_thread().name}",
            f"exception: {exc_text}",
            "traceback:",
            _safe_text(formatted),
        ]

        if extra is not None:
            lines.append("extra:")
            lines.append(_safe_text(extra))

        _write_block("PYTHON_EXCEPTION", lines, prefix="crash")
    except Exception:
        pass


def log_message(context, message, extra=None):
    try:
        lines = [
            f"context: {context}",
            f"thread: {threading.current_thread().name}",
            f"message: {_safe_text(message)}",
        ]
        if extra is not None:
            lines.append("extra:")
            lines.append(_safe_text(extra))
        _write_block("INFO", lines, prefix="crash")
    except Exception:
        pass


def install_global_exception_hooks():
    """
    Einmalig globale Hooks aktivieren:
    - sys.excepthook
    - threading.excepthook
    - faulthandler fuer native/harte Abstuerze
    """
    global _HOOKS_INSTALLED
    global _FAULTHANDLER_FILE

    if _HOOKS_INSTALLED:
        return
    _HOOKS_INSTALLED = True

    old_sys_hook = sys.excepthook

    def _sys_hook(exc_type, exc_value, exc_tb):
        try:
            tb = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
            _write_block(
                "UNHANDLED_SYS_EXCEPTION",
                [
                    f"thread: {threading.current_thread().name}",
                    f"exception: {_safe_text(exc_value)}",
                    "traceback:",
                    _safe_text(tb),
                ],
                prefix="crash",
            )
        except Exception:
            pass
        old_sys_hook(exc_type, exc_value, exc_tb)

    sys.excepthook = _sys_hook

    if hasattr(threading, "excepthook"):
        old_thread_hook = threading.excepthook

        def _thread_hook(args):
            try:
                tb = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
                _write_block(
                    "UNHANDLED_THREAD_EXCEPTION",
                    [
                        f"thread: {getattr(args.thread, 'name', '?')}",
                        f"exception: {_safe_text(args.exc_value)}",
                        "traceback:",
                        _safe_text(tb),
                    ],
                    prefix="crash",
                )
            except Exception:
                pass
            old_thread_hook(args)

        threading.excepthook = _thread_hook

    try:
        fatal_path = _log_path(prefix="fatal_native")
        _FAULTHANDLER_FILE = open(fatal_path, "a", encoding="utf-8")
        faulthandler.enable(_FAULTHANDLER_FILE, all_threads=True)
        log_message("crash_logger.install_global_exception_hooks", "faulthandler enabled", extra=fatal_path)
    except Exception as exc:
        log_exception("crash_logger.install_global_exception_hooks", exc)
