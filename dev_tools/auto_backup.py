import datetime
import os
from pathlib import Path
import zipfile

from storage_paths import app_root_dir, storage_path


SOURCE_DIR = app_root_dir()
BACKUP_DIR = storage_path("backups", "auto_backups")
MAX_BACKUPS = 3
EXCLUDE_DIRS = {".git", "__pycache__", "backups", "refactor_backups", ".codex", ".claude", ".venv", "venv"}
EXCLUDE_PREFIXES = ("_backup_",)
EXCLUDE_RELATIVE_DIRS = {
    Path("data") / "debug_logs",
    Path("data") / "crash_logs",
    Path("data") / "media" / "products",
    Path("data") / "media" / "screenshots",
    Path("data") / "media" / "shops",
}
EXCLUDE_FILE_SUFFIXES = {".log", ".jsonl", ".pyc"}


def _should_skip_dir(source_dir: Path, root: str, directory_name: str) -> bool:
    if directory_name in EXCLUDE_DIRS or directory_name.startswith(EXCLUDE_PREFIXES):
        return True

    relative_dir = (Path(root) / directory_name).relative_to(source_dir)
    return relative_dir in EXCLUDE_RELATIVE_DIRS


def _should_skip_file(source_dir: Path, file_path: Path) -> bool:
    if file_path.suffix.lower() in EXCLUDE_FILE_SUFFIXES:
        return True

    relative_file = file_path.relative_to(source_dir)
    return any(relative_file.is_relative_to(excluded_dir) for excluded_dir in EXCLUDE_RELATIVE_DIRS)


def _cleanup_old_backups() -> None:
    try:
        backups = sorted(
            BACKUP_DIR.glob("backup_*.zip"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return

    for stale_backup in backups[MAX_BACKUPS:]:
        try:
            stale_backup.unlink()
        except OSError:
            pass

def create_backup():
    source_dir = SOURCE_DIR
    backup_dir = BACKUP_DIR

    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = backup_dir / f"backup_{timestamp}.zip"

    try:
        with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(source_dir):
                dirs[:] = [directory for directory in dirs if not _should_skip_dir(source_dir, root, directory)]

                for file in files:
                    file_path = Path(root) / file
                    if file_path == zip_filename:
                        continue

                    if _should_skip_file(source_dir, file_path):
                        continue

                    arcname = file_path.relative_to(source_dir)
                    zipf.write(file_path, arcname)

        _cleanup_old_backups()
        print(f"Backup erfolgreich erstellt: {zip_filename}")
    except Exception as e:
        print(f"Fehler beim Erstellen des Backups: {e}")

if __name__ == "__main__":
    create_backup()
