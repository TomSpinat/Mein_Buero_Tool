from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from module.media.media_store import LocalMediaStore
from module.database_manager import DatabaseManager
from storage_paths import app_root_dir, storage_root_dir


_LEGACY_STORAGE_PATHS = (
    Path("data") / "debug_logs",
    Path("data") / "crash_logs",
    Path("data") / "media",
    Path("backups") / "auto_backups",
)


def _build_safe_target_path(target_path: Path) -> Path:
    if not target_path.exists():
        return target_path

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = target_path.stem
    suffix = target_path.suffix
    parent = target_path.parent
    counter = 1

    while True:
        candidate = parent / f"{stem}_migrated_{timestamp}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def _remove_empty_dirs(start_dir: Path) -> int:
    removed_count = 0
    if not start_dir.exists():
        return removed_count

    nested_dirs = sorted(
        (path for path in start_dir.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )

    for directory in nested_dirs + [start_dir]:
        try:
            directory.rmdir()
            removed_count += 1
        except OSError:
            pass

    return removed_count


def _move_tree(source_dir: Path, target_dir: Path, result: dict) -> None:
    file_paths = sorted(path for path in source_dir.rglob("*") if path.is_file())
    for source_file in file_paths:
        relative_path = source_file.relative_to(source_dir)
        target_file = _build_safe_target_path(target_dir / relative_path)
        try:
            target_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_file), str(target_file))
            result["moved_files"] += 1
        except Exception as exc:
            result["errors"].append(f"{source_file} -> {target_file}: {exc}")

    result["removed_dirs"] += _remove_empty_dirs(source_dir)


def migrate_legacy_storage_to_external() -> dict:
    source_root = app_root_dir()
    target_root = storage_root_dir()
    result = {
        "source_root": str(source_root),
        "target_root": str(target_root),
        "moved_files": 0,
        "removed_dirs": 0,
        "errors": [],
    }

    if source_root.resolve() == target_root.resolve():
        return result

    for relative_path in _LEGACY_STORAGE_PATHS:
        source_path = source_root / relative_path
        target_path = target_root / relative_path
        if not source_path.exists():
            continue

        if source_path.is_dir():
            _move_tree(source_path, target_path, result)
            continue

        safe_target = _build_safe_target_path(target_path)
        try:
            safe_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_path), str(safe_target))
            result["moved_files"] += 1
        except Exception as exc:
            result["errors"].append(f"{source_path} -> {safe_target}: {exc}")

    return result


def _resolve_media_source_path(file_path: str, source_store: LocalMediaStore, target_store: LocalMediaStore) -> Path | None:
    text = str(file_path or "").strip()
    if not text:
        return None

    candidates = []
    raw_path = Path(text)
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.append(Path(source_store.resolve_path(text)))
        candidates.append(Path(target_store.resolve_path(text)))

    for candidate in candidates:
        try:
            if candidate.exists():
                return candidate
        except OSError:
            continue
    return None


def _media_relative_path_from_absolute(abs_path: Path, source_store: LocalMediaStore, target_store: LocalMediaStore) -> Path | None:
    for store in (source_store, target_store):
        try:
            return abs_path.relative_to(Path(store.media_root))
        except ValueError:
            continue
    return None


def _find_renamed_media_candidate(asset, relative_dir: Path, target_store: LocalMediaStore) -> Path | None:
    asset_id = asset.get("id")
    source_name = asset.get("original_name") or asset.get("media_type") or "asset"
    extension = str(asset.get("file_ext", "") or "").strip()
    media_token = asset.get("sha256") or asset.get("media_key") or f"asset-{asset_id}"
    expected_name = target_store.build_managed_filename(
        source_name,
        str(media_token),
        extension=extension,
        fallback=str(asset.get("media_type") or "asset"),
        token=f"asset-{asset_id}",
    )
    target_dir = Path(target_store.media_root) / relative_dir
    exact_candidate = target_dir / expected_name
    if exact_candidate.exists():
        return exact_candidate

    expected_prefix = exact_candidate.stem.split("_", 1)[0]
    if not expected_prefix:
        return None

    matches = sorted(
        path
        for path in target_dir.glob(f"{expected_prefix}_*")
        if path.is_file() and (not extension or path.suffix.lower() == extension.lower())
    )
    if matches:
        return matches[0]
    return None


def migrate_existing_media_assets(settings_manager) -> dict:
    source_root = app_root_dir()
    target_root = storage_root_dir()
    result = {
        "source_root": str(source_root),
        "target_root": str(target_root),
        "moved_files": 0,
        "renamed_files": 0,
        "errors": [],
    }

    if source_root.resolve() == target_root.resolve():
        return result

    db = DatabaseManager(settings_manager)
    source_store = LocalMediaStore(base_dir=source_root)
    target_store = LocalMediaStore(base_dir=target_root)
    target_store.ensure_structure()

    for asset in db.list_local_media_assets():
        asset_id = asset.get("id")
        file_path = str(asset.get("file_path", "") or "").strip()
        if not file_path:
            continue

        source_abs = _resolve_media_source_path(file_path, source_store, target_store)
        if source_abs is None:
            continue

        relative_media_path = _media_relative_path_from_absolute(source_abs, source_store, target_store)
        if relative_media_path is None:
            continue

        relative_dir = relative_media_path.parent
        source_name = asset.get("original_name") or source_abs.stem or asset.get("media_type") or "asset"
        extension = source_abs.suffix or str(asset.get("file_ext", "") or "").strip()
        media_token = asset.get("sha256") or asset.get("media_key") or f"asset-{asset_id}"
        new_name = target_store.build_managed_filename(
            source_name,
            str(media_token),
            extension=extension,
            fallback=str(asset.get("media_type") or "asset"),
            token=f"asset-{asset_id}",
        )
        target_abs = Path(target_store.media_root) / relative_dir / new_name
        if target_abs.exists() and target_abs.resolve() != source_abs.resolve():
            target_abs = _build_safe_target_path(target_abs)
        elif not target_abs.exists():
            fallback_candidate = _find_renamed_media_candidate(asset, relative_dir, target_store)
            if fallback_candidate is not None:
                target_abs = fallback_candidate

        try:
            target_abs.parent.mkdir(parents=True, exist_ok=True)
            if source_abs.resolve() != target_abs.resolve():
                shutil.move(str(source_abs), str(target_abs))
                result["moved_files"] += 1

            relative_target = target_abs.relative_to(Path(target_store.base_dir))
            if db.update_media_asset_file_path(asset_id, str(relative_target), original_name=str(asset.get("original_name", "") or "")):
                result["renamed_files"] += 1
        except Exception as exc:
            result["errors"].append(f"{source_abs} -> {target_abs}: {exc}")

    return result
