"""TDX statistics resource cache paths and metadata helpers."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import time as time_module
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Iterator
from uuid import uuid4
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from .stats_models import TdxStatsResource


DEFAULT_TDX_STATS_RESOURCE_PATH = "zhb.zip"
DEFAULT_TDX_STATS_META_PATH = "zhb.meta.json"
DEFAULT_TDX_STATS_LOCK_PATH = "zhb.refresh.lock"
DEFAULT_REFRESH_RETRY_SECONDS = 300
DEFAULT_REFRESH_LOCK_TIMEOUT_SECONDS = 30.0
DEFAULT_REFRESH_LOCK_STALE_SECONDS = 120.0
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def default_tdx_stats_cache_root() -> Path:
    raw = os.getenv("AXDATA_TDX_STATS_ROOT", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return user_tdx_stats_cache_root()


def user_tdx_stats_cache_root() -> Path:
    if os.name == "nt":
        base = Path(os.getenv("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
        return (base / "AxData" / "cache" / "tdx" / "stats").resolve()
    if sys.platform == "darwin":
        return (Path.home() / "Library" / "Caches" / "AxData" / "tdx" / "stats").resolve()
    base = Path(os.getenv("XDG_CACHE_HOME") or (Path.home() / ".cache"))
    return (base / "axdata" / "tdx" / "stats").resolve()


def default_tdx_stats_resource_path() -> Path:
    return default_tdx_stats_cache_root() / DEFAULT_TDX_STATS_RESOURCE_PATH


def default_tdx_stats_metadata_path() -> Path:
    return default_tdx_stats_cache_root() / DEFAULT_TDX_STATS_META_PATH


def resolve_stats_source(root: str | Path | None) -> Path:
    if root not in (None, ""):
        source = Path(str(root)).expanduser()
        if source.is_file():
            return source
        if (source / "tdxstat.cfg").exists() and (source / "tdxstat2.cfg").exists():
            return source
        if (source / DEFAULT_TDX_STATS_RESOURCE_PATH).exists():
            return source / DEFAULT_TDX_STATS_RESOURCE_PATH
        raise FileNotFoundError(f"TDX stats resource not found under {source}")

    source = default_tdx_stats_resource_path()
    if source.exists():
        return source
    raise FileNotFoundError(
        "TDX stats resource not found in AxData cache. "
        "Refresh stats resource from TDX source first."
    )


def stats_cache_root(cache_root: str | Path | None) -> Path:
    if cache_root in (None, ""):
        return default_tdx_stats_cache_root()
    return Path(str(cache_root)).expanduser().resolve()


def stats_cache_should_refresh(
    resource: TdxStatsResource,
    *,
    target_trade_date: object = None,
    previous_trade_date: object = None,
) -> bool:
    target = _optional_text(target_trade_date)
    if target is None:
        return False
    acceptable_dates = {target}
    previous = _optional_text(previous_trade_date)
    if previous is not None:
        acceptable_dates.add(previous)
    if resource.stats_date in acceptable_dates:
        return False

    metadata = resource.metadata or {}
    return not (
        previous is None
        and _optional_text(metadata.get("checked_target_trade_date")) == target
    )


def build_stats_metadata(
    resource: TdxStatsResource,
    *,
    payload: bytes,
    source_path: str,
    target: Path,
    target_trade_date: object = None,
    previous_trade_date: object = None,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "validation_version": 1,
        "downloaded_at": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds"),
        "stats_date": resource.stats_date,
        "stats_date_coverage": round(resource.stats_date_coverage, 8),
        "stats_date_counts": resource.stats_date_counts,
        "source_resource_path": source_path,
        "cache_path": str(target),
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "stat_rows": len(resource.stat),
        "stat2_rows": len(resource.stat2),
    }
    target_date = _optional_text(target_trade_date)
    previous = _optional_text(previous_trade_date)
    if target_date is not None:
        metadata["checked_target_trade_date"] = target_date
    if previous is not None:
        metadata["checked_previous_trade_date"] = previous
    return metadata


def read_stats_metadata(source: Path) -> dict[str, object] | None:
    meta_path = metadata_path_for_source(source)
    if not meta_path.exists():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def write_stats_metadata(path: Path, metadata: dict[str, object]) -> None:
    write_text_atomic(path, json.dumps(metadata, ensure_ascii=False, indent=2))


def metadata_path_for_source(source: Path) -> Path:
    if source.suffix.lower() == ".zip" or source.is_file():
        return source.with_name(DEFAULT_TDX_STATS_META_PATH)
    return source / DEFAULT_TDX_STATS_META_PATH


def refresh_retry_remaining_seconds(
    source: Path,
    *,
    now: datetime | None = None,
) -> int:
    metadata = read_stats_metadata(source) or {}
    failed_at = parse_datetime(metadata.get("last_refresh_failed_at"))
    if failed_at is None:
        return 0
    try:
        retry_seconds = max(0, int(metadata.get("refresh_retry_seconds", 0)))
    except (TypeError, ValueError):
        return 0
    now_value = now or datetime.now(SHANGHAI_TZ)
    elapsed = (
        now_value.astimezone(SHANGHAI_TZ) - failed_at.astimezone(SHANGHAI_TZ)
    ).total_seconds()
    return min(retry_seconds, max(0, int(retry_seconds - elapsed + 0.999)))


def record_refresh_failure(
    source: Path,
    error: BaseException,
    *,
    retry_seconds: int = DEFAULT_REFRESH_RETRY_SECONDS,
) -> None:
    metadata = dict(read_stats_metadata(source) or {})
    metadata.update(
        {
            "last_refresh_failed_at": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds"),
            "last_refresh_error_code": str(getattr(error, "code", "TDX_STATS_REFRESH_FAILED")),
            "last_refresh_error_message": str(error),
            "refresh_retry_seconds": max(0, int(retry_seconds)),
        }
    )
    write_stats_metadata(metadata_path_for_source(source), metadata)


@contextmanager
def stats_refresh_lock(
    root: Path,
    *,
    timeout_seconds: float = DEFAULT_REFRESH_LOCK_TIMEOUT_SECONDS,
    stale_seconds: float = DEFAULT_REFRESH_LOCK_STALE_SECONDS,
) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / DEFAULT_TDX_STATS_LOCK_PATH
    token = f"{os.getpid()}:{uuid4().hex}"
    deadline = time_module.monotonic() + max(0.0, float(timeout_seconds))

    while True:
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                age_seconds = time_module.time() - lock_path.stat().st_mtime
                if age_seconds > max(1.0, float(stale_seconds)):
                    lock_path.unlink()
                    continue
            except FileNotFoundError:
                continue
            if time_module.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for TDX stats refresh lock: {lock_path}")
            time_module.sleep(0.1)
            continue
        try:
            with os.fdopen(descriptor, "w", encoding="ascii") as handle:
                handle.write(token)
        except BaseException:
            try:
                lock_path.unlink()
            except OSError:
                pass
            raise
        break

    try:
        yield
    finally:
        try:
            if lock_path.read_text(encoding="ascii") == token:
                lock_path.unlink()
        except OSError:
            pass


def write_bytes_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
        os.replace(tmp_path, path)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, path)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def parse_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
