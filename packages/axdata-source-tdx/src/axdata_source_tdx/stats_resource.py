"""TDX local statistics resource parser and validated cache lifecycle."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any

from .stats_cache import (
    DEFAULT_REFRESH_RETRY_SECONDS,
    DEFAULT_TDX_STATS_META_PATH,
    DEFAULT_TDX_STATS_RESOURCE_PATH,
    SHANGHAI_TZ,
    build_stats_metadata as _cache_build_stats_metadata,
    default_tdx_stats_cache_root,
    default_tdx_stats_metadata_path,
    default_tdx_stats_resource_path,
    metadata_path_for_source as _cache_metadata_path_for_source,
    parse_datetime as _cache_parse_datetime,
    read_stats_metadata as _cache_read_stats_metadata,
    record_refresh_failure as _cache_record_refresh_failure,
    refresh_retry_remaining_seconds as _cache_refresh_retry_remaining_seconds,
    resolve_stats_source as _cache_resolve_stats_source,
    stats_cache_root as _cache_stats_cache_root,
    stats_cache_should_refresh as _cache_stats_cache_should_refresh,
    stats_refresh_lock,
    write_bytes_atomic as _cache_write_bytes_atomic,
    write_stats_metadata as _cache_write_stats_metadata,
    write_text_atomic as _cache_write_text_atomic,
)
from .stats_errors import (
    TdxStatsDateError,
    TdxStatsDownloadError,
    TdxStatsError,
    TdxStatsRefreshDeferredError,
    TdxStatsValidationError,
)
from .stats_models import (
    TdxStat2Row,
    TdxStatRow,
    TdxStatsResource,
    decode_lines as _decode_lines,
    float_value as _float_value,
    int_value as _int_value,
    parse_stat2_rows as _parse_stat2_rows,
    parse_stat_rows as _parse_stat_rows,
    stats_resource_from_lines,
    text_value as _text_value,
)
from .stats_validation import (
    StatsValidationReport,
    stats_resource_from_text_lines,
    stats_resource_from_zip_payload,
    validate_stats_resource,
)


DEFAULT_TDX_STATS_CHUNK_SIZE = 30000
MAX_MEMORY_CACHED_RESOURCES = 32

_RESOURCE_CACHE: dict[Path, tuple[tuple[tuple[str, int, int], ...], TdxStatsResource]] = {}
_RESOURCE_CACHE_LOCK = RLock()


def load_tdx_stats_resource(root: str | Path | None = None) -> TdxStatsResource:
    """Load ``tdxstat.cfg`` and ``tdxstat2.cfg`` from a directory or ZIP.

    Explicit resources may be intentionally small fixtures. Full-size validation
    is applied by the automatic download/cache path instead.
    """

    source = _resolve_stats_source(root).resolve()
    fingerprint = _source_fingerprint(source)
    with _RESOURCE_CACHE_LOCK:
        cached = _RESOURCE_CACHE.get(source)
        if cached is not None and cached[0] == fingerprint:
            return cached[1]

    metadata = _read_stats_metadata(source)
    if source.is_file() and source.suffix.lower() == ".zip":
        resource, _ = stats_resource_from_zip_payload(
            source.read_bytes(),
            source_path=str(source),
            metadata=metadata,
            require_full=False,
        )
    else:
        stat_lines = (source / "tdxstat.cfg").read_text(
            encoding="gbk", errors="ignore"
        ).splitlines()
        stat2_lines = (source / "tdxstat2.cfg").read_text(
            encoding="gbk", errors="ignore"
        ).splitlines()
        resource, _ = stats_resource_from_text_lines(
            stat_lines,
            stat2_lines,
            source_path=str(source),
            metadata=metadata,
            require_full=False,
        )
    _cache_loaded_resource(source, resource)
    return resource


def _resolve_stats_source(root: str | Path | None) -> Path:
    return _cache_resolve_stats_source(root)


def refresh_tdx_stats_resource(
    client: Any,
    *,
    cache_root: str | Path | None = None,
    source_path: str = DEFAULT_TDX_STATS_RESOURCE_PATH,
    chunk_size: int = DEFAULT_TDX_STATS_CHUNK_SIZE,
    target_trade_date: Any = None,
    previous_trade_date: Any = None,
) -> TdxStatsResource:
    """Force a validated refresh while preserving any existing cache on failure."""

    root = _cache_stats_cache_root(cache_root)
    root.mkdir(parents=True, exist_ok=True)
    target = root / Path(source_path).name
    try:
        with stats_refresh_lock(root):
            return _refresh_tdx_stats_resource_unlocked(
                client,
                target=target,
                source_path=source_path,
                chunk_size=chunk_size,
                target_trade_date=target_trade_date,
                previous_trade_date=previous_trade_date,
            )
    except Exception as exc:
        error = _as_stats_refresh_error(exc, source_path=source_path)
        _record_refresh_failure_safely(target, error)
        if error is exc:
            raise
        raise error from exc


def request_tdx_stats_resource(
    client: Any,
    *,
    source_path: str = DEFAULT_TDX_STATS_RESOURCE_PATH,
    chunk_size: int = DEFAULT_TDX_STATS_CHUNK_SIZE,
    target_trade_date: Any = None,
    previous_trade_date: Any = None,
) -> TdxStatsResource:
    """Download and validate the TDX statistics resource without writing cache."""

    payload = _download_stats_payload(client, source_path, chunk_size=chunk_size)
    resource, report = _validated_download_resource(payload, source_path=source_path)
    _require_usable_stats_date(
        resource,
        target_trade_date=target_trade_date,
        previous_trade_date=previous_trade_date,
    )
    metadata = _request_stats_metadata(
        resource,
        report=report,
        payload=payload,
        source_path=source_path,
        target_trade_date=target_trade_date,
        previous_trade_date=previous_trade_date,
    )
    return TdxStatsResource(
        stat=resource.stat,
        stat2=resource.stat2,
        source_path=f"tdx://{source_path}",
        metadata=metadata,
    )


def ensure_tdx_stats_resource(
    client: Any,
    *,
    root: str | Path | None = None,
    cache_root: str | Path | None = None,
    refresh: bool = False,
    target_trade_date: Any = None,
    previous_trade_date: Any = None,
) -> tuple[TdxStatsResource, bool]:
    """Return a complete, date-usable resource, refreshing only when required."""

    if root not in (None, ""):
        return load_tdx_stats_resource(root), False

    target_date, previous_date = _expected_market_dates(
        client,
        target_trade_date=target_trade_date,
        previous_trade_date=previous_trade_date,
    )
    cache_dir = _cache_stats_cache_root(cache_root)
    target = cache_dir / DEFAULT_TDX_STATS_RESOURCE_PATH
    existing = _complete_cached_resource(target)
    if not refresh and existing is not None and not _stats_cache_should_refresh(
        existing,
        target_trade_date=target_date,
        previous_trade_date=previous_date,
    ):
        return existing, False
    if not refresh:
        _raise_if_refresh_deferred(target)

    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        with stats_refresh_lock(cache_dir):
            existing = _complete_cached_resource(target)
            if not refresh and existing is not None and not _stats_cache_should_refresh(
                existing,
                target_trade_date=target_date,
                previous_trade_date=previous_date,
            ):
                return existing, False
            if not refresh:
                _raise_if_refresh_deferred(target)
            resource = _refresh_tdx_stats_resource_unlocked(
                client,
                target=target,
                source_path=DEFAULT_TDX_STATS_RESOURCE_PATH,
                chunk_size=DEFAULT_TDX_STATS_CHUNK_SIZE,
                target_trade_date=target_date,
                previous_trade_date=previous_date,
            )
            return resource, True
    except Exception as exc:
        error = _as_stats_refresh_error(exc, source_path=DEFAULT_TDX_STATS_RESOURCE_PATH)
        if not isinstance(error, TdxStatsRefreshDeferredError):
            _record_refresh_failure_safely(target, error)
        if error is exc:
            raise
        raise error from exc


def ensure_tdx_stats_resource_for_params(
    client: Any,
    params: Mapping[str, object] | None,
    *,
    validation_error: type[ValueError] = ValueError,
    target_trade_date: Any = None,
    previous_trade_date: Any = None,
) -> tuple[TdxStatsResource, bool]:
    params = params or {}
    return ensure_tdx_stats_resource(
        client,
        root=params.get("stats_root"),
        cache_root=params.get("stats_cache_root"),
        refresh=_refresh_stats_param(
            params.get("refresh_stats", False),
            validation_error=validation_error,
        ),
        target_trade_date=target_trade_date,
        previous_trade_date=previous_trade_date,
    )


def _refresh_stats_param(
    value: object,
    *,
    validation_error: type[ValueError] = ValueError,
) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "升序"}:
        return True
    if text in {"0", "false", "no", "n", "off", "降序", ""}:
        return False
    raise validation_error("refresh_stats must be a boolean")


def _download_tdx_file(client: Any, path: str, *, chunk_size: int) -> bytes:
    if hasattr(client, "download_file_resource"):
        return client.download_file_resource(path, chunk_size=chunk_size)
    if hasattr(client, "resources") and hasattr(client.resources, "download_file"):
        return client.resources.download_file(path, chunk_size=chunk_size)
    raise RuntimeError("TDX client does not expose 0x06b9 file resource requests.")


def _load_tdx_stats_resource_from_zip_payload(
    payload: bytes,
    *,
    source_path: str,
) -> TdxStatsResource:
    resource, _ = stats_resource_from_zip_payload(
        payload,
        source_path=f"tdx://{source_path}",
        metadata=None,
        require_full=False,
    )
    return resource


def _validate_stats_zip(payload: bytes) -> None:
    _validated_download_resource(payload, source_path=DEFAULT_TDX_STATS_RESOURCE_PATH)


def _stats_cache_should_refresh(
    resource: TdxStatsResource,
    *,
    target_trade_date: Any = None,
    previous_trade_date: Any = None,
) -> bool:
    return _cache_stats_cache_should_refresh(
        resource,
        target_trade_date=target_trade_date,
        previous_trade_date=previous_trade_date,
    )


def _build_stats_metadata(
    resource: TdxStatsResource,
    *,
    payload: bytes,
    source_path: str,
    target: Path,
    target_trade_date: Any = None,
    previous_trade_date: Any = None,
) -> dict[str, object]:
    return _cache_build_stats_metadata(
        resource,
        payload=payload,
        source_path=source_path,
        target=target,
        target_trade_date=target_trade_date,
        previous_trade_date=previous_trade_date,
    )


def _read_stats_metadata(source: Path) -> dict[str, object] | None:
    return _cache_read_stats_metadata(source)


def _write_stats_metadata(path: Path, metadata: dict[str, object]) -> None:
    _cache_write_stats_metadata(path, metadata)


def _metadata_path_for_source(source: Path) -> Path:
    return _cache_metadata_path_for_source(source)


def _write_bytes_atomic(path: Path, payload: bytes) -> None:
    _cache_write_bytes_atomic(path, payload)


def _write_text_atomic(path: Path, text: str) -> None:
    _cache_write_text_atomic(path, text)


def _parse_datetime(value: object) -> datetime | None:
    return _cache_parse_datetime(value)


def _refresh_tdx_stats_resource_unlocked(
    client: Any,
    *,
    target: Path,
    source_path: str,
    chunk_size: int,
    target_trade_date: Any,
    previous_trade_date: Any,
) -> TdxStatsResource:
    payload = _download_stats_payload(client, source_path, chunk_size=chunk_size)
    resource, report = _validated_download_resource(payload, source_path=source_path)
    _require_usable_stats_date(
        resource,
        target_trade_date=target_trade_date,
        previous_trade_date=previous_trade_date,
    )
    metadata = _build_stats_metadata(
        resource,
        payload=payload,
        source_path=source_path,
        target=target,
        target_trade_date=target_trade_date,
        previous_trade_date=previous_trade_date,
    )
    metadata.update(_validation_report_metadata(report))

    _write_bytes_atomic(target, payload)
    _write_stats_metadata(_metadata_path_for_source(target), metadata)
    cached_resource = TdxStatsResource(
        stat=resource.stat,
        stat2=resource.stat2,
        source_path=str(target),
        metadata=metadata,
    )
    _cache_loaded_resource(target.resolve(), cached_resource)
    return cached_resource


def _download_stats_payload(client: Any, source_path: str, *, chunk_size: int) -> bytes:
    try:
        payload = _download_tdx_file(client, source_path, chunk_size=chunk_size)
    except Exception as exc:
        raise TdxStatsDownloadError(
            f"Failed to download TDX stats resource {source_path!r}: {exc}",
            source_resource_path=source_path,
        ) from exc
    if not isinstance(payload, bytes):
        raise TdxStatsDownloadError(
            f"TDX stats resource {source_path!r} returned a non-bytes payload.",
            source_resource_path=source_path,
        )
    return payload


def _validated_download_resource(
    payload: bytes,
    *,
    source_path: str,
) -> tuple[TdxStatsResource, StatsValidationReport]:
    return stats_resource_from_zip_payload(
        payload,
        source_path=f"tdx://{source_path}",
        metadata=None,
        require_full=True,
    )


def _complete_cached_resource(target: Path) -> TdxStatsResource | None:
    if not target.exists():
        return None
    try:
        resource = load_tdx_stats_resource(target)
        validate_stats_resource(resource, require_full=True)
    except (FileNotFoundError, OSError, TdxStatsValidationError):
        return None
    return resource


def _expected_market_dates(
    client: Any,
    *,
    target_trade_date: Any,
    previous_trade_date: Any,
) -> tuple[str | None, str | None]:
    target = _optional_text(target_trade_date)
    previous = _optional_text(previous_trade_date)
    if target is not None:
        return target, previous
    try:
        from .market_dates import resolve_market_date_context

        context = resolve_market_date_context(client)
    except Exception:
        return None, None
    return context.target_trade_date, context.previous_trade_date


def _require_usable_stats_date(
    resource: TdxStatsResource,
    *,
    target_trade_date: Any,
    previous_trade_date: Any,
) -> None:
    target = _optional_text(target_trade_date)
    previous = _optional_text(previous_trade_date)
    if target is None or previous is None:
        return
    acceptable = {target, previous}
    if resource.stats_date in acceptable:
        return
    raise TdxStatsDateError(
        "TDX stats server has not published a resource usable for the target session: "
        f"resource stats_date={resource.stats_date or 'unknown'}, "
        f"expected {target} or {previous}. The existing cache was preserved.",
        stats_date=resource.stats_date,
        target_trade_date=target,
        previous_trade_date=previous,
    )


def _raise_if_refresh_deferred(target: Path) -> None:
    remaining = _cache_refresh_retry_remaining_seconds(target)
    if remaining <= 0:
        return
    metadata = _read_stats_metadata(target) or {}
    last_code = str(metadata.get("last_refresh_error_code") or "TDX_STATS_REFRESH_FAILED")
    last_message = str(metadata.get("last_refresh_error_message") or "unknown refresh failure")
    raise TdxStatsRefreshDeferredError(
        "TDX stats refresh is temporarily deferred after a recent failure; "
        f"retry in about {remaining} seconds. Last error [{last_code}]: {last_message}",
        retry_after_seconds=remaining,
        last_error_code=last_code,
        last_error_message=last_message,
        cache_path=str(target),
    )


def _as_stats_refresh_error(exc: Exception, *, source_path: str) -> TdxStatsError:
    if isinstance(exc, TdxStatsError):
        return exc
    return TdxStatsDownloadError(
        f"Failed to refresh TDX stats resource {source_path!r}: {exc}",
        source_resource_path=source_path,
    )


def _record_refresh_failure_safely(target: Path, error: TdxStatsError) -> None:
    try:
        _cache_record_refresh_failure(
            target,
            error,
            retry_seconds=DEFAULT_REFRESH_RETRY_SECONDS,
        )
    except OSError:
        pass


def _request_stats_metadata(
    resource: TdxStatsResource,
    *,
    report: StatsValidationReport,
    payload: bytes,
    source_path: str,
    target_trade_date: Any,
    previous_trade_date: Any,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "validation_version": 1,
        "downloaded_at": datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds"),
        "stats_date": resource.stats_date,
        "stats_date_counts": resource.stats_date_counts,
        "source_resource_path": source_path,
        "cache_path": None,
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
        **_validation_report_metadata(report),
    }
    target = _optional_text(target_trade_date)
    previous = _optional_text(previous_trade_date)
    if target is not None:
        metadata["checked_target_trade_date"] = target
    if previous is not None:
        metadata["checked_previous_trade_date"] = previous
    return metadata


def _validation_report_metadata(report: StatsValidationReport) -> dict[str, object]:
    return {
        "stat_rows": report.stat_rows,
        "stat2_rows": report.stat2_rows,
        "code_overlap_ratio": round(report.code_overlap_ratio, 8),
        "stats_date_coverage": round(report.dominant_date_coverage, 8),
    }


def _source_fingerprint(source: Path) -> tuple[tuple[str, int, int], ...]:
    paths = [source] if source.is_file() else [source / "tdxstat.cfg", source / "tdxstat2.cfg"]
    metadata_path = _metadata_path_for_source(source)
    if metadata_path.exists():
        paths.append(metadata_path)
    fingerprint: list[tuple[str, int, int]] = []
    for path in paths:
        stat = path.stat()
        fingerprint.append((str(path), stat.st_mtime_ns, stat.st_size))
    return tuple(fingerprint)


def _cache_loaded_resource(source: Path, resource: TdxStatsResource) -> None:
    fingerprint = _source_fingerprint(source)
    with _RESOURCE_CACHE_LOCK:
        _RESOURCE_CACHE[source] = (fingerprint, resource)
        while len(_RESOURCE_CACHE) > MAX_MEMORY_CACHED_RESOURCES:
            del _RESOURCE_CACHE[next(iter(_RESOURCE_CACHE))]


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
