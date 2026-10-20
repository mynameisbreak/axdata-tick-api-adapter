"""Validation for downloaded TDX ``zhb.zip`` resources."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Iterable, TypeVar
from zipfile import BadZipFile, ZipFile

from .stats_errors import TdxStatsValidationError
from .stats_models import (
    TdxStat2Row,
    TdxStatRow,
    TdxStatsResource,
    decode_lines,
    parse_stat2_rows,
    parse_stat_rows,
)


MIN_FULL_STATS_ROWS = 1000
MIN_CODE_OVERLAP_RATIO = 0.95
MIN_DOMINANT_DATE_COVERAGE = 0.95
MAX_STATS_ZIP_BYTES = 32 * 1024 * 1024
MAX_STATS_ENTRY_BYTES = 32 * 1024 * 1024
MAX_STATS_UNCOMPRESSED_BYTES = 64 * 1024 * 1024
MAX_STATS_ARCHIVE_ENTRIES = 128
REQUIRED_STATS_MEMBERS = ("tdxstat.cfg", "tdxstat2.cfg")

RowT = TypeVar("RowT", TdxStatRow, TdxStat2Row)


@dataclass(frozen=True, slots=True)
class StatsValidationReport:
    stat_rows: int
    stat2_rows: int
    code_overlap_ratio: float
    dominant_stats_date: str | None
    dominant_date_coverage: float


def stats_resource_from_zip_payload(
    payload: bytes,
    *,
    source_path: str,
    metadata: dict[str, object] | None = None,
    require_full: bool,
) -> tuple[TdxStatsResource, StatsValidationReport]:
    stat_payload, stat2_payload = _read_stats_members(payload)
    return stats_resource_from_text_lines(
        decode_lines(stat_payload),
        decode_lines(stat2_payload),
        source_path=source_path,
        metadata=metadata,
        require_full=require_full,
    )


def stats_resource_from_text_lines(
    stat_lines: Iterable[str],
    stat2_lines: Iterable[str],
    *,
    source_path: str,
    metadata: dict[str, object] | None = None,
    require_full: bool,
) -> tuple[TdxStatsResource, StatsValidationReport]:
    stat_rows = list(parse_stat_rows(stat_lines))
    stat2_rows = list(parse_stat2_rows(stat2_lines))
    _require_unique_keys(stat_rows, member="tdxstat.cfg")
    _require_unique_keys(stat2_rows, member="tdxstat2.cfg")
    resource = TdxStatsResource(
        stat={row.key: row for row in stat_rows},
        stat2={row.key: row for row in stat2_rows},
        source_path=source_path,
        metadata=metadata,
    )
    report = validate_stats_resource(resource, require_full=require_full)
    return resource, report


def validate_stats_resource(
    resource: TdxStatsResource,
    *,
    require_full: bool,
) -> StatsValidationReport:
    stat_count = len(resource.stat)
    stat2_count = len(resource.stat2)
    stat_keys = set(resource.stat)
    stat2_keys = set(resource.stat2)
    overlap_denominator = max(stat_count, stat2_count, 1)
    overlap_ratio = len(stat_keys & stat2_keys) / overlap_denominator
    dominant_date = resource.stats_date
    dominant_coverage = resource.stats_date_coverage

    report = StatsValidationReport(
        stat_rows=stat_count,
        stat2_rows=stat2_count,
        code_overlap_ratio=overlap_ratio,
        dominant_stats_date=dominant_date,
        dominant_date_coverage=dominant_coverage,
    )
    if not require_full:
        return report

    if stat_count < MIN_FULL_STATS_ROWS or stat2_count < MIN_FULL_STATS_ROWS:
        raise TdxStatsValidationError(
            "TDX stats resource is incomplete: expected at least "
            f"{MIN_FULL_STATS_ROWS} rows in each CFG, got {stat_count} and {stat2_count}.",
            stat_rows=stat_count,
            stat2_rows=stat2_count,
            minimum_rows=MIN_FULL_STATS_ROWS,
        )
    if overlap_ratio < MIN_CODE_OVERLAP_RATIO:
        raise TdxStatsValidationError(
            "TDX stats resource code coverage is inconsistent: "
            f"overlap={overlap_ratio:.2%}, required={MIN_CODE_OVERLAP_RATIO:.0%}.",
            code_overlap_ratio=overlap_ratio,
            minimum_code_overlap_ratio=MIN_CODE_OVERLAP_RATIO,
        )

    stat_date, stat_coverage = _dominant_date_and_coverage(resource.stat.values())
    stat2_date, stat2_coverage = _dominant_date_and_coverage(resource.stat2.values())
    if stat_date is None or stat2_date is None:
        raise TdxStatsValidationError(
            "TDX stats resource does not contain a dominant stats_date in both CFG files."
        )
    if stat_date != stat2_date:
        raise TdxStatsValidationError(
            "TDX stats resource CFG dates disagree: "
            f"tdxstat.cfg={stat_date}, tdxstat2.cfg={stat2_date}.",
            stat_date=stat_date,
            stat2_date=stat2_date,
        )
    _validate_stats_date(stat_date)
    if (
        stat_coverage < MIN_DOMINANT_DATE_COVERAGE
        or stat2_coverage < MIN_DOMINANT_DATE_COVERAGE
    ):
        raise TdxStatsValidationError(
            "TDX stats resource dominant-date coverage is too low: "
            f"tdxstat.cfg={stat_coverage:.2%}, tdxstat2.cfg={stat2_coverage:.2%}, "
            f"required={MIN_DOMINANT_DATE_COVERAGE:.0%}.",
            stat_date_coverage=stat_coverage,
            stat2_date_coverage=stat2_coverage,
            minimum_date_coverage=MIN_DOMINANT_DATE_COVERAGE,
        )
    return report


def _read_stats_members(payload: bytes) -> tuple[bytes, bytes]:
    if not isinstance(payload, bytes) or not payload:
        raise TdxStatsValidationError("TDX stats resource download returned an empty payload.")
    if len(payload) > MAX_STATS_ZIP_BYTES:
        raise TdxStatsValidationError(
            f"TDX stats resource ZIP exceeds {MAX_STATS_ZIP_BYTES} bytes.",
            size_bytes=len(payload),
            maximum_size_bytes=MAX_STATS_ZIP_BYTES,
        )
    try:
        with ZipFile(BytesIO(payload)) as archive:
            infos = archive.infolist()
            if len(infos) > MAX_STATS_ARCHIVE_ENTRIES:
                raise TdxStatsValidationError(
                    f"TDX stats resource ZIP contains too many entries: {len(infos)}.",
                    archive_entries=len(infos),
                    maximum_archive_entries=MAX_STATS_ARCHIVE_ENTRIES,
                )
            names = [info.filename for info in infos]
            missing = set(REQUIRED_STATS_MEMBERS) - set(names)
            if missing:
                raise TdxStatsValidationError(
                    "TDX stats resource is missing files: " + ", ".join(sorted(missing)),
                    missing_members=sorted(missing),
                )
            duplicated = [name for name in REQUIRED_STATS_MEMBERS if names.count(name) != 1]
            if duplicated:
                raise TdxStatsValidationError(
                    "TDX stats resource contains duplicate CFG entries: "
                    + ", ".join(duplicated),
                    duplicate_members=duplicated,
                )
            total_size = 0
            for info in infos:
                if info.flag_bits & 0x1:
                    raise TdxStatsValidationError(
                        f"TDX stats resource contains an encrypted entry: {info.filename}."
                    )
                if info.file_size > MAX_STATS_ENTRY_BYTES:
                    raise TdxStatsValidationError(
                        f"TDX stats resource entry is too large: {info.filename}.",
                        member=info.filename,
                        size_bytes=info.file_size,
                        maximum_size_bytes=MAX_STATS_ENTRY_BYTES,
                    )
                total_size += info.file_size
            if total_size > MAX_STATS_UNCOMPRESSED_BYTES:
                raise TdxStatsValidationError(
                    "TDX stats resource uncompressed content is too large.",
                    uncompressed_size_bytes=total_size,
                    maximum_uncompressed_size_bytes=MAX_STATS_UNCOMPRESSED_BYTES,
                )
            return archive.read(REQUIRED_STATS_MEMBERS[0]), archive.read(REQUIRED_STATS_MEMBERS[1])
    except TdxStatsValidationError:
        raise
    except (BadZipFile, OSError, RuntimeError, ValueError) as exc:
        raise TdxStatsValidationError(f"TDX stats resource is not a valid ZIP: {exc}") from exc


def _require_unique_keys(rows: list[RowT], *, member: str) -> None:
    counts = Counter(row.key for row in rows)
    duplicates = [key for key, count in counts.items() if count > 1]
    if duplicates:
        sample = ", ".join(f"{market}:{code}" for market, code in duplicates[:5])
        raise TdxStatsValidationError(
            f"TDX stats resource {member} contains duplicate security keys: {sample}.",
            member=member,
            duplicate_count=len(duplicates),
        )


def _dominant_date_and_coverage(rows: Iterable[RowT]) -> tuple[str | None, float]:
    materialized = list(rows)
    counts = Counter(row.stats_date for row in materialized if row.stats_date)
    if not counts:
        return None, 0.0
    dominant = max(counts, key=lambda value: (counts[value], value))
    return dominant, counts[dominant] / max(1, len(materialized))


def _validate_stats_date(value: str) -> None:
    try:
        parsed = datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise TdxStatsValidationError(
            f"TDX stats resource contains an invalid dominant stats_date: {value!r}."
        ) from exc
    if parsed.strftime("%Y%m%d") != value:
        raise TdxStatsValidationError(
            f"TDX stats resource contains an invalid dominant stats_date: {value!r}."
        )
