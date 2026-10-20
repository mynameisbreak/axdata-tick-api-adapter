"""Auction-derived fetch helpers for TDX requests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class AuctionIndicatorRowsResult:
    rows: list[dict[str, Any]]
    quote_count: int
    kline_page_count: int
    kline_volume_days: int
    target_trade_date: str | None
    previous_trade_date: str | None
    market_date_source: str
    alignment_counts: dict[str, int]


@dataclass(frozen=True)
class AuctionIndicatorRequestResult:
    rows: list[dict[str, Any]]
    meta: dict[str, Any]


def auction_indicator_request_result(
    client: Any,
    params: Mapping[str, Any],
    *,
    requested_codes: Callable[[Any], Sequence[str]],
    validation_error: type[ValueError],
    rows_func: Callable[..., AuctionIndicatorRowsResult],
    meta_func: Callable[..., dict[str, Any]],
    market_to_id: Mapping[str, int],
    quote_security_from_tdx_code: Callable[[str], tuple[Any, Any]],
    tdx_explicit_quotes: Callable[[Any, Sequence[tuple[Any, Any]]], Any],
    as_list: Callable[[Any], list[Any]],
    get_value: Callable[[Any, str, Any], Any],
    finance_rows_by_tdx_code: Callable[..., tuple[dict[str, dict[str, Any]], int]],
    request_recent_daily_bars: Callable[..., tuple[list[Any], int]],
    normalize_auction_indicator_row: Callable[..., dict[str, Any]],
    emit_source_progress: Callable[..., None],
    progress_callback: Callable[..., None] | None = None,
) -> AuctionIndicatorRequestResult:
    tdx_codes = requested_codes(params.get("code"))
    from .market_dates import resolve_market_date_context

    date_context = resolve_market_date_context(client)
    if not date_context.ready:
        from .stats_errors import TdxAuctionNotReadyError

        raise TdxAuctionNotReadyError(
            "TDX auction snapshot is not ready for the target trade date "
            f"{date_context.target_trade_date or 'unknown'} ({date_context.phase}).",
            target_trade_date=date_context.target_trade_date,
            phase=date_context.phase,
            market_date_source=date_context.source,
        )
    stats, stats_refreshed = _ensure_tdx_stats_resource_for_params(
        client,
        params,
        validation_error=validation_error,
        target_trade_date=date_context.target_trade_date,
        previous_trade_date=date_context.previous_trade_date,
    )
    if date_context.target_trade_date is None:
        date_context = resolve_market_date_context(
            client,
            fallback_trade_date=stats.stats_date,
        )
    emit_source_progress(
        progress_callback,
        30,
        f"已读取盘前统计资源，数据日期 {stats.stats_date}",
        progress_current=0,
        progress_total=len(tdx_codes),
        progress_unit="只",
        eta_ms=None,
    )
    result = rows_func(
        client,
        tdx_codes,
        stats=stats,
        market_to_id=market_to_id,
        quote_security_from_tdx_code=quote_security_from_tdx_code,
        tdx_explicit_quotes=tdx_explicit_quotes,
        as_list=as_list,
        get_value=get_value,
        finance_rows_by_tdx_code=finance_rows_by_tdx_code,
        request_recent_daily_bars=request_recent_daily_bars,
        normalize_auction_indicator_row=normalize_auction_indicator_row,
        date_context=date_context,
    )
    return AuctionIndicatorRequestResult(
        rows=result.rows,
        meta=meta_func(
            result,
            stats=stats,
            stats_refreshed=stats_refreshed,
            requested_code_count=len(tdx_codes),
        ),
    )


def auction_indicator_meta(
    result: AuctionIndicatorRowsResult,
    *,
    stats: Any,
    stats_refreshed: bool,
    requested_code_count: int,
) -> dict[str, Any]:
    return {
        "tdx_protocol": "0x054c+0x052d+0x0010+tdxstat",
        "tdx_stats_source_path": stats.source_path,
        "tdx_stats_refreshed": stats_refreshed,
        "tdx_stats_date": stats.stats_date,
        "tdx_requested_code_count": requested_code_count,
        "tdx_quote_count": result.quote_count,
        "tdx_returned_count": len(result.rows),
        "tdx_kline_page_count": result.kline_page_count,
        "tdx_kline_volume_days": result.kline_volume_days,
        "tdx_target_trade_date": result.target_trade_date,
        "tdx_previous_trade_date": result.previous_trade_date,
        "tdx_market_date_source": result.market_date_source,
        "tdx_stats_alignment_counts": result.alignment_counts,
        "partial": any(key not in {"same_day", "previous_trading_day"} for key in result.alignment_counts),
    }


def auction_indicator_rows(
    client: Any,
    tdx_codes: Sequence[str],
    *,
    stats: Any,
    market_to_id: Mapping[str, int],
    quote_security_from_tdx_code: Callable[[str], tuple[Any, Any]],
    tdx_explicit_quotes: Callable[[Any, Sequence[tuple[Any, Any]]], Any],
    as_list: Callable[[Any], list[Any]],
    get_value: Callable[[Any, str, Any], Any],
    finance_rows_by_tdx_code: Callable[..., tuple[dict[str, dict[str, Any]], int]],
    request_recent_daily_bars: Callable[..., tuple[list[Any], int]],
    normalize_auction_indicator_row: Callable[..., dict[str, Any]],
    date_context: Any,
) -> AuctionIndicatorRowsResult:
    securities = [quote_security_from_tdx_code(tdx_code) for tdx_code in tdx_codes]
    quotes = as_list(tdx_explicit_quotes(client, securities))
    quote_by_tdx_code = {
        str(get_value(quote, "full_code", "") or "").lower(): quote
        for quote in quotes
    }
    finance_by_tdx_code, _finance_batch_count = finance_rows_by_tdx_code(client, tdx_codes)

    rows: list[dict[str, Any]] = []
    kline_page_count = 0
    kline_volume_days = 0
    alignment_counts: dict[str, int] = {}
    for tdx_code in tdx_codes:
        quote = quote_by_tdx_code.get(tdx_code)
        if quote is None:
            continue
        stat_row, stat2_row = stats.row(market_to_id.get(tdx_code[:2], 0), tdx_code[2:])
        target_trade_date = date_context.target_trade_date or get_value(
            stat2_row,
            "stats_date",
            get_value(stat_row, "stats_date", None),
        )
        recent_daily_bars, page_count = request_recent_daily_bars(
            client,
            tdx_code,
            count=8,
            target_trade_date=target_trade_date,
            require_trading_activity=True,
        )
        kline_page_count += page_count
        kline_volume_days += min(5, len(recent_daily_bars))
        row = normalize_auction_indicator_row(
            quote,
            stat_row=stat_row,
            stat2_row=stat2_row,
            target_trade_date=target_trade_date,
            previous_trade_date=_bar_trade_date(recent_daily_bars[0]) if recent_daily_bars else None,
            recent_daily_bars=recent_daily_bars,
            finance_row=finance_by_tdx_code.get(tdx_code),
        )
        alignment_status = str(row.pop("_indicator_status", "unknown"))
        alignment_counts[alignment_status] = alignment_counts.get(alignment_status, 0) + 1
        rows.append(row)

    rows.sort(key=lambda row: str(row.get("instrument_id") or ""))
    return AuctionIndicatorRowsResult(
        rows=rows,
        quote_count=len(quotes),
        kline_page_count=kline_page_count,
        kline_volume_days=kline_volume_days,
        target_trade_date=date_context.target_trade_date,
        previous_trade_date=date_context.previous_trade_date,
        market_date_source=date_context.source,
        alignment_counts=alignment_counts,
    )


def _bar_trade_date(bar: Any) -> str | None:
    from .normalize_utils import bar_trade_date

    return bar_trade_date(bar)


def _ensure_tdx_stats_resource_for_params(*args: Any, **kwargs: Any) -> tuple[Any, bool]:
    from .stats_resource import ensure_tdx_stats_resource_for_params

    return ensure_tdx_stats_resource_for_params(*args, **kwargs)
