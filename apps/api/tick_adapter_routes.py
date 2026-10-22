"""Tick Stock Panel 适配路由。

将 AxData SDK 本地数据包装为 tick-stock-panel 兼容的 HTTP 端点。
接口形态对齐 eltdx-tick-adapter：
- GET  /tick/realtime            全市场实时快照（面板契约要求）
- POST /tick/daily               批量日 K（symbols + start_time/end_time）
- POST /tick/adj_factor          批量复权因子
- POST /tick/financial           批量财务报表（支持 report_type）
- POST /tick/minute              批量分钟 K（分时数据聚合为 1m OHLC）
- POST /tick/full_minute         同 minute
- POST /tick/depth5              批量盘口（快照买一/卖一档）
- GET  单代码查询保留作调试用途

响应统一为 {"data": [...]}；批量接口全部失败时附加 error 汇总。

注意：所有含同步阻塞调用的端点均声明为 def（非 async def），
由 FastAPI 自动放入线程池执行，避免阻塞事件循环。
"""

from __future__ import annotations

import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from threading import Condition
from typing import Any
from zoneinfo import ZoneInfo

from axdata_core.source_errors import SourceRequestValidationError
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tick", tags=["tick-adapter"])

_SNAPSHOT_WORKERS = 16
_MAX_BATCH_SYMBOLS = 100
_INTRADAY_PAGE_SIZE = 60
_MAX_INTRADAY_PAGES = 200

_client: Any = None


def _discover_data_root() -> str | None:
    for candidate in [
        os.environ.get("AXDATA_DATA_DIR"),
        os.environ.get("AXDATA_DATA_ROOT"),
    ]:
        if candidate and Path(candidate).is_dir():
            return candidate
    project_data = Path(__file__).resolve().parents[2] / "data"
    if project_data.is_dir():
        return str(project_data)
    return None


def _get_client() -> Any:
    global _client
    if _client is None:
        from axdata import AxDataClient

        data_root = _discover_data_root()
        _client = AxDataClient(data_root=data_root) if data_root else AxDataClient()
        logger.info(
            "AxData tick adapter client initialized (data_root=%s)",
            data_root or "default",
        )
    return _client


def _ok(data: Any) -> dict[str, Any]:
    return {"data": data if data is not None else []}


def _sanitize_value(v: Any) -> Any:
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, dict):
        return {k: _sanitize_value(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_sanitize_value(item) for item in v]
    return v


def _normalize(result: Any) -> list[dict[str, Any]]:
    if result is None:
        return []
    if isinstance(result, list):
        return _sanitize_value(result)
    if isinstance(result, dict):
        return _sanitize_value([result])
    if hasattr(result, "to_dict"):
        return _sanitize_value(result.to_dict(orient="records"))
    return []


def _parse_fields(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [f.strip() for f in raw.split(",") if f.strip()]


def _is_index_code(code: str) -> bool:
    """判断是否为指数代码：000xxx.SH 或 399xxx.SZ。"""
    bare, _, suffix = code.partition(".")
    if suffix == "SH" and bare.startswith("000"):
        return True
    if suffix == "SZ" and bare.startswith("399"):
        return True
    return False


def _fetch_index_daily(client: Any, symbol: str, start_s: str | None, end_s: str | None) -> list[dict[str, Any]]:
    """通过 index_kline_tdx 接口获取指数日K，转换为 daily 格式。"""
    result = client.call("index_kline_tdx", code=_normalize_code(symbol), period="day", count=800)
    rows = _normalize(result)
    out: list[dict[str, Any]] = []
    for r in rows:
        trade_date = str(r.get("trade_time", ""))[:10].replace("-", "")
        if start_s and trade_date < start_s:
            continue
        if end_s and trade_date > end_s:
            continue
        out.append({
            "ts_code": r.get("instrument_id", symbol),
            "trade_date": trade_date,
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "close": r.get("close"),
            "vol": r.get("volume"),
            "amount": r.get("amount"),
        })
    return out


def _normalize_code(code: str) -> str:
    if "." in code:
        return code
    return f"{code}.SH" if code.startswith("6") else f"{code}.SZ"


def _normalize_date(value: str) -> str:
    v = value.replace("-", "").replace("/", "").strip()
    return v[:8] if len(v) >= 8 else v


def _date_text(value: datetime | None) -> str | None:
    return value.strftime("%Y%m%d") if value else None


def _validate_batch_size(request: BatchRequest) -> None:
    if len(request.symbols) > _MAX_BATCH_SYMBOLS:
        raise HTTPException(
            status_code=422,
            detail=f"symbols cannot contain more than {_MAX_BATCH_SYMBOLS} entries",
        )


def _batch_error(symbols: list[str], failed: list[str]) -> dict[str, Any] | None:
    if failed and len(failed) == len(symbols) and symbols:
        preview = ", ".join(failed[:5])
        more = f" ...等 {len(failed)} 个" if len(failed) > 5 else ""
        plural = "s" if len(failed) > 1 else ""
        return {
            "data": [],
            "error": f"all {len(failed)} symbol{plural} failed: {preview}{more}",
        }
    return None


def _run_parallel(
    symbols: list[str], fetch_one
) -> tuple[list[dict[str, Any]], list[str]]:
    data: list[dict[str, Any]] = []
    failed: list[str] = []
    with ThreadPoolExecutor(max_workers=_SNAPSHOT_WORKERS) as pool:
        futures = {pool.submit(fetch_one, s): s for s in symbols}
        for future, symbol in futures.items():
            try:
                rows = future.result()
                if rows:
                    data.extend(rows)
            except Exception as exc:
                failed.append(symbol)
                logger.warning("batch fetch failed for %s: %s", symbol, exc)
    return data, failed


class BatchRequest(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=_MAX_BATCH_SYMBOLS)
    start_time: datetime | None = None
    end_time: datetime | None = None

    @model_validator(mode="after")
    def validate_time_range(self):
        if self.start_time and self.end_time and self.start_time > self.end_time:
            raise ValueError("start_time must be before or equal to end_time")
        return self


class FinancialRequest(BatchRequest):
    report_type: str | None = None


@router.get("/health")
async def health():
    return {
        "status": "ok",
        "backend": "axdata-sdk",
        "provider": "axdata-tick-adapter",
    }


_REALTIME_FIELDS = [
    "instrument_id",
    "symbol",
    "last_price",
    "open",
    "high",
    "low",
    "pre_close",
    "volume",
    "amount",
    "change_pct",
    "change",
    "amplitude_pct",
]


def _snapshot_one(client: Any, code: str) -> list[dict[str, Any]]:
    result = client.call(
        "stock_realtime_snapshot_tdx",
        code=_normalize_code(code),
        fields=_REALTIME_FIELDS,
    )
    return _normalize(result)


def _realtime_rows(client: Any, codes: list[str]) -> list[dict[str, Any]]:
    data: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=_SNAPSHOT_WORKERS) as pool:
        futures = {
            pool.submit(_snapshot_one, client, code): code for code in codes
        }
        for future, code in futures.items():
            try:
                data.extend(future.result())
            except Exception as exc:
                logger.warning("realtime snapshot failed for %s: %s", code, exc)
    return data


@router.get("/realtime")
def realtime(
    symbols: str | None = Query(default=None, description="可选过滤，逗号分隔；缺省返回全市场快照"),
    code: str | None = Query(default=None, description="单代码查询（调试用途）"),
    fields: str | None = Query(None, description="字段列表，逗号分隔"),
):
    try:
        client = _get_client()
        if code:
            result = client.call(
                "stock_realtime_snapshot_tdx",
                code=_normalize_code(code),
                fields=_parse_fields(fields) or _REALTIME_FIELDS,
            )
            return _ok(_normalize(result))
        if symbols:
            codes = [c.strip() for c in symbols.split(",") if c.strip()]
            if len(codes) > _MAX_BATCH_SYMBOLS:
                raise HTTPException(
                    status_code=422,
                    detail=f"symbols cannot contain more than {_MAX_BATCH_SYMBOLS} entries",
                )
        else:
            basics = _normalize(
                client.stock_basic_exchange(fields=["instrument_id"])
            )
            codes = [row["instrument_id"] for row in basics if row.get("instrument_id")]
        return _ok(_realtime_rows(client, codes))
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("tick adapter realtime failed")
        return {"data": [], "error": str(exc)}


_DAILY_FIELDS = [
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
    "pct_chg",
]


@router.post("/daily")
def daily_batch(request: BatchRequest):
    _validate_batch_size(request)
    client = _get_client()
    start_s = _date_text(request.start_time)
    end_s = _date_text(request.end_time)

    def fetch_one(symbol: str) -> list[dict[str, Any]]:
        if _is_index_code(symbol):
            return _fetch_index_daily(client, symbol, start_s, end_s)
        params: dict[str, Any] = {
            "ts_code": _normalize_code(symbol),
            "fields": _DAILY_FIELDS,
        }
        if start_s:
            params["start"] = start_s
        if end_s:
            params["end"] = end_s
        return _normalize(client.daily(**params))

    data, failed = _run_parallel(request.symbols, fetch_one)
    return _batch_error(request.symbols, failed) or _ok(data)


@router.get("/daily")
def daily(
    code: str = Query(..., description="股票代码"),
    start: str | None = Query(None, description="起始日期"),
    end: str | None = Query(None, description="截止日期"),
    fields: str | None = Query(None, description="字段列表，逗号分隔"),
):
    try:
        params: dict[str, Any] = {"ts_code": _normalize_code(code)}
        if start:
            params["start"] = _normalize_date(start)
        if end:
            params["end"] = _normalize_date(end)
        result = _get_client().daily(
            fields=_parse_fields(fields) or _DAILY_FIELDS, **params
        )
        return _ok(_normalize(result))
    except Exception as exc:
        logger.exception("tick adapter daily failed")
        return {"data": [], "error": str(exc)}


def _parse_minute_bound(value: str, *, end_of_minute: bool) -> str | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        for fmt in ("%Y%m%d%H%M", "%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                parsed = datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(ZoneInfo("Asia/Shanghai"))
    if end_of_minute and parsed.hour == 0 and parsed.minute == 0 and len(value) <= 10:
        parsed = parsed.replace(hour=23, minute=59)
    return parsed.strftime("%Y-%m-%d %H:%M")


def _minute_key(tick: dict[str, Any], date_str: str) -> str | None:
    raw = str(tick.get("tick_time") or tick.get("time") or "")
    if not raw:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 14:
        return digits[:12]
    if len(digits) == 6:
        return date_str + digits[:4]
    if len(digits) == 4:
        return date_str + digits
    return None


def _fetch_intraday_day(client: Any, symbol: str, day: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page = 1
    while True:
        page_rows = _normalize(
            client.call(
                "stock_intraday_sina",
                symbol=symbol,
                date=day,
                page=page,
                limit=_INTRADAY_PAGE_SIZE,
            )
        )
        if page_rows and rows and page_rows == rows[-len(page_rows) :]:
            raise RuntimeError("intraday source repeated a page")
        rows.extend(page_rows)
        if len(page_rows) < _INTRADAY_PAGE_SIZE:
            return rows
        if page >= _MAX_INTRADAY_PAGES:
            raise RuntimeError("intraday response exceeded the maximum page count")
        page += 1


def _aggregate_minute_bars(
    ticks: list[dict[str, Any]], symbol: str, date_str: str
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for tick in ticks:
        key = _minute_key(tick, date_str)
        price = tick.get("price")
        if not key or price is None:
            continue
        bucket = buckets.setdefault(
            key, {"prices": [], "volume": 0.0, "amount": 0.0}
        )
        bucket["prices"].append(float(price))
        vol = float(tick.get("volume") or 0)
        bucket["volume"] += vol
        bucket["amount"] += float(price) * vol
    bars: list[dict[str, Any]] = []
    for key in sorted(buckets):
        bucket = buckets[key]
        prices = bucket["prices"]
        bars.append(
            {
                "symbol": symbol,
                "datetime": (
                    f"{key[:4]}-{key[4:6]}-{key[6:8]} "
                    f"{key[8:10]}:{key[10:12]}:00"
                ),
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "volume": bucket["volume"],
                "amount": bucket["amount"],
            }
        )
    return bars


def _minute_batch_impl(request: BatchRequest) -> dict[str, Any]:
    _validate_batch_size(request)
    client = _get_client()
    date_str = (
        _date_text(request.start_time)
        or _date_text(request.end_time)
        or datetime.now().strftime("%Y%m%d")
    )
    start_s = (
        request.start_time.strftime("%Y-%m-%d %H:%M") if request.start_time else None
    )
    end_s = (
        request.end_time.strftime("%Y-%m-%d %H:%M") if request.end_time else None
    )
    dates: list[str] = []
    if request.start_time and request.end_time:
        start_day = request.start_time.date()
        end_day = request.end_time.date()
        if (end_day - start_day).days >= 40:
            raise HTTPException(
                status_code=422,
                detail="minute range cannot exceed 40 calendar days",
            )
        dates = [
            (start_day + timedelta(days=offset)).strftime("%Y%m%d")
            for offset in range((end_day - start_day).days + 1)
        ]
    if not dates:
        dates = [date_str]

    def fetch_one(symbol: str) -> list[dict[str, Any]]:
        bare = symbol.split(".")[0]
        bars: list[dict[str, Any]] = []
        for day in dates:
            rows = _fetch_intraday_day(client, bare, day)
            bars.extend(_aggregate_minute_bars(rows, symbol, day))
        return [
            bar
            for bar in bars
            if (not start_s or bar["datetime"][:16] >= start_s)
            and (not end_s or bar["datetime"][:16] <= end_s)
        ]

    data, failed = _run_parallel(request.symbols, fetch_one)
    return _batch_error(request.symbols, failed) or _ok(data)


@router.post("/minute")
def minute_batch(request: BatchRequest):
    return _minute_batch_impl(request)


@router.post("/full_minute")
def full_minute_batch(request: BatchRequest):
    return _minute_batch_impl(request)


@router.get("/minute")
def minute(
    code: str = Query(..., description="股票代码"),
    start: str = Query(..., description="起始时间"),
    end: str = Query(..., description="截止日期（用于过滤返回的分钟线）"),
    period: str = Query(default="1m", description="周期（当前仅支持 1m）"),
    fields: str | None = Query(None, description="字段列表"),
    full: bool = Query(default=False, description="全量返回"),
):
    if period != "1m":
        raise HTTPException(status_code=422, detail="period must be 1m")
    if len(code) > 32:
        raise HTTPException(status_code=422, detail="code is too long")
    try:
        symbol = code.split(".")[0] if "." in code else code
        start_s = _parse_minute_bound(start, end_of_minute=False)
        end_s = _parse_minute_bound(end, end_of_minute=True)
        if start_s is None or end_s is None:
            raise HTTPException(
                status_code=422,
                detail="start and end must include a valid date",
            )
        if start_s > end_s:
            raise HTTPException(status_code=422, detail="start must be before or equal to end")
        date_str = start_s[:10].replace("-", "")
        dates = [date_str]
        if start_s[:10] != end_s[:10]:
            start_day = datetime.strptime(start_s[:10], "%Y-%m-%d").date()
            end_day = datetime.strptime(end_s[:10], "%Y-%m-%d").date()
            if (end_day - start_day).days >= 40:
                raise HTTPException(
                    status_code=422,
                    detail="minute range cannot exceed 40 calendar days",
                )
            dates = [
                (start_day + timedelta(days=offset)).strftime("%Y%m%d")
                for offset in range((end_day - start_day).days + 1)
            ]
        client = _get_client()
        data = []
        for day in dates:
            ticks = _fetch_intraday_day(client, symbol, day)
            data.extend(_aggregate_minute_bars(ticks, code, day))
        data = [
            bar
            for bar in data
            if bar["datetime"][:16] >= start_s and bar["datetime"][:16] <= end_s
        ]
        if not full and len(data) > 800:
            data = data[:800]
        if fields:
            selected = set(_parse_fields(fields) or [])
            data = [
                {key: value for key, value in row.items() if key in selected}
                for row in data
            ]
        return _ok(data)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("tick adapter minute failed")
        return {"data": [], "error": str(exc)}


@router.post("/adj_factor")
def adj_factor_batch(request: BatchRequest):
    _validate_batch_size(request)
    client = _get_client()
    start_s = _date_text(request.start_time)
    end_s = _date_text(request.end_time)

    def fetch_one(symbol: str) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"ts_code": _normalize_code(symbol)}
        if start_s:
            params["start"] = start_s
        if end_s:
            params["end"] = end_s
        return _normalize(client.adj_factor(**params))

    data, failed = _run_parallel(request.symbols, fetch_one)
    return _batch_error(request.symbols, failed) or _ok(data)


@router.get("/adj_factor")
def adj_factor(
    code: str = Query(..., description="股票代码"),
    start: str | None = Query(None, description="起始日期"),
    end: str | None = Query(None, description="截止日期"),
):
    try:
        params: dict[str, Any] = {"ts_code": _normalize_code(code)}
        if start:
            params["start"] = _normalize_date(start)
        if end:
            params["end"] = _normalize_date(end)
        result = _get_client().adj_factor(**params)
        return _ok(_normalize(result))
    except Exception as exc:
        logger.exception("tick adapter adj_factor failed")
        return {"data": [], "error": str(exc)}


@router.post("/financial")
def financial_batch(request: FinancialRequest):
    _validate_batch_size(request)
    client = _get_client()
    report_type_state = {"status": "unknown"}
    report_type_condition = Condition()

    def fetch_one(symbol: str) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"stock": _normalize_code(symbol)}
        if not request.report_type:
            return _normalize(client.call("stock_financial_report_sina", **params))
        with report_type_condition:
            while report_type_state["status"] == "checking":
                report_type_condition.wait()
            if report_type_state["status"] == "unsupported":
                use_report_type = False
            else:
                report_type_state["status"] = "checking"
                use_report_type = True
        if not use_report_type:
            return _normalize(client.call("stock_financial_report_sina", **params))
        try:
            result = _normalize(
                client.call(
                    "stock_financial_report_sina",
                    **params,
                    report_type=request.report_type,
                )
            )
        except SourceRequestValidationError as report_type_exc:
            logger.warning(
                "financial report_type=%s failed for %s (%s), fallback to full query",
                request.report_type,
                symbol,
                report_type_exc,
            )
            with report_type_condition:
                report_type_state["status"] = "unsupported"
                report_type_condition.notify_all()
            return _normalize(client.call("stock_financial_report_sina", **params))
        except Exception:
            with report_type_condition:
                report_type_state["status"] = "unknown"
                report_type_condition.notify_all()
            raise
        with report_type_condition:
            report_type_state["status"] = "supported"
            report_type_condition.notify_all()
        return result

    data, failed = _run_parallel(request.symbols, fetch_one)
    return _batch_error(request.symbols, failed) or _ok(data)


@router.get("/financial")
def financial(
    code: str = Query(..., description="股票代码"),
):
    try:
        result = _get_client().call(
            "stock_financial_report_sina", stock=_normalize_code(code)
        )
        if result is None:
            return {"data": [], "error": "no financial data available"}
        return _ok(_normalize(result))
    except Exception as exc:
        logger.exception("tick adapter financial failed")
        return {"data": [], "error": str(exc)}


@router.post("/depth5")
def depth5_batch(request: BatchRequest):
    _validate_batch_size(request)
    client = _get_client()

    def fetch_one(symbol: str) -> list[dict[str, Any]]:
        snapshot = client.call(
            "stock_realtime_snapshot_tdx",
            code=_normalize_code(symbol),
            fields=[
                "instrument_id",
                "bid1_price",
                "bid1_volume",
                "ask1_price",
                "ask1_volume",
            ],
        )
        rows = _normalize(snapshot)
        if not rows:
            return []
        row = rows[0]
        return [
            {
                "symbol": symbol,
                "asks": [
                    {
                        "price": row.get("ask1_price"),
                        "volume": row.get("ask1_volume"),
                    },
                ],
                "bids": [
                    {
                        "price": row.get("bid1_price"),
                        "volume": row.get("bid1_volume"),
                    },
                ],
            }
        ]

    data, failed = _run_parallel(request.symbols, fetch_one)
    return _batch_error(request.symbols, failed) or _ok(data)


@router.get("/depth5")
def depth5(
    code: str = Query(..., description="股票代码"),
):
    try:
        snapshot = _get_client().call(
            "stock_realtime_snapshot_tdx",
            code=_normalize_code(code),
            fields=[
                "instrument_id",
                "bid1_price",
                "bid1_volume",
                "ask1_price",
                "ask1_volume",
            ],
        )
        rows = _normalize(snapshot)
        if not rows:
            return {"data": [], "error": "no snapshot data"}
        row = rows[0]
        depth = {
            "instrument_id": row.get("instrument_id", code),
            "bids": [
                {"price": row.get("bid1_price"), "volume": row.get("bid1_volume")},
            ],
            "asks": [
                {"price": row.get("ask1_price"), "volume": row.get("ask1_volume")},
            ],
        }
        return _ok(depth)
    except Exception as exc:
        logger.exception("tick adapter depth5 failed")
        return {"data": [], "error": str(exc)}


@router.get("/trade_cal")
def trade_cal(
    exchange: str = Query(default="SSE", description="交易所 SSE/SZSE"),
    start: str | None = Query(None, description="起始日期"),
    end: str | None = Query(None, description="截止日期"),
):
    try:
        result = _get_client().trade_cal(
            exchange=exchange,
            start=_normalize_date(start) if start else None,
            end=_normalize_date(end) if end else None,
        )
        return _ok(_normalize(result))
    except Exception as exc:
        logger.exception("tick adapter trade_cal failed")
        return {"data": [], "error": str(exc)}


@router.get("/stock_basic")
def stock_basic(
    exchange: str | None = Query(None, description="交易所 SSE/SZSE/BSE"),
    fields: str | None = Query(None, description="字段列表"),
):
    try:
        default_fields = [
            "instrument_id",
            "name",
            "region",
            "company_full_name",
        ]
        if exchange:
            result = _get_client().stock_basic_exchange(
                exchange=exchange,
                fields=_parse_fields(fields) or default_fields,
            )
        else:
            result = _get_client().stock_basic_exchange(
                fields=_parse_fields(fields) or default_fields,
            )
        return _ok(_normalize(result))
    except Exception as exc:
        logger.exception("tick adapter stock_basic failed")
        return {"data": [], "error": str(exc)}
