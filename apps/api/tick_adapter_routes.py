"""Tick Stock Panel 适配路由。

将 AxData SDK 本地数据包装为 tick-stock-panel 兼容的 HTTP 端点。
接口形态对齐 eltdx-tick-adapter：
- GET  /tick/realtime            全市场实时快照（面板契约要求）
- POST /tick/daily               批量日 K（symbols + start_time/end_time）
- POST /tick/adj_factor          批量复权因子
- POST /tick/financial           批量财务报表
- POST /tick/minute              批量分钟 K（分时数据聚合为 1m OHLC）
- POST /tick/full_minute         同 minute
- POST /tick/depth5              批量盘口（快照买一/卖一档）
- GET  单代码查询保留作调试用途
响应统一为 {"data": [...]}。
"""

from __future__ import annotations

import logging
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tick", tags=["tick-adapter"])

# ── 懒加载 AxDataClient ──────────────────────────────────────

_client: Any = None


def _discover_data_root() -> str | None:
    """按优先级发现 AxData 数据目录。"""
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


# ── 响应格式与工具 ───────────────────────────────────────────

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


def _normalize_code(code: str) -> str:
    """裸代码自动补全交易所后缀：6 开头 → .SH，其余 → .SZ。"""
    if "." in code:
        return code
    return f"{code}.SH" if code.startswith("6") else f"{code}.SZ"


def _normalize_date(value: str) -> str:
    """兼容 YYYY-MM-DD / YYYYMMDD / YYYYMMDDHHMM，统一转为 YYYYMMDD。"""
    v = value.replace("-", "").replace("/", "").strip()
    return v[:8] if len(v) >= 8 else v


def _date_text(value: datetime | None) -> str | None:
    return value.strftime("%Y%m%d") if value else None


# ── 批量请求模型 ─────────────────────────────────────────────


class BatchRequest(BaseModel):
    symbols: list[str]
    start_time: datetime | None = None
    end_time: datetime | None = None


class FinancialRequest(BatchRequest):
    report_type: str | None = None


# ── 健康检查 ─────────────────────────────────────────────────


@router.get("/health")
async def health():
    return {
        "status": "ok",
        "backend": "axdata-sdk",
        "provider": "axdata-tick-adapter",
    }


# ── 实时行情（全市场快照，面板契约） ────────────────────────

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


def _realtime_rows(client: Any, codes: list[str]) -> list[dict[str, Any]]:
    data: list[dict[str, Any]] = []
    for code in codes:
        try:
            result = client.call(
                "stock_realtime_snapshot_tdx",
                code=_normalize_code(code),
                fields=_REALTIME_FIELDS,
            )
            data.extend(_normalize(result))
        except Exception as exc:  # 单标的失败不影响整体快照
            logger.warning("realtime snapshot failed for %s: %s", code, exc)
    return data


@router.get("/realtime")
async def realtime(
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
        else:
            basics = _normalize(
                client.stock_basic_exchange(fields=["instrument_id"])
            )
            codes = [
                row["instrument_id"]
                for row in basics
                if row.get("instrument_id")
            ]
        return _ok(_realtime_rows(client, codes))
    except Exception as e:
        logger.exception("tick adapter realtime failed")
        return {"data": [], "error": str(e)}


# ── 日 K 线 ──────────────────────────────────────────────────

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
async def daily_batch(request: BatchRequest):
    """批量日 K：逐标的查询后按日期区间过滤聚合。"""
    client = _get_client()
    start_s = _date_text(request.start_time)
    end_s = _date_text(request.end_time)
    data: list[dict[str, Any]] = []
    for symbol in request.symbols:
        try:
            result = client.daily(
                ts_code=_normalize_code(symbol), fields=_DAILY_FIELDS
            )
            for row in _normalize(result):
                td = str(row.get("trade_date", ""))
                if start_s and td < start_s:
                    continue
                if end_s and td > end_s:
                    continue
                data.append(row)
        except Exception as exc:
            logger.warning("daily failed for %s: %s", symbol, exc)
    return _ok(data)


@router.get("/daily")
async def daily(
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
    except Exception as e:
        logger.exception("tick adapter daily failed")
        return {"data": [], "error": str(e)}


# ── 分钟 K 线（分时数据聚合为 1m OHLC） ─────────────────────


def _minute_key(tick: dict[str, Any], date_str: str) -> str | None:
    """从分时记录提取 YYYYMMDDHHMM 键。"""
    raw = str(tick.get("tick_time") or tick.get("time") or "")
    if not raw:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 14:
        return digits[:12]
    if len(digits) == 6:  # HHMMSS
        return date_str + digits[:4]
    if len(digits) == 4:  # HHMM
        return date_str + digits
    return None


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
        b = buckets[key]
        prices = b["prices"]
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
                "volume": b["volume"],
                "amount": b["amount"],
            }
        )
    return bars


async def _minute_batch_impl(request: BatchRequest) -> dict[str, Any]:
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
    data: list[dict[str, Any]] = []
    for symbol in request.symbols:
        try:
            bare = symbol.split(".")[0]
            result = client.call(
                "stock_intraday_sina", symbol=bare, date=date_str
            )
            bars = _aggregate_minute_bars(_normalize(result), symbol, date_str)
            for bar in bars:
                dt = bar["datetime"][:16]
                if start_s and dt < start_s:
                    continue
                if end_s and dt > end_s:
                    continue
                data.append(bar)
        except Exception as exc:
            logger.warning("minute failed for %s: %s", symbol, exc)
    return _ok(data)


@router.post("/minute")
async def minute_batch(request: BatchRequest):
    return await _minute_batch_impl(request)


@router.post("/full_minute")
async def full_minute_batch(request: BatchRequest):
    return await _minute_batch_impl(request)


@router.get("/minute")
async def minute(
    code: str = Query(..., description="股票代码"),
    start: str = Query(..., description="起始时间"),
    end: str = Query(..., description="截止时间"),
    period: str = Query(default="1m", description="周期"),
    fields: str | None = Query(None, description="字段列表"),
    full: bool = Query(default=False, description="全量返回"),
):
    try:
        symbol = code.split(".")[0] if "." in code else code
        date_str = _normalize_date(start)
        result = _get_client().call(
            "stock_intraday_sina", symbol=symbol, date=date_str
        )
        data = _aggregate_minute_bars(_normalize(result), code, date_str)
        if not full and len(data) > 800:
            data = data[:800]
        return _ok(data)
    except Exception as e:
        logger.exception("tick adapter minute failed")
        return {"data": [], "error": str(e)}


# ── 复权因子 ─────────────────────────────────────────────────


@router.post("/adj_factor")
async def adj_factor_batch(request: BatchRequest):
    client = _get_client()
    start_s = _date_text(request.start_time)
    end_s = _date_text(request.end_time)
    data: list[dict[str, Any]] = []
    for symbol in request.symbols:
        try:
            params: dict[str, Any] = {"ts_code": _normalize_code(symbol)}
            if start_s:
                params["start"] = start_s
            if end_s:
                params["end"] = end_s
            result = client.adj_factor(**params)
            data.extend(_normalize(result))
        except Exception as exc:
            logger.warning("adj_factor failed for %s: %s", symbol, exc)
    return _ok(data)


@router.get("/adj_factor")
async def adj_factor(
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
    except Exception as e:
        logger.exception("tick adapter adj_factor failed")
        return {"data": [], "error": str(e)}


# ── 财务报表 ─────────────────────────────────────────────────


@router.post("/financial")
async def financial_batch(request: FinancialRequest):
    client = _get_client()
    data: list[dict[str, Any]] = []
    for symbol in request.symbols:
        try:
            result = client.call(
                "stock_financial_report_sina", stock=_normalize_code(symbol)
            )
            data.extend(_normalize(result))
        except Exception as exc:
            logger.warning("financial failed for %s: %s", symbol, exc)
    return _ok(data)


@router.get("/financial")
async def financial(
    code: str = Query(..., description="股票代码"),
):
    try:
        result = _get_client().call(
            "stock_financial_report_sina", stock=_normalize_code(code)
        )
        if result is None:
            return {"data": [], "error": "no financial data available"}
        return _ok(_normalize(result))
    except Exception as e:
        logger.exception("tick adapter financial failed")
        return {"data": [], "error": str(e)}


# ── 盘口深度 ─────────────────────────────────────────────────


@router.post("/depth5")
async def depth5_batch(request: BatchRequest):
    """批量盘口：AxData 快照仅含买一/卖一档。"""
    client = _get_client()
    data: list[dict[str, Any]] = []
    for symbol in request.symbols:
        try:
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
                continue
            row = rows[0]
            data.append(
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
            )
        except Exception as exc:
            logger.warning("depth5 failed for %s: %s", symbol, exc)
    return _ok(data)


@router.get("/depth5")
async def depth5(
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
    except Exception as e:
        logger.exception("tick adapter depth5 failed")
        return {"data": [], "error": str(e)}


# ── 交易日历 ─────────────────────────────────────────────────


@router.get("/trade_cal")
async def trade_cal(
    exchange: str = Query(default="SSE", description="交易所 SSE/SZSE"),
    start: str | None = Query(None, description="起始日期"),
    end: str | None = Query(None, description="截止日期"),
):
    try:
        result = _get_client().trade_cal(exchange=exchange, start=start, end=end)
        return _ok(_normalize(result))
    except Exception as e:
        logger.exception("tick adapter trade_cal failed")
        return {"data": [], "error": str(e)}


# ── 股票基础信息 ─────────────────────────────────────────────


@router.get("/stock_basic")
async def stock_basic(
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
    except Exception as e:
        logger.exception("tick adapter stock_basic failed")
        return {"data": [], "error": str(e)}
