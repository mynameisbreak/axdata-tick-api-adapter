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
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tick", tags=["tick-adapter"])

# 批量端点与全市场快照并行拉取的线程数。
# 注意：全市场模式（5000+ 标的）逐标的快照仍可能超过面板 60s timeout，
# 扩容到全市场前需要上游提供批量快照接口或引入本地快照缓存。
_SNAPSHOT_WORKERS = 16

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
    """裸代码自动补全交易所后缀：6 开头 → .SH，其余 → .SZ。

    已知限制（面向 A 股股票面板够用）：
    - 北交所代码（8/4 开头，应为 .BJ）会被误判为 .SZ；
    - 基金（5 开头，应为 .SH）会被误判为 .SZ；
    - 上证指数（如 000001.SH）与深市股票（000001.SZ）前缀重叠，
      裸代码无法区分，一律按深市股票处理。
    调用方如需精确控制，请直接传入带后缀的完整代码。
    """
    if "." in code:
        return code
    return f"{code}.SH" if code.startswith("6") else f"{code}.SZ"


def _normalize_date(value: str) -> str:
    """兼容 YYYY-MM-DD / YYYYMMDD / YYYYMMDDHHMM，统一转为 YYYYMMDD。"""
    v = value.replace("-", "").replace("/", "").strip()
    return v[:8] if len(v) >= 8 else v


def _date_text(value: datetime | None) -> str | None:
    return value.strftime("%Y%m%d") if value else None


def _batch_error(symbols: list[str], failed: list[str]) -> dict[str, Any] | None:
    """全部标的失败时生成 error 汇总，避免静默返回空列表。"""
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
    """并行执行逐标的抓取，返回 (data, failed)。

    单标的失败不影响整体，与 realtime 端点一致的容错语义。
    """
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


def _snapshot_one(client: Any, code: str) -> list[dict[str, Any]]:
    result = client.call(
        "stock_realtime_snapshot_tdx",
        code=_normalize_code(code),
        fields=_REALTIME_FIELDS,
    )
    return _normalize(result)


def _realtime_rows(client: Any, codes: list[str]) -> list[dict[str, Any]]:
    """并行拉取多标的快照；单标的失败不影响整体。"""
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
def daily_batch(request: BatchRequest):
    """批量日 K：日期区间下推给 AxData 查询，减少传输与内存开销。"""
    client = _get_client()
    start_s = _date_text(request.start_time)
    end_s = _date_text(request.end_time)
    def fetch_one(symbol: str) -> list[dict[str, Any]]:
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
    # 8 位 YYYYMMDD 无时分信息，无法归入分钟桶，跳过（当前数据源不会出现该格式）
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


def _minute_batch_impl(request: BatchRequest) -> dict[str, Any]:
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
    # 跨日区间：逐日拉取分时再聚合（上限 40 天，防止误传超长区间）
    dates: list[str] = []
    if request.start_time and request.end_time:
        d = request.start_time.date()
        end_d = request.end_time.date()
        while d <= end_d and len(dates) < 40:
            dates.append(d.strftime("%Y%m%d"))
            d += timedelta(days=1)
    if not dates:
        dates = [date_str]

    def fetch_one(symbol: str) -> list[dict[str, Any]]:
        bare = symbol.split(".")[0]
        bars: list[dict[str, Any]] = []
        for day in dates:
            result = client.call("stock_intraday_sina", symbol=bare, date=day)
            bars.extend(_aggregate_minute_bars(_normalize(result), symbol, day))
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
    try:
        symbol = code.split(".")[0] if "." in code else code
        date_str = _normalize_date(start)
        result = _get_client().call(
            "stock_intraday_sina", symbol=symbol, date=date_str
        )
        data = _aggregate_minute_bars(_normalize(result), code, date_str)
        if end:
            end_s = _normalize_date(end)
            data = [b for b in data if b["datetime"][:10].replace("-", "") <= end_s]
        if not full and len(data) > 800:
            data = data[:800]
        return _ok(data)
    except Exception as e:
        logger.exception("tick adapter minute failed")
        return {"data": [], "error": str(e)}


# ── 复权因子 ─────────────────────────────────────────────────


@router.post("/adj_factor")
def adj_factor_batch(request: BatchRequest):
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
    except Exception as e:
        logger.exception("tick adapter adj_factor failed")
        return {"data": [], "error": str(e)}


# ── 财务报表 ─────────────────────────────────────────────────


@router.post("/financial")
def financial_batch(request: FinancialRequest):
    """批量财务报表；report_type 透传给上游，上游不支持时自动降级。"""
    client = _get_client()
    # 首次降级后记住该 report_type 不被上游支持，后续标的直接走全量查询，
    # 避免每个标的都先失败一次再降级。
    report_type_unsupported = False

    def fetch_one(symbol: str) -> list[dict[str, Any]]:
        nonlocal report_type_unsupported
        params: dict[str, Any] = {"stock": _normalize_code(symbol)}
        if request.report_type and not report_type_unsupported:
            params["report_type"] = request.report_type
        try:
            return _normalize(client.call("stock_financial_report_sina", **params))
        except Exception as report_type_exc:
            if not request.report_type or report_type_unsupported:
                raise
            # 上游接口不接受 report_type 参数或该报表类型不可用时，
            # 降级为不带 report_type 的全量查询
            logger.warning(
                "financial report_type=%s failed for %s (%s), fallback to full query",
                request.report_type, symbol, report_type_exc,
            )
            report_type_unsupported = True
            params.pop("report_type", None)
            return _normalize(client.call("stock_financial_report_sina", **params))

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
    except Exception as e:
        logger.exception("tick adapter financial failed")
        return {"data": [], "error": str(e)}


# ── 盘口深度 ─────────────────────────────────────────────────


@router.post("/depth5")
def depth5_batch(request: BatchRequest):
    """批量盘口：AxData 快照仅含买一/卖一档。"""
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
    except Exception as e:
        logger.exception("tick adapter depth5 failed")
        return {"data": [], "error": str(e)}


# ── 交易日历 ─────────────────────────────────────────────────


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
    except Exception as e:
        logger.exception("tick adapter trade_cal failed")
        return {"data": [], "error": str(e)}


# ── 股票基础信息 ─────────────────────────────────────────────


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
    except Exception as e:
        logger.exception("tick adapter stock_basic failed")
        return {"data": [], "error": str(e)}
