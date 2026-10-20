"""Tick Stock Panel 适配路由。

将 AxData SDK 本地数据包装为 tick-stock-panel 兼容的 HTTP 端点，
格式保持与原 eltdx-tick-adapter 一致：{"data": [...]}。
"""

from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query

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
    # 回退到项目内 data 目录
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


# ── 响应格式 ─────────────────────────────────────────────────

def _ok(data: Any) -> dict[str, Any]:
    return {"data": data if data is not None else []}


def _sanitize_value(v: Any) -> Any:
    """递归清理 NaN/Inf 值，转换为 None 以避免 JSON 序列化失败。"""
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, dict):
        return {k: _sanitize_value(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_sanitize_value(item) for item in v]
    return v


def _normalize(result: Any) -> list[dict[str, Any]]:
    """AxData 返回值统一为 list[dict]，并清理 NaN/Inf。"""
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


# ── 健康检查 ─────────────────────────────────────────────────

@router.get("/health")
async def health():
    return {
        "status": "ok",
        "backend": "axdata-sdk",
        "provider": "axdata-tick-adapter",
    }


# ── 实时行情 ─────────────────────────────────────────────────

@router.get("/realtime")
async def realtime(
    code: str = Query(..., description="股票代码，如 000001.SZ"),
    fields: str | None = Query(None, description="字段列表，逗号分隔"),
):
    try:
        result = _get_client().call(
            "stock_realtime_snapshot_tdx",
            code=code,
            fields=_parse_fields(fields)
            or [
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
            ],
        )
        return _ok(_normalize(result))
    except Exception as e:
        logger.exception("tick adapter realtime failed")
        return {"data": [], "error": str(e)}


# ── 日 K 线 ──────────────────────────────────────────────────

@router.get("/daily")
async def daily(
    code: str = Query(..., description="股票代码"),
    start: str | None = Query(None, description="起始日期 YYYYMMDD"),
    end: str | None = Query(None, description="截止日期 YYYYMMDD"),
    fields: str | None = Query(None, description="字段列表，逗号分隔"),
):
    try:
        params: dict[str, Any] = {"ts_code": _normalize_code(code)}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        result = _get_client().daily(
            fields=_parse_fields(fields)
            or [
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "vol",
                "amount",
                "pct_chg",
            ],
            **params,
        )
        return _ok(_normalize(result))
    except Exception as e:
        logger.exception("tick adapter daily failed")
        return {"data": [], "error": str(e)}


# ── 分钟 K 线 ────────────────────────────────────────────────

@router.get("/minute")
async def minute(
    code: str = Query(..., description="股票代码"),
    start: str = Query(..., description="起始时间 YYYYMMDDHHMM 或 YYYYMMDD"),
    end: str = Query(..., description="截止时间 YYYYMMDDHHMM 或 YYYYMMDD"),
    period: str = Query(default="1m", description="周期 1m/5m/15m/30m/60m"),
    fields: str | None = Query(None, description="字段列表"),
    full: bool = Query(default=False, description="全量返回，不做分页截断"),
):
    try:
        # 从 code 中提取 symbol（去除后缀如 .SZ/.SH）
        symbol = code.split(".")[0] if "." in code else code
        # 兼容 YYYY-MM-DD / YYYYMMDD / YYYYMMDDHHMM，统一转为 YYYYMMDD
        date_str = _normalize_date(start)
        # 使用 sina 分时数据接口
        result = _get_client().call(
            "stock_intraday_sina",
            symbol=symbol,
            date=date_str,
        )
        data = _normalize(result)
        if not full and len(data) > 800:
            data = data[:800]
        return _ok(data)
    except Exception as e:
        logger.exception("tick adapter minute failed")
        return {"data": [], "error": str(e)}


# ── 复权因子 ─────────────────────────────────────────────────

@router.get("/adj_factor")
async def adj_factor(
    code: str = Query(..., description="股票代码"),
    start: str | None = Query(None, description="起始日期"),
    end: str | None = Query(None, description="截止日期"),
):
    try:
        params: dict[str, Any] = {"ts_code": _normalize_code(code)}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        result = _get_client().adj_factor(**params)
        return _ok(_normalize(result))
    except Exception as e:
        logger.exception("tick adapter adj_factor failed")
        return {"data": [], "error": str(e)}


# ── 财务报表 ─────────────────────────────────────────────────

@router.get("/financial")
async def financial(
    code: str = Query(..., description="股票代码"),
):
    try:
        # stock_financial_report_sina 参数: stock 或 symbol
        result = _get_client().call("stock_financial_report_sina", stock=code)
        if result is None:
            return {"data": [], "error": "no financial data available"}
        return _ok(_normalize(result))
    except Exception as e:
        logger.exception("tick adapter financial failed")
        return {"data": [], "error": str(e)}


# ── 盘口深度 ─────────────────────────────────────────────────

@router.get("/depth5")
async def depth5(
    code: str = Query(..., description="股票代码"),
):
    try:
        # AxData 实时快照仅含 bid1/ask1（无二至五档）。
        # 从 snapshot 提取盘口字段并组装为 depth5 语义。
        snapshot = _get_client().call(
            "stock_realtime_snapshot_tdx",
            code=code,
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
