#!/usr/bin/env python3
"""tick-stock-panel 自定义数据源配置校验脚本。

在部署前预检 YAML 配置，把面板的『静默校验失败』变成明确的错误提示。
校验规则对齐 tick-stock-panel backend/app/data_providers/custom/provider.py 的 _REQUIRED。

用法:
    python3 scripts/validate_panel_config.py tick-stock-panel.yaml

退出码: 0 = 校验通过, 1 = 校验失败
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

# tick-stock-panel 各数据集的必填内部字段（_REQUIRED）
REQUIRED_FIELDS = {
    "daily": {"symbol", "date", "open", "high", "low", "close", "volume", "amount"},
    "adj_factor": {"symbol", "trade_date", "ex_factor"},
    "realtime": {"symbol", "last_price", "prev_close", "open", "high", "low", "volume"},
    "minute": {"symbol", "datetime", "open", "high", "low", "close", "volume"},
    "full_minute": {"symbol", "datetime", "open", "high", "low", "close", "volume"},
    "depth5": {"symbol", "asks", "bids"},
    "financial": {"symbol"},
}

VALID_DATASETS = set(REQUIRED_FIELDS)
NAME_RE = re.compile(r"^[a-z0-9_]+$")


def validate(path: Path) -> list[str]:
    errors: list[str] = []

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        return [f"YAML 解析失败: {exc}"]

    if not isinstance(raw, dict):
        return ["配置根节点必须是 dict"]

    # name 校验
    name = str(raw.get("name") or "").strip()
    if not name:
        errors.append("缺少 name 字段")
    elif not NAME_RE.match(name):
        errors.append(f"name '{name}' 不合法: 只允许小写字母/数字/下划线")

    if not str(raw.get("display_name") or "").strip():
        errors.append("缺少 display_name 字段")

    # auth 校验
    auth = raw.get("auth") or {}
    auth_type = str(auth.get("type", "none") or "none").lower()
    if auth_type not in ("none", "bearer", "header", "query"):
        errors.append(f"auth.type '{auth_type}' 不合法: 只允许 none/bearer/header/query")
    if auth_type in ("bearer", "header", "query") and not auth.get("token_env"):
        errors.append(f"auth.type={auth_type} 需要配置 token_env")

    # datasets 校验
    datasets = raw.get("datasets") or {}
    if not datasets:
        errors.append("缺少 datasets 配置")
    for ds_name, cfg in datasets.items():
        if ds_name not in VALID_DATASETS:
            errors.append(f"数据集 '{ds_name}' 不在支持列表 {sorted(VALID_DATASETS)} 中")
            continue
        if not isinstance(cfg, dict):
            errors.append(f"{ds_name}: 配置必须是 dict")
            continue

        if not str(cfg.get("url") or "").strip():
            errors.append(f"{ds_name}: 缺少 url")

        method = str(cfg.get("method", "GET") or "GET").upper()
        if method not in ("GET", "POST"):
            errors.append(f"{ds_name}: method 只允许 GET/POST")

        # 必填字段映射校验（field_map 格式: 上游字段: 内部字段）
        field_map = cfg.get("field_map") or {}
        mapped_internal = set(str(v) for v in field_map.values())
        missing = sorted(REQUIRED_FIELDS[ds_name] - mapped_internal)
        if missing:
            errors.append(
                f"{ds_name}: 缺少必填内部字段映射: {', '.join(missing)} "
                f"(field_map 格式为 '上游字段: 内部字段')"
            )

        # pct_unit 仅用于 realtime
        pct_unit = str(cfg.get("pct_unit") or "").strip().lower() or None
        if pct_unit is not None:
            if ds_name != "realtime":
                errors.append(f"{ds_name}: pct_unit 仅用于 realtime 数据集")
            elif pct_unit not in ("percent", "decimal"):
                errors.append(f"{ds_name}: pct_unit 必须是 percent 或 decimal")

        # 请求参数名重复校验（非 realtime/depth5 数据集）
        if ds_name not in ("realtime", "depth5"):
            params = [
                str(cfg.get("symbols_param", "symbols") or "symbols"),
                str(cfg.get("start_param", "start_time") or "start_time"),
                str(cfg.get("end_param", "end_time") or "end_time"),
            ]
            if ds_name in ("minute", "full_minute"):
                params.extend(
                    p for p in (cfg.get("asset_type_param"), cfg.get("freq_param")) if p
                )
            dupes = sorted({p for p in params if params.count(p) > 1})
            if dupes:
                errors.append(f"{ds_name}: 请求参数名重复: {', '.join(dupes)}")

        # timeout 范围校验
        timeout_raw = cfg.get("timeout")
        if timeout_raw is not None:
            try:
                timeout = float(timeout_raw)
                if not 0 < timeout <= 300:
                    errors.append(f"{ds_name}: timeout 必须在 (0, 300] 秒之间")
            except (TypeError, ValueError):
                errors.append(f"{ds_name}: timeout 必须是数字")

    return errors


def main() -> int:
    if len(sys.argv) != 2:
        print(f"用法: python3 {sys.argv[0]} <config.yaml>", file=sys.stderr)
        return 1
    path = Path(sys.argv[1])
    if not path.is_file():
        print(f"错误: 文件不存在: {path}", file=sys.stderr)
        return 1

    errors = validate(path)
    if errors:
        print(f"✗ 校验失败（{len(errors)} 个问题）:")
        for err in errors:
            print(f"  - {err}")
        print()
        print("提示: 面板对校验失败的源会静默排除（不出现在数据源列表）。")
        print("      修复上述问题后再部署到 data/data_sources/ 目录。")
        return 1

    print(f"✓ 校验通过: {path}")
    datasets = sorted((yaml.safe_load(path.read_text(encoding='utf-8')) or {}).get('datasets', {}))
    print(f"  数据集: {', '.join(datasets)}")
    print("  可部署到 tick-stock-panel 的 data/data_sources/ 目录。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
