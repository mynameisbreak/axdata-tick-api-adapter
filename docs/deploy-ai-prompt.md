# axdata-tick-api-adapter 部署 AI 提示词

将本提示词投喂给 AI 运维助手（或人工照做），按标准流程完成 axdata-tick-api-adapter 的部署与 tick-stock-panel 对接。

---

## 提示词模板

```text
请帮我部署 axdata-tick-api-adapter 并接入 tick-stock-panel。按以下标准流程执行，每步完成后执行对应的验证命令，验证失败时按【故障判定】处理，不要跳步。

【环境约定】
- AxData API 端口: 8666（含 /tick/* 路由）
- Web 控制台端口: 8667
- tick-stock-panel 以 Docker 部署，容器内数据目录为 /app/data（挂载自主机项目目录的 data/）
- 自定义数据源配置目录: <tick-stock-panel 项目>/data/data_sources/

【步骤 1：启动 AxData API】
cd <axdata-tick-api-adapter 项目目录>
nohup python3 -m uvicorn apps.api.main:app --host 0.0.0.0 --port 8666 > /tmp/axdata-api.log 2>&1 &
验证: curl http://127.0.0.1:8666/tick/health
预期: {"status":"ok","backend":"axdata-sdk","provider":"axdata-tick-adapter"}

【步骤 2：验证批量端点】
curl -X POST http://127.0.0.1:8666/tick/daily -H 'Content-Type: application/json' -d '{"symbols":["000001.SZ"],"start_time":"2024-01-01T00:00:00","end_time":"2024-12-31T00:00:00"}'
curl http://127.0.0.1:8666/tick/realtime
预期: 均返回 {"data":[...]} 且非空（realtime 为全市场快照）

【步骤 3：部署前校验配置】
python3 scripts/validate_panel_config.py tick-stock-panel.yaml
预期: 输出 ✓ 校验通过
注意: 校验失败的源会被面板静默排除（不出现在数据源列表），必须先修复再部署。

【步骤 4：部署配置到面板】
cp tick-stock-panel.yaml <tick-stock-panel 项目>/data/data_sources/axdata_source.yaml
（目录为 root 所有时需 sudo cp；Docker 部署时确认容器挂载为 /app/data 而非容器内部路径）

【步骤 5：触发面板重新加载】
在面板 Web 界面『设置 → 数据源』点击『重新加载』，或:
curl -X POST http://127.0.0.1:3018/api/settings/data-sources/reload
（需登录会话；命令行方式受认证限制时用 Web 界面操作）

【步骤 6：验证数据源加载】
浏览器访问 http://<面板地址>:3018/api/settings/data-sources
预期: custom 列表包含 axdata_source，datasets 含全部 7 个数据集，errors 为空。

【步骤 7：切换数据提供方并试拉】
在面板设置中将需要的数据集提供方切换为 axdata_source，用『试拉测试』逐项验证。

【故障判定】
1. 步骤 6 中 custom 为空且 errors 为空:
   → 大概率容器挂载异常。检查返回的 config_dir: 若为 /data/data_sources（容器内部路径）而非 /app/data/data_sources，说明容器 DATA_DIR 未指向挂载卷。
   → 修复: sudo docker rm -f <容器名> && sudo docker compose up -d 重建容器。
2. 步骤 6 中 errors 有 missing mapped fields:
   → field_map 缺少必填内部字段或方向写反。field_map 格式为『上游字段: 内部字段』，如 pre_close: prev_close。
   → 修复后重新执行步骤 3 校验。
3. 步骤 1/2 端点无响应:
   → 检查进程: ps aux | grep uvicorn；检查日志: tail -50 /tmp/axdata-api.log。
4. git push 报 GnuTLS recv error (-110) 或挂起:
   → 服务器到 github.com 网络间歇性中断，用循环重试: for i in 1..6; do timeout 30 git push origin main && break; sleep 10; done。
5. 面板数据为空但端点正常:
   → AxData 数据目录仅样本数据时属预期；分钟线依赖 stock_intraday_sina 数据源，需确认已启用。
```

## 校验规则速查（对齐面板 _REQUIRED）

| 数据集 | 必填内部字段 |
|--------|--------------|
| `daily` | symbol, date, open, high, low, close, volume, amount |
| `adj_factor` | symbol, trade_date, ex_factor |
| `realtime` | symbol, last_price, prev_close, open, high, low, volume |
| `minute` / `full_minute` | symbol, datetime, open, high, low, close, volume |
| `depth5` | symbol, asks, bids |
| `financial` | symbol |

其他规则：`pct_unit` 仅用于 realtime（percent/decimal）；非 realtime/depth5 数据集的请求参数名不得重复；timeout 范围 (0, 300] 秒；日期为 `YYYYMMDD` 时需加 `transforms: "parse_date(value, '%Y%m%d')"`。
