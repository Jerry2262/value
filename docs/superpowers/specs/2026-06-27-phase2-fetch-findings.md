# Phase 2 Task 9 — Fetcher 真实数据集成测试结论

**日期：** 2026-06-27
**分支：** `phase2-data-pipeline`
**集成测试文件：** `tests/data_pipeline/test_integration_real_fetch.py`（4 个 `@pytest.mark.integration` 测试，默认套件 deselect）

## 1. 集成测试运行结论（诚实记录）

运行命令：`/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_integration_real_fetch.py -m integration -v`

**结果：3 passed, 1 failed。**

| 测试 | Fetcher / 数据源 | 结果 | 说明 |
|---|---|---|---|
| `test_real_a_share_quote` | `AShareQuoteFetcher` / akshare `stock_zh_a_hist`（600519 茅台） | ✅ PASS | 端到端可用，字段映射正确 |
| `test_real_us_quote` | `USQuoteFetcher` / yfinance（AAPL） | ❌ FAIL | **环境性失败**：本环境无法访问 Yahoo Finance（见 §4） |
| `test_real_fx` | `FXFetcher` / akshare `currency_boc_sina`（USD/CNY） | ✅ PASS（修正后） | 原 fetcher 字段映射错误，Task 9 已修正（见 §3） |
| `test_real_benchmark_a_share` | `BenchmarkFetcher` / akshare `index_zh_a_hist`（000300 沪深300） | ✅ PASS | 端到端可用，字段映射正确 |

> **默认套件硬门槛：** `pytest -m "not integration" -q` → **79 passed, 5 deselected**（4 个本任务新增 + 1 个 Phase 1 既有 `tests/test_probes/test_integration_real_sources.py`）。默认套件全绿。

## 2. akshare / yfinance 实际字段名 vs 代码假设

### 2.1 A 股行情 — `stock_zh_a_hist`（✅ 一致）

实测返回列（akshare 1.18.64，2026-06-20~2026-06-26）：

```
['日期', '股票代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
```

`quote.py::_normalize_quote` 假设的中文列名 `日期/开盘/收盘/最高/最低/成交量` 全部存在，映射正确。`股票代码` 列被忽略（fetcher 用入参 `code`）。`adj_factor` 不在源数据中 → fetcher 默认 `1.0`（不复权快照，符合设计）。返回 5 个交易日行（跳过周末）。

### 2.2 A 股基准 — `index_zh_a_hist`（✅ 一致）

实测返回列（symbol=000300）：

```
['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
```

`macro.py::BenchmarkFetcher` 假设的 `日期/收盘` 列均存在，映射正确。降级映射 `a_share → (000300, akshare)` 与探针实测约束一致。返回 5 个交易日行。

### 2.3 汇率 — `currency_boc_sina`（❌ 原假设错误 → Task 9 已修正）

**实测接口签名与代码假设严重不符**（此 fetcher 未被 Phase 1 五项探针覆盖，故未在探针阶段暴露）：

| 项 | 代码原假设 | 实测（akshare 1.18.64） | 处理 |
|---|---|---|---|
| `symbol` 入参 | `pair.replace("/", "")` → `"USDCNY"` | 接受**中文货币名**，如 `"美元"`；`"USDCNY"` 抛 `KeyError: 'USDCNY'` | 已修正：base ISO → 中文货币名映射表 `_BOC_SYMBOL_MAP` |
| 返回列 | 含 `收盘` 列 → rename 为 `rate` | 实际列 `['日期', '中行汇买价', '中行钞买价', '中行钞卖价/汇卖价', '央行中间价', '中行折算价']`，**无 `收盘` 列** | 已修正：`rate = 央行中间价.fillna(中行折算价)` |

实测 USD/CNY（symbol=`美元`）2026-06-20~2026-06-26 返回 6 行。`央行中间价` 仅交易日有值（周末 NaN），`中行折算价` 逐日有值且交易日与中间价一致 → 用 `fillna` 得无缺口日线序列。

**`rate` 刻度说明：** BoC 牌价按「每 100 外币单位」报价（如 USD `中行折算价`=681.30 ≈ 6.8130 CNY/USD）。本修正**保留原始刻度**（不除以 100），因 spec §3 回测汇率变动公式 `CNY/FX_end / CNY/FX_start - 1` 为比值、刻度无关。**Phase 3+ 若 `get_position_pnl(fx_rate)` 按单位折算外币资产，需在此处 /100 归一化为「CNY per 1 base unit」**——记为待办。

### 2.4 美股行情 — yfinance（⚠️ 代码正确，环境不可达）

`USQuoteFetcher` 代码（DatetimeIndex reset + Open/Close/High/Low/Volume 小写化）已被 `test_quote.py` mock 测试验证逻辑正确。集成测试失败为**环境性网络阻断**，非字段映射问题（见 §4）。

## 3. 本任务对 fetcher 的最小修正

**文件：** `src/data_pipeline/fetchers/macro.py`（`FXFetcher`）

1. 新增 `_BOC_SYMBOL_MAP`：base ISO 代码 → akshare 中文货币名（覆盖 docstring 所列全部 17 种货币）。
2. `fetch`：用映射后的中文 symbol 调 `currency_boc_sina`；未知 base 抛 `FetcherError`。
3. `rate` 列：`央行中间价.fillna(中行折算价)`（优先 PBoC 中间价，周末/假日用 BoC 折算价填缺口）；两者皆无时响亮失败。

**同步修正：** `tests/data_pipeline/test_macro.py::test_fx_fetcher_normalizes` 的 mock 原返回 `收盘` 列（基于错误假设），已改为返回实测列 `央行中间价/中行折算价`，并新增断言确认传入 akshare 的 symbol 为 `"美元"`。此为修正 fetcher 的必要连带改动，非新增测试。

修正后默认套件仍 **79 passed, 5 deselected**；`test_real_fx` 集成测试由 FAIL 转 PASS。

## 4. 美股 fetcher 失败原因（环境性，非代码缺陷）

`test_real_us_quote` 失败根因为**本运行环境无法访问 Yahoo Finance**：

- yfinance 日志：`Failed to perform, curl: (28) Connection timed out after 30018 milliseconds`（连接 Yahoo 超时）
- 重试后转为：`YFRateLimitError('Too Many Requests. Rate limited. Try after a while.')`（429 限流）
- 直接探测 `yf.download('AAPL', ...)` 返回空 DataFrame（`shape: (0, 6)`）

**判定：** 本环境对 Yahoo Finance 的出站连接被阻断/限流（curl 28 超时 + 429），属环境网络策略，**非 fetcher 代码缺陷**。`USQuoteFetcher` 逻辑已由 mock 测试覆盖验证。在可访问 Yahoo 的环境（如本地开发机）重跑本测试预期通过。**本任务不为此修改 fetcher 代码**——无代码可修。

## 5. Phase 3 待办

1. **FX `rate` 刻度归一化：** 若 Portfolio State 的 `get_position_pnl(fx_rate)` 按单位折算外币资产，需在 `FXFetcher` 将 BoC 牌价 /100 转为「CNY per 1 base unit」。当前保留原始刻度（回测比值公式刻度无关）。
2. **美股行情可用性：** 上线环境须确认可访问 Yahoo Finance；否则需备用源（如 akshare 美股接口或券商数据）。本环境无法验证。
3. **港股 fetcher（`HKQuoteFetcher`）未在本次集成测试覆盖**（brief 仅要求 4 项测试）；其 akshare 主源 `stock_hk_hist` + yfinance 备源的端到端可用性待后续手动验证。

## 6. 自检（YAGNI）

- 仅新增 `test_integration_real_fetch.py`（brief verbatim）+ 本结论文档；无多余文件。
- fetcher 修正为最小必要（FX 字段映射 + 连带 mock 修正）；未触及 quote/benchmark 等已验证正确的 fetcher。
- `@pytest.mark.integration` 标记已在 `pyproject.toml` 注册；默认套件 deselect 正常。
- 默认套件硬门槛达成（79 passed, 5 deselected）。
- 集成测试结论诚实：3 PASS / 1 FAIL（环境性），未将失败标记为通过。
