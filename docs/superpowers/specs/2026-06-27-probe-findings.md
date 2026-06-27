# 五项数据探针实测结论与降级决策

> 日期：2026-06-27
> 来源：Task 10 真实数据集成运行（`@pytest.mark.integration`，联网）
> 环境：akshare 1.18.64 + yfinance 1.4.1，`.venv`，分支 `phase1-data-probes`
> 测试命令：`pytest tests/test_probes/test_integration_real_sources.py -m integration -v`

> **v1.4 定位更新：** 退市处理整体降级为非重点（spec §3.5）。下文探针 2、3（港股/美股退市）现为**参考性低优先级**探针——其价值在于确认"免费源不可得"并触发回测标注路径，不阻塞策略开发。探针 1、4、5 仍是硬门槛。

## 0. 集成测试总体结论

| 项 | 结论 |
|---|---|
| 默认套件（`-m "not integration"`） | 36 passed, 1 deselected — 绿色，集成测试被正确跳过 |
| 集成测试（`-m integration`） | **未在 ~47 分钟观察窗口内完成**（探针 1 全量 300 只串行调用新浪财报接口被限速，~6–10s/只）；详见 §6.2 |
| §12 硬门槛 | 逐探针小样本实测均无 FAILED（见 §6.3）；WARNING 触发既定降级路径，可接受。全量集成测试因性能瓶颈未跑完，**未实际验证通过** |

> 降级总原则：FAILED = 取数完全失败（不可接受）；WARNING = 部分缺失/不可用（可接受，走降级）。

## 1. 探针 1 — A 股财报披露日期覆盖率（announcement_date_coverage）

**实测：**
- `stock_zh_a_spot_em()` 返回 5867 只 A 股代码（耗时 ~90s，58 页分页拉取）。
- 抽样 8 只代码调用 `stock_financial_report_sina`，返回资产负债表行，列为 `报告日, 流动资产, 货币资金, ...`。
- **关键字段缺失：返回结构中无 `公告日` 列**，仅有 `报告日`。因此 `announcement_date` 恒为 `None`。
- 8 只代码得 151 行，覆盖率 = **0.0%**（< 80% 阈值）→ WARNING。
- 全量 `sample_size=300` 时，逐只调用 `stock_financial_report_sina`（~2s/只）耗时 ~10 分钟以上，是该探针的性能瓶颈。

**降级决策：启用「报告期 + 固定滞后」近似（spec §12 #1 降级方案）。**
- 年报：报告期 + 4 个月；中报：报告期 + 2 个月；季报：报告期 + 1 个月。
- Phase 2 fetchers 应改用真正含 `公告日` 的接口（如 `stock_report_disclosure` 或交易所披露日历），并取消逐只串行调用（改批量接口）。

## 2. 探针 2 — 港股退市列表完整性（hk_delisting_completeness）

**实测：**
- `stock_hk_spot_em()` 返回 4689 行（耗时 ~66s）。**注意：该接口返回的是当前在市港股列表，并非退市列表**；`delist_date`/`reason` 均为空字符串（RealFetcher 占位）。
- 已知退市样本 5 只：`03692, 02333, 01378, 00358, 01876`，5 只全部命中在市列表 → hits = 5/5 ≥ 4 阈值 → PASSED。

**降级决策：探针数值通过，但底层数据并非真退市列表，需人工补录。**
- 「命中」是因为这些代码当前仍在 `stock_hk_spot_em` 返回中（疑为未真退市或接口含已退市代码），不代表退市数据可用。
- 回测前需人工补录港股历史退市清单（代码 + 退市日 + 原因），否则港股回测须标注「退市数据覆盖不足」。

## 3. 探针 3 — 美股退市列表可行性（us_delisting_feasibility）

**实测：**
- `_us_delisting_source` 返回 `{available: False, source: "", sample_count: 0}`（yfinance 不提供退市列表）→ WARNING。

**降级决策：确认降级 — 美股仅标注，不做退市修正。**
- 回测仅对 A 股做退市修正；美股标注「未修正幸存者偏差，收益可能被高估」。
- Phase 2 若需修正，须引入付费源（如 CRSP / Compustat）或自建退市表。

## 4. 探针 4 — 价值风格基准指数覆盖率（value_benchmark_coverage）

**实测：**
- `benchmark_daily`（沪深300，symbol=000300，2015-01-01 ~ 2026-06-27）：2788 条日线，首 `2015-01-05 收盘 3641.54`，末 `2026-06-26 收盘 4868.22`。
- `trading_calendar`（新浪 `tool_trade_date_hist_sina`）：8797 个日期，`1990-12-19 ~ 2026-12-31`。
- missing_rate = **68.3%**（> 5% 阈值）→ WARNING → 降级 csi300_value → csi300。

**降级决策：降级沪深300价值 → 沪深300宽基。**
- 注：缺失率高主要因交易日历起点 1990 而基准序列起点 2015，属「日历范围错配」放大缺失率；基准序列本身 11.5 年 2788 条（~242/年）合理。
- Phase 2 应：①按基准实际起止日裁剪日历再算缺失率；②尝试获取真正的「沪深300价值」指数代码（当前 RealFetcher 硬编码 000300 即沪深300宽基本身，未取到价值风格指数）。

## 5. 探针 5 — 财务字段完整性（fundamental_field_completeness）

**实测：**
- `fundamental_sample`：对 a_share/us/hk 三个 market 均调用 `stock_zh_a_spot_em()`（仅返回 A 股实时行情），各取 head(20)，得 300 行（20×3×5 字段）。
- 字段缺失率（三市场相同，因均取自 A 股 spot）：

| 字段 | 列名映射 | 缺失率 |
|---|---|---|
| roe | `ROE` | 100%（列不存在） |
| fcf | — | 100%（硬编码 None） |
| revenue | `营收` | 100%（列不存在） |
| net_profit | `净利润` | 100%（列不存在） |
| market_cap | `总市值` | 0%（列存在） |

- 仅 `market_cap` 可用；4/5 字段超 15% 阈值 → WARNING。

**降级决策：超阈值字段该期从因子体系移除并记 data_quality_log。**
- Phase 2 fetchers 须改用财务报表接口（如 `stock_financial_abstract` / 利润表/资产负债表/现金流量表）取 roe/revenue/net_profit/fcf；`stock_zh_a_spot_em` 仅适合取总市值。
- 同时三市场应分别走对应数据源（A 股 akshare、美股 yfinance、港股 akshare 港股接口），当前 RealFetcher 三市场均打 A 股 spot，属占位实现。

## 6. 集成测试运行记录

### 6.1 默认套件（必须绿色 — 硬门槛）

```
$ pytest -m "not integration" -v
... (36 项) ...
======================= 36 passed, 1 deselected in 1.78s =======================
```

集成测试被正确 deselect；RealFetcher 因 akshare/yfinance 懒加载（写在方法内部、非模块顶部）未被非集成测试导入，故默认套件不依赖 akshare 即可通过。

### 6.2 集成测试（`-m integration`，联网）

命令：`pytest tests/test_probes/test_integration_real_sources.py -m integration -v`

**实测结论：未能在观察窗口（~47 分钟）内完成。** 进程始终活跃（CPU 0.5–4%、保持一条到数据源的 keep-alive 连接），但探针 1 的全量 `sample_size=300` 串行调用 `stock_financial_report_sina`（新浪财报接口）在持续负载下被限速，单次约 6–10 秒，300 只累计需 30–50 分钟以上，构成不可接受的实际运行瓶颈。

pytest 在测试函数返回前不输出逐探针日志，故日志仅停留在 `collected 1 item` 直至结束/超时。

### 6.3 逐探针实测（小样本，已成功完成）

为保证 §12 硬门槛有实测依据，另以临时脚本（`/tmp/probe_explore.py`，未入库）逐探针调用 RealFetcher 各方法，对探针 1 取 8 只代码抽样、其余探针按真实参数运行，全部成功完成。结果：

| 探针 | 状态 | 关键实测值 |
|---|---|---|
| 1 announcement_date_coverage | WARNING | 8 只代码得 151 行；`stock_financial_report_sina` 返回列含 `报告日` 但**无 `公告日`** → coverage=0.0% (<80%)；非空 → 非 FAILED |
| 2 hk_delisting_completeness | PASSED | `stock_hk_spot_em` 返回 4689 行（在市列表，非退市列表）；已知样本 5/5 命中 ≥4 |
| 3 us_delisting_feasibility | WARNING | `{available: False}` → 无免费源，降级（仅 A 股修正） |
| 4 value_benchmark_coverage | WARNING | 沪深300日线 2788 条(2015-2026)；交易日历 8797 日(1990-2026)；missing_rate=68.3% (>5%) → 降级为宽基 |
| 5 fundamental_field_completeness | WARNING | 300 行；仅 `market_cap`(总市值) 0% 缺失，roe/revenue/net_profit/fcf 均 100% 缺失 → 超 15% 阈值 |

**五探针均无 FAILED**（探针 1 因 rows 非空而为 WARNING，非 FAILED）。因此集成测试的断言 `assert not failed` 预期能成立，但全量测试因探针 1 性能瓶颈未在观察窗口内跑完，**未实际验证通过**。

### 6.4 对 Phase 2 的性能提示

探针 1 的 `_a_share_announcement_dates` 逐只串行调用是设计缺陷（spec §12 探针阶段可接受，Phase 2 须改批量接口）。建议 Phase 2 fetchers：①换用批量披露日历接口；②并发或缓存；③`stock_zh_a_spot_em` 在探针 5 中被三市场各调用一次（共 3×~90s），应缓存复用。

## 7. Phase 2 fetchers 行动项汇总

1. A 股披露日：换用含 `公告日` 的批量披露日历接口；否则启用「报告期+固定滞后」近似。
2. 港股退市：人工补录历史退市清单（akshare 无现成退市列表）。
3. 美股退市：确认无免费源，回测仅标注不修正（或引入付费源）。
4. 基准指数：获取真正「沪深300价值」指数代码；按基准起止日裁剪交易日历再算缺失率。
5. 财务字段：改用财务报表接口取 roe/revenue/net_profit/fcf；三市场分别走对应源。
