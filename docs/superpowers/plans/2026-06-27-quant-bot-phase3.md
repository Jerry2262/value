# Phase 3 — PIT 服务 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 PIT（点时间）数据切片服务——给定 `as_of_date`，返回回测/因子计算在该日期实际可见的数据切片（财报披露日 PIT、后复权价 PIT、PE 分位 PIT），是回测可信度的核心。

**Architecture:** 两层——`pit/indexer.py`（纯函数，按数据类型对 `read_parquet` 的输出做行级 PIT 过滤 + 跨分区去重，不引入新存储）+ `pit/slicer.py`（面向因子/回测的高层切片 API：`slice_quote`/`slice_fundamental`/`slice_pe_percentile`，封装 indexer 并提供分位计算）。复用 Phase 2 的 `store.read_parquet(as_of=)`（已实现分区级 PIT 过滤）和 `store.incremental_merge`（跨分区去重）。`pit_index.db` 仅在 Task 5（可选缓存）引入，v1 默认不持久化索引（YAGNI——纯函数切片足够快）。

**Tech Stack:** Python 3.12、pandas、numpy、pyarrow、pytest、pytest-mock

## Global Constraints

（摘自 spec §3.4 + Phase 2 交付，逐字执行）

- **PIT 核心约束：** 回测在任意历史日期 T 只能看到 T 日实际已公开的数据，不得混入未来信息
- 行情 PIT：后复权价，T 日只取 `date <= T` 的行
- 财务 PIT：按**财报披露日期**（`announcement_date_approx`）建立视图，T 日只能看到 `announcement_date_approx <= T` 的所有已披露财报（spec §3.4 降级：用报告期+固定滞后近似公告日）
- PE/PB 分位 PIT：lookback=5 年分位，用 T 日可用的 5 年数据，**不含 T 之后的任何数据点**
- 行业分类 PIT：按历史行业归属快照（T 日时该公司属于哪个行业），不用最新分类回溯覆盖
- 股票池 PIT：T 日已上市且未退市（含退市前最后交易日数据）
- 复用 Phase 2：`store.read_parquet(kind, market, as_of)` 已做分区级过滤（partition_date ≤ as_of）；`store.incremental_merge(new, existing, keys)` 做跨分区去重
- **Phase 2 待办约束（本阶段处理）：** read_parquet 不跨分区去重——PIT slicer 必须在分区过滤后用 incremental_merge（或等价去重）合并同主键行
- 存储格式：后复权（cleaner 已在 fetch 后变换 OHLC×adj_factor；store 存的是已清洗后复权数据）
- 列契约（Phase 2 base.py，不可漂移）：`QUOTE_COLUMNS=[date,code,market,open,high,low,close,volume,adj_factor]`、`FUNDAMENTAL_COLUMNS=[code,market,report_period,announcement_date_approx,revenue,net_profit,roe,debt_ratio,fcf,total_market_cap]`、`FX_COLUMNS=[date,base,quote,rate]`、`DELISTING_COLUMNS=[code,market,delist_date,reason]`
- SQLite 全部 WAL（用 Phase 1 `src.storage.sqlite`）；不引入新依赖

---

## File Structure（Phase 3 范围）

```
value/
├── src/pit/
│   ├── __init__.py                 # Task 1
│   ├── indexer.py                  # Task 1（行级 PIT 过滤 + 跨分区去重，纯函数）
│   ├── slicer.py                   # Task 2（高层切片 API + PE 分位）
│   └── (pit_index.db 缓存 — v1 不实现，YAGNI)
├── tests/pit/
│   ├── __init__.py                 # Task 1
│   ├── conftest.py                 # Task 1（多分区 PIT fixtures）
│   ├── test_indexer.py             # Task 1
│   ├── test_slicer.py              # Task 2
│   └── test_pit_no_lookahead.py    # Task 3（无前视回归测试，spec §10.3）
```

---

## Task 1: PIT Indexer（行级过滤 + 跨分区去重）

**Files:**
- Create: `src/pit/__init__.py`
- Create: `src/pit/indexer.py`
- Create: `tests/pit/__init__.py`
- Create: `tests/pit/conftest.py`
- Create: `tests/pit/test_indexer.py`

**Interfaces:**
- Consumes: `src.data_pipeline.store.read_parquet(kind, market, as_of)`、`store.incremental_merge(new, existing, keys)`（Phase 2）；`QUOTE_COLUMNS`、`FUNDAMENTAL_COLUMNS`、`DELISTING_COLUMNS`（Phase 2 base.py）
- Produces: `pit_quote_as_of(as_of: str, market: str, code: str | None = None) -> pd.DataFrame` — 返回 `date <= as_of` 的后复权行情（跨分区去重，按 date 取最新分区版本）
- Produces: `pit_fundamental_as_of(as_of: str, market: str, code: str | None = None) -> pd.DataFrame` — 返回 `announcement_date_approx <= as_of` 的已披露财报（跨分区去重，按 report_period 取最新；同 report_period 多版本时取 announcement_date 较新的）
- Produces: `pit_delisted_before(as_of: str, market: str) -> pd.DataFrame` — 返回 `delist_date <= as_of` 的退市股票（spec §3.5：仅 A 股有效，港美股为空）
- Produces: `pit_active_universe(as_of: str, market: str, all_codes: list[str]) -> list[str]` — 从 all_codes 中扣除 `delist_date <= as_of` 的，返回 T 日仍在市的股票

- [ ] **Step 1: 创建包初始化**

```python
# src/pit/__init__.py
"""PIT（点时间）数据切片服务：按 as_of_date 返回回测可见的数据（spec §3.4）。"""
```

```python
# tests/pit/__init__.py
```

- [ ] **Step 2: 写 tests/pit/conftest.py（多分区 PIT fixtures）**

```python
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import (
    DELISTING_COLUMNS,
    FUNDAMENTAL_COLUMNS,
    QUOTE_COLUMNS,
)
from src.data_pipeline import store


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    """把 VALUE_DATA_DIR 指向临时目录，刷新 config。"""
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    import importlib
    import src.config as cfg
    importlib.reload(cfg)
    yield tmp_path
    importlib.reload(cfg)


@pytest.fixture
def multi_partition_quote(isolated_data_dir):
    """两个拉取分区的行情：分区 2026-06-25 含 06-23/06-24；分区 2026-06-27 含 06-26 修正版 06-24 + 06-26。

    用于验证：(1) 行级 date<=as_of 过滤；(2) 跨分区去重——06-24 在两个分区都有，
    取最新分区（2026-06-27）的版本。
    """
    part1 = pd.DataFrame([
        {"date": "2026-06-23", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10, "volume": 100, "adj_factor": 1.0},
        {"date": "2026-06-24", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10.0, "volume": 100, "adj_factor": 1.0},
    ], columns=QUOTE_COLUMNS)
    part2 = pd.DataFrame([
        # 06-24 修正：close 10.0 → 10.5（应覆盖 part1 的 06-24）
        {"date": "2026-06-24", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100, "adj_factor": 1.0},
        {"date": "2026-06-26", "code": "C", "market": "a_share",
         "open": 11, "high": 12, "low": 10, "close": 11, "volume": 110, "adj_factor": 1.0},
    ], columns=QUOTE_COLUMNS)
    store.write_parquet_partition(part1, "market", "2026-06-25", "a_share")
    store.write_parquet_partition(part2, "market", "2026-06-27", "a_share")


@pytest.fixture
def multi_partition_fundamental(isolated_data_dir):
    """两个披露分区的财报：分区 2024-04-30（年报 2023 披露）含 2023 年报；
    分区 2024-08-31（中报 2024 披露）含 2024 中报 + 2023 年报修正版。

    用于验证：(1) 行级 announcement_date_approx<=as_of 过滤；
    (2) 跨分区去重——2023 年报在两分区都有，取较新披露版本。
    """
    part1 = pd.DataFrame([
        {"code": "C", "market": "a_share", "report_period": "2023-12-31",
         "announcement_date_approx": "2024-04-30",
         "revenue": 1.5e11, "net_profit": 7e10, "roe": 30.0,
         "debt_ratio": 20.0, "fcf": 5e10, "total_market_cap": 1e12},
    ], columns=FUNDAMENTAL_COLUMNS)
    part2 = pd.DataFrame([
        # 2023 年报修正：roe 30.0 → 31.0（应覆盖 part1）
        {"code": "C", "market": "a_share", "report_period": "2023-12-31",
         "announcement_date_approx": "2024-04-30",
         "revenue": 1.5e11, "net_profit": 7e10, "roe": 31.0,
         "debt_ratio": 20.0, "fcf": 5e10, "total_market_cap": 1e12},
        {"code": "C", "market": "a_share", "report_period": "2024-06-30",
         "announcement_date_approx": "2024-08-31",
         "revenue": 8e10, "net_profit": 4e10, "roe": 32.0,
         "debt_ratio": 21.0, "fcf": 3e10, "total_market_cap": 1.1e12},
    ], columns=FUNDAMENTAL_COLUMNS)
    store.write_parquet_partition(part1, "fundamental", "2024-04-30", "a_share")
    store.write_parquet_partition(part2, "fundamental", "2024-08-31", "a_share")


@pytest.fixture
def a_share_delisting(isolated_data_dir):
    """A 股退市列表：C1 2020 退市，C2 2025 退市，C3 在市。"""
    df = pd.DataFrame([
        {"code": "C1", "market": "a_share", "delist_date": "2020-01-01", "reason": "强制退市"},
        {"code": "C2", "market": "a_share", "delist_date": "2025-06-30", "reason": "吸收合并"},
    ], columns=DELISTING_COLUMNS)
    store.write_parquet_partition(df, "delisting", "2026-06-27", "a_share")
```

- [ ] **Step 3: 写失败测试 tests/pit/test_indexer.py**

```python
import pandas as pd
import pytest

from src.pit.indexer import (
    pit_active_universe,
    pit_delisted_before,
    pit_fundamental_as_of,
    pit_quote_as_of,
)


def test_pit_quote_filters_future_rows(multi_partition_quote):
    """as_of=2026-06-25 → 只含 date<=2026-06-25 的行（06-26 被过滤）。"""
    df = pit_quote_as_of("2026-06-25", "a_share")
    dates = set(df["date"])
    assert dates == {"2026-06-23", "2026-06-24"}
    assert "2026-06-26" not in dates


def test_pit_quote_dedup_across_partitions(multi_partition_quote):
    """跨分区去重：06-24 在两分区都有，取最新分区版本（close=10.5）。"""
    df = pit_quote_as_of("2026-06-27", "a_share")
    row_24 = df[df["date"] == "2026-06-24"].iloc[0]
    assert row_24["close"] == 10.5  # part2 修正值，非 part1 的 10.0
    assert len(df) == 3  # 06-23, 06-24(去重), 06-26


def test_pit_quote_code_filter(multi_partition_quote):
    df = pit_quote_as_of("2026-06-27", "a_share", code="C")
    assert (df["code"] == "C").all()


def test_pit_quote_empty_when_no_data(isolated_data_dir):
    df = pit_quote_as_of("2026-06-27", "us")
    assert df.empty


def test_pit_fundamental_filters_by_announcement_date(multi_partition_fundamental):
    """as_of=2024-05-15 → 只含 announcement_date_approx<=2024-05-15（2023年报已披露，2024中报未披露）。"""
    df = pit_fundamental_as_of("2024-05-15", "a_share")
    periods = set(df["report_period"])
    assert periods == {"2023-12-31"}
    assert "2024-06-30" not in periods


def test_pit_fundamental_dedup_takes_latest_disclosure(multi_partition_fundamental):
    """2023 年报在两分区都有（part1 roe=30, part2 roe=31），取较新披露版本（31.0）。"""
    df = pit_fundamental_as_of("2024-05-15", "a_share")
    row_2023 = df[df["report_period"] == "2023-12-31"].iloc[0]
    assert row_2023["roe"] == 31.0


def test_pit_fundamental_includes_both_after_later_disclosure(multi_partition_fundamental):
    """as_of=2024-09-01 → 2023年报 + 2024中报都可见。"""
    df = pit_fundamental_as_of("2024-09-01", "a_share")
    assert set(df["report_period"]) == {"2023-12-31", "2024-06-30"}


def test_pit_delisted_before(a_share_delisting):
    """as_of=2024-01-01 → C1 已退市，C2 未退市。"""
    df = pit_delisted_before("2024-01-01", "a_share")
    codes = set(df["code"])
    assert codes == {"C1"}


def test_pit_active_universe(a_share_delisting):
    """as_of=2024-01-01：C1 已退市剔除，C2/C3 在市。"""
    active = pit_active_universe("2024-01-01", "a_share", all_codes=["C1", "C2", "C3"])
    assert set(active) == {"C2", "C3"}


def test_pit_active_universe_all_active_later(a_share_delisting):
    """as_of=2026-01-01：C1/C2 都已退市，只剩 C3。"""
    active = pit_active_universe("2026-01-01", "a_share", all_codes=["C1", "C2", "C3"])
    assert set(active) == {"C3"}
```

- [ ] **Step 4: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/pit/test_indexer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.pit'`

- [ ] **Step 5: 写 src/pit/indexer.py**

```python
"""PIT Indexer：行级 PIT 过滤 + 跨分区去重（spec §3.4）。

复用 Phase 2 store.read_parquet(as_of=) 的分区级过滤（partition_date <= as_of），
在此基础上做行级过滤（date/announcement_date <= as_of）与跨分区去重。
"""
from __future__ import annotations

import pandas as pd

from src.data_pipeline import store


def pit_quote_as_of(as_of: str, market: str, code: str | None = None) -> pd.DataFrame:
    """返回 as_of 日可见的后复权行情（date <= as_of，跨分区去重）。

    分区级过滤已由 read_parquet(as_of=) 完成（partition_date <= as_of）。
    此处再做行级 date <= as_of 过滤 + 跨分区去重（同 (code,date) 取最新分区版本）。
    """
    # 读所有 partition_date <= as_of 的分区（含历史分区，用于去重取最新版本）
    raw = store.read_parquet("market", market, as_of=as_of)
    if raw.empty:
        return raw
    df = raw[raw["date"] <= as_of].copy()
    if code is not None:
        df = df[df["code"] == code]
    if df.empty:
        return df
    # 跨分区去重：同 (code,date) 保留最后一条（read_parquet 按 partition_date 升序拼接，
    # 后拼接的分区更新 → keep="last"）
    df = df.drop_duplicates(subset=["code", "date"], keep="last")
    return df.reset_index(drop=True)


def pit_fundamental_as_of(as_of: str, market: str, code: str | None = None) -> pd.DataFrame:
    """返回 as_of 日可见的已披露财报（announcement_date_approx <= as_of，跨分区去重）。

    spec §3.4：T 日只能看到 <= T 的所有已披露财报。
    跨分区去重：同 report_period 保留较新披露版本（announcement_date 较大者）。
    """
    raw = store.read_parquet("fundamental", market, as_of=as_of)
    if raw.empty:
        return raw
    # 行级过滤：announcement_date_approx <= as_of（缺失则视为不可见）
    mask = raw["announcement_date_approx"].notna() & (raw["announcement_date_approx"] <= as_of)
    df = raw[mask].copy()
    if code is not None:
        df = df[df["code"] == code]
    if df.empty:
        return df
    # 跨分区去重：同 (code, report_period) 保留 announcement_date_approx 较大者（较新披露）
    df = df.sort_values("announcement_date_approx")
    df = df.drop_duplicates(subset=["code", "report_period"], keep="last")
    return df.reset_index(drop=True)


def pit_delisted_before(as_of: str, market: str) -> pd.DataFrame:
    """返回 as_of 日前已退市的股票（delist_date <= as_of）。

    spec §3.5（v1.4）：仅 A 股有效；港美股退市为人工补录非重点。
    """
    from src.data_pipeline.fetchers.base import DELISTING_COLUMNS
    raw = store.read_parquet("delisting", market, as_of=as_of)
    if raw.empty:
        return pd.DataFrame(columns=DELISTING_COLUMNS)
    mask = raw["delist_date"].notna() & (raw["delist_date"] <= as_of)
    return raw[mask].reset_index(drop=True)


def pit_active_universe(as_of: str, market: str, all_codes: list[str]) -> list[str]:
    """从 all_codes 中扣除 as_of 日前已退市的，返回 T 日仍在市的股票。"""
    delisted = pit_delisted_before(as_of, market)
    delisted_codes = set(delisted["code"]) if not delisted.empty else set()
    return [c for c in all_codes if c not in delisted_codes]
```

- [ ] **Step 6: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/pit/test_indexer.py -v`
Expected: PASS（10 个测试）

- [ ] **Step 7: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 全部 PASS（Phase 1+2 的 80 + 本任务 10 = 90）

- [ ] **Step 8: 提交**

```bash
git add src/pit/__init__.py src/pit/indexer.py tests/pit/__init__.py tests/pit/conftest.py tests/pit/test_indexer.py
git commit -m "feat(pit): PIT indexer — 行级过滤 + 跨分区去重"
```

---

## Task 2: PIT Slicer（高层切片 API + PE 分位）

**Files:**
- Create: `src/pit/slicer.py`
- Create: `tests/pit/test_slicer.py`

**Interfaces:**
- Consumes: `pit.indexer.pit_quote_as_of`、`pit_fundamental_as_of`、`pit_active_universe`（Task 1）；`QUOTE_COLUMNS`、`FUNDAMENTAL_COLUMNS`（Phase 2）
- Produces: `slice_quote_panel(as_of: str, market: str, codes: list[str]) -> pd.DataFrame` — 多股票行情面板（date<=as_of，仅 codes），用于因子计算
- Produces: `slice_latest_fundamental(as_of: str, market: str, codes: list[str]) -> pd.DataFrame` — 每只股票截至 as_of 最新一期财报（report_period 最大）
- Produces: `pe_percentile(as_of: str, market: str, code: str, lookback_years: int = 5) -> float | None` — PE 历史分位（PIT：用 as_of 前 lookback 年的总市值/净利润，不含未来），None 表示数据不足
- Produces: `pe_ratio_at(as_of: str, market: str, code: str) -> float | None` — T 日 PE（总市值法：total_market_cap / net_profit，spec §4.4），用 T 日最新财报的 net_profit

- [ ] **Step 1: 写失败测试 tests/pit/test_slicer.py**

```python
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import FUNDAMENTAL_COLUMNS, QUOTE_COLUMNS
from src.data_pipeline import store
from src.pit.slicer import (
    pe_percentile,
    pe_ratio_at,
    slice_latest_fundamental,
    slice_quote_panel,
)


@pytest.fixture
def pe_panel(isolated_data_dir):
    """构造 6 年行情 + 财务，用于 PE 分位测试。

    PE = total_market_cap / net_profit。构造历史 PE 序列以便验证分位。
    """
    # 6 年行情（每年一个收盘价点，2020-2025），后复权 close 即价
    quote_rows = []
    for yr, close in [(2020, 10), (2021, 20), (2022, 15), (2023, 30), (2024, 25), (2025, 40)]:
        quote_rows.append({"date": f"{yr}-06-30", "code": "C", "market": "a_share",
                           "open": close, "high": close, "low": close, "close": close,
                           "volume": 100, "adj_factor": 1.0})
    store.write_parquet_partition(pd.DataFrame(quote_rows, columns=QUOTE_COLUMNS),
                                  "market", "2025-12-31", "a_share")

    # 6 年财报，net_profit 固定 1.0，total_market_cap = close（使 PE = close）
    # 这样 PE 历史序列 = [10,20,15,30,25,40]，便于手算分位
    fund_rows = []
    for yr in range(2020, 2026):
        fund_rows.append({"code": "C", "market": "a_share",
                          "report_period": f"{yr}-12-31",
                          "announcement_date_approx": f"{yr+1}-04-30",
                          "revenue": 1e10, "net_profit": 1.0, "roe": 20.0,
                          "debt_ratio": 30.0, "fcf": 1e9,
                          "total_market_cap": {2020:10,2021:20,2022:15,2023:30,2024:25,2025:40}[yr]})
    store.write_parquet_partition(pd.DataFrame(fund_rows, columns=FUNDAMENTAL_COLUMNS),
                                  "fundamental", "2026-04-30", "a_share")


def test_slice_quote_panel_filters_codes_and_date(pe_panel):
    df = slice_quote_panel("2025-06-30", "a_share", ["C", "X"])
    assert set(df["code"]) == {"C"}  # X 无数据
    assert (df["date"] <= "2025-06-30").all()


def test_slice_latest_fundamental(pe_panel):
    """as_of=2026-05-01 → 最新可见财报是 2025 年报（披露日 2026-04-30）。"""
    df = slice_latest_fundamental("2026-05-01", "a_share", ["C"])
    assert len(df) == 1
    assert df.iloc[0]["report_period"] == "2025-12-31"


def test_pe_ratio_at(pe_panel):
    """T=2026-05-01 的 PE = total_market_cap(40) / net_profit(1.0) = 40.0。"""
    pe = pe_ratio_at("2026-05-01", "a_share", "C")
    assert pe == 40.0


def test_pe_percentile_uses_only_past(pe_panel):
    """as_of=2026-05-01，lookback=5 年 → 用 2021~2025 的 PE 序列 [20,15,30,25,40]。

    当前 PE=40 是序列最大值 → 分位 = 100%（5 个值中 40 排第 5/5）。
    验证不含 2020（超出5年窗口）和不含未来。
    """
    pct = pe_percentile("2026-05-01", "a_share", "C", lookback_years=5)
    # 序列 [20,15,30,25,40]，当前 40，rank=5/5 → percentile=1.0
    assert pct == 1.0


def test_pe_percentile_mid(pe_panel):
    """as_of=2024-05-01，当前 PE=30（2023年报），lookback=5 → 用 2019~2023，但只有 2020~2023=[10,20,15,30]。
    30 是最大 → 1.0。"""
    pct = pe_percentile("2024-05-01", "a_share", "C", lookback_years=5)
    assert pct == 1.0


def test_pe_percentile_none_when_insufficient(isolated_data_dir):
    """数据不足 → None。"""
    pct = pe_percentile("2026-05-01", "a_share", "NOPE", lookback_years=5)
    assert pct is None
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/pit/test_slicer.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/pit/slicer.py**

```python
"""PIT Slicer：面向因子/回测的高层切片 API（spec §3.4）。

封装 indexer，提供多股票面板、最新财报、PE 分位等。
"""
from __future__ import annotations

import pandas as pd

from src.pit.indexer import pit_fundamental_as_of, pit_quote_as_of


def slice_quote_panel(as_of: str, market: str, codes: list[str]) -> pd.DataFrame:
    """多股票行情面板（date <= as_of，仅 codes）。"""
    if not codes:
        return pd.DataFrame()
    frames = [pit_quote_as_of(as_of, market, code=c) for c in codes]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def slice_latest_fundamental(as_of: str, market: str, codes: list[str]) -> pd.DataFrame:
    """每只股票截至 as_of 最新一期财报（report_period 最大）。"""
    if not codes:
        return pd.DataFrame()
    frames = [pit_fundamental_as_of(as_of, market, code=c) for c in codes]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    all_f = pd.concat(frames, ignore_index=True)
    # 每只股票取 report_period 最大的一期
    idx = all_f.groupby("code")["report_period"].idxmax()
    return all_f.loc[idx].reset_index(drop=True)


def pe_ratio_at(as_of: str, market: str, code: str) -> float | None:
    """T 日 PE（总市值法：total_market_cap / net_profit，spec §4.4）。

    用 T 日最新可见财报的 net_profit。net_profit<=0 或缺失 → None。
    """
    fund = pit_fundamental_as_of(as_of, market, code=code)
    if fund.empty:
        return None
    latest = fund.loc[fund["report_period"].idxmax()]
    np_ = latest.get("net_profit")
    mcap = latest.get("total_market_cap")
    if pd.isna(np_) or pd.isna(mcap) or np_ <= 0:
        return None
    return float(mcap) / float(np_)


def pe_percentile(
    as_of: str, market: str, code: str, lookback_years: int = 5
) -> float | None:
    """PE 历史分位（PIT，spec §3.4/§4.4）。

    用 as_of 前 lookback_years 年内每年一期的 PE 序列，计算当前 PE 在序列中的分位。
    PIT 约束：只用 announcement_date_approx <= as_of 的财报，不含未来。
    数据不足（<2 个历史点或无当前 PE）→ None。
    """
    fund = pit_fundamental_as_of(as_of, market, code=code)
    if fund.empty:
        return None
    fund = fund.copy()
    fund["report_period_dt"] = pd.to_datetime(fund["report_period"], errors="coerce")
    as_of_dt = pd.to_datetime(as_of)
    window_start = as_of_dt - pd.DateOffset(years=lookback_years)
    # PIT 窗口：report_period <= as_of 且 >= window_start
    in_window = fund[
        (fund["report_period_dt"] <= as_of_dt)
        & (fund["report_period_dt"] >= window_start)
    ].copy()
    # 每年取一期（report_period 末月），按年去重取最新
    in_window["year"] = in_window["report_period_dt"].dt.year
    in_window = in_window.sort_values("report_period_dt")
    in_window = in_window.drop_duplicates(subset=["year"], keep="last")
    # 计算 PE = total_market_cap / net_profit
    in_window["pe"] = in_window["total_market_cap"] / in_window["net_profit"]
    valid = in_window.dropna(subset=["pe"])
    valid = valid[valid["pe"] > 0]
    if len(valid) < 2:
        return None
    # 当前 PE = 最新一期（report_period 最大）
    current_pe = valid.sort_values("report_period_dt").iloc[-1]["pe"]
    series = valid["pe"].values
    # 分位 = 序列中 <= current_pe 的比例
    pct = float((series <= current_pe).sum()) / len(series)
    return pct
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/pit/test_slicer.py -v`
Expected: PASS（6 个测试）

- [ ] **Step 5: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 90 + 6 = 96 passed

- [ ] **Step 6: 提交**

```bash
git add src/pit/slicer.py tests/pit/test_slicer.py
git commit -m "feat(pit): slicer — 多股面板/最新财报/PE分位（PIT 安全）"
```

---

## Task 3: PIT 无前视回归测试（spec §10.3）

**Files:**
- Create: `tests/pit/test_pit_no_lookahead.py`

> spec §10.3：选取已知财报披露日 D，测试 D-1 日的 PIT 切片不包含该财报数据；测试 PE 分位只用截至当日历史数据。这是回测可信度的守门测试。

- [ ] **Step 1: 写 tests/pit/test_pit_no_lookahead.py**

```python
"""PIT 无前视回归测试（spec §10.3）。

守门测试：确保回测/因子计算在任意日期 T 不会看到 T 之后的数据。
若这些测试失败，回测结果不可信——立即修复，不得跳过。
"""
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import FUNDAMENTAL_COLUMNS, QUOTE_COLUMNS
from src.data_pipeline import store
from src.pit.indexer import pit_fundamental_as_of, pit_quote_as_of
from src.pit.slicer import pe_percentile, pe_ratio_at


@pytest.fixture
def known_disclosure(isolated_data_dir):
    """构造一份 2023 年报，披露日 2024-04-30（announcement_date_approx）。

    spec §10.3：D=2024-04-30，测试 D-1=2024-04-29 的 PIT 切片不含该财报。
    """
    fund = pd.DataFrame([
        {"code": "C", "market": "a_share", "report_period": "2023-12-31",
         "announcement_date_approx": "2024-04-30",
         "revenue": 1e11, "net_profit": 5e9, "roe": 25.0,
         "debt_ratio": 30.0, "fcf": 2e9, "total_market_cap": 5e10},
    ], columns=FUNDAMENTAL_COLUMNS)
    store.write_parquet_partition(fund, "fundamental", "2024-04-30", "a_share")
    # 配套行情（2023-12-31 收盘）
    quote = pd.DataFrame([
        {"date": "2023-12-31", "code": "C", "market": "a_share",
         "open": 50, "high": 51, "low": 49, "close": 50, "volume": 1000, "adj_factor": 1.0},
    ], columns=QUOTE_COLUMNS)
    store.write_parquet_partition(quote, "market", "2024-04-30", "a_share")


def test_pit_fundamental_excludes_future_disclosure(known_disclosure):
    """D-1=2024-04-29 的 PIT 切片不含 2023 年报（披露日 2024-04-30）。"""
    df = pit_fundamental_as_of("2024-04-29", "a_share")
    assert df.empty or "2023-12-31" not in set(df["report_period"])


def test_pit_fundamental_includes_after_disclosure(known_disclosure):
    """D=2024-04-30 当天可见该财报。"""
    df = pit_fundamental_as_of("2024-04-30", "a_share")
    assert "2023-12-31" in set(df["report_period"])


def test_pe_ratio_none_before_disclosure(known_disclosure):
    """D-1 日无可见财报 → PE 为 None（不用未来财报算 PE）。"""
    assert pe_ratio_at("2024-04-29", "a_share", "C") is None


def test_pe_ratio_available_after_disclosure(known_disclosure):
    """D 日可见财报 → PE = 5e10/5e9 = 10.0。"""
    pe = pe_ratio_at("2024-04-30", "a_share", "C")
    assert pe == 10.0


def test_pe_percentile_none_before_disclosure(known_disclosure):
    """D-1 日数据不足 → 分位 None（不偷看未来）。"""
    assert pe_percentile("2024-04-29", "a_share", "C", lookback_years=5) is None


def test_pit_quote_excludes_future_dates(isolated_data_dir):
    """行情 PIT：T 日切片不含 date > T 的行。"""
    quote = pd.DataFrame([
        {"date": "2024-01-01", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10, "volume": 100, "adj_factor": 1.0},
        {"date": "2024-01-05", "code": "C", "market": "a_share",
         "open": 12, "high": 13, "low": 11, "close": 12, "volume": 110, "adj_factor": 1.0},
    ], columns=QUOTE_COLUMNS)
    store.write_parquet_partition(quote, "market", "2024-01-10", "a_share")
    df = pit_quote_as_of("2024-01-03", "a_share")
    assert set(df["date"]) == {"2024-01-01"}
    assert "2024-01-05" not in set(df["date"])
```

- [ ] **Step 2: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/pit/test_pit_no_lookahead.py -v`
Expected: PASS（6 个测试）

- [ ] **Step 3: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 96 + 6 = 102 passed

- [ ] **Step 4: 提交**

```bash
git add tests/pit/test_pit_no_lookahead.py
git commit -m "test(pit): 无前视回归测试（spec §10.3，回测可信度守门）"
```

- [ ] **Step 5: 运行完整 PIT 套件确认全绿**

Run: `/home/jerry/value/.venv/bin/pytest tests/pit/ -v`
Expected: 22 passed（indexer 10 + slicer 6 + no_lookahead 6）

> **Phase 3 完成条件：** 全部非集成测试通过（102 passed）；PIT 无前视测试全绿。此后方可进入 Phase 4（Factor Engine）。
