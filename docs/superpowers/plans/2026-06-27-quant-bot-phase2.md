# Phase 2 — Data Pipeline Fetchers 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现三地市场（A股/美股/港股）的行情、财务、汇率、基准、A股退市列表的 fetchers + 清洗 + Parquet 日期分区存储，为 Phase 3 PIT 服务提供可靠的本地数据源。

**Architecture:** 三层结构——`fetchers/`（按数据类型+市场封装 akshare/yfinance 真实调用，懒加载，批量接口优先）→ `cleaners.py`（统一列名、类型、后复权、异常标记）→ `store.py`（不可变 Parquet 日期分区写入 + 增量更新）。每个 fetcher 实现 `Fetcher` 协议（Phase 1 已定义在 `src/data_pipeline/probes/base.py`），但用专用接口名而非 `fetch(kind=...)`，便于类型清晰。真实网络调用全部隔离在 fetchers 内部，cleaners/store 用纯 pandas 逻辑可单测。

**Tech Stack:** Python 3.12、akshare、yfinance、pandas、numpy、pyarrow、pytest、pytest-mock

## Global Constraints

（摘自 spec §3 + §1.4 + Phase 1 探针实测，逐字执行）

- 存储格式：**后复权**为默认存储（后复权价不被未来分红拆股修改，回测安全）
- 本地存储：`data/raw/market/YYYY-MM-DD/{a_share,us,hk}.parquet`（按拉取日期分区，不可变）；`data/raw/fundamental/YYYY-MM-DD/`（按财报披露日期分区，PIT 关键）；`data/raw/macro/YYYY-MM-DD/{rates,fx}.parquet`
- 增量更新：每次只拉最新数据，不做全量覆盖
- SQLite 全部开启 WAL 模式（用 Phase 1 的 `src.storage.sqlite`）
- 拉取失败重试 3 次，指数退避（1s/3s/9s）；连续 3 天失败标记 STALE
- akshare/yfinance 懒加载（import 写在函数内部，非模块顶部），使非集成套件不依赖网络库
- **探针实测约束（关键，违反会导致数据无效）：**
  - A 股财报：akshare 的 `stock_financial_report_sina` 无「公告日」列，仅有「报告日」→ **PIT 降级为「报告期 + 固定滞后」**（年报+4月/中报+2月/季报+1月），存入字段 `announcement_date_approx`
  - 财务字段（roe/营收/净利润/fcf）：**禁止用 `stock_zh_a_spot_em`**（探针实测该接口无这些列，仅总市值可用）；须用财务报表接口（`stock_financial_abstract` 等）
  - 基准指数：沪深300价值指数免费源缺失率 68.3% → **降级为沪深300宽基**（symbol `000300`）；标普500价值→标普500；恒生综合→恒生指数
  - A 股退市列表：用 akshare，尽力修正（spec §3.5）；港股退市是人工补录非重点，美股不修正
  - **批量接口优先**：禁止逐只串行调用（探针1 的 `stock_financial_report_sina` 逐只循环导致 47 分钟超时）；财务用 `stock_financial_abstract` 批量，行情用 `index_zh_a_hist`/yfinance 多票批量
- 货币：本地货币 CNY/USD/HKD；汇率日线 USD/CNY、HKD/CNY
- API key 通过环境变量，不写入配置文件或代码
- 写操作串行化（Phase 1 storage 已实现）

---

## File Structure（Phase 2 范围）

```
value/
├── src/data_pipeline/
│   ├── fetchers/
│   │   ├── __init__.py                 # Task 1
│   │   ├── base.py                     # Task 1（重试装饰器 + 标准化列契约 + Fetcher 基类）
│   │   ├── quote.py                    # Task 2（三地日线行情 fetcher）
│   │   ├── fundamental.py              # Task 3（三地财务 fetcher，含公告日降级）
│   │   ├── macro.py                    # Task 4（汇率 + 基准指数 fetcher）
│   │   └── delisting.py               # Task 5（A股退市列表 fetcher，港美股人工补录占位）
│   ├── cleaners.py                     # Task 6（统一列名/类型/后复权/异常标记）
│   ├── store.py                        # Task 7（Parquet 日期分区写入 + 增量更新 + 读接口）
│   └── pipeline.py                     # Task 8（编排：fetch→clean→store 一日全链路）
├── tests/data_pipeline/
│   ├── __init__.py                     # Task 1
│   ├── conftest.py                     # Task 1（sample DataFrame fixtures）
│   ├── test_base.py                    # Task 1
│   ├── test_quote.py                   # Task 2
│   ├── test_fundamental.py             # Task 3
│   ├── test_macro.py                   # Task 4
│   ├── test_delisting.py               # Task 5
│   ├── test_cleaners.py                # Task 6
│   ├── test_store.py                   # Task 7
│   ├── test_pipeline.py                # Task 8
│   └── test_integration_real_fetch.py  # Task 9（@integration 联网）
└── (existing) src/config.py, src/storage/sqlite.py, src/data_pipeline/probes/*
```

---

## Task 1: Fetcher 基础设施（重试 + 标准化列契约 + 基类）

**Files:**
- Create: `src/data_pipeline/fetchers/__init__.py`
- Create: `src/data_pipeline/fetchers/base.py`
- Create: `tests/data_pipeline/__init__.py`
- Create: `tests/data_pipeline/conftest.py`
- Create: `tests/data_pipeline/test_base.py`

**Interfaces:**
- Consumes: `src.config.DATA_DIR` / `METADATA_DIR`（Phase 1）
- Produces: `retry_with_backoff(retries=3, delays=(1,3,9))` 装饰器
- Produces: 标准化列名常量 `QUOTE_COLUMNS`、`FUNDAMENTAL_COLUMNS`、`FX_COLUMNS`、`BENCHMARK_COLUMNS`、`DELISTING_COLUMNS`（下游 cleaners/store 依赖这些确切名）
- Produces: `FetcherError` 异常类（fetcher 失败时抛出，被 store/pipeline 捕获标记 STALE）

- [ ] **Step 1: 创建包初始化与测试目录**

```python
# src/data_pipeline/fetchers/__init__.py
"""数据拉取器：封装 akshare/yfinance 真实调用（懒加载）。"""
```

```python
# tests/data_pipeline/__init__.py
```

- [ ] **Step 2: 写 tests/data_pipeline/conftest.py（sample DataFrame fixtures，下游测试复用）**

```python
import pandas as pd
import pytest


@pytest.fixture
def sample_a_share_quote_raw():
    """模拟 akshare index/个股接口返回的原始 DataFrame（中文列名）。

    后续 cleaner 测试和 fetcher mock 测试复用。
    """
    return pd.DataFrame({
        "日期": ["2026-06-25", "2026-06-26"],
        "开盘": [10.0, 10.5],
        "收盘": [10.2, 10.4],
        "最高": [10.5, 10.6],
        "最低": [9.9, 10.1],
        "成交量": [100000, 120000],
    })


@pytest.fixture
def sample_us_quote_raw():
    """模拟 yfinance 返回的原始 DataFrame（英文列名、DatetimeIndex）。"""
    idx = pd.to_datetime(["2026-06-24", "2026-06-25"])
    return pd.DataFrame(
        {
            "Open": [100.0, 101.0],
            "Close": [101.5, 100.8],
            "High": [102.0, 101.5],
            "Low": [99.5, 100.0],
            "Volume": [1000000, 1100000],
        },
        index=idx,
    )


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    """把 VALUE_DATA_DIR 指向临时目录，刷新 config 模块常量。"""
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    import importlib
    import src.config as cfg
    importlib.reload(cfg)
    yield tmp_path
    importlib.reload(cfg)
```

- [ ] **Step 3: 写失败测试 tests/data_pipeline/test_base.py**

```python
import time

import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import (
    DELISTING_COLUMNS,
    FETCHER_MARKET_STALE,
    FX_COLUMNS,
    FUNDAMENTAL_COLUMNS,
    FetcherError,
    QUOTE_COLUMNS,
    BENCHMARK_COLUMNS,
    retry_with_backoff,
)


def test_standard_columns_are_canonical():
    """标准化列名常量是下游契约，名字不可漂移。"""
    assert QUOTE_COLUMNS == ["date", "code", "market", "open", "high", "low", "close", "volume", "adj_factor"]
    assert FUNDAMENTAL_COLUMNS == [
        "code", "market", "report_period", "announcement_date_approx",
        "revenue", "net_profit", "roe", "debt_ratio", "fcf", "total_market_cap",
    ]
    assert FX_COLUMNS == ["date", "base", "quote", "rate"]
    assert BENCHMARK_COLUMNS == ["date", "code", "market", "close"]
    assert DELISTING_COLUMNS == ["code", "market", "delist_date", "reason"]


def test_retry_succeeds_on_first_try(mocker):
    calls = {"n": 0}

    @retry_with_backoff(retries=3, delays=(0, 0, 0))
    def ok():
        calls["n"] += 1
        return "done"

    assert ok() == "done"
    assert calls["n"] == 1


def test_retry_succeeds_after_transient_failures(mocker):
    """前两次抛异常、第三次成功 → 返回成功值，调用 3 次。"""
    mocker.patch("src.data_pipeline.fetchers.base.time.sleep")  # 跳过真实退避
    calls = {"n": 0}

    @retry_with_backoff(retries=3, delays=(0, 0, 0))
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


def test_retry_raises_after_exhausting(mocker):
    mocker.patch("src.data_pipeline.fetchers.base.time.sleep")
    calls = {"n": 0}

    @retry_with_backoff(retries=3, delays=(0, 0, 0))
    def always_fail():
        calls["n"] += 1
        raise RuntimeError("boom")

    with pytest.raises(FetcherError) as exc_info:
        always_fail()
    assert calls["n"] == 3
    assert "boom" in str(exc_info.value)


def test_fetcher_error_wraps_cause():
    cause = ValueError("network down")
    err = FetcherError("拉取 A 股行情失败", cause=cause)
    assert err.__cause__ is cause
    assert "A 股行情" in str(err)


def test_stale_constant_value():
    assert FETCHER_MARKET_STALE == "STALE"
```

- [ ] **Step 4: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_base.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.data_pipeline.fetchers.base'`

- [ ] **Step 5: 写 src/data_pipeline/fetchers/base.py**

```python
"""Fetcher 基础设施：重试装饰器、标准化列名契约、异常类。"""
from __future__ import annotations

import functools
import time
from typing import Callable, TypeVar

T = TypeVar("T")

# 下游 cleaners/store 依赖的标准化列名（不可漂移）
QUOTE_COLUMNS = [
    "date", "code", "market", "open", "high", "low", "close", "volume", "adj_factor",
]
FUNDAMENTAL_COLUMNS = [
    "code", "market", "report_period", "announcement_date_approx",
    "revenue", "net_profit", "roe", "debt_ratio", "fcf", "total_market_cap",
]
FX_COLUMNS = ["date", "base", "quote", "rate"]
BENCHMARK_COLUMNS = ["date", "code", "market", "close"]
DELISTING_COLUMNS = ["code", "market", "delist_date", "reason"]

# 连续失败标记值（spec §3.6：连续3天失败标记 STALE）
FETCHER_MARKET_STALE = "STALE"


class FetcherError(Exception):
    """Fetcher 拉取失败（重试耗尽后抛出）。"""

    def __init__(self, message: str, *, cause: Exception | None = None):
        super().__init__(message)
        if cause is not None:
            self.__cause__ = cause


def retry_with_backoff(retries: int = 3, delays: tuple = (1, 3, 9)):
    """重试装饰器：失败按 delays 退避重试，耗尽后抛 FetcherError 包装原异常。

    delays 长度应 >= retries-1；不足时末次退避为 0。
    """
    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs) -> T:
            last_exc: Exception | None = None
            for attempt in range(retries):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt < retries - 1:
                        delay = delays[attempt] if attempt < len(delays) else 0
                        time.sleep(delay)
            raise FetcherError(f"{fn.__name__} 重试 {retries} 次仍失败", cause=last_exc)
        return wrapper
    return decorator
```

- [ ] **Step 6: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_base.py -v`
Expected: PASS（5 个测试全过）

- [ ] **Step 7: 提交**

```bash
git add src/data_pipeline/fetchers/__init__.py src/data_pipeline/fetchers/base.py tests/data_pipeline/__init__.py tests/data_pipeline/conftest.py tests/data_pipeline/test_base.py
git commit -m "feat(data_pipeline): fetcher 基础设施 — 重试装饰器与标准化列契约"
```

---

## Task 2: 三地日线行情 Fetcher

**Files:**
- Create: `src/data_pipeline/fetchers/quote.py`
- Create: `tests/data_pipeline/test_quote.py`

**Interfaces:**
- Consumes: `retry_with_backoff`、`FetcherError`、`QUOTE_COLUMNS`（Task 1）
- Produces: `AShareQuoteFetcher.fetch_daily(code: str, start: str, end: str) -> pd.DataFrame`（列 = QUOTE_COLUMNS，adj_factor 后复权因子，akshare 懒加载）
- Produces: `USQuoteFetcher.fetch_daily(code: str, start: str, end: str) -> pd.DataFrame`（yfinance 懒加载，自动调整 adj_factor）
- Produces: `HKQuoteFetcher.fetch_daily(code: str, start: str, end: str) -> pd.DataFrame`（akshare 主 + yfinance 备）

- [ ] **Step 1: 写失败测试 tests/data_pipeline/test_quote.py**

```python
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import QUOTE_COLUMNS
from src.data_pipeline.fetchers.quote import (
    AShareQuoteFetcher,
    HKQuoteFetcher,
    USQuoteFetcher,
)


def test_a_share_fetcher_normalizes_columns(mocker, sample_a_share_quote_raw):
    """akshare 返回中文列名 → 标准化列，并补 code/market/adj_factor。"""
    mocker.patch(
        "akshare.stock_zh_a_hist",
        return_value=sample_a_share_quote_raw,
    )
    df = AShareQuoteFetcher().fetch_daily("600519", "2026-06-25", "2026-06-26")
    assert list(df.columns) == QUOTE_COLUMNS
    assert len(df) == 2
    assert (df["code"] == "600519").all()
    assert (df["market"] == "a_share").all()
    assert (df["adj_factor"] == 1.0).all()  # akshare 默认不复权，因子=1；后复权由 cleaner 处理
    assert df["close"].iloc[0] == 10.2


def test_a_share_fetcher_retry_then_fail(mocker):
    """连续失败 → 重试耗尽抛 FetcherError。"""
    mocker.patch("src.data_pipeline.fetchers.quote.time.sleep")
    mocker.patch(
        "akshare.stock_zh_a_hist",
        side_effect=RuntimeError("network"),
    )
    from src.data_pipeline.fetchers.base import FetcherError
    with pytest.raises(FetcherError):
        AShareQuoteFetcher().fetch_daily("600519", "2026-06-25", "2026-06-26")


def test_us_fetcher_normalizes_yfinance(mocker, sample_us_quote_raw):
    mocker.patch("yfinance.download", return_value=sample_us_quote_raw)
    df = USQuoteFetcher().fetch_daily("AAPL", "2026-06-24", "2026-06-25")
    assert list(df.columns) == QUOTE_COLUMNS
    assert (df["code"] == "AAPL").all()
    assert (df["market"] == "us").all()
    assert df["close"].iloc[0] == 101.5


def test_hk_fetcher_uses_akshare_primary(mocker, sample_a_share_quote_raw):
    mocker.patch("akshare.stock_hk_hist", return_value=sample_a_share_quote_raw)
    df = HKQuoteFetcher().fetch_daily("00700", "2026-06-25", "2026-06-26")
    assert list(df.columns) == QUOTE_COLUMNS
    assert (df["market"] == "hk").all()
    assert (df["code"] == "00700").all()


def test_hk_fetcher_fallback_to_yfinance(mocker, sample_us_quote_raw):
    """akshare 失败 → 降级 yfinance 备源（spec §3.6 港股主备互补）。"""
    mocker.patch("src.data_pipeline.fetchers.quote.time.sleep")
    mocker.patch("akshare.stock_hk_hist", side_effect=RuntimeError("akshare down"))
    mocker.patch("yfinance.download", return_value=sample_us_quote_raw)
    df = HKQuoteFetcher().fetch_daily("00700", "2026-06-24", "2026-06-25")
    assert len(df) == 2
    assert (df["market"] == "hk").all()
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_quote.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/fetchers/quote.py**

```python
"""三地日线行情 Fetcher（akshare/yfinance 懒加载）。

列契约：QUOTE_COLUMNS（date/code/market/open/high/low/close/volume/adj_factor）。
adj_factor 默认 1.0（不复权快照）；后复权变换在 cleaners 中应用。
"""
from __future__ import annotations

import time

import pandas as pd

from src.data_pipeline.fetchers.base import FetcherError, QUOTE_COLUMNS, retry_with_backoff


def _normalize_quote(df: pd.DataFrame, code: str, market: str) -> pd.DataFrame:
    """把原始行情统一为 QUOTE_COLUMNS 列。"""
    df = df.copy()
    # 统一 date 列为字符串 YYYY-MM-DD
    if "日期" in df.columns:
        df = df.rename(columns={
            "日期": "date", "开盘": "open", "收盘": "close",
            "最高": "high", "最低": "low", "成交量": "volume",
        })
    elif "Date" in df.columns:
        df = df.rename(columns={
            "Date": "date", "Open": "open", "Close": "close",
            "High": "high", "Low": "low", "Volume": "volume",
        })
    else:
        # yfinance 用 DatetimeIndex
        if not isinstance(df.index, pd.RangeIndex) and df.index.name is None:
            df = df.reset_index().rename(columns={"index": "date"})
        if "Date" in df.columns:
            df = df.rename(columns={"Date": "date"})

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["code"] = code
    df["market"] = market
    df["adj_factor"] = df.get("adj_factor", 1.0)
    if "adj_factor" not in df.columns:
        df["adj_factor"] = 1.0
    return df[QUOTE_COLUMNS]


class AShareQuoteFetcher:
    """A 股日线行情（akshare stock_zh_a_hist）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch_daily(self, code: str, start: str, end: str) -> pd.DataFrame:
        import akshare as ak
        try:
            raw = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=start.replace("-", ""), end_date=end.replace("-", ""),
                adjust="",  # 不复权快照；后复权在 cleaner 处理
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"akshare stock_zh_a_hist 失败 {code}") from exc
        return _normalize_quote(raw, code, "a_share")


class USQuoteFetcher:
    """美股日线行情（yfinance download）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch_daily(self, code: str, start: str, end: str) -> pd.DataFrame:
        import yfinance as yf
        try:
            raw = yf.download(code, start=start, end=end, progress=False, auto_adjust=False)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"yfinance 失败 {code}") from exc
        if raw.empty:
            raise RuntimeError(f"yfinance 返回空 {code}")
        return _normalize_quote(raw, code, "us")


class HKQuoteFetcher:
    """港股日线行情：akshare 主源，失败降级 yfinance 备源（spec §3.6）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch_daily(self, code: str, start: str, end: str) -> pd.DataFrame:
        # 主源 akshare
        try:
            import akshare as ak
            raw = ak.stock_hk_hist(
                symbol=code, period="daily",
                start_date=start.replace("-", ""), end_date=end.replace("-", ""),
                adjust="",
            )
            return _normalize_quote(raw, code, "hk")
        except Exception:  # noqa: BLE001
            pass  # 降级备源
        # 备源 yfinance（港股代码加 .HK 后缀）
        try:
            import yfinance as yf
            raw = yf.download(f"{code}.HK", start=start, end=end, progress=False, auto_adjust=False)
            if raw.empty:
                raise RuntimeError("yfinance 港股返回空")
            return _normalize_quote(raw, code, "hk")
        except Exception as exc:  # noqa: BLE001
            raise FetcherError(f"港股 {code} 主备源均失败") from exc
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_quote.py -v`
Expected: PASS（5 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/fetchers/quote.py tests/data_pipeline/test_quote.py
git commit -m "feat(data_pipeline): 三地日线行情 fetcher（akshare/yfinance 懒加载）"
```

---

## Task 3: 三地财务 Fetcher（含公告日降级）

**Files:**
- Create: `src/data_pipeline/fetchers/fundamental.py`
- Create: `tests/data_pipeline/test_fundamental.py`

**Interfaces:**
- Consumes: `retry_with_backoff`、`FetcherError`、`FUNDAMENTAL_COLUMNS`（Task 1）
- Produces: `AShareFundamentalFetcher.fetch(code: str) -> pd.DataFrame`（列 = FUNDAMENTAL_COLUMNS，`announcement_date_approx` = 报告期+固定滞后）
- Produces: `approx_announcement_date(report_period: str, report_type: str) -> str` 纯函数（年报+4月/中报+2月/季报+1月）
- Produces: `USFundamentalFetcher.fetch(code: str) -> pd.DataFrame`、`HKFundamentalFetcher.fetch(code: str) -> pd.DataFrame`

- [ ] **Step 1: 写失败测试 tests/data_pipeline/test_fundamental.py**

```python
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import FUNDAMENTAL_COLUMNS
from src.data_pipeline.fetchers.fundamental import (
    AShareFundamentalFetcher,
    approx_announcement_date,
)


def test_approx_announcement_date_annual():
    """年报报告期 12-31 → +4月 → 次年 4-30。"""
    assert approx_announcement_date("2023-12-31", "annual") == "2024-04-30"


def test_approx_announcement_date_interim():
    """中报报告期 06-30 → +2月 → 08-31。"""
    assert approx_announcement_date("2023-06-30", "interim") == "2023-08-31"


def test_approx_announcement_date_q1():
    """一季报报告期 03-31 → +1月 → 04-30。"""
    assert approx_announcement_date("2023-03-31", "q1") == "2023-04-30"


def test_approx_announcement_date_q3():
    assert approx_announcement_date("2023-09-30", "q3") == "2023-10-31"


def test_approx_announcement_date_invalid_month():
    """非标准报告期 → 返回 None（该期不计入 PIT）。"""
    assert approx_announcement_date("2023-05-15", "annual") is None


def test_a_share_fundamental_uses_financial_abstract(mocker):
    """财务字段须用 stock_financial_abstract（探针实测：禁用 stock_zh_a_spot_em）。"""
    raw = pd.DataFrame({
        "股票代码": ["600519", "600519"],
        "报告期": ["2023-12-31", "2023-06-30"],
        "营业收入": [1.5e11, 7e10],
        "净利润": [7e10, 3.5e10],
        "净资产收益率": [30.0, 29.0],
        "资产负债率": [20.0, 22.0],
        "经营现金流": [5e10, 2.5e10],
    })
    mocker.patch("akshare.stock_financial_abstract", return_value=raw)
    df = AShareFundamentalFetcher().fetch("600519")
    assert list(df.columns) == FUNDAMENTAL_COLUMNS
    assert len(df) == 2
    assert (df["code"] == "600519").all()
    assert (df["market"] == "a_share").all()
    # 公告日降级：年报 2023-12-31 → 2024-04-30
    assert df.loc[df["report_period"] == "2023-12-31", "announcement_date_approx"].iloc[0] == "2024-04-30"
    # roe 是数值
    assert df["roe"].iloc[0] == 30.0


def test_a_share_fundamental_retry_exhausts(mocker):
    mocker.patch("src.data_pipeline.fetchers.fundamental.time.sleep")
    mocker.patch("akshare.stock_financial_abstract", side_effect=RuntimeError("net"))
    from src.data_pipeline.fetchers.base import FetcherError
    with pytest.raises(FetcherError):
        AShareFundamentalFetcher().fetch("600519")
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_fundamental.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/fetchers/fundamental.py**

```python
"""三地财务 Fetcher。

探针实测约束：
- A 股 akshare 无「公告日」列 → announcement_date_approx = 报告期 + 固定滞后（spec §3.4 降级）
- 财务字段（roe/营收/净利润/fcf）禁用 stock_zh_a_spot_em（无这些列），用 stock_financial_abstract
"""
from __future__ import annotations

import calendar
from datetime import date, timedelta

import pandas as pd

from src.data_pipeline.fetchers.base import (
    FUNDAMENTAL_COLUMNS,
    FetcherError,
    retry_with_backoff,
)

# 报告期 → 公告日近似滞后（月）
_LAG_MONTHS = {"annual": 4, "interim": 2, "q1": 1, "q3": 1}
# 报告期月份 → 报告类型
_PERIOD_MONTH_TO_TYPE = {3: "q1", 6: "interim", 9: "q3", 12: "annual"}


def _add_months(d: date, months: int) -> date:
    """加月（处理月末，如 1-31 +1月 → 2-28/29）。"""
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, min(d.day, last_day))


def approx_announcement_date(report_period: str, report_type: str) -> str | None:
    """报告期 + 固定滞后 → 近似公告日（YYYY-MM-DD）。

    spec §3.4 降级：年报+4月、中报+2月、季报+1月。
    report_period 须为标准月末（03-31/06-30/09-30/12-31），否则返回 None。
    """
    try:
        rp = date.fromisoformat(report_period)
    except (ValueError, TypeError):
        return None
    if rp.month not in _PERIOD_MONTH_TO_TYPE:
        return None
    lag = _LAG_MONTHS[report_type]
    approx = _add_months(rp, lag)
    return approx.isoformat()


def _classify_period(report_period: str) -> str | None:
    """根据报告期月份推断报告类型。"""
    try:
        rp = date.fromisoformat(report_period)
    except (ValueError, TypeError):
        return None
    return _PERIOD_MONTH_TO_TYPE.get(rp.month)


def _normalize_a_share(raw: pd.DataFrame, code: str) -> pd.DataFrame:
    df = raw.copy()
    rename = {
        "股票代码": "code", "报告期": "report_period",
        "营业收入": "revenue", "净利润": "net_profit",
        "净资产收益率": "roe", "资产负债率": "debt_ratio",
        "经营现金流": "fcf",  # 近似：经营现金流作为 FCF 代理（探针阶段 FCF 字段常缺）
        "总市值": "total_market_cap",
    }
    df = df.rename(columns=rename)
    df["market"] = "a_share"
    # 公告日降级
    df["announcement_date_approx"] = df.apply(
        lambda r: (
            approx_announcement_date(r["report_period"], _classify_period(r["report_period"]))
            if pd.notna(r.get("report_period")) else None
        ),
        axis=1,
    )
    # 缺失字段补 None
    for col in FUNDAMENTAL_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df["code"] = code
    return df[FUNDAMENTAL_COLUMNS]


class AShareFundamentalFetcher:
    """A 股财务（akshare stock_financial_abstract，批量接口）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, code: str) -> pd.DataFrame:
        import akshare as ak
        try:
            raw = ak.stock_financial_abstract(symbol=code)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"akshare stock_financial_abstract 失败 {code}") from exc
        if raw is None or raw.empty:
            return pd.DataFrame(columns=FUNDAMENTAL_COLUMNS)
        return _normalize_a_share(raw, code)


class USFundamentalFetcher:
    """美股财务（yfinance Ticker.financials/info）。v1 用基础字段。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, code: str) -> pd.DataFrame:
        import yfinance as yf
        try:
            tkr = yf.Ticker(code)
            info = tkr.info or {}
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"yfinance 财务失败 {code}") from exc
        row = {
            "code": code, "market": "us",
            "report_period": None, "announcement_date_approx": None,
            "revenue": info.get("totalRevenue"),
            "net_profit": info.get("netIncomeToCommon"),
            "roe": info.get("returnOnEquity"),
            "debt_ratio": None,
            "fcf": info.get("freeCashflow"),
            "total_market_cap": info.get("marketCap"),
        }
        return pd.DataFrame([row])[FUNDAMENTAL_COLUMNS]


class HKFundamentalFetcher:
    """港股财务（akshare 主，字段不全时部分为 None）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, code: str) -> pd.DataFrame:
        import akshare as ak
        try:
            raw = ak.stock_financial_hk_report_em(symbol=code, symbol_type="资产负债表")
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"akshare 港股财务失败 {code}") from exc
        if raw is None or raw.empty:
            return pd.DataFrame(columns=FUNDAMENTAL_COLUMNS)
        df = raw.copy()
        df["code"] = code
        df["market"] = "hk"
        df["announcement_date_approx"] = None  # 港股 PIT 标注不修正（spec §3.5）
        for col in FUNDAMENTAL_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df[FUNDAMENTAL_COLUMNS]
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_fundamental.py -v`
Expected: PASS（7 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/fetchers/fundamental.py tests/data_pipeline/test_fundamental.py
git commit -m "feat(data_pipeline): 三地财务 fetcher + 公告日报告期滞后降级"
```

---

## Task 4: 汇率 + 基准指数 Fetcher

**Files:**
- Create: `src/data_pipeline/fetchers/macro.py`
- Create: `tests/data_pipeline/test_macro.py`

**Interfaces:**
- Consumes: `retry_with_backoff`、`FetcherError`、`FX_COLUMNS`、`BENCHMARK_COLUMNS`（Task 1）
- Produces: `FXFetcher.fetch(pair: str, start: str, end: str) -> pd.DataFrame`（pair 如 "USD/CNY"，列 = FX_COLUMNS）
- Produces: `BenchmarkFetcher.fetch(market: str, start: str, end: str) -> pd.DataFrame`（market ∈ {a_share, us, hk}，自动降级：a_share→沪深300宽基 000300，us→标普500 ^GSPC，hk→恒生 ^HSI）

- [ ] **Step 1: 写失败测试 tests/data_pipeline/test_macro.py**

```python
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import BENCHMARK_COLUMNS, FX_COLUMNS
from src.data_pipeline.fetchers.macro import BenchmarkFetcher, FXFetcher


def test_fx_fetcher_normalizes(mocker):
    raw = pd.DataFrame({
        "日期": ["2026-06-25", "2026-06-26"],
        "收盘": [7.20, 7.18],
    })
    mocker.patch("akshare.currency_boc_sina", return_value=raw)
    df = FXFetcher().fetch("USD/CNY", "2026-06-25", "2026-06-26")
    assert list(df.columns) == FX_COLUMNS
    assert (df["base"] == "USD").all()
    assert (df["quote"] == "CNY").all()
    assert df["rate"].iloc[0] == 7.20


def test_fx_fetcher_pair_parse():
    """pair 字符串解析为 base/quote。"""
    from src.data_pipeline.fetchers.macro import _parse_pair
    assert _parse_pair("USD/CNY") == ("USD", "CNY")
    assert _parse_pair("HKD/CNY") == ("HKD", "CNY")


def test_benchmark_a_share_degrades_to_csi300(mocker):
    """A 股基准降级为沪深300宽基（探针实测：沪深300价值缺失率68%）。"""
    raw = pd.DataFrame({
        "日期": ["2026-06-25", "2026-06-26"],
        "收盘": [4800.0, 4820.0],
    })
    mock_hist = mocker.patch("akshare.index_zh_a_hist", return_value=raw)
    df = BenchmarkFetcher().fetch("a_share", "2026-06-25", "2026-06-26")
    assert list(df.columns) == BENCHMARK_COLUMNS
    assert (df["market"] == "a_share").all()
    # 确认调用了宽基 000300（而非价值指数）
    args, kwargs = mock_hist.call_args
    assert kwargs.get("symbol") == "000300"


def test_benchmark_us_uses_yfinance(mocker):
    raw = pd.DataFrame({"Close": [5000.0, 5050.0]},
                       index=pd.to_datetime(["2026-06-24", "2026-06-25"]))
    mocker.patch("yfinance.download", return_value=raw)
    df = BenchmarkFetcher().fetch("us", "2026-06-24", "2026-06-25")
    assert list(df.columns) == BENCHMARK_COLUMNS
    assert (df["market"] == "us").all()
    assert df["close"].iloc[0] == 5000.0


def test_benchmark_hk(mocker):
    raw = pd.DataFrame({"Close": [18000.0, 18100.0]},
                       index=pd.to_datetime(["2026-06-25", "2026-06-26"]))
    mocker.patch("yfinance.download", return_value=raw)
    df = BenchmarkFetcher().fetch("hk", "2026-06-25", "2026-06-26")
    assert (df["market"] == "hk").all()
    assert len(df) == 2
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_macro.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/fetchers/macro.py**

```python
"""汇率 + 基准指数 Fetcher。

探针实测约束：沪深300价值指数缺失率 68.3% → 降级宽基。
"""
from __future__ import annotations

import pandas as pd

from src.data_pipeline.fetchers.base import (
    BENCHMARK_COLUMNS,
    FX_COLUMNS,
    FetcherError,
    retry_with_backoff,
)

# 降级映射（spec §6.1 + 探针实测）：市场 → (代码, 源)
BENCHMARK_MAP = {
    "a_share": ("000300", "akshare"),   # 沪深300宽基（价值指数降级）
    "us": ("^GSPC", "yfinance"),        # 标普500宽基
    "hk": ("^HSI", "yfinance"),         # 恒生指数（恒生综合降级）
}


def _parse_pair(pair: str) -> tuple[str, str]:
    base, quote = pair.split("/")
    return base, quote


class FXFetcher:
    """汇率日线（akshare currency_boc_sina）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, pair: str, start: str, end: str) -> pd.DataFrame:
        import akshare as ak
        base, quote = _parse_pair(pair)
        try:
            raw = ak.currency_boc_sina(
                symbol=pair.replace("/", ""),  # akshare 用 USDCNY 形式
                start_date=start.replace("-", ""), end_date=end.replace("-", ""),
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"akshare 汇率失败 {pair}") from exc
        df = raw.copy()
        if "日期" in df.columns:
            df = df.rename(columns={"日期": "date", "收盘": "rate"})
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["base"] = base
        df["quote"] = quote
        return df[FX_COLUMNS]


class BenchmarkFetcher:
    """基准指数日线（自动降级为宽基）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self, market: str, start: str, end: str) -> pd.DataFrame:
        if market not in BENCHMARK_MAP:
            raise FetcherError(f"未知市场 {market}")
        symbol, source = BENCHMARK_MAP[market]
        if source == "akshare":
            import akshare as ak
            try:
                raw = ak.index_zh_a_hist(
                    symbol=symbol, period="daily",
                    start_date=start.replace("-", ""), end_date=end.replace("-", ""),
                )
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"akshare 基准失败 {symbol}") from exc
            df = raw.rename(columns={"日期": "date", "收盘": "close"})
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df["code"] = symbol
            df["market"] = market
            return df[BENCHMARK_COLUMNS]
        else:  # yfinance
            import yfinance as yf
            try:
                raw = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=False)
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"yfinance 基准失败 {symbol}") from exc
            if raw.empty:
                raise RuntimeError(f"yfinance 基准返回空 {symbol}")
            df = raw.reset_index().rename(columns={"Date": "date", "Close": "close"})
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df["code"] = symbol
            df["market"] = market
            return df[BENCHMARK_COLUMNS]
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_macro.py -v`
Expected: PASS（5 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/fetchers/macro.py tests/data_pipeline/test_macro.py
git commit -m "feat(data_pipeline): 汇率 + 基准指数 fetcher（基准降级宽基）"
```

---

## Task 5: A 股退市列表 Fetcher（港美股人工补录占位）

**Files:**
- Create: `src/data_pipeline/fetchers/delisting.py`
- Create: `tests/data_pipeline/test_delisting.py`

**Interfaces:**
- Consumes: `retry_with_backoff`、`FetcherError`、`DELISTING_COLUMNS`（Task 1）
- Produces: `AShareDelistingFetcher.fetch() -> pd.DataFrame`（akshare 退市列表，列 = DELISTING_COLUMNS）
- Produces: `load_manual_delisting(market: str, csv_path: Path) -> pd.DataFrame`（港美股人工补录 CSV 读取，spec §3.5）

- [ ] **Step 1: 写失败测试 tests/data_pipeline/test_delisting.py**

```python
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import DELISTING_COLUMNS
from src.data_pipeline.fetchers.delisting import (
    AShareDelistingFetcher,
    load_manual_delisting,
)


def test_a_share_delisting_normalizes(mocker):
    raw = pd.DataFrame({
        "公司代码": ["000001", "000002"],
        "退市日期": ["2020-01-01", "2021-06-30"],
        "退市原因": ["强制退市", "吸收合并"],
    })
    mocker.patch("akshare.stock_info_sh_name_code", return_value=raw)  # 占位接口名
    df = AShareDelistingFetcher().fetch()
    assert list(df.columns) == DELISTING_COLUMNS
    assert (df["market"] == "a_share").all()
    assert len(df) == 2


def test_load_manual_delisting_hk(tmp_path):
    """港美股退市人工补录 CSV（spec §3.5 非重点，人工维护）。"""
    csv = tmp_path / "hk_delist.csv"
    csv.write_text("code,delist_date,reason\n00700,2020-01-01,私有化\n00358,2019-06-30,收购\n", encoding="utf-8")
    df = load_manual_delisting("hk", csv)
    assert list(df.columns) == DELISTING_COLUMNS
    assert (df["market"] == "hk").all()
    assert len(df) == 2
    assert df.loc[df["code"] == "00700", "reason"].iloc[0] == "私有化"


def test_load_manual_delisting_missing_file(tmp_path):
    """CSV 不存在 → 返回空 DataFrame（不报错，降级为标注）。"""
    df = load_manual_delisting("us", tmp_path / "nonexistent.csv")
    assert list(df.columns) == DELISTING_COLUMNS
    assert len(df) == 0
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_delisting.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/fetchers/delisting.py**

```python
"""退市列表 Fetcher。

spec §3.5（v1.4 降级）：A 股用 akshare 尽力修正；港美股人工补录 CSV，非重点。
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data_pipeline.fetchers.base import DELISTING_COLUMNS, retry_with_backoff


class AShareDelistingFetcher:
    """A 股退市列表（akshare）。"""

    @retry_with_backoff(retries=3, delays=(1, 3, 9))
    def fetch(self) -> pd.DataFrame:
        import akshare as ak
        try:
            # akshare 退市股票列表接口（字段名以实际为准，cleaner 容错）
            raw = ak.stock_info_sh_name_code(symbol="退市")
        except Exception:
            try:
                raw = ak.stock_info_a_code_name()  # 退市接口变更时的兜底
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError("akshare 退市列表失败") from exc
        df = raw.copy()
        # 容错列名映射
        col_map = {
            "公司代码": "code", "code": "code", "证券代码": "code",
            "退市日期": "delist_date", "delist_date": "delist_date",
            "退市原因": "reason", "reason": "reason",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        df["market"] = "a_share"
        for col in DELISTING_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df[DELISTING_COLUMNS]


def load_manual_delisting(market: str, csv_path: Path) -> pd.DataFrame:
    """读取港美股人工补录退市 CSV（spec §3.5：非重点，人工维护）。

    CSV 列：code,delist_date,reason。文件不存在 → 空 DataFrame（降级为标注）。
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        return pd.DataFrame(columns=DELISTING_COLUMNS)
    df = pd.read_csv(csv_path, dtype={"code": str})
    df["market"] = market
    for col in DELISTING_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[DELISTING_COLUMNS]
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_delisting.py -v`
Expected: PASS（3 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/fetchers/delisting.py tests/data_pipeline/test_delisting.py
git commit -m "feat(data_pipeline): A股退市 fetcher + 港美股人工补录占位"
```

---

## Task 6: Cleaners（统一列名/类型/后复权/异常标记）

**Files:**
- Create: `src/data_pipeline/cleaners.py`
- Create: `tests/data_pipeline/test_cleaners.py`

**Interfaces:**
- Consumes: `QUOTE_COLUMNS`、`FUNDAMENTAL_COLUMNS`（Task 1）；fetcher 输出的 DataFrame
- Produces: `clean_quote(df: pd.DataFrame) -> pd.DataFrame`（数值类型转换 + 后复权变换：`open/high/low/close *= adj_factor`，volume 不变；异常 OHLC 标记）
- Produces: `clean_fundamental(df: pd.DataFrame) -> pd.DataFrame`（数值类型转换，缺失标 None）
- Produces: `flag_quote_anomalies(df: pd.DataFrame) -> pd.DataFrame`（返回异常行：low>high / close<=0 / 单日涨跌>50%）

- [ ] **Step 1: 写失败测试 tests/data_pipeline/test_cleaners.py**

```python
import pandas as pd
import pytest

from src.data_pipeline.cleaners import clean_fundamental, clean_quote, flag_quote_anomalies
from src.data_pipeline.fetchers.base import QUOTE_COLUMNS


def _quote_df(rows):
    return pd.DataFrame(rows, columns=QUOTE_COLUMNS)


def test_clean_quote_applies_adj_factor():
    """后复权：OHLC 乘 adj_factor，volume 不变（spec §3.3）。"""
    df = _quote_df([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": "10.0", "high": "10.5", "low": "9.9", "close": "10.2",
         "volume": "100000", "adj_factor": "2.0"},
    ])
    out = clean_quote(df)
    assert out["close"].iloc[0] == 20.4  # 10.2 * 2.0
    assert out["open"].iloc[0] == 20.0
    assert out["volume"].iloc[0] == 100000  # volume 不复权
    assert out["adj_factor"].iloc[0] == 2.0


def test_clean_quote_dtypes_numeric():
    df = _quote_df([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": "10", "high": "11", "low": "9", "close": "10",
         "volume": "100", "adj_factor": "1"},
    ])
    out = clean_quote(df)
    assert pd.api.types.is_numeric_dtype(out["close"])
    assert pd.api.types.is_numeric_dtype(out["volume"])


def test_flag_anomalies_low_gt_high():
    df = _quote_df([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": 10, "high": 9, "low": 11, "close": 10,  # low>high
         "volume": 100, "adj_factor": 1.0},
        {"date": "2026-06-26", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10,
         "volume": 100, "adj_factor": 1.0},
    ])
    anomalies = flag_quote_anomalies(df)
    assert len(anomalies) == 1
    assert anomalies["date"].iloc[0] == "2026-06-25"


def test_flag_anomalies_close_zero():
    df = _quote_df([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 0,  # close<=0
         "volume": 100, "adj_factor": 1.0},
    ])
    anomalies = flag_quote_anomalies(df)
    assert len(anomalies) == 1


def test_flag_anomalies_huge_swing():
    """单日涨跌 >50% → 异常（spec §3.6）。"""
    df = _quote_df([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": 10, "high": 20, "low": 9, "close": 18,  # 18/10=1.8 涨80%
         "volume": 100, "adj_factor": 1.0},
    ])
    anomalies = flag_quote_anomalies(df)
    assert len(anomalies) == 1


def test_clean_fundamental_numeric_coercion():
    from src.data_pipeline.fetchers.base import FUNDAMENTAL_COLUMNS
    df = pd.DataFrame([{
        "code": "C", "market": "a_share", "report_period": "2023-12-31",
        "announcement_date_approx": "2024-04-30",
        "revenue": "1.5e11", "net_profit": "7e10", "roe": "30.0",
        "debt_ratio": "20.0", "fcf": "5e10", "total_market_cap": "1e12",
    }], columns=FUNDAMENTAL_COLUMNS)
    out = clean_fundamental(df)
    assert out["revenue"].iloc[0] == 1.5e11
    assert pd.api.types.is_numeric_dtype(out["roe"])
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_cleaners.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/cleaners.py**

```python
"""数据清洗：类型转换、后复权变换、异常标记（spec §3.3/§3.6）。"""
from __future__ import annotations

import numpy as np
import pandas as pd

_PRICE_COLS = ["open", "high", "low", "close"]
_HUGE_SWING = 0.50  # 单日涨跌阈值（spec §3.6）


def clean_quote(df: pd.DataFrame) -> pd.DataFrame:
    """行情清洗：数值类型转换 + 后复权（OHLC *= adj_factor，volume 不变）。"""
    out = df.copy()
    for col in _PRICE_COLS + ["volume", "adj_factor"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    # 后复权变换（spec §3.3）
    adj = out["adj_factor"].fillna(1.0)
    for col in _PRICE_COLS:
        out[col] = out[col] * adj
    return out


def clean_fundamental(df: pd.DataFrame) -> pd.DataFrame:
    """财务清洗：数值字段转 numeric，缺失保留 None。"""
    out = df.copy()
    numeric_cols = ["revenue", "net_profit", "roe", "debt_ratio", "fcf", "total_market_cap"]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def flag_quote_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """返回异常行情行（spec §3.6）：low>high / close<=0 / 单日涨跌>50%。"""
    if df.empty:
        return df
    out = df.copy()
    for col in _PRICE_COLS + ["volume", "adj_factor"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.sort_values(["code", "date"])
    prev_close = out.groupby("code")["close"].shift(1)
    swing = (out["close"] - prev_close).abs() / prev_close
    mask = (
        (out["low"] > out["high"])
        | (out["close"] <= 0)
        | (swing > _HUGE_SWING)
    )
    return out[mask].copy()
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_cleaners.py -v`
Expected: PASS（6 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/cleaners.py tests/data_pipeline/test_cleaners.py
git commit -m "feat(data_pipeline): cleaners — 后复权变换与异常标记"
```

---

## Task 7: Store（Parquet 日期分区写入 + 增量更新 + 读接口）

**Files:**
- Create: `src/data_pipeline/store.py`
- Create: `tests/data_pipeline/test_store.py`

**Interfaces:**
- Consumes: `src.config.DATA_DIR`、`METADATA_DIR`（Phase 1）；cleaner 输出的 DataFrame
- Produces: `write_parquet_partition(df, kind: str, partition_date: str, market: str) -> Path`（写 `data/raw/{kind}/{partition_date}/{market}.parquet`，不可变——若存在则报错或合并增量）
- Produces: `read_parquet(kind: str, market: str, as_of: str | None = None) -> pd.DataFrame`（读最近 ≤ as_of 的分区并拼接）
- Produces: `incremental_merge(df_new, kind, market) -> pd.DataFrame`（与已有分区按主键合并去重）

- [ ] **Step 1: 写失败测试 tests/data_pipeline/test_store.py**

```python
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import QUOTE_COLUMNS
from src.data_pipeline.store import (
    incremental_merge,
    read_parquet,
    write_parquet_partition,
)


def _quote(rows):
    return pd.DataFrame(rows, columns=QUOTE_COLUMNS)


def test_write_partition_creates_dated_file(isolated_data_dir):
    df = _quote([{"date": "2026-06-25", "code": "C", "market": "a_share",
                  "open": 10, "high": 11, "low": 9, "close": 10,
                  "volume": 100, "adj_factor": 1.0}])
    path = write_parquet_partition(df, kind="market", partition_date="2026-06-27", market="a_share")
    assert path.exists()
    assert "raw/market/2026-06-27/a_share.parquet" in str(path)


def test_read_partition_roundtrip(isolated_data_dir):
    df = _quote([{"date": "2026-06-25", "code": "C", "market": "a_share",
                  "open": 10, "high": 11, "low": 9, "close": 10,
                  "volume": 100, "adj_factor": 1.0}])
    write_parquet_partition(df, "market", "2026-06-27", "a_share")
    out = read_parquet("market", "a_share")
    assert len(out) == 1
    assert out["code"].iloc[0] == "C"


def test_read_as_of_filters_future_partitions(isolated_data_dir):
    """as_of 只读 ≤ 该日期的分区（PIT 约束）。"""
    df1 = _quote([{"date": "2026-06-25", "code": "C", "market": "a_share",
                   "open": 10, "high": 11, "low": 9, "close": 10,
                   "volume": 100, "adj_factor": 1.0}])
    df2 = _quote([{"date": "2026-06-26", "code": "C", "market": "a_share",
                   "open": 11, "high": 12, "low": 10, "close": 11,
                   "volume": 110, "adj_factor": 1.0}])
    write_parquet_partition(df1, "market", "2026-06-25", "a_share")
    write_parquet_partition(df2, "market", "2026-06-26", "a_share")
    # as_of=2026-06-25 → 只看到 df1
    out = read_parquet("market", "a_share", as_of="2026-06-25")
    assert len(out) == 1
    assert out["date"].iloc[0] == "2026-06-25"


def test_incremental_merge_dedups(isolated_data_dir):
    """增量更新：新数据覆盖同主键旧行，新增行追加。"""
    old = _quote([{"date": "2026-06-25", "code": "C", "market": "a_share",
                   "open": 10, "high": 11, "low": 9, "close": 10,
                   "volume": 100, "adj_factor": 1.0}])
    write_parquet_partition(old, "market", "2026-06-25", "a_share")
    existing = read_parquet("market", "a_share")
    # 新数据：修正 06-25 + 新增 06-26
    new = _quote([
        {"date": "2026-06-25", "code": "C", "market": "a_share",
         "open": 10, "high": 11, "low": 9, "close": 10.5,  # 修正
         "volume": 100, "adj_factor": 1.0},
        {"date": "2026-06-26", "code": "C", "market": "a_share",
         "open": 11, "high": 12, "low": 10, "close": 11,
         "volume": 110, "adj_factor": 1.0},
    ])
    merged = incremental_merge(new, existing, keys=["code", "date"])
    assert len(merged) == 2
    # 06-25 被新值覆盖
    row_25 = merged[merged["date"] == "2026-06-25"].iloc[0]
    assert row_25["close"] == 10.5


def test_read_missing_returns_empty(isolated_data_dir):
    out = read_parquet("market", "us")
    assert len(out) == 0
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_store.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/store.py**

```python
"""Parquet 日期分区存储 + 增量更新 + PIT 读接口（spec §3.2/§3.4）。"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from src import config


def _raw_dir() -> Path:
    return config.DATA_DIR / "raw"


def write_parquet_partition(
    df: pd.DataFrame, kind: str, partition_date: str, market: str
) -> Path:
    """写不可变 Parquet 分区：data/raw/{kind}/{partition_date}/{market}.parquet。

    partition_date 为拉取日期（行情）或财报披露日（财务）。
    若分区已存在，覆盖写（同日重拉以最新为准）。
    """
    path = _raw_dir() / kind / partition_date / f"{market}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def read_parquet(
    kind: str, market: str, as_of: str | None = None
) -> pd.DataFrame:
    """读取某 kind/market 的全部分区并拼接。

    as_of 给定时，只读 partition_date ≤ as_of 的分区（PIT 约束，spec §3.4）。
    无数据 → 空 DataFrame。
    """
    base = _raw_dir() / kind
    if not base.exists():
        return pd.DataFrame()
    frames = []
    for part_dir in sorted(base.iterdir()):
        if not part_dir.is_dir():
            continue
        if as_of is not None and part_dir.name > as_of:
            continue
        f = part_dir / f"{market}.parquet"
        if f.exists():
            frames.append(pd.read_parquet(f))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def incremental_merge(
    new_df: pd.DataFrame, existing_df: pd.DataFrame, keys: list[str]
) -> pd.DataFrame:
    """按 keys 去重合并：new_df 覆盖 existing_df 同主键行，新行追加。"""
    if existing_df.empty:
        return new_df.copy()
    if new_df.empty:
        return existing_df.copy()
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    # 保留每个 key 组合的最后一条（new 在后，覆盖 old）
    combined = combined.drop_duplicates(subset=keys, keep="last")
    return combined.reset_index(drop=True)
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_store.py -v`
Expected: PASS（5 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/store.py tests/data_pipeline/test_store.py
git commit -m "feat(data_pipeline): Parquet 日期分区存储 + 增量更新 + PIT 读接口"
```

---

## Task 8: Pipeline 编排（fetch→clean→store 一日全链路）

**Files:**
- Create: `src/data_pipeline/pipeline.py`
- Create: `tests/data_pipeline/test_pipeline.py`

**Interfaces:**
- Consumes: 全部 fetchers（Task 2-5）、cleaners（Task 6）、store（Task 7）、config（Phase 1）
- Produces: `run_daily_pipeline(run_date: str, codes: dict[str, list[str]], fx_pairs: list[str]) -> dict`（编排一日全链路，返回各步骤状态 + STALE 标记）
- Produces: `PipelineResult` dataclass（status: dict, anomalies: pd.DataFrame, stale_markets: list）

- [ ] **Step 1: 写失败测试 tests/data_pipeline/test_pipeline.py**

```python
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import QUOTE_COLUMNS
from src.data_pipeline.pipeline import run_daily_pipeline


def test_run_daily_pipeline_quote_clean_store(mocker, isolated_data_dir):
    """一日链路：fetch 行情 → clean（后复权）→ store → 可读回。"""
    raw = pd.DataFrame({
        "日期": ["2026-06-26"], "开盘": [10.0], "收盘": [10.2],
        "最高": [10.5], "最低": [9.9], "成交量": [100000],
    })
    mocker.patch("akshare.stock_zh_a_hist", return_value=raw)
    fx_raw = pd.DataFrame({"日期": ["2026-06-26"], "收盘": [7.18]})
    mocker.patch("akshare.currency_boc_sina", return_value=fx_raw)

    result = run_daily_pipeline(
        run_date="2026-06-27",
        codes={"a_share": ["600519"]},
        fx_pairs=["USD/CNY"],
    )
    assert result.status["quote"]["a_share"] == "ok"
    # 行情已清洗存储，可读回
    from src.data_pipeline.store import read_parquet
    df = read_parquet("market", "a_share", as_of="2026-06-27")
    assert len(df) == 1
    assert df["close"].iloc[0] == 10.2  # adj_factor=1 → 后复权=原值


def test_run_daily_pipeline_marks_stale_on_failure(mocker, isolated_data_dir):
    """某市场 fetcher 重试耗尽 → 标记 STALE，不阻塞其他市场（spec §3.6）。"""
    mocker.patch("src.data_pipeline.fetchers.quote.time.sleep")
    mocker.patch("akshare.stock_zh_a_hist", side_effect=RuntimeError("net"))
    us_raw = pd.DataFrame({"Close": [100.0]}, index=pd.to_datetime(["2026-06-26"]))
    mocker.patch("yfinance.download", return_value=us_raw)

    result = run_daily_pipeline(
        run_date="2026-06-27",
        codes={"a_share": ["600519"], "us": ["AAPL"]},
        fx_pairs=[],
    )
    assert result.status["quote"]["a_share"] == "STALE"
    assert result.status["quote"]["us"] == "ok"
    assert "a_share" in result.stale_markets


def test_pipeline_result_dataclass():
    from src.data_pipeline.pipeline import PipelineResult
    r = PipelineResult(status={"quote": {}}, anomalies=pd.DataFrame(), stale_markets=[])
    assert r.stale_markets == []
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_pipeline.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/pipeline.py**

```python
"""Pipeline 编排：fetch → clean → store 一日全链路（spec §3.6 STALE 不阻塞）。"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.data_pipeline.cleaners import clean_quote, flag_quote_anomalies
from src.data_pipeline.fetchers.base import FETCHER_MARKET_STALE, FetcherError
from src.data_pipeline.fetchers.macro import FXFetcher
from src.data_pipeline.fetchers.quote import AShareQuoteFetcher, HKQuoteFetcher, USQuoteFetcher
from src.data_pipeline.store import write_parquet_partition

_QUOTE_FETCHERS = {
    "a_share": AShareQuoteFetcher,
    "us": USQuoteFetcher,
    "hk": HKQuoteFetcher,
}


@dataclass
class PipelineResult:
    status: dict = field(default_factory=dict)
    anomalies: pd.DataFrame = field(default_factory=pd.DataFrame)
    stale_markets: list = field(default_factory=list)


def run_daily_pipeline(
    run_date: str,
    codes: dict[str, list[str]],
    fx_pairs: list[str],
) -> PipelineResult:
    """执行一日数据链路：行情 + 汇率 → 清洗 → 存储。

    某市场失败标记 STALE，不阻塞其他市场（spec §3.6）。
    """
    result = PipelineResult(status={"quote": {}, "fx": {}})
    all_anomalies = []

    # 行情
    for market, market_codes in codes.items():
        if market not in _QUOTE_FETCHERS:
            continue
        fetcher = _QUOTE_FETCHERS[market]()
        frames = []
        for code in market_codes:
            try:
                raw = fetcher.fetch_daily(code, start=run_date, end=run_date)
                frames.append(raw)
            except FetcherError:
                result.status["quote"][market] = FETCHER_MARKET_STALE
                result.stale_markets.append(market)
                break
        else:
            if frames:
                combined = pd.concat(frames, ignore_index=True)
                cleaned = clean_quote(combined)
                write_parquet_partition(cleaned, "market", run_date, market)
                anomalies = flag_quote_anomalies(cleaned)
                if not anomalies.empty:
                    all_anomalies.append(anomalies)
                result.status["quote"][market] = "ok"
            else:
                result.status["quote"][market] = "empty"

    # 汇率
    fx_frames = []
    for pair in fx_pairs:
        try:
            raw = FXFetcher().fetch(pair, start=run_date, end=run_date)
            fx_frames.append(raw)
            result.status["fx"][pair] = "ok"
        except FetcherError:
            result.status["fx"][pair] = FETCHER_MARKET_STALE
    if fx_frames:
        fx_df = pd.concat(fx_frames, ignore_index=True)
        write_parquet_partition(fx_df, "macro", run_date, "fx")

    if all_anomalies:
        result.anomalies = pd.concat(all_anomalies, ignore_index=True)
    return result
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_pipeline.py -v`
Expected: PASS（3 个测试）

- [ ] **Step 5: 运行全部非集成测试套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 全部 PASS（Phase 1 的 38 + Phase 2 新增）

- [ ] **Step 6: 提交**

```bash
git add src/data_pipeline/pipeline.py tests/data_pipeline/test_pipeline.py
git commit -m "feat(data_pipeline): pipeline 编排（fetch→clean→store，STALE 不阻塞）"
```

---

## Task 9: 真实数据集成测试（@integration 联网）

**Files:**
- Create: `tests/data_pipeline/test_integration_real_fetch.py`

> 此任务验证 fetchers 对真实 akshare/yfinance 的端到端可用性。`@pytest.mark.integration` 默认跳过，仅本地手动运行。

- [ ] **Step 1: 写集成测试 tests/data_pipeline/test_integration_real_fetch.py**

```python
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import QUOTE_COLUMNS
from src.data_pipeline.fetchers.macro import BenchmarkFetcher, FXFetcher
from src.data_pipeline.fetchers.quote import AShareQuoteFetcher, USQuoteFetcher


@pytest.mark.integration
def test_real_a_share_quote(isolated_data_dir):
    """真实拉取茅台一日行情（联网）。"""
    df = AShareQuoteFetcher().fetch_daily("600519", "2026-06-20", "2026-06-26")
    assert list(df.columns) == QUOTE_COLUMNS
    assert (df["code"] == "600519").all()
    assert len(df) > 0


@pytest.mark.integration
def test_real_us_quote(isolated_data_dir):
    df = USQuoteFetcher().fetch_daily("AAPL", "2026-06-20", "2026-06-26")
    assert list(df.columns) == QUOTE_COLUMNS
    assert len(df) > 0


@pytest.mark.integration
def test_real_fx(isolated_data_dir):
    df = FXFetcher().fetch("USD/CNY", "2026-06-20", "2026-06-26")
    assert len(df) > 0
    assert (df["base"] == "USD").all()


@pytest.mark.integration
def test_real_benchmark_a_share(isolated_data_dir):
    """A 股基准降级为沪深300宽基（探针实测约束）。"""
    df = BenchmarkFetcher().fetch("a_share", "2026-06-20", "2026-06-26")
    assert len(df) > 0
    assert (df["market"] == "a_share").all()
```

- [ ] **Step 2: 确认默认套件跳过集成测试**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 全部非集成测试 PASS，集成测试 deselected

- [ ] **Step 3: 手动运行集成测试（联网，需 akshare/yfinance 已装）**

Run: `/home/jerry/value/.venv/bin/pytest tests/data_pipeline/test_integration_real_fetch.py -m integration -v`
Expected: 联网成功时 PASS。若 akshare 字段名变更导致异常，记录到 `docs/superpowers/specs/2026-06-27-phase2-fetch-findings.md`。

- [ ] **Step 4: 记录集成测试结论**

创建 `docs/superpowers/specs/2026-06-27-phase2-fetch-findings.md`，记录：哪些 fetcher 端到端可用、akshare/yfinance 实际字段名与代码假设是否一致、有无字段映射需修正。若全部通过，简短记录即可。

```bash
git add tests/data_pipeline/test_integration_real_fetch.py docs/superpowers/specs/2026-06-27-phase2-fetch-findings.md
git commit -m "test(data_pipeline): 真实数据集成测试 + fetcher 实测结论"
```

> **Phase 2 完成条件：** 全部非集成测试通过；集成测试已手动运行并记录结论。此后方可进入 Phase 3（PIT 服务）。
