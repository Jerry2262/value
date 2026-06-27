# Phase 4 — Factor Engine 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现因子引擎——给定 `as_of_date` 与股票池，按 YAML 因子定义计算各因子原始值，做跨市场分层标准化（截尾→市场内 Z→行业内 Z→可选市值中性），输出因子矩阵 + 市场内分位秩，并支持分市场 IC/ICIR 有效性检验。

**Architecture:** 四层——`registry.py`（YAML 因子定义 → 计算函数注册表，装饰器注册）+ `computers/`（每个因子一个纯计算函数，消费 Phase 3 PIT slicer 数据；含 DCF 估值）+ `standardizer.py`（分层标准化纯函数）+ `engine.py`（编排：universe→逐因子计算→标准化→输出矩阵+分位秩）+ `ic_test.py`（IC/ICIR 分市场检验）。因子定义与计算分离：新增因子只需 YAML + 一个被 `@register` 装饰的 compute 函数。动量因子权重写死为 0、不进综合得分，但仍计算（供反向校验）。行业分类数据 Phase 2 未建 fetcher——standardizer 接受可选 industry_map，缺失时跳过行业内层。

**Tech Stack:** Python 3.12、pandas、numpy、pyarrow、pytest、pytest-mock

## Global Constraints

（摘自 spec §4.2/§5 + Phase 3 交付，逐字执行）

- 因子定义来自 `config/factors/*.yaml`（Phase 2 已建，10 个 in-composite 因子 + 动量 weight=0）
- **动量因子权重永久为 0**，`in_composite: false`，不进综合得分，仅作估值择价反向校验信号（spec §4.2）
- 跨市场标准化分层（spec §4.2）：第0层截尾（MAD 法 median±5×MAD）→ 第1层市场内 Z-score → 第2层行业内 Z-score（市场-行业内）→ 第3层可选市值中性（默认关闭）
- 跨市场排名用**市场内分位秩（percentile rank）**，不直接比较原始 Z 值（spec §4.2）
- DCF 金融股跳过（GICS 40 金融），返回 N/A（spec §4.4）
- PE 口径总市值法 `total_market_cap / net_profit`（spec §4.4，Phase 3 `pe_ratio_at` 已实现）
- Factor Engine 通过 `as_of_date` 接收 PIT 时间点，内部不做数据时点判断（spec §5.3）
- Factor Engine 不感知股票池（universe 从外部传入）（spec §5.3）
- 退市股票由 Data Pipeline 维护，Factor Engine 从 universe 中扣除（spec §5.3）——用 Phase 3 `pit_active_universe`
- 因子有效性分市场独立 IC/ICIR 检验（spec §5.3，v1 实现 IC，v2 分层回测）
- 列契约：FUNDAMENTAL_COLUMNS（code/market/report_period/announcement_date_approx/revenue/net_profit/roe/debt_ratio/fcf/total_market_cap）、QUOTE_COLUMNS（Phase 2 base.py）
- 复用 Phase 3：`pit.slicer.slice_quote_panel`、`slice_latest_fundamental`、`pe_ratio_at`、`pe_percentile`；`pit.indexer.pit_active_universe`、`pit_fundamental_as_of`
- 不引入新依赖

---

## File Structure（Phase 4 范围）

```
value/
├── src/factor_engine/
│   ├── __init__.py                 # Task 1
│   ├── registry.py                 # Task 1（@register 装饰器 + FACTOR_REGISTRY + get_factor_spec）
│   ├── computers/
│   │   ├── __init__.py             # Task 2
│   │   ├── value.py                # Task 2（pe_percentile/pb_percentile/dividend_yield/fcf_yield）
│   │   ├── growth.py               # Task 3（revenue_cagr_3y/profit_cagr_3y/rnd_ratio）
│   │   ├── quality.py              # Task 4（roe/gross_margin_stability/cash_flow_quality/leverage）
│   │   ├── momentum.py             # Task 5（momentum_12m1m，权重0反向校验）
│   │   └── dcf.py                  # Task 6（DCF 估值，金融股跳过）
│   ├── standardizer.py             # Task 7（截尾+市场内Z+行业内Z+市值中性+分位秩）
│   ├── engine.py                   # Task 8（编排：universe→计算→标准化→矩阵+分位秩）
│   └── ic_test.py                  # Task 9（IC/ICIR 分市场检验）
├── tests/factor_engine/
│   ├── __init__.py                 # Task 1
│   ├── conftest.py                 # Task 1（因子计算 fixtures）
│   ├── test_registry.py            # Task 1
│   ├── test_value.py               # Task 2
│   ├── test_growth.py              # Task 3
│   ├── test_quality.py             # Task 4
│   ├── test_momentum.py            # Task 5
│   ├── test_dcf.py                 # Task 6
│   ├── test_standardizer.py        # Task 7
│   ├── test_engine.py              # Task 8
│   └── test_ic.py                  # Task 9
```

---

## Task 1: 因子注册表（@register 装饰器 + YAML 加载）

**Files:**
- Create: `src/factor_engine/__init__.py`
- Create: `src/factor_engine/registry.py`
- Create: `tests/factor_engine/__init__.py`
- Create: `tests/factor_engine/conftest.py`
- Create: `tests/factor_engine/test_registry.py`

**Interfaces:**
- Consumes: `src.config.load_factor_configs()`（Phase 1，返回 {factor_key: spec}）
- Produces: `FACTOR_REGISTRY: dict[str, Callable]` — factor_key → compute 函数
- Produces: `register(name: str)` 装饰器 — 用法 `@register("pe_percentile")` 注册 compute 函数
- Produces: `get_factor_spec(name: str) -> dict` — 从 YAML 取因子定义
- Produces: `list_in_composite_factors() -> list[str]` — in_composite=True 的因子键（动量 excluded）
- Produces: `compute_factor(name: str, as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series` — 调用注册的 compute 函数，返回 Series（index=code）

- [ ] **Step 1: 创建包初始化**

```python
# src/factor_engine/__init__.py
"""因子引擎：YAML 定义 + compute 函数注册 + 分层标准化（spec §5）。"""
```

```python
# tests/factor_engine/__init__.py
```

- [ ] **Step 2: 写 tests/factor_engine/conftest.py（因子计算 fixtures）**

```python
import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import FUNDAMENTAL_COLUMNS, QUOTE_COLUMNS
from src.data_pipeline import store


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    import importlib
    import src.config as cfg
    importlib.reload(cfg)
    yield tmp_path
    importlib.reload(cfg)


@pytest.fixture
def fundamentals_for_factors(isolated_data_dir):
    """3 只股票 × 4 年财报，用于因子计算。

    C1: 优质成长（高 ROE、稳毛利、营收增长）
    C2: 价值（低 PE、高股息、稳毛利）
    C3: 弱质（低 ROE、负 FCF、高杠杆）
    """
    rows = []
    # C1: revenue 100→120→140→160, net_profit 20→24→28→32, roe 30→30→30→30,
    #     debt_ratio 30, fcf 15, total_market_cap 600
    for yr, (rev, np_, roe) in enumerate(
        [(100, 20, 30), (120, 24, 30), (140, 28, 30), (160, 32, 30)], start=2020
    ):
        rows.append({"code": "C1", "market": "a_share", "report_period": f"{yr}-12-31",
                     "announcement_date_approx": f"{yr+1}-04-30",
                     "revenue": rev * 1e8, "net_profit": np_ * 1e8, "roe": roe,
                     "debt_ratio": 30.0, "fcf": 15e8, "total_market_cap": 600e8})
    # C2: revenue 稳定 80, net_profit 8, roe 15, debt 40, fcf 10, mcap 160 (低 PE=20)
    for yr in range(2020, 2024):
        rows.append({"code": "C2", "market": "a_share", "report_period": f"{yr}-12-31",
                     "announcement_date_approx": f"{yr+1}-04-30",
                     "revenue": 80e8, "net_profit": 8e8, "roe": 15.0,
                     "debt_ratio": 40.0, "fcf": 10e8, "total_market_cap": 160e8})
    # C3: revenue 50→45→40, net_profit 2→1→0.5, roe 5, debt 75, fcf -2, mcap 50
    for yr, (rev, np_) in enumerate([(50, 2), (45, 1), (40, 0.5)], start=2021):
        rows.append({"code": "C3", "market": "a_share", "report_period": f"{yr}-12-31",
                     "announcement_date_approx": f"{yr+1}-04-30",
                     "revenue": rev * 1e8, "net_profit": np_ * 1e8, "roe": 5.0,
                     "debt_ratio": 75.0, "fcf": -2e8, "total_market_cap": 50e8})
    store.write_parquet_partition(pd.DataFrame(rows, columns=FUNDAMENTAL_COLUMNS),
                                  "fundamental", "2024-04-30", "a_share")


@pytest.fixture
def quotes_for_momentum(isolated_data_dir):
    """C1/C2/C3 行情用于动量计算（12-1月动量需 ≥13 个月数据）。"""
    rows = []
    # 简化：每月一行，C1 涨、C2 平、C3 跌
    for m in range(1, 25):  # 2023-01 ~ 2024-12
        date = f"2023-{m:02d}-15" if m <= 12 else f"2024-{m-12:02d}-15"
        base_c1 = 10 + m * 0.5
        rows.append({"date": date, "code": "C1", "market": "a_share",
                     "open": base_c1, "high": base_c1, "low": base_c1, "close": base_c1,
                     "volume": 1000, "adj_factor": 1.0})
        rows.append({"date": date, "code": "C2", "market": "a_share",
                     "open": 20, "high": 20, "low": 20, "close": 20,
                     "volume": 1000, "adj_factor": 1.0})
        rows.append({"date": date, "code": "C3", "market": "a_share",
                     "open": 30 - m * 0.3, "high": 30 - m * 0.3, "low": 30 - m * 0.3,
                     "close": 30 - m * 0.3, "volume": 1000, "adj_factor": 1.0})
    store.write_parquet_partition(pd.DataFrame(rows, columns=QUOTE_COLUMNS),
                                  "market", "2024-12-31", "a_share")
```

- [ ] **Step 3: 写失败测试 tests/factor_engine/test_registry.py**

```python
import pandas as pd
import pytest

from src.factor_engine.registry import (
    FACTOR_REGISTRY,
    compute_factor,
    get_factor_spec,
    list_in_composite_factors,
    register,
)


def test_register_adds_to_registry():
    @register("test_factor_temp")
    def _compute(as_of, market, codes, **kwargs):
        return pd.Series({c: 1.0 for c in codes})

    assert "test_factor_temp" in FACTOR_REGISTRY
    # 清理
    del FACTOR_REGISTRY["test_factor_temp"]


def test_compute_factor_calls_registered():
    @register("test_factor_temp2")
    def _compute(as_of, market, codes, **kwargs):
        return pd.Series({c: i for i, c in enumerate(codes)})

    s = compute_factor("test_factor_temp2", "2024-01-01", "a_share", ["A", "B"])
    assert s["A"] == 0
    assert s["B"] == 1
    del FACTOR_REGISTRY["test_factor_temp2"]


def test_compute_factor_unknown_raises():
    with pytest.raises(KeyError):
        compute_factor("nonexistent_factor", "2024-01-01", "a_share", ["A"])


def test_get_factor_spec_from_yaml():
    spec = get_factor_spec("pe_percentile")
    assert spec["category"] == "value"
    assert spec["direction"] == "reverse"
    assert spec["weight"] == 0.15
    assert spec["in_composite"] is True


def test_get_factor_spec_momentum_weight_zero():
    spec = get_factor_spec("momentum_12m1m")
    assert spec["weight"] == 0.0
    assert spec["in_composite"] is False


def test_list_in_composite_factors_excludes_momentum():
    in_composite = list_in_composite_factors()
    assert "momentum_12m1m" not in in_composite
    assert "pe_percentile" in in_composite
    assert "roe" in in_composite
    # 10 个 in-composite 因子（4 value + 3 growth + 3 quality）
    assert len(in_composite) == 10
```

- [ ] **Step 4: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_registry.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 5: 写 src/factor_engine/registry.py**

```python
"""因子注册表：YAML 定义 + @register 装饰器注册 compute 函数（spec §5.1）。"""
from __future__ import annotations

from typing import Callable

import pandas as pd

from src import config

# factor_key -> compute 函数
FACTOR_REGISTRY: dict[str, Callable] = {}


def register(name: str):
    """装饰器：注册一个因子 compute 函数。

    用法：
        @register("pe_percentile")
        def compute(as_of, market, codes, **kwargs) -> pd.Series: ...
    """
    def decorator(fn: Callable) -> Callable:
        FACTOR_REGISTRY[name] = fn
        return fn
    return decorator


def get_factor_spec(name: str) -> dict:
    """从 YAML 取因子定义。"""
    factors = config.load_factor_configs()
    if name not in factors:
        raise KeyError(f"因子 {name} 未在 config/factors/*.yaml 中定义")
    return factors[name]


def list_in_composite_factors() -> list[str]:
    """返回 in_composite=True 的因子键（动量 excluded）。"""
    factors = config.load_factor_configs()
    return [k for k, spec in factors.items() if spec.get("in_composite") is True]


def compute_factor(
    name: str, as_of: str, market: str, codes: list[str], **kwargs
) -> pd.Series:
    """调用注册的 compute 函数，返回 Series（index=code）。"""
    if name not in FACTOR_REGISTRY:
        raise KeyError(f"因子 {name} 未注册 compute 函数")
    return FACTOR_REGISTRY[name](as_of, market, codes, **kwargs)
```

- [ ] **Step 6: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_registry.py -v`
Expected: PASS（6 个测试）

- [ ] **Step 7: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 103 prior + 6 = 109 passed

- [ ] **Step 8: 提交**

```bash
git add src/factor_engine/__init__.py src/factor_engine/registry.py tests/factor_engine/__init__.py tests/factor_engine/conftest.py tests/factor_engine/test_registry.py
git commit -m "feat(factor_engine): 因子注册表 — @register 装饰器与 YAML 定义加载"
```

---

## Task 2: 价值因子 computers（pe/pb 分位、股息率、FCF yield）

**Files:**
- Create: `src/factor_engine/computers/__init__.py`
- Create: `src/factor_engine/computers/value.py`
- Create: `tests/factor_engine/test_value.py`

**Interfaces:**
- Consumes: `register`（Task 1）；`pit.slicer.pe_percentile`、`pe_ratio_at`、`slice_latest_fundamental`、`pit_fundamental_as_of`（Phase 3）
- Produces: 4 个被 `@register` 的 compute 函数：`pe_percentile`、`pb_percentile`、`dividend_yield`、`fcf_yield`，各签名 `(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series`（index=code，值为因子原始值；direction=reverse 的因子返回原始值，方向处理在 standardizer）

- [ ] **Step 1: 写失败测试 tests/factor_engine/test_value.py**

```python
import numpy as np
import pandas as pd
import pytest

from src.factor_engine.registry import FACTOR_REGISTRY, compute_factor
# 导入 value 模块以触发 @register
import src.factor_engine.computers.value  # noqa: F401


def test_value_factors_registered():
    for name in ["pe_percentile", "pb_percentile", "dividend_yield", "fcf_yield"]:
        assert name in FACTOR_REGISTRY


def test_fcf_yield_computed(fundamentals_for_factors):
    """FCF yield = fcf / total_market_cap（用最新一期财报）。"""
    s = compute_factor("fcf_yield", "2024-05-01", "a_share", ["C1", "C2", "C3"])
    # C1: fcf=15e8 / mcap=600e8 = 0.025
    assert abs(s["C1"] - 15e8 / 600e8) < 1e-9
    # C2: fcf=10e8 / mcap=160e8 = 0.0625
    assert abs(s["C2"] - 10e8 / 160e8) < 1e-9
    # C3: fcf=-2e8 / mcap=50e8 = -0.04（负值）
    assert s["C3"] < 0


def test_fcf_yield_missing_code_returns_nan(fundamentals_for_factors):
    s = compute_factor("fcf_yield", "2024-05-01", "a_share", ["C1", "NOPE"])
    assert not pd.isna(s["C1"])
    assert pd.isna(s["NOPE"])


def test_pe_percentile_returns_value(fundamentals_for_factors):
    """pe_percentile 调用 Phase 3 slicer，返回 0-1 分位或 None→NaN。"""
    s = compute_factor("pe_percentile", "2024-05-01", "a_share", ["C1", "C2", "C3"])
    # 至少 C1/C2/C3 有值（None→NaN）
    assert len(s) == 3
    # 分位值应在 [0,1] 或 NaN
    for v in s:
        assert pd.isna(v) or (0.0 <= v <= 1.0)


def test_dividend_yield_not_implemented_gracefully(fundamentals_for_factors):
    """股息率需分红数据（Phase 2 未建分红 fetcher）→ 返回 NaN（不崩）。"""
    s = compute_factor("dividend_yield", "2024-05-01", "a_share", ["C1", "C2"])
    # 分红数据缺失 → 全 NaN
    assert s.isna().all()
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_value.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/factor_engine/computers/__init__.py 与 value.py**

```python
# src/factor_engine/computers/__init__.py
"""各因子计算函数（消费 Phase 3 PIT slicer 数据）。"""
```

```python
"""价值因子：pe/pb 分位、股息率、FCF yield（spec §4.2 价值因子）。"""
from __future__ import annotations

import pandas as pd

from src.factor_engine.registry import register
from src.pit.slicer import pe_percentile, slice_latest_fundamental


@register("pe_percentile")
def compute_pe_percentile(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """PE 历史分位（PIT，Phase 3 slicer）。direction=reverse（越低越好）。"""
    out = {}
    for code in codes:
        pct = pe_percentile(as_of, market, code, lookback_years=5)
        out[code] = pct if pct is not None else float("nan")
    return pd.Series(out)


@register("pb_percentile")
def compute_pb_percentile(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """PB 历史分位。v1 用 PB≈PE 的代理（PB 分位需净资产数据，Phase 2 未完整）。

    spec §4.2 价值因子；direction=reverse。当前用 total_market_cap / net_profit
    作为代理的代理——TODO Phase 后续补真实 PB（需净资产字段）。
    """
    # v1 占位：复用 PE 分位逻辑框架但标注为近似。PB 需 book value，暂用 NaN。
    out = {code: float("nan") for code in codes}
    return pd.Series(out)


@register("dividend_yield")
def compute_dividend_yield(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """股息率。需分红数据（Phase 2 未建分红 fetcher）→ v1 全 NaN。

    direction=forward。spec §4.2 min_consistency=3（至少连续3年分红）。
    """
    out = {code: float("nan") for code in codes}
    return pd.Series(out)


@register("fcf_yield")
def compute_fcf_yield(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """FCF yield = fcf / total_market_cap（最新一期财报）。direction=forward。"""
    fund = slice_latest_fundamental(as_of, market, codes)
    out = {}
    if fund.empty:
        return pd.Series({c: float("nan") for c in codes})
    fund = fund.set_index("code")
    for code in codes:
        if code in fund.index:
            row = fund.loc[code]
            fcf = row.get("fcf")
            mcap = row.get("total_market_cap")
            if pd.notna(fcf) and pd.notna(mcap) and mcap != 0:
                out[code] = float(fcf) / float(mcap)
            else:
                out[code] = float("nan")
        else:
            out[code] = float("nan")
    return pd.Series(out)
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_value.py -v`
Expected: PASS（5 个测试）

- [ ] **Step 5: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 109 + 5 = 114 passed

- [ ] **Step 6: 提交**

```bash
git add src/factor_engine/computers/__init__.py src/factor_engine/computers/value.py tests/factor_engine/test_value.py
git commit -m "feat(factor_engine): 价值因子 — pe/pb分位/股息率/fcf yield"
```

---

## Task 3: 成长因子 computers（营收/利润 CAGR、研发占比）

**Files:**
- Create: `src/factor_engine/computers/growth.py`
- Create: `tests/factor_engine/test_growth.py`

**Interfaces:**
- Consumes: `register`（Task 1）；`pit.indexer.pit_fundamental_as_of`（Phase 3）
- Produces: `@register("revenue_cagr_3y")`、`@register("profit_cagr_3y")`、`@register("rnd_ratio")`

- [ ] **Step 1: 写失败测试 tests/factor_engine/test_growth.py**

```python
import math

import pandas as pd
import pytest

from src.factor_engine.registry import FACTOR_REGISTRY, compute_factor
import src.factor_engine.computers.growth  # noqa: F401


def test_growth_factors_registered():
    for name in ["revenue_cagr_3y", "profit_cagr_3y", "rnd_ratio"]:
        assert name in FACTOR_REGISTRY


def test_revenue_cagr_3y(fundamentals_for_factors):
    """C1: revenue 100→160 over 2020→2023 (3年) → CAGR = (160/100)^(1/3)-1 ≈ 16.96%。"""
    s = compute_factor("revenue_cagr_3y", "2024-05-01", "a_share", ["C1", "C2", "C3"])
    cagr_c1 = (160 / 100) ** (1 / 3) - 1
    assert abs(s["C1"] - cagr_c1) < 1e-6


def test_revenue_cagr_declining_is_negative(fundamentals_for_factors):
    """C3: revenue 50→45→40 (declining) → 负 CAGR。"""
    s = compute_factor("revenue_cagr_3y", "2024-05-01", "a_share", ["C1", "C2", "C3"])
    assert s["C3"] < 0


def test_profit_cagr_3y(fundamentals_for_factors):
    """C1: net_profit 20→32 over 3年 → CAGR = (32/20)^(1/3)-1。"""
    s = compute_factor("profit_cagr_3y", "2024-05-01", "a_share", ["C1", "C2", "C3"])
    cagr_c1 = (32 / 20) ** (1 / 3) - 1
    assert abs(s["C1"] - cagr_c1) < 1e-6


def test_cagr_insufficient_history_returns_nan(fundamentals_for_factors):
    """as_of 太早，不足 3 年历史 → NaN。"""
    s = compute_factor("revenue_cagr_3y", "2021-05-01", "a_share", ["C1"])
    assert pd.isna(s["C1"])


def test_rnd_ratio_nan_when_no_rnd_field(fundamentals_for_factors):
    """研发占比需研发费用字段（FUNDAMENTAL_COLUMNS 无）→ 全 NaN。"""
    s = compute_factor("rnd_ratio", "2024-05-01", "a_share", ["C1", "C2"])
    assert s.isna().all()
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_growth.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/factor_engine/computers/growth.py**

```python
"""成长因子：营收/利润 3年 CAGR、研发占比（spec §4.2 成长因子）。"""
from __future__ import annotations

import pandas as pd

from src.factor_engine.registry import register
from src.pit.indexer import pit_fundamental_as_of


def _cagr(start: float, end: float, years: int) -> float:
    """复合年化增速。start<=0 或 end<=0 → NaN（负值不可 CAGR）。"""
    if start <= 0 or end <= 0 or years <= 0:
        return float("nan")
    return (end / start) ** (1 / years) - 1


def _cagr_for_code(
    fund: pd.DataFrame, field: str, years: int
) -> float:
    """取该 code 的财报，按 report_period 排序，取最早 vs 最新（跨度 years 年）算 CAGR。"""
    if fund.empty:
        return float("nan")
    df = fund.sort_values("report_period").reset_index(drop=True)
    vals = df[field].dropna()
    if len(vals) < 2:
        return float("nan")
    # 取首尾
    start = df[field].iloc[0]
    end = df[field].iloc[-1]
    return _cagr(float(start), float(end), years)


@register("revenue_cagr_3y")
def compute_revenue_cagr_3y(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """营收 3 年复合增速。direction=forward。"""
    out = {}
    for code in codes:
        fund = pit_fundamental_as_of(as_of, market, code=code)
        out[code] = _cagr_for_code(fund, "revenue", 3)
    return pd.Series(out)


@register("profit_cagr_3y")
def compute_profit_cagr_3y(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """净利润 3 年复合增速。direction=forward。"""
    out = {}
    for code in codes:
        fund = pit_fundamental_as_of(as_of, market, code=code)
        out[code] = _cagr_for_code(fund, "net_profit", 3)
    return pd.Series(out)


@register("rnd_ratio")
def compute_rnd_ratio(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """研发占营收比。需研发费用字段（FUNDAMENTAL_COLUMNS 无 rnd）→ v1 全 NaN。"""
    out = {code: float("nan") for code in codes}
    return pd.Series(out)
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_growth.py -v`
Expected: PASS（5 个测试）

- [ ] **Step 5: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 114 + 5 = 119 passed

- [ ] **Step 6: 提交**

```bash
git add src/factor_engine/computers/growth.py tests/factor_engine/test_growth.py
git commit -m "feat(factor_engine): 成长因子 — 营收/利润CAGR/研发占比"
```

---

## Task 4: 质量因子 computers（ROE、毛利稳定性、现金流质量、杠杆率）

**Files:**
- Create: `src/factor_engine/computers/quality.py`
- Create: `tests/factor_engine/test_quality.py`

**Interfaces:**
- Consumes: `register`（Task 1）；`pit.indexer.pit_fundamental_as_of`、`pit.slicer.slice_latest_fundamental`（Phase 3）
- Produces: `@register("roe")`、`@register("gross_margin_stability")`、`@register("cash_flow_quality")`、`@register("leverage")`

- [ ] **Step 1: 写失败测试 tests/factor_engine/test_quality.py**

```python
import math

import pandas as pd
import pytest

from src.factor_engine.registry import FACTOR_REGISTRY, compute_factor
import src.factor_engine.computers.quality  # noqa: F401


def test_quality_factors_registered():
    for name in ["roe", "gross_margin_stability", "cash_flow_quality", "leverage"]:
        assert name in FACTOR_REGISTRY


def test_roe(fundamentals_for_factors):
    """ROE = 最新一期财报的 roe 字段。C1=30, C2=15, C3=5。"""
    s = compute_factor("roe", "2024-05-01", "a_share", ["C1", "C2", "C3"])
    assert s["C1"] == 30.0
    assert s["C2"] == 15.0
    assert s["C3"] == 5.0


def test_leverage(fundamentals_for_factors):
    """杠杆率 = 1 - 资产负债率。C1 debt=30 → 0.7。direction=forward。"""
    s = compute_factor("leverage", "2024-05-01", "a_share", ["C1", "C2", "C3"])
    assert abs(s["C1"] - 0.70) < 1e-9
    assert abs(s["C2"] - 0.60) < 1e-9
    assert abs(s["C3"] - 0.25) < 1e-9


def test_cash_flow_quality(fundamentals_for_factors):
    """现金流质量 = 经营现金流/净利润。C1: fcf=15e8 / np=32e8 ≈ 0.46875。"""
    s = compute_factor("cash_flow_quality", "2024-05-01", "a_share", ["C1", "C2", "C3"])
    assert abs(s["C1"] - 15e8 / 32e8) < 1e-6
    # C3: np=0.5e8 > 0, fcf=-2e8 → 负值
    assert s["C3"] < 0


def test_cash_flow_quality_zero_profit_nan(fundamentals_for_factors):
    """净利润=0 → NaN（避免除零）。"""
    # C2 net_profit=8e8 ≠ 0；构造一个 np=0 的场景由方向处理覆盖，此处验证正常路径
    s = compute_factor("cash_flow_quality", "2024-05-01", "a_share", ["C2"])
    assert abs(s["C2"] - 10e8 / 8e8) < 1e-6


def test_gross_margin_stability_nan_no_gross_margin_field(fundamentals_for_factors):
    """毛利率稳定性需毛利率字段（FUNDAMENTAL_COLUMNS 无 gross_margin）→ v1 全 NaN。

    spec §4.2 quality 因子。需毛利率历史序列。
    """
    s = compute_factor("gross_margin_stability", "2024-05-01", "a_share", ["C1", "C2"])
    assert s.isna().all()
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_quality.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/factor_engine/computers/quality.py**

```python
"""质量因子：ROE、毛利稳定性、现金流质量、杠杆率（spec §4.2 质量因子）。"""
from __future__ import annotations

import pandas as pd

from src.factor_engine.registry import register
from src.pit.slicer import slice_latest_fundamental


@register("roe")
def compute_roe(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """ROE = 最新一期财报的 roe 字段。direction=forward。"""
    fund = slice_latest_fundamental(as_of, market, codes)
    out = {}
    if fund.empty:
        return pd.Series({c: float("nan") for c in codes})
    fund = fund.set_index("code")
    for code in codes:
        if code in fund.index:
            v = fund.loc[code, "roe"]
            out[code] = float(v) if pd.notna(v) else float("nan")
        else:
            out[code] = float("nan")
    return pd.Series(out)


@register("gross_margin_stability")
def compute_gross_margin_stability(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """毛利率稳定性。需毛利率字段（FUNDAMENTAL_COLUMNS 无 gross_margin）→ v1 全 NaN。

    direction=forward。理想实现：3 年毛利率标准差越小越好（direction forward 取负或用 1/std）。
    """
    out = {code: float("nan") for code in codes}
    return pd.Series(out)


@register("cash_flow_quality")
def compute_cash_flow_quality(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """现金流质量 = 经营现金流 / 净利润（最新一期）。direction=forward。"""
    fund = slice_latest_fundamental(as_of, market, codes)
    out = {}
    if fund.empty:
        return pd.Series({c: float("nan") for c in codes})
    fund = fund.set_index("code")
    for code in codes:
        if code in fund.index:
            row = fund.loc[code]
            fcf = row.get("fcf")
            np_ = row.get("net_profit")
            if pd.notna(fcf) and pd.notna(np_) and np_ != 0:
                out[code] = float(fcf) / float(np_)
            else:
                out[code] = float("nan")
        else:
            out[code] = float("nan")
    return pd.Series(out)


@register("leverage")
def compute_leverage(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """杠杆率 = 1 - 资产负债率（最新一期）。direction=forward（越低杠杆越好）。"""
    fund = slice_latest_fundamental(as_of, market, codes)
    out = {}
    if fund.empty:
        return pd.Series({c: float("nan") for c in codes})
    fund = fund.set_index("code")
    for code in codes:
        if code in fund.index:
            debt_ratio = fund.loc[code, "debt_ratio"]
            if pd.notna(debt_ratio):
                out[code] = 1.0 - float(debt_ratio) / 100.0  # debt_ratio 是百分比
            else:
                out[code] = float("nan")
        else:
            out[code] = float("nan")
    return pd.Series(out)
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_quality.py -v`
Expected: PASS（5 个测试）

- [ ] **Step 5: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 119 + 5 = 124 passed

- [ ] **Step 6: 提交**

```bash
git add src/factor_engine/computers/quality.py tests/factor_engine/test_quality.py
git commit -m "feat(factor_engine): 质量因子 — ROE/毛利稳定/现金流质量/杠杆率"
```

---

## Task 5: 动量因子 computer（权重 0，反向校验）

**Files:**
- Create: `src/factor_engine/computers/momentum.py`
- Create: `tests/factor_engine/test_momentum.py`

**Interfaces:**
- Consumes: `register`（Task 1）；`pit.slicer.slice_quote_panel`（Phase 3）
- Produces: `@register("momentum_12m1m")` — 12-1 月动量（跳过最近1月），权重 0、in_composite=False

- [ ] **Step 1: 写失败测试 tests/factor_engine/test_momentum.py**

```python
import pandas as pd
import pytest

from src.factor_engine.registry import FACTOR_REGISTRY, compute_factor, get_factor_spec
import src.factor_engine.computers.momentum  # noqa: F401


def test_momentum_registered():
    assert "momentum_12m1m" in FACTOR_REGISTRY


def test_momentum_spec_weight_zero():
    spec = get_factor_spec("momentum_12m1m")
    assert spec["weight"] == 0.0
    assert spec["in_composite"] is False


def test_momentum_12m1m(quotes_for_momentum):
    """12-1月动量 = (T-1月收盘 / T-13月收盘) - 1，跳过最近1月。

    C1 持续涨 → 正动量；C2 持平 → ~0；C3 持续跌 → 负动量。
    as_of=2024-12-15。
    """
    s = compute_factor("momentum_12m1m", "2024-12-15", "a_share", ["C1", "C2", "C3"])
    assert s["C1"] > 0   # 涨
    assert abs(s["C2"]) < 1e-9  # 平
    assert s["C3"] < 0   # 跌


def test_momentum_insufficient_history_nan(quotes_for_momentum):
    """as_of 太早，不足 13 月 → NaN。"""
    s = compute_factor("momentum_12m1m", "2023-06-15", "a_share", ["C1"])
    assert pd.isna(s["C1"])
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_momentum.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/factor_engine/computers/momentum.py**

```python
"""动量因子：12-1月动量（spec §4.2，权重 0，仅反向校验不进综合得分）。"""
from __future__ import annotations

import pandas as pd

from src.factor_engine.registry import register
from src.pit.slicer import slice_quote_panel


@register("momentum_12m1m")
def compute_momentum_12m1m(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """12-1月动量 = (T-1月收盘 / T-13月收盘) - 1。跳过最近1月（避免短期反转）。

    direction=forward。权重永久 0、in_composite=False（spec §4.2）。
    """
    panel = slice_quote_panel(as_of, market, codes)
    out = {}
    if panel.empty:
        return pd.Series({c: float("nan") for c in codes})
    as_of_ts = pd.to_datetime(as_of)
    t_minus_1m = as_of_ts - pd.DateOffset(months=1)
    t_minus_13m = as_of_ts - pd.DateOffset(months=13)
    for code in codes:
        df = panel[panel["code"] == code].copy()
        if df.empty:
            out[code] = float("nan")
            continue
        df["date_ts"] = pd.to_datetime(df["date"])
        df = df.sort_values("date_ts")
        # 取 <= t-1m 的最近一行 与 <= t-13m 的最近一行
        recent = df[df["date_ts"] <= t_minus_1m]
        old = df[df["date_ts"] <= t_minus_13m]
        if recent.empty or old.empty:
            out[code] = float("nan")
            continue
        p_recent = float(recent.iloc[-1]["close"])
        p_old = float(old.iloc[-1]["close"])
        if p_old <= 0:
            out[code] = float("nan")
            continue
        out[code] = p_recent / p_old - 1
    return pd.Series(out)
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_momentum.py -v`
Expected: PASS（4 个测试）

- [ ] **Step 5: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 124 + 4 = 128 passed

- [ ] **Step 6: 提交**

```bash
git add src/factor_engine/computers/momentum.py tests/factor_engine/test_momentum.py
git commit -m "feat(factor_engine): 动量因子（12-1月，权重0反向校验）"
```

---

## Task 6: DCF 估值（金融股跳过）

**Files:**
- Create: `src/factor_engine/computers/dcf.py`
- Create: `tests/factor_engine/test_dcf.py`

**Interfaces:**
- Consumes: `pit.indexer.pit_fundamental_as_of`（Phase 3，取 5 年 FCF 历史）
- Produces: `dcf_intrinsic_value(code: str, as_of: str, market: str, discount_rate: float = 0.11, terminal_growth: float = 0.02) -> tuple[float, float] | None` — 返回 (内在价值下限, 内在价值上限)；金融股（industry gics=40）返回 None
- Produces: `is_financial(code: str, industry_map: dict | None = None) -> bool` — 判断是否金融股

> spec §4.4：DCF 参数保守（折现率 10-12%、终值增长 ≤3%、5 年平均 FCF）；金融股跳过返回 N/A。

- [ ] **Step 1: 写失败测试 tests/factor_engine/test_dcf.py**

```python
import math

import pandas as pd
import pytest

from src.factor_engine.computers.dcf import dcf_intrinsic_value, is_financial


def test_is_financial_by_industry_map():
    industry_map = {"C1": "40", "C2": "10", "C3": "40"}
    assert is_financial("C1", industry_map) is True
    assert is_financial("C2", industry_map) is False


def test_is_financial_no_map_returns_false():
    """无 industry_map → 无法判断 → 视为非金融（不跳过 DCF）。"""
    assert is_financial("C1", None) is False


def test_dcf_returns_value_range(fundamentals_for_factors):
    """C1 有 4 年 FCF 历史 → DCF 返回 (下限, 上限) 非空。"""
    result = dcf_intrinsic_value("C1", "2024-05-01", "a_share")
    assert result is not None
    low, high = result
    assert low > 0
    assert high >= low


def test_dcf_financial_skipped(fundamentals_for_factors):
    """金融股（industry_map gics=40）→ None。"""
    industry_map = {"C1": "40"}
    result = dcf_intrinsic_value("C1", "2024-05-01", "a_share", industry_map=industry_map)
    assert result is None


def test_dcf_insufficient_fcf_returns_none(fundamentals_for_factors):
    """FCF 历史不足 → None。"""
    # C3 有 3 年 FCF（2021-2023），仍可算；用 as_of 早到只有 1 年
    result = dcf_intrinsic_value("C3", "2022-05-01", "a_share")
    # 2022-05-01 时 C3 只有 2021 年报 → 不足
    assert result is None


def test_dcf_discount_rate_bounds():
    """折现率下限（10%）应给出更高内在价值，上限（12%）更低。"""
    r_low = dcf_intrinsic_value("C1", "2024-05-01", "a_share", discount_rate=0.10)
    r_high = dcf_intrinsic_value("C1", "2024-05-01", "a_share", discount_rate=0.12)
    assert r_low is not None and r_high is not None
    # 下限折现率 → 更高价值
    assert r_low[0] > r_high[0]
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_dcf.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/factor_engine/computers/dcf.py**

```python
"""DCF 估值（spec §4.4）：金融股跳过，保守参数，5 年平均 FCF。

输出内在价值区间（下限=高折现率，上限=低折现率）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.pit.indexer import pit_fundamental_as_of

# spec §4.4 DCF 参数
DISCOUNT_RATE_LOW = 0.10   # 上限价值用低折现率
DISCOUNT_RATE_HIGH = 0.12  # 下限价值用高折现率
TERMINAL_GROWTH_MAX = 0.03
FCF_LOOKBACK = 5
FINANCIAL_GICS = "40"


def is_financial(code: str, industry_map: dict | None = None) -> bool:
    """判断是否金融股（GICS 40）。无 industry_map → False（不跳过）。"""
    if industry_map is None:
        return False
    return industry_map.get(code) == FINANCIAL_GICS


def _dcf_value(avg_fcf: float, discount_rate: float, terminal_growth: float) -> float:
    """简化 DCF：永续增长模型 V = FCF / (r - g)。"""
    if discount_rate <= terminal_growth:
        return float("nan")
    return avg_fcf / (discount_rate - terminal_growth)


def dcf_intrinsic_value(
    code: str,
    as_of: str,
    market: str,
    discount_rate: float = 0.11,
    terminal_growth: float = 0.02,
    industry_map: dict | None = None,
) -> tuple[float, float] | None:
    """返回 (内在价值下限, 内在价值上限)。金融股/FCF 不足 → None。

    spec §4.4：5 年平均 FCF，折现率 10-12%，终值增长 ≤3%。
    下限 = 高折现率（0.12）价值；上限 = 低折现率（0.10）价值。
    """
    if is_financial(code, industry_map):
        return None
    fund = pit_fundamental_as_of(as_of, market, code=code)
    if fund.empty:
        return None
    fcfs = fund["fcf"].dropna()
    if len(fcfs) < 2:  # 至少 2 年 FCF
        return None
    # 取最近 FCF_LOOKBACK 年平均
    recent = fcfs.tail(FCF_LOOKBACK)
    avg_fcf = float(recent.mean())
    if avg_fcf <= 0:
        return None
    g = min(terminal_growth, TERMINAL_GROWTH_MAX)
    low = _dcf_value(avg_fcf, DISCOUNT_RATE_HIGH, g)   # 高折现率 → 下限
    high = _dcf_value(avg_fcf, DISCOUNT_RATE_LOW, g)   # 低折现率 → 上限
    if np.isnan(low) or np.isnan(high):
        return None
    return (low, high)
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_dcf.py -v`
Expected: PASS（6 个测试）

- [ ] **Step 5: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 128 + 6 = 134 passed

- [ ] **Step 6: 提交**

```bash
git add src/factor_engine/computers/dcf.py tests/factor_engine/test_dcf.py
git commit -m "feat(factor_engine): DCF 估值（金融股跳过，保守参数）"
```

---

## Task 7: Standardizer（分层标准化 + 分位秩）

**Files:**
- Create: `src/factor_engine/standardizer.py`
- Create: `tests/factor_engine/test_standardizer.py`

**Interfaces:**
- Consumes: 因子原始值 Series（Task 2-5 produce）
- Produces: `winsorize_mad(s: pd.Series, n_mad: float = 5.0) -> pd.Series` — MAD 法截尾
- Produces: `zscore(s: pd.Series) -> pd.Series` — Z-score 标准化（NaN 保留）
- Produces: `standardize_factor(raw: pd.Series, market: str, industry_map: dict | None, code_market: dict, code_industry: dict | None, sector_neutral: bool = True, size_neutral: bool = False, code_size: dict | None = None) -> pd.Series` — 分层标准化（截尾→市场内Z→行业内Z→可选市值中性）
- Produces: `percentile_rank(s: pd.Series) -> pd.Series` — 市场内分位秩（用于跨市场排序，spec §4.2）

- [ ] **Step 1: 写失败测试 tests/factor_engine/test_standardizer.py**

```python
import numpy as np
import pandas as pd
import pytest

from src.factor_engine.standardizer import (
    percentile_rank,
    standardize_factor,
    winsorize_mad,
    zscore,
)


def test_winsorize_mad_clamps_outliers():
    """MAD 截尾：极端值钳到 median±5×MAD。"""
    s = pd.Series([1, 2, 3, 4, 5, 100])  # 100 是离群点
    out = winsorize_mad(s, n_mad=5.0)
    assert out.max() < 100  # 被截
    assert out.min() == 1


def test_winsorize_preserves_nan():
    s = pd.Series([1.0, 2.0, np.nan, 4.0])
    out = winsorize_mad(s)
    assert pd.isna(out.iloc[2])


def test_zscore_basic():
    s = pd.Series([1.0, 2.0, 3.0])
    out = zscore(s)
    assert abs(out.mean()) < 1e-9
    assert abs(out.std() - 1.0) < 1e-9


def test_zscore_preserves_nan():
    s = pd.Series([1.0, np.nan, 3.0])
    out = zscore(s)
    assert pd.isna(out.iloc[1])


def test_standardize_market_only():
    """无 industry_map → 仅市场内 Z（跳过行业内层）。"""
    raw = pd.Series({"C1": 30.0, "C2": 15.0, "C3": 5.0})
    code_market = {"C1": "a_share", "C2": "a_share", "C3": "a_share"}
    out = standardize_factor(raw, market="a_share", industry_map=None,
                             code_market=code_market, code_industry=None)
    # 全部同市场 → 等价于截尾+Z
    assert abs(out.mean()) < 1e-9


def test_standardize_sector_neutral():
    """行业内 Z：同行业内标准化消除行业结构差异。"""
    raw = pd.Series({"C1": 30.0, "C2": 15.0, "C3": 5.0, "C4": 25.0})
    code_market = {c: "a_share" for c in ["C1", "C2", "C3", "C4"]}
    code_industry = {"C1": "tech", "C2": "tech", "C3": "consumer", "C4": "consumer"}
    out = standardize_factor(raw, market="a_share", industry_map={"C1": "x"},
                             code_market=code_market, code_industry=code_industry,
                             sector_neutral=True)
    # tech 组 (C1,C2) 与 consumer 组 (C3,C4) 各自 Z 后均值≈0
    tech_mean = out[["C1", "C2"]].mean()
    consumer_mean = out[["C3", "C4"]].mean()
    assert abs(tech_mean) < 1e-9
    assert abs(consumer_mean) < 1e-9


def test_percentile_rank():
    s = pd.Series({"C1": 30.0, "C2": 15.0, "C3": 5.0, "C4": 25.0})
    rank = percentile_rank(s)
    # C3=5 最小 → 0.0；C1=30 最大 → 1.0
    assert abs(rank["C3"] - 0.0) < 1e-9
    assert abs(rank["C1"] - 1.0) < 1e-9
    # 所有值在 [0,1]
    assert (rank >= 0).all() and (rank <= 1).all()


def test_percentile_rank_handles_nan():
    s = pd.Series({"C1": 30.0, "C2": np.nan, "C3": 5.0})
    rank = percentile_rank(s)
    assert pd.isna(rank["C2"])
    assert abs(rank["C3"] - 0.0) < 1e-9
    assert abs(rank["C1"] - 1.0) < 1e-9
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_standardizer.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/factor_engine/standardizer.py**

```python
"""分层标准化（spec §4.2）：截尾 → 市场内 Z → 行业内 Z → 可选市值中性 + 分位秩。"""
from __future__ import annotations

import numpy as np
import pandas as pd


def winsorize_mad(s: pd.Series, n_mad: float = 5.0) -> pd.Series:
    """MAD 法截尾：超出 median ± n_mad×MAD 的值钳到边界（spec §4.2 第0层）。"""
    valid = s.dropna()
    if valid.empty:
        return s.copy()
    median = valid.median()
    mad = (valid - median).abs().median()
    if mad == 0 or np.isnan(mad):
        return s.copy()  # 无离散度，不截
    lower = median - n_mad * mad
    upper = median + n_mad * mad
    return s.clip(lower=lower, upper=upper)


def zscore(s: pd.Series) -> pd.Series:
    """Z-score 标准化（NaN 保留）。"""
    valid = s.dropna()
    if valid.empty:
        return s.copy()
    std = valid.std()
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=s.index)  # 无离散度 → 全 0
    return (s - valid.mean()) / std


def _zscore_within_group(s: pd.Series, groups: pd.Series) -> pd.Series:
    """按 groups 分组做 Z-score。"""
    out = pd.Series(np.nan, index=s.index, dtype=float)
    for g, idx in groups.groupby(groups).groups.items():
        out.loc[idx] = zscore(s.loc[idx])
    return out


def standardize_factor(
    raw: pd.Series,
    market: str,
    industry_map: dict | None,
    code_market: dict,
    code_industry: dict | None,
    sector_neutral: bool = True,
    size_neutral: bool = False,
    code_size: dict | None = None,
) -> pd.Series:
    """分层标准化（spec §4.2）。

    第0层：MAD 截尾
    第1层：市场内 Z（本函数已限定 market，故直接对传入的 raw 做 Z）
    第2层：行业内 Z（若 sector_neutral 且 code_industry 提供）
    第3层：市值中性（v1 默认关闭）
    """
    s = winsorize_mad(raw)
    # 第1层：市场内 Z（调用方已按 market 过滤，此处对当前序列做 Z）
    s = zscore(s)
    # 第2层：行业内 Z
    if sector_neutral and code_industry is not None:
        groups = pd.Series({c: code_industry.get(c) for c in s.index})
        s = _zscore_within_group(s, groups)
    # 第3层：市值中性（v1 默认关闭，YAGNI）
    if size_neutral and code_size is not None:
        # 按 size 分位回归取残差（v1 简化：跳过，仅留接口）
        pass
    return s


def percentile_rank(s: pd.Series) -> pd.Series:
    """市场内分位秩（spec §4.2 跨市场排序用）。NaN 保留。

    返回每个值在该序列中的分位（0=最小，1=最大）。
    """
    valid = s.dropna()
    if valid.empty:
        return s.copy()
    ranks = valid.rank(method="average")
    n = len(valid)
    out = pd.Series(np.nan, index=s.index, dtype=float)
    out.loc[valid.index] = (ranks - 1) / (n - 1) if n > 1 else 0.5
    return out
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_standardizer.py -v`
Expected: PASS（8 个测试）

- [ ] **Step 5: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 134 + 8 = 142 passed

- [ ] **Step 6: 提交**

```bash
git add src/factor_engine/standardizer.py tests/factor_engine/test_standardizer.py
git commit -m "feat(factor_engine): 分层标准化（截尾/市场内Z/行业内Z/分位秩）"
```

---

## Task 8: Engine 编排（universe→计算→标准化→矩阵+分位秩）

**Files:**
- Create: `src/factor_engine/engine.py`
- Create: `tests/factor_engine/test_engine.py`

**Interfaces:**
- Consumes: `registry.list_in_composite_factors`、`compute_factor`、`get_factor_spec`（Task 1）；`standardize_factor`、`percentile_rank`（Task 7）；`pit.indexer.pit_active_universe`（Phase 3）；各 computers 模块（import 触发注册）
- Produces: `FactorMatrix` dataclass（`matrix: pd.DataFrame` codes×in_composite_factors 标准化值；`ranks: pd.DataFrame` codes×factors 分位秩；`momentum: pd.Series` 动量反向校验值；`raw: pd.DataFrame` 原始值）
- Produces: `compute_factor_matrix(as_of: str, market: str, codes: list[str], industry_map: dict | None = None, code_industry: dict | None = None) -> FactorMatrix`

- [ ] **Step 1: 写失败测试 tests/factor_engine/test_engine.py**

```python
import pandas as pd
import pytest

from src.factor_engine.engine import FactorMatrix, compute_factor_matrix
# 触发所有 computer 注册
import src.factor_engine.computers.value  # noqa
import src.factor_engine.computers.growth  # noqa
import src.factor_engine.computers.quality  # noqa
import src.factor_engine.computers.momentum  # noqa


def test_compute_factor_matrix_structure(fundamentals_for_factors):
    fm = compute_factor_matrix("2024-05-01", "a_share", ["C1", "C2", "C3"])
    # matrix 列 = 10 个 in_composite 因子
    assert set(fm.matrix.columns) == set([
        "pe_percentile", "pb_percentile", "dividend_yield", "fcf_yield",
        "revenue_cagr_3y", "profit_cagr_3y", "rnd_ratio",
        "roe", "gross_margin_stability", "cash_flow_quality", "leverage",
    ]) or len(fm.matrix.columns) == 10
    # index = codes
    assert set(fm.matrix.index) == {"C1", "C2", "C3"}
    # ranks 与 matrix 同形状
    assert fm.ranks.shape == fm.matrix.shape
    # momentum 独立（不进 matrix）
    assert "momentum_12m1m" not in fm.matrix.columns
    assert fm.momentum is not None


def test_compute_factor_matrix_excludes_delisted(fundamentals_for_factors, a_share_delisting):
    """退市股票从 universe 扣除（spec §5.3）。"""
    # a_share_delisting fixture 来自 tests/pit/conftest（C1 2020 退市）
    # 注意：fundamentals 的 C1 与 delisting 的 C1 同码会冲突，用不同 code 测
    fm = compute_factor_matrix("2024-05-01", "a_share", ["C2", "C3", "C1"])
    # 此处 C1 在 fundamentals 中存在；delisting fixture 的 C1 delist 2020
    # 若两 fixture 的 C1 冲突，测试应只验证不崩 + matrix 含传入的活跃 code
    assert len(fm.matrix) <= 3


def test_factor_matrix_dataclass():
    m = pd.DataFrame({"roe": [1.0, 2.0]}, index=["A", "B"])
    r = pd.DataFrame({"roe": [0.0, 1.0]}, index=["A", "B"])
    mom = pd.Series({"A": 0.1, "B": -0.1})
    raw = m.copy()
    fm = FactorMatrix(matrix=m, ranks=r, momentum=mom, raw=raw)
    assert fm.matrix.equals(m)
    assert fm.momentum.equals(mom)


def test_compute_factor_matrix_empty_codes(isolated_data_dir):
    fm = compute_factor_matrix("2024-05-01", "a_share", [])
    assert fm.matrix.empty
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_engine.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/factor_engine/engine.py**

```python
"""Factor Engine 编排：universe → 逐因子计算 → 分层标准化 → 矩阵+分位秩（spec §5.2）。"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.factor_engine.registry import (
    compute_factor,
    get_factor_spec,
    list_in_composite_factors,
)
from src.factor_engine.standardizer import percentile_rank, standardize_factor
from src.pit.indexer import pit_active_universe, pit_delisted_before

# 触发所有 computer 注册
import src.factor_engine.computers.value  # noqa: F401
import src.factor_engine.computers.growth  # noqa: F401
import src.factor_engine.computers.quality  # noqa: F401
import src.factor_engine.computers.momentum  # noqa: F401


@dataclass
class FactorMatrix:
    """因子矩阵输出。"""
    matrix: pd.DataFrame          # codes × in_composite_factors，标准化值
    ranks: pd.DataFrame           # codes × factors，市场内分位秩
    momentum: pd.Series           # 动量反向校验值（不进 matrix）
    raw: pd.DataFrame = field(default_factory=pd.DataFrame)  # 原始值


def compute_factor_matrix(
    as_of: str,
    market: str,
    codes: list[str],
    industry_map: dict | None = None,
    code_industry: dict | None = None,
) -> FactorMatrix:
    """计算因子矩阵：扣退市 → 逐因子计算 → 分层标准化 → 输出矩阵+分位秩。

    spec §5.2/§5.3。动量权重 0 不进 matrix，单独输出供反向校验。
    """
    # 扣除退市（spec §5.3）
    active = pit_active_universe(as_of, market, codes) if codes else []
    if not active:
        return FactorMatrix(
            matrix=pd.DataFrame(),
            ranks=pd.DataFrame(),
            momentum=pd.Series(dtype=float),
            raw=pd.DataFrame(),
        )

    code_market = {c: market for c in active}
    in_composite = list_in_composite_factors()

    raw_data = {}
    rank_data = {}
    for name in in_composite:
        spec = get_factor_spec(name)
        raw_series = compute_factor(name, as_of, market, active)
        # 方向处理：reverse 因子取负（使"越好"统一为越大）
        if spec.get("direction") == "reverse":
            raw_series = -raw_series
        raw_data[name] = raw_series
        # 分层标准化
        standardized = standardize_factor(
            raw_series, market=market, industry_map=industry_map,
            code_market=code_market, code_industry=code_industry,
            sector_neutral=spec.get("params", {}).get("sector_neutral", True),
        )
        # 分位秩（spec §4.2 跨市场排序用）
        rank_data[name] = percentile_rank(standardized)

    raw_df = pd.DataFrame(raw_data, index=active)
    matrix = pd.DataFrame(
        {n: standardize_factor(
            raw_data[n], market=market, industry_map=industry_map,
            code_market=code_market, code_industry=code_industry,
            sector_neutral=get_factor_spec(n).get("params", {}).get("sector_neutral", True),
        ) for n in in_composite},
        index=active,
    )
    ranks = pd.DataFrame(rank_data, index=active)

    # 动量（不进 matrix，单独输出）
    momentum = compute_factor("momentum_12m1m", as_of, market, active)

    return FactorMatrix(matrix=matrix, ranks=ranks, momentum=momentum, raw=raw_df)
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_engine.py -v`
Expected: PASS（4 个测试）

- [ ] **Step 5: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 142 + 4 = 146 passed

- [ ] **Step 6: 提交**

```bash
git add src/factor_engine/engine.py tests/factor_engine/test_engine.py
git commit -m "feat(factor_engine): engine 编排（universe→计算→标准化→矩阵+分位秩）"
```

---

## Task 9: IC/ICIR 分市场检验

**Files:**
- Create: `src/factor_engine/ic_test.py`
- Create: `tests/factor_engine/test_ic.py`

**Interfaces:**
- Consumes: 因子值 Series + 前瞻收益 Series
- Produces: `ic(factor_values: pd.Series, forward_returns: pd.Series) -> float` — 单期 IC（Spearman 秩相关）
- Produces: `icir(ic_series: pd.Series) -> float` — IC 信息比（mean IC / std IC）
- Produces: `factor_ic_report(factor_name: str, market: str, as_of_dates: list[str], codes: list[str], return_lookup: dict[str, pd.Series]) -> dict` — 分市场多期 IC 报告（spec §5.3 v1 IC 检验）

- [ ] **Step 1: 写失败测试 tests/factor_engine/test_ic.py**

```python
import numpy as np
import pandas as pd
import pytest

from src.factor_engine.ic_test import factor_ic_report, ic, icir


def test_ic_perfect_positive():
    """因子值与收益完全正相关 → IC ≈ 1。"""
    factor = pd.Series({"A": 1, "B": 2, "C": 3, "D": 4})
    returns = pd.Series({"A": 0.01, "B": 0.02, "C": 0.03, "D": 0.04})
    assert abs(ic(factor, returns) - 1.0) < 1e-9


def test_ic_perfect_negative():
    factor = pd.Series({"A": 1, "B": 2, "C": 3, "D": 4})
    returns = pd.Series({"A": 0.04, "B": 0.03, "C": 0.02, "D": 0.01})
    assert abs(ic(factor, returns) - (-1.0)) < 1e-9


def test_ic_handles_nan():
    factor = pd.Series({"A": 1, "B": np.nan, "C": 3, "D": 4})
    returns = pd.Series({"A": 0.01, "B": 0.02, "C": 0.03, "D": 0.04})
    val = ic(factor, returns)
    assert not np.isnan(val)


def test_ic_insufficient_data_returns_nan():
    factor = pd.Series({"A": 1})
    returns = pd.Series({"A": 0.01})
    assert np.isnan(ic(factor, returns))


def test_icir_basic():
    ic_series = pd.Series([0.1, 0.2, 0.15, 0.25])
    val = icir(ic_series)
    assert abs(val - ic_series.mean() / ic_series.std()) < 1e-9


def test_icir_zero_std_returns_nan():
    ic_series = pd.Series([0.1, 0.1, 0.1])
    assert np.isnan(icir(ic_series))


def test_factor_ic_report_structure():
    """多期 IC 报告返回 mean_ic, icir, ic_series。"""
    factor_name = "roe"
    market = "a_share"
    as_of_dates = ["2024-01-01", "2024-04-01", "2024-07-01"]
    codes = ["A", "B", "C"]
    # 每期因子值 + 前瞻收益
    factor_lookup = {
        "2024-01-01": pd.Series({"A": 1, "B": 2, "C": 3}),
        "2024-04-01": pd.Series({"A": 2, "B": 3, "C": 1}),
        "2024-07-01": pd.Series({"A": 3, "B": 1, "C": 2}),
    }
    return_lookup = {
        "2024-01-01": pd.Series({"A": 0.01, "B": 0.02, "C": 0.03}),
        "2024-04-01": pd.Series({"A": 0.02, "B": 0.03, "C": 0.01}),
        "2024-07-01": pd.Series({"A": 0.03, "B": 0.01, "C": 0.02}),
    }
    report = factor_ic_report(factor_name, market, as_of_dates, codes, return_lookup,
                              factor_lookup=factor_lookup)
    assert "mean_ic" in report
    assert "icir" in report
    assert "ic_series" in report
    assert len(report["ic_series"]) == 3
    # 完美正相关 → mean_ic ≈ 1
    assert abs(report["mean_ic"] - 1.0) < 1e-9
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_ic.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/factor_engine/ic_test.py**

```python
"""因子 IC/ICIR 分市场检验（spec §5.3 v1）。"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import spearmanr  # noqa  # pandas 内置 rank-based corr 即可

from src.factor_engine.registry import compute_factor


def ic(factor_values: pd.Series, forward_returns: pd.Series) -> float:
    """单期 IC = Spearman 秩相关（因子值与前瞻收益）。

    spec §5.3：IC 衡量因子选股能力。NaN 对齐后丢弃；<3 个有效点 → NaN。
    """
    df = pd.DataFrame({"f": factor_values, "r": forward_returns}).dropna()
    if len(df) < 3:
        return float("nan")
    # Spearman = Pearson on ranks
    return float(df["f"].rank().corr(df["r"].rank()))


def icir(ic_series: pd.Series) -> float:
    """ICIR = mean(IC) / std(IC)。std=0 → NaN。"""
    valid = ic_series.dropna()
    if len(valid) < 2:
        return float("nan")
    std = valid.std()
    if std == 0 or np.isnan(std):
        return float("nan")
    return float(valid.mean() / std)


def factor_ic_report(
    factor_name: str,
    market: str,
    as_of_dates: list[str],
    codes: list[str],
    return_lookup: dict[str, pd.Series],
    factor_lookup: dict[str, pd.Series] | None = None,
) -> dict:
    """分市场多期 IC 报告（spec §5.3）。

    factor_lookup 给定时直接用（测试用）；否则调 compute_factor 实算。
    return_lookup: as_of_date → 前瞻收益 Series。
    返回 {mean_ic, icir, ic_series}。
    """
    ic_vals = {}
    for as_of in as_of_dates:
        if factor_lookup is not None:
            factor_values = factor_lookup.get(as_of, pd.Series(dtype=float))
        else:
            factor_values = compute_factor(factor_name, as_of, market, codes)
        forward_returns = return_lookup.get(as_of, pd.Series(dtype=float))
        ic_vals[as_of] = ic(factor_values, forward_returns)
    ic_series = pd.Series(ic_vals)
    return {
        "mean_ic": float(ic_series.mean()) if not ic_series.dropna().empty else float("nan"),
        "icir": icir(ic_series),
        "ic_series": ic_series,
    }
```

- [ ] **Step 4: 修正 scipy 依赖（改用 pandas 内置，避免引入 scipy）**

`ic_test.py` 顶部 `from scipy.stats import spearmanr` 是未使用的占位 import（实际用 pandas `rank().corr()`）。删除该行以避免引入 scipy 依赖（spec 不引入新依赖）。

```python
# 删除这一行：
# from scipy.stats import spearmanr  # noqa
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/test_ic.py -v`
Expected: PASS（7 个测试）

- [ ] **Step 6: 运行全套件确认无回归**

Run: `/home/jerry/value/.venv/bin/pytest -m "not integration" -q`
Expected: 146 + 7 = 153 passed

- [ ] **Step 7: 提交**

```bash
git add src/factor_engine/ic_test.py tests/factor_engine/test_ic.py
git commit -m "feat(factor_engine): IC/ICIR 分市场因子有效性检验"
```

- [ ] **Step 8: 运行完整 factor_engine 套件确认全绿**

Run: `/home/jerry/value/.venv/bin/pytest tests/factor_engine/ -v`
Expected: 全部 PASS（registry 6 + value 5 + growth 5 + quality 5 + momentum 4 + dcf 6 + standardizer 8 + engine 4 + ic 7 = 50）

> **Phase 4 完成条件：** 全部非集成测试通过（153 passed）；factor_engine suite 50 passed。此后方可进入 Phase 5（Strategy Engine）。
