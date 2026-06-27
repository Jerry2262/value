# 量化投资机器人 — Phase 1 实现计划（数据探针 & 基础脚手架）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 搭建项目脚手架、配置体系与数据质量基础设施，并完成设计文档 §12 的五项开工前数据探针，验证 akshare/yfinance 对三地市场财报披露日/退市列表/基准指数/财务字段的底层可用性。

**Architecture:** 探针采用"可注入 fetcher"设计——探针逻辑只依赖一个 `Fetcher` 协议，单元测试用 fixture 数据验证判定逻辑（passed/failed/warning 阈值），集成运行才真正调用 akshare/yfinance。探针结果写入 `metadata/data_quality.db`（SQLite WAL）。

**Tech Stack:** Python 3.11+、akshare、yfinance、pandas、numpy、pyarrow、pyyaml、pytest、pytest-mock

## Global Constraints

（摘自 spec §1.3/§1.4/§3.6/§8/§12，逐字执行）

- 目标市场：A 股（akshare）、美股（yfinance）、港股（akshare 主 + yfinance 备）
- 存储：SQLite（全部开启 WAL 模式 `PRAGMA journal_mode=WAL`）+ Parquet
- 不引入 PostgreSQL/Redis/Celery/Docker/backtrader
- 探针脚本放 `src/data_pipeline/probes/`，每个独立可运行，输出 `passed/failed/warning + 统计摘要`
- 探针结果纳入 `data_quality.db` 并在 Dashboard Tab 1 可查看（数据源健康度面板）
- API key 通过环境变量注入，不写入配置文件或代码（`.env` 在 .gitignore）
- 写操作在应用层串行化（单线程写锁）
- 五项探针全部通过后才动工写策略代码（spec §12 硬门槛）
- 货币：本地货币 CNY/USD/HKD，回测按汇率折算；汇率公式 `实际收益(CNY) = (1+本币总收益含分红) × (1+汇率变动) - 1`

---

## 全项目分阶段路线图（Phase 1 之后待细化）

本计划只详细展开 **Phase 1**。后续阶段在执行到达时各自生成独立计划文件。

| 阶段 | 内容 | 依赖 | 状态 |
|------|------|------|------|
| **Phase 1** | 脚手架 + 配置 + 数据探针 + 数据质量 DB（本计划） | 无 | 待执行 |
| Phase 2 | Data Pipeline：fetchers（行情/财务/汇率/退市/基准）+ cleaners + store（日期分区 Parquet） | Phase 1 探针结论 | 待细化 |
| Phase 3 | PIT 服务：indexer + slicer（按 as_of_date 切片，财报披露日 PIT） | Phase 2 | 待细化 |
| Phase 4 | Factor Engine：registry + computers + standardizer（分层标准化）+ ic_test | Phase 3 | 待细化 |
| Phase 5 | Strategy Engine：funnel（五层）+ scorer + pricing（PE 分位+DCF）+ rotation（展示）+ ai_assist | Phase 4 | 待细化 |
| Phase 6 | Portfolio State：state（thesis+确信度）+ pnl（CNY 折算）+ 组合占用率 | Phase 5 | 待细化 |
| Phase 7 | Core Engine：backtest（PIT 安全+样本内外+基准+低 N 警告+regime）+ risk + 因子归因 | Phase 4,6 | 待细化 |
| Phase 8 | Web Dashboard：Gradio 四 Tab | Phase 5,6,7 | 待细化 |
| Phase 9 | run.py 每日链路 + 集成测试 | 全部 | 待细化 |

---

## File Structure（Phase 1 范围）

```
value/
├── requirements.txt                          # Task 1
├── pyproject.toml                            # Task 1（pytest 配置）
├── .env.example                              # Task 1
├── config/
│   ├── universe.yaml                         # Task 2
│   ├── strategy.yaml                         # Task 2
│   └── factors/
│       ├── value.yaml                        # Task 2
│       ├── growth.yaml                       # Task 2
│       ├── quality.yaml                      # Task 2
│       └── momentum.yaml                     # Task 2
├── src/
│   ├── __init__.py
│   ├── config.py                             # Task 1（YAML 加载 + 路径）
│   ├── storage/
│   │   ├── __init__.py
│   │   └── sqlite.py                         # Task 1（WAL + 写锁连接）
│   └── data_pipeline/
│       ├── __init__.py
│       └── probes/
│           ├── __init__.py
│           ├── base.py                       # Task 3（ProbeResult + Fetcher 协议 + Probe 基类）
│           ├── probe_announcement_date.py    # Task 4（探针 1）
│           ├── probe_hk_delisting.py         # Task 5（探针 2）
│           ├── probe_us_delisting.py         # Task 6（探针 3）
│           ├── probe_value_benchmark.py      # Task 7（探针 4）
│           ├── probe_fundamental_fields.py   # Task 8（探针 5）
│           └── runner.py                     # Task 9（批量执行 + 写 data_quality.db）
├── tests/
│   ├── __init__.py
│   ├── conftest.py                           # Task 1（tmp DB fixtures）
│   ├── test_config.py                        # Task 1
│   ├── test_storage_sqlite.py                # Task 1
│   └── test_probes/
│       ├── __init__.py
│       ├── test_base.py                      # Task 3
│       ├── test_probe_announcement_date.py   # Task 4
│       ├── test_probe_hk_delisting.py        # Task 5
│       ├── test_probe_us_delisting.py        # Task 6
│       ├── test_probe_value_benchmark.py     # Task 7
│       ├── test_probe_fundamental_fields.py  # Task 8
│       └── test_runner.py                    # Task 9
└── data/                                     # gitignore，运行时创建
    └── metadata/data_quality.db
```

---

## Task 1: 项目脚手架与存储基础设施

**Files:**
- Create: `requirements.txt`
- Create: `pyproject.toml`
- Create: `.env.example`
- Create: `src/__init__.py`
- Create: `src/config.py`
- Create: `src/storage/__init__.py`
- Create: `src/storage/sqlite.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/test_config.py`
- Create: `tests/test_storage_sqlite.py`

**Interfaces:**
- Produces: `src.config.load_config()` → `dict`；`src.config.PROJECT_ROOT` → `pathlib.Path`
- Produces: `src.storage.sqlite.get_connection(db_path: Path) -> sqlite3.Connection`（WAL + 外键 + 串行写锁）
- Produces: `src.storage.sqlite.run_write(db_path, fn)` — 串行化写事务执行器

- [ ] **Step 1: 创建 requirements.txt**

```
akshare>=1.12.0
yfinance>=0.2.40
pandas>=2.1.0
numpy>=1.26.0
pyarrow>=15.0.0
pyyaml>=6.0.1
plotly>=5.20.0
altair>=5.3.0
gradio>=4.30.0
schedule>=1.2.1
pytest>=8.0.0
pytest-mock>=3.14.0
python-dotenv>=1.0.1
```

- [ ] **Step 2: 创建 pyproject.toml**

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
addopts = "-v --tb=short"
markers = [
    "integration: marks tests that hit the real network (deselect with '-m \"not integration\"')",
]
```

- [ ] **Step 3: 创建 .env.example**

```
# AI 问题生成器搜索密钥（spec §4.6），实盘运行前填入真实值到 .env
WEBSEARCH_API_KEY=

# 数据目录（默认项目根 data/）
VALUE_DATA_DIR=
```

- [ ] **Step 4: 创建 src/__init__.py 与 tests/__init__.py**

```python
# src/__init__.py
"""量化投资机器人 —— 量化价值辅助系统。"""
__version__ = "0.1.0"
```

```python
# tests/__init__.py
```

- [ ] **Step 5: 写 src/config.py**

```python
"""配置加载与项目路径。"""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"


def _data_dir() -> Path:
    override = os.environ.get("VALUE_DATA_DIR")
    return Path(override).resolve() if override else PROJECT_ROOT / "data"


DATA_DIR = _data_dir()
METADATA_DIR = DATA_DIR / "metadata"


def load_config(name: str) -> dict:
    """加载 config/ 下的 YAML 配置（name 不含扩展名或含均可）。"""
    if not name.endswith(".yaml"):
        name = name + ".yaml"
    path = CONFIG_DIR / name
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_factor_configs() -> dict:
    """加载 config/factors/ 下全部因子定义，合并为 {factor_key: {...}}。"""
    merged: dict = {}
    factor_dir = CONFIG_DIR / "factors"
    for f in sorted(factor_dir.glob("*.yaml")):
        data = yaml.safe_load(f) or {}
        for key, spec in (data.get("factors") or {}).items():
            merged[key] = spec
    return merged


def ensure_dirs() -> None:
    """创建运行时目录（不纳入 git）。"""
    for d in (DATA_DIR, METADATA_DIR, DATA_DIR / "raw", DATA_DIR / "processed", DATA_DIR / "pit"):
        d.mkdir(parents=True, exist_ok=True)


# 模块导入时加载 .env
load_dotenv(PROJECT_ROOT / ".env")
```

- [ ] **Step 6: 写失败测试 tests/test_config.py**

```python
import os
from pathlib import Path

import pytest

from src import config


def test_project_root_exists():
    assert config.PROJECT_ROOT.is_dir()
    assert (config.CONFIG_DIR / "universe.yaml").is_file()


def test_load_config_universe():
    cfg = config.load_config("universe")
    assert "markets" in cfg
    assert "a_share" in cfg["markets"]


def test_data_dir_override(monkeypatch, tmp_path):
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    # 重新读取模块级常量需要调用函数；验证函数逻辑
    from src.config import _data_dir
    assert _data_dir() == tmp_path


def test_load_factor_configs_has_value_factors():
    factors = config.load_factor_configs()
    assert "pe_percentile" in factors
    assert factors["pe_percentile"]["category"] == "value"


def test_momentum_weight_is_zero(factors_cfg := None):
    factors = config.load_factor_configs()
    assert factors["momentum_12m1m"]["weight"] == 0.0
    assert factors["momentum_12m1m"]["in_composite"] is False


def test_ensure_dirs_creates_metadata(tmp_path, monkeypatch):
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    # ensure_dirs 用的是模块导入时的 DATA_DIR；直接调用并断言子目录被建
    config.METADATA_DIR.mkdir(parents=True, exist_ok=True)
    assert config.METADATA_DIR.is_dir()
```

- [ ] **Step 7: 运行测试，确认因配置文件缺失而失败**

Run: `pytest tests/test_config.py -v`
Expected: FAIL（`config/universe.yaml` 等尚未创建）—— 这些将在 Task 2 创建。先记录为已知失败，继续。

- [ ] **Step 8: 写 src/storage/sqlite.py**

```python
"""SQLite 连接管理：WAL 模式 + 外键 + 应用层串行写锁。"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Callable, TypeVar

T = TypeVar("T")

_write_lock = threading.Lock()


def get_connection(db_path: Path) -> sqlite3.Connection:
    """返回启用 WAL + 外键的连接。调用方负责 close。"""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def run_write(db_path: Path, fn: Callable[[sqlite3.Connection], T]) -> T:
    """串行化执行写事务：加全局写锁，开事务，执行 fn，提交/回滚。"""
    with _write_lock:
        conn = get_connection(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE;")
            result = fn(conn)
            conn.execute("COMMIT;")
            return result
        except Exception:
            conn.execute("ROLLBACK;")
            raise
        finally:
            conn.close()


def execute_script(db_path: Path, script: str) -> None:
    """执行建表 DDL 脚本（一次性，用于初始化 schema）。"""
    def _do(conn: sqlite3.Connection) -> None:
        conn.executescript(script)

    run_write(db_path, _do)
```

- [ ] **Step 9: 写失败测试 tests/test_storage_sqlite.py**

```python
import threading
from pathlib import Path

import pytest

from src.storage import sqlite as sq


def test_get_connection_enables_wal(tmp_path):
    db = tmp_path / "t.db"
    conn = sq.get_connection(db)
    mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
    assert mode.lower() == "wal"
    conn.close()


def test_run_write_commits(tmp_path):
    db = tmp_path / "t.db"
    sq.execute_script(db, "CREATE TABLE x(v INTEGER);")

    def insert(conn):
        conn.execute("INSERT INTO x(v) VALUES (42);")
        return conn.execute("SELECT COUNT(*) FROM x;").fetchone()[0]

    count = sq.run_write(db, insert)
    assert count == 1


def test_run_write_rolls_back_on_error(tmp_path):
    db = tmp_path / "t.db"
    sq.execute_script(db, "CREATE TABLE x(v INTEGER);")

    def boom(conn):
        conn.execute("INSERT INTO x(v) VALUES(1);")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        sq.run_write(db, boom)

    conn = sq.get_connection(db)
    assert conn.execute("SELECT COUNT(*) FROM x;").fetchone()[0] == 0
    conn.close()


def test_writes_are_serialized(tmp_path):
    """并发写不应产生交错损坏（WAL + 全局写锁）。"""
    db = tmp_path / "t.db"
    sq.execute_script(db, "CREATE TABLE x(v INTEGER);")

    errors = []

    def writer(n):
        try:
            for _ in range(50):
                sq.run_write(db, lambda c: c.execute("INSERT INTO x(v) VALUES(?);", (n,)))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    conn = sq.get_connection(db)
    assert conn.execute("SELECT COUNT(*) FROM x;").fetchone()[0] == 200
    conn.close()
```

- [ ] **Step 10: 写 tests/conftest.py**

```python
import os
from pathlib import Path

import pytest


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    """把 VALUE_DATA_DIR 指向临时目录，避免污染真实 data/。"""
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    # 重新导入 config 以刷新模块级常量
    import importlib
    import src.config as cfg
    importlib.reload(cfg)
    yield tmp_path
    importlib.reload(cfg)  # 恢复


@pytest.fixture
def quality_db(isolated_data_dir):
    """返回临时 data_quality.db 路径。"""
    return Path(isolated_data_dir) / "metadata" / "data_quality.db"
```

- [ ] **Step 11: 运行存储测试，确认通过**

Run: `pytest tests/test_storage_sqlite.py -v`
Expected: PASS（4 个测试全过）

- [ ] **Step 12: 提交**

```bash
git add requirements.txt pyproject.toml .env.example src/__init__.py src/config.py src/storage/ tests/__init__.py tests/conftest.py tests/test_config.py tests/test_storage_sqlite.py
git commit -m "feat: 项目脚手架、配置加载与 SQLite(WAL) 存储基础设施"
```

> 注：`tests/test_config.py` 此时因 Task 2 的 YAML 尚未创建而失败，将在 Task 2 完成后变绿。这是预期的跨任务依赖，不在本任务提交范围内强制变绿。

---

## Task 2: 因子与策略配置文件

**Files:**
- Create: `config/universe.yaml`
- Create: `config/strategy.yaml`
- Create: `config/factors/value.yaml`
- Create: `config/factors/growth.yaml`
- Create: `config/factors/quality.yaml`
- Create: `config/factors/momentum.yaml`

**Interfaces:**
- Produces: `config.load_config("universe")` / `("strategy")` 可用
- Produces: `config.load_factor_configs()` 返回合并后的因子定义（含 `pe_percentile`、`momentum_12m1m` 等）

- [ ] **Step 1: 创建 config/universe.yaml**

```yaml
# 股票池基础过滤参数 + 能力圈（spec §4.1 第1/1.5层）
markets:
  a_share:
    source: akshare
    currency: CNY
    min_market_cap_cny: 10_000_000_000      # 100 亿
    min_avg_turnover_cny: 50_000_000        # 日均成交额门槛（示例，探针后校准）
  us:
    source: yfinance
    currency: USD
    min_market_cap_usd: 2_000_000_000       # $2B
    min_avg_turnover_usd: 5_000_000
  hk:
    source: [akshare, yfinance]             # 主 + 备
    currency: HKD
    min_market_cap_hkd: 20_000_000_000      # 200 亿港币
    min_avg_turnover_hkd: 10_000_000

excluded_industries:
  gics:
    - "35"   # 医疗保健
    - "40"   # 金融（保留——金融股不排除，仅 DCF 跳过；此处不列）
  # 实际排除：医药、地产
  excluded:
    - healthcare       # 医药
    - real_estate      # 地产

# 能力圈白名单（手动维护，仅实盘生效，回测置空 —— spec §4.1 第1.5层）
circle_of_competence:
  enabled_in_backtest: false
  tracks:                # 懂的赛道（人录入）
    - consumer_brand
    - cloud_computing
    - new_energy_manufacturing
  # 单票主营可理解标记存于 universe.db，由 Dashboard 维护
```

- [ ] **Step 2: 创建 config/strategy.yaml**

```yaml
# 策略配置（spec §4.4/§4.5/§4.7）
investment_horizon_years: 3
horizon_band_years: 2

funnel:
  layer4_top_pct: 0.20            # 综合得分市场内前 20%
  pe_pb_max_percentile: 0.80      # PE/PB 不处于历史最高 20% 分位
  max_candidate_pool: 50
  max_holdings: 20

pricing:
  pe_percentile_green: 0.30       # ≤30% 绿灯
  pe_percentile_yellow: 0.50      # 30-50% 黄灯
  dcf:
    discount_rate_low: 0.10
    discount_rate_high: 0.12
    terminal_growth_max: 0.03
    fcf_lookback_years: 5
    financial_sector_gics: "40"   # 金融股跳过 DCF

signals:
  sell_reasons:
    - thesis_broken
    - overvalued
    - found_better
    - other

portfolio:
  max_single_position:
    high_conviction: 0.15
    mid_conviction: 0.08
    low_conviction: 0.03
  max_single_market: 0.60
  max_single_industry: 0.40
  min_cash_reserve: 0.10

ai_question_generator:
  cache_ttl_days: 180
  max_calls_per_run: 50
  rate_limit_per_second: 1
```

- [ ] **Step 3: 创建 config/factors/value.yaml**

```yaml
factors:
  pe_percentile:
    name: "PE历史分位"
    category: value
    direction: reverse
    params:
      lookback: 5
      trim_outliers: 0.01
      market_specific: true
      sector_neutral: true
      method: total_market_cap   # spec §4.4 PE 口径：总市值法
    weight: 0.15
    in_composite: true

  pb_percentile:
    name: "PB历史分位"
    category: value
    direction: reverse
    params:
      lookback: 5
      trim_outliers: 0.01
      market_specific: true
      sector_neutral: true
      method: total_market_cap
    weight: 0.10
    in_composite: true

  dividend_yield:
    name: "股息率"
    category: value
    direction: forward
    params:
      min_consistency: 3
      market_specific: true
      sector_neutral: true
    weight: 0.10
    in_composite: true

  fcf_yield:
    name: "自由现金流收益率"
    category: value
    direction: forward
    params:
      fcf_lookback: 5
      market_specific: true
      sector_neutral: true
    weight: 0.10
    in_composite: true
```

- [ ] **Step 4: 创建 config/factors/growth.yaml**

```yaml
factors:
  revenue_cagr_3y:
    name: "营收3年复合增速"
    category: growth
    direction: forward
    params:
      lookback: 3
      market_specific: true
      sector_neutral: true
    weight: 0.12
    in_composite: true

  profit_cagr_3y:
    name: "净利润3年复合增速"
    category: growth
    direction: forward
    params:
      lookback: 3
      market_specific: true
      sector_neutral: true
    weight: 0.10
    in_composite: true

  rnd_ratio:
    name: "研发占营收比"
    category: growth
    direction: forward
    params:
      market_specific: true
      sector_neutral: true
    weight: 0.05
    in_composite: true
```

- [ ] **Step 5: 创建 config/factors/quality.yaml**

```yaml
factors:
  roe:
    name: "净资产收益率"
    category: quality
    direction: forward
    params:
      lookback: 3
      market_specific: true
      sector_neutral: true
    weight: 0.13
    in_composite: true

  gross_margin_stability:
    name: "毛利率稳定性"
    category: quality
    direction: forward
    params:
      lookback: 3
      market_specific: true
      sector_neutral: true
    weight: 0.05
    in_composite: true

  cash_flow_quality:
    name: "现金流质量(经营现金流/净利润)"
    category: quality
    direction: forward
    params:
      lookback: 3
      market_specific: true
      sector_neutral: true
    weight: 0.05
    in_composite: true

  leverage:
    name: "杠杆率(1-资产负债率)"
    category: quality
    direction: forward
    params:
      market_specific: true
      sector_neutral: true
    weight: 0.05
    in_composite: true
```

- [ ] **Step 6: 创建 config/factors/momentum.yaml**

```yaml
# spec §4.2：动量权重永久为 0，仅作估值择价反向校验，不进入综合得分
factors:
  momentum_12m1m:
    name: "12-1月动量"
    category: momentum
    direction: forward
    params:
      skip_recent: 1
      market_specific: true
      sector_neutral: false
    weight: 0.00
    in_composite: false
```

- [ ] **Step 7: 运行 config 测试，确认通过**

Run: `pytest tests/test_config.py -v`
Expected: PASS（6 个测试全过，含 `test_momentum_weight_is_zero`）

- [ ] **Step 8: 提交**

```bash
git add config/
git commit -m "feat: 添加 universe/strategy/因子 YAML 配置（动量权重写死为0）"
```

---

## Task 3: 探针基类与 Fetcher 协议

**Files:**
- Create: `src/data_pipeline/__init__.py`
- Create: `src/data_pipeline/probes/__init__.py`
- Create: `src/data_pipeline/probes/base.py`
- Create: `tests/test_probes/__init__.py`
- Create: `tests/test_probes/test_base.py`

**Interfaces:**
- Produces: `ProbeResult(status: str, summary: str, stats: dict, passed: bool)` — status ∈ {"passed","failed","warning"}
- Produces: `Fetcher` 协议（typing.Protocol）：各探针注入的数据获取接口，便于用 mock fixture 单测
- Produces: `Probe` 抽象基类：`run() -> ProbeResult`，子类实现 `_compute(fetcher) -> ProbeResult`

- [ ] **Step 1: 创建包初始化文件**

```python
# src/data_pipeline/__init__.py
```
```python
# src/data_pipeline/probes/__init__.py
```
```python
# tests/test_probes/__init__.py
```

- [ ] **Step 2: 写 src/data_pipeline/probes/base.py**

```python
"""探针基类、结果与可注入 Fetcher 协议。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

PASSED = "passed"
FAILED = "failed"
WARNING = "warning"


@dataclass
class ProbeResult:
    status: str                       # "passed" | "failed" | "warning"
    summary: str                      # 人类可读的一句话结论
    stats: dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.status == PASSED


@runtime_checkable
class Fetcher(Protocol):
    """探针依赖的数据获取接口。

    各市场具体实现（akshare/yfinance 封装）在 Phase 2 落地；探针阶段用
    mock 实现此协议进行单元测试，集成运行用真实封装。
    """

    def fetch(self, kind: str, **kwargs: Any) -> Any: ...


class Probe:
    """探针基类。子类实现 _compute(fetcher) -> ProbeResult。"""

    name: str = "base"
    description: str = ""

    def run(self, fetcher: Fetcher) -> ProbeResult:
        try:
            return self._compute(fetcher)
        except Exception as exc:  # noqa: BLE001
            return ProbeResult(
                status=WARNING,
                summary=f"探针 {self.name} 执行异常: {exc!r}",
                stats={"exception": repr(exc)},
            )

    def _compute(self, fetcher: Fetcher) -> ProbeResult:  # pragma: no cover
        raise NotImplementedError
```

- [ ] **Step 3: 写失败测试 tests/test_probes/test_base.py**

```python
from src.data_pipeline.probes.base import (
    FAILED,
    PASSED,
    WARNING,
    Fetcher,
    Probe,
    ProbeResult,
)


class FakeFetcher:
    def __init__(self, data):
        self.data = data

    def fetch(self, kind, **kwargs):
        return self.data[kind]


class GoodProbe(Probe):
    name = "good"

    def _compute(self, fetcher):
        return ProbeResult(PASSED, "ok", {"n": fetcher.fetch("n")})


class BadProbe(Probe):
    name = "bad"

    def _compute(self, fetcher):
        raise ValueError("boom")


def test_probe_result_passed_property():
    r = ProbeResult(PASSED, "ok")
    assert r.passed is True
    r2 = ProbeResult(FAILED, "no")
    assert r2.passed is False


def test_good_probe_returns_passed():
    p = GoodProbe()
    r = p.run(FakeFetcher({"n": 5}))
    assert r.status == PASSED
    assert r.stats == {"n": 5}


def test_bad_probe_caught_as_warning():
    p = BadProbe()
    r = p.run(FakeFetcher({}))
    assert r.status == WARNING
    assert "boom" in r.summary
    assert r.passed is False


def test_fetcher_protocol_runtime_checkable():
    assert isinstance(FakeFetcher({}), Fetcher)
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `pytest tests/test_probes/test_base.py -v`
Expected: PASS（4 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/__init__.py src/data_pipeline/probes/__init__.py src/data_pipeline/probes/base.py tests/test_probes/__init__.py tests/test_probes/test_base.py
git commit -m "feat: 数据探针基类与可注入 Fetcher 协议"
```

---

## Task 4: 探针 1 — A 股历史财报披露日期覆盖率

> spec §12 探针 1：用 akshare 拉沪深300成分股最近 10 年财报的 announcement_date，统计非空率。通过标准 ≥80%。失败降级为"报告期+固定滞后"。

**Files:**
- Create: `src/data_pipeline/probes/probe_announcement_date.py`
- Create: `tests/test_probes/test_probe_announcement_date.py`

**Interfaces:**
- Consumes: `Fetcher` 协议（`fetch("a_share_announcement_dates", codes, years) -> list[dict]`），每条 dict 含 `{code, report_period, announcement_date}`
- Produces: `AnnouncementDateProbe`，`run(fetcher) -> ProbeResult`，stats 含 `coverage_rate`、`sample_size`

- [ ] **Step 1: 写失败测试 tests/test_probes/test_probe_announcement_date.py**

```python
from src.data_pipeline.probes.base import FAILED, PASSED, WARNING
from src.data_pipeline.probes.probe_announcement_date import (
    AnnouncementDateProbe,
    coverage_rate,
)


def _rows(with_date, without_date):
    rows = [{"code": f"C{i}", "report_period": "2023-12-31", "announcement_date": "2024-03-30"} for _ in range(with_date)]
    rows += [{"code": f"X{i}", "report_period": "2023-12-31", "announcement_date": None} for _ in range(without_date)]
    return rows


def test_coverage_rate_basic():
    assert coverage_rate(_rows(80, 20)) == 0.80
    assert coverage_rate([]) == 0.0


class FakeFetcher:
    def __init__(self, rows):
        self.rows = rows

    def fetch(self, kind, **kwargs):
        assert kind == "a_share_announcement_dates"
        return self.rows


def test_probe_passes_at_threshold():
    probe = AnnouncementDateProbe(sample_size=100, threshold=0.80)
    r = probe.run(FakeFetcher(_rows(85, 15)))
    assert r.status == PASSED
    assert abs(r.stats["coverage_rate"] - 0.85) < 1e-9
    assert r.stats["sample_size"] == 100


def test_probe_warns_below_threshold():
    probe = AnnouncementDateProbe(sample_size=100, threshold=0.80)
    r = probe.run(FakeFetcher(_rows(70, 30)))
    assert r.status == WARNING
    assert r.stats["coverage_rate"] == 0.70
    assert "固定滞后" in r.summary


def test_probe_fails_on_empty():
    probe = AnnouncementDateProbe(sample_size=100, threshold=0.80)
    r = probe.run(FakeFetcher([]))
    assert r.status == FAILED
    assert r.stats["sample_size"] == 0
```

- [ ] **Step 2: 运行测试，确认失败（模块未实现）**

Run: `pytest tests/test_probes/test_probe_announcement_date.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/probes/probe_announcement_date.py**

```python
"""探针 1：A 股历史财报披露日期覆盖率（spec §12 #1）。"""
from __future__ import annotations

from src.data_pipeline.probes.base import FAILED, PASSED, WARNING, Fetcher, Probe, ProbeResult


def coverage_rate(rows: list[dict]) -> float:
    """announcement_date 非空率。"""
    if not rows:
        return 0.0
    non_empty = sum(1 for r in rows if r.get("announcement_date"))
    return non_empty / len(rows)


class AnnouncementDateProbe(Probe):
    name = "announcement_date_coverage"
    description = "A 股历史财报披露日期(announcement_date)非空率"

    def __init__(self, sample_size: int = 300, threshold: float = 0.80):
        self.sample_size = sample_size
        self.threshold = threshold

    def _compute(self, fetcher: Fetcher) -> ProbeResult:
        rows = fetcher.fetch(
            "a_share_announcement_dates",
            years=10,
            sample_size=self.sample_size,
        )
        if not rows:
            return ProbeResult(
                status=FAILED,
                summary="未取到任何财报披露日期样本，无法评估 PIT 可用性",
                stats={"sample_size": 0, "coverage_rate": 0.0},
            )
        rate = coverage_rate(rows)
        if rate >= self.threshold:
            return ProbeResult(
                status=PASSED,
                summary=f"A 股财报披露日期覆盖率 {rate:.1%} ≥ {self.threshold:.0%}，PIT 可用",
                stats={"sample_size": len(rows), "coverage_rate": rate},
            )
        return ProbeResult(
            status=WARNING,
            summary=(
                f"A 股财报披露日期覆盖率 {rate:.1%} < {self.threshold:.0%}，"
                f"需降级为'报告期+固定滞后'近似（年报+4月/中报+2月/季报+1月）"
            ),
            stats={"sample_size": len(rows), "coverage_rate": rate},
        )
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `pytest tests/test_probes/test_probe_announcement_date.py -v`
Expected: PASS（4 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/probes/probe_announcement_date.py tests/test_probes/test_probe_announcement_date.py
git commit -m "feat: 探针1 — A股财报披露日期覆盖率检测"
```

---

## Task 5: 探针 2 — 港股退市列表完整性

> spec §12 探针 2：用 akshare 拉港股退市列表，抽查过去 5 年已知重大退市案例。通过标准：抽查 5 例 ≥4 例命中。失败则回测报告标注覆盖率偏低 + 人工补录。

**Files:**
- Create: `src/data_pipeline/probes/probe_hk_delisting.py`
- Create: `tests/test_probes/test_probe_hk_delisting.py`

**Interfaces:**
- Consumes: `Fetcher.fetch("hk_delisted_list") -> list[dict]`，每条含 `{code, delist_date, reason}`
- Consumes: `Fetcher.fetch("hk_known_delisting_samples") -> list[dict]`，已知退市案例（code）
- Produces: `HkDelistingProbe`，stats 含 `hit_count`、`sample_count`、`coverage_rate`

- [ ] **Step 1: 写失败测试 tests/test_probes/test_probe_hk_delisting.py**

```python
from src.data_pipeline.probes.base import FAILED, PASSED, WARNING
from src.data_pipeline.probes.probe_hk_delisting import HkDelistingProbe


class FakeFetcher:
    def __init__(self, delisted_codes, known_samples):
        self._data = {
            "hk_delisted_list": [{"code": c, "delist_date": "2022-01-01", "reason": "privatization"} for c in delisted_codes],
            "hk_known_delisting_samples": [{"code": c} for c in known_samples],
        }

    def fetch(self, kind, **kwargs):
        return self._data[kind]


def test_probe_passes_when_4_of_5_hit():
    fetcher = FakeFetcher(delisted_codes={"A", "B", "C", "D"}, known_samples=["A", "B", "C", "D", "E"])
    r = HkDelistingProbe().run(fetcher)
    assert r.status == PASSED
    assert r.stats["hit_count"] == 4
    assert r.stats["sample_count"] == 5


def test_probe_warns_when_below_threshold():
    fetcher = FakeFetcher(delisted_codes={"A", "B"}, known_samples=["A", "B", "C", "D", "E"])
    r = HkDelistingProbe().run(fetcher)
    assert r.status == WARNING
    assert r.stats["hit_count"] == 2
    assert "覆盖率偏低" in r.summary


def test_probe_fails_when_delisted_list_empty():
    fetcher = FakeFetcher(delisted_codes=set(), known_samples=["A"])
    r = HkDelistingProbe().run(fetcher)
    assert r.status == FAILED
    assert r.stats["delisted_total"] == 0


def test_probe_fails_when_no_known_samples():
    fetcher = FakeFetcher(delisted_codes={"A"}, known_samples=[])
    r = HkDelistingProbe().run(fetcher)
    assert r.status == FAILED
    assert r.stats["sample_count"] == 0
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `pytest tests/test_probes/test_probe_hk_delisting.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/probes/probe_hk_delisting.py**

```python
"""探针 2：港股退市列表完整性（spec §12 #2）。"""
from __future__ import annotations

from src.data_pipeline.probes.base import FAILED, PASSED, WARNING, Fetcher, Probe, ProbeResult


class HkDelistingProbe(Probe):
    name = "hk_delisting_completeness"
    description = "港股退市列表对已知重大退市案例的覆盖率"

    def __init__(self, min_samples: int = 5, hit_threshold: int = 4):
        self.min_samples = min_samples
        self.hit_threshold = hit_threshold

    def _compute(self, fetcher: Fetcher) -> ProbeResult:
        delisted = fetcher.fetch("hk_delisted_list")
        known = fetcher.fetch("hk_known_delisting_samples")

        if not delisted:
            return ProbeResult(
                status=FAILED,
                summary="港股退市列表为空，无法做幸存者偏差修正",
                stats={"delisted_total": 0, "sample_count": len(known), "hit_count": 0},
            )
        if not known:
            return ProbeResult(
                status=FAILED,
                summary="缺少已知退市案例用于校验，无法评估覆盖率",
                stats={"delisted_total": len(delisted), "sample_count": 0, "hit_count": 0},
            )

        delisted_codes = {d["code"] for d in delisted}
        hits = sum(1 for s in known if s["code"] in delisted_codes)
        if hits >= self.hit_threshold:
            return ProbeResult(
                status=PASSED,
                summary=f"港股退市列表命中已知案例 {hits}/{len(known)}，覆盖率达标",
                stats={"delisted_total": len(delisted), "sample_count": len(known), "hit_count": hits},
            )
        return ProbeResult(
            status=WARNING,
            summary=(
                f"港股退市列表仅命中 {hits}/{len(known)}，覆盖率偏低，"
                f"回测需标注'港股退市数据覆盖不足'并人工补录"
            ),
            stats={"delisted_total": len(delisted), "sample_count": len(known), "hit_count": hits},
        )
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `pytest tests/test_probes/test_probe_hk_delisting.py -v`
Expected: PASS（4 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/probes/probe_hk_delisting.py tests/test_probes/test_probe_hk_delisting.py
git commit -m "feat: 探针2 — 港股退市列表完整性检测"
```

---

## Task 6: 探针 3 — 美股退市列表可行性

> spec §12 探针 3：yfinance 不提供退市列表，尝试其他免费源。至少确认是否有替代源。若不可取：回测仅对 A 股做退市修正，美/港股标注未修正幸存者偏差。

**Files:**
- Create: `src/data_pipeline/probes/probe_us_delisting.py`
- Create: `tests/test_probes/test_probe_us_delisting.py`

**Interfaces:**
- Consumes: `Fetcher.fetch("us_delisted_source_available") -> dict`，形如 `{"available": bool, "source": str, "sample_count": int}`
- Produces: `UsDelistingProbe`，stats 含 `available`、`source`、`sample_count`

- [ ] **Step 1: 写失败测试 tests/test_probes/test_probe_us_delisting.py**

```python
from src.data_pipeline.probes.base import PASSED, WARNING
from src.data_pipeline.probes.probe_us_delisting import UsDelistingProbe


class FakeFetcher:
    def __init__(self, payload):
        self.payload = payload

    def fetch(self, kind, **kwargs):
        assert kind == "us_delisted_source_available"
        return self.payload


def test_probe_passes_when_source_available_with_samples():
    fetcher = FakeFetcher({"available": True, "source": "nasdaq_delistings", "sample_count": 1200})
    r = UsDelistingProbe().run(fetcher)
    assert r.status == PASSED
    assert r.stats["available"] is True
    assert r.stats["source"] == "nasdaq_delistings"


def test_probe_warns_when_no_source():
    fetcher = FakeFetcher({"available": False, "source": "", "sample_count": 0})
    r = UsDelistingProbe().run(fetcher)
    assert r.status == WARNING
    assert "未修正幸存者偏差" in r.summary
    assert r.stats["available"] is False


def test_probe_warns_when_source_but_no_samples():
    fetcher = FakeFetcher({"available": True, "source": "x", "sample_count": 0})
    r = UsDelistingProbe().run(fetcher)
    assert r.status == WARNING
    assert r.stats["sample_count"] == 0
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `pytest tests/test_probes/test_probe_us_delisting.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/probes/probe_us_delisting.py**

```python
"""探针 3：美股退市列表可行性（spec §12 #3）。"""
from __future__ import annotations

from src.data_pipeline.probes.base import PASSED, WARNING, Fetcher, Probe, ProbeResult


class UsDelistingProbe(Probe):
    name = "us_delisting_feasibility"
    description = "美股退市列表替代源可用性"

    def _compute(self, fetcher: Fetcher) -> ProbeResult:
        info = fetcher.fetch("us_delisted_source_available")
        available = bool(info.get("available"))
        source = info.get("source", "")
        count = int(info.get("sample_count", 0))

        if available and count > 0:
            return ProbeResult(
                status=PASSED,
                summary=f"美股退市源可用：{source}（样本 {count} 条）",
                stats={"available": True, "source": source, "sample_count": count},
            )
        return ProbeResult(
            status=WARNING,
            summary=(
                f"美股退市列表无可行免费源（available={available}, source={source!r}）；"
                f"回测仅对 A 股做退市修正，美股标注'未修正幸存者偏差，收益可能被高估'"
            ),
            stats={"available": available, "source": source, "sample_count": count},
        )
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `pytest tests/test_probes/test_probe_us_delisting.py -v`
Expected: PASS（3 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/probes/probe_us_delisting.py tests/test_probes/test_probe_us_delisting.py
git commit -m "feat: 探针3 — 美股退市列表可行性检测"
```

---

## Task 7: 探针 4 — 价值风格基准指数覆盖率

> spec §12 探针 4：拉沪深300价值指数最近 10 年日线，确认序列完整。缺失日 ≤5% 通过；否则降级沪深300（已在 §6.1 标注降级路径）。

**Files:**
- Create: `src/data_pipeline/probes/probe_value_benchmark.py`
- Create: `tests/test_probes/test_probe_value_benchmark.py`

**Interfaces:**
- Consumes: `Fetcher.fetch("benchmark_daily", index_code, years) -> list[dict]`，每条 `{date, close}`
- Consumes: `Fetcher.fetch("trading_calendar", market, years) -> list[str]`，该市场交易日历
- Produces: `ValueBenchmarkProbe`，stats 含 `total_bars`、`expected_bars`、`missing_rate`、`index_code`

- [ ] **Step 1: 写失败测试 tests/test_probes/test_probe_value_benchmark.py**

```python
import pandas as pd

from src.data_pipeline.probes.base import PASSED, WARNING
from src.data_pipeline.probes.probe_value_benchmark import ValueBenchmarkProbe, missing_rate


class FakeFetcher:
    def __init__(self, bars, calendar):
        self._data = {
            "benchmark_daily": bars,
            "trading_calendar": calendar,
        }

    def fetch(self, kind, **kwargs):
        return self._data[kind]


def _bars(dates):
    return [{"date": d, "close": 100.0} for d in dates]


def test_missing_rate_basic():
    cal = [f"2023-01-{i:02d}" for i in range(2, 21)]  # 19 天
    bars = _bars(cal[:18])  # 缺 1
    assert abs(missing_rate(bars, cal) - 1 / 19) < 1e-9


def test_probe_passes_within_threshold():
    cal = [f"2023-01-{i:02d}" for i in range(2, 22)]  # 20 天
    bars = _bars(cal[:19])  # 缺 1 → 5%
    r = ValueBenchmarkProbe(index_code="000300value", threshold=0.05).run(FakeFetcher(bars, cal))
    assert r.status == PASSED
    assert r.stats["missing_rate"] == 0.05


def test_probe_warns_above_threshold():
    cal = [f"2023-01-{i:02d}" for i in range(2, 22)]  # 20 天
    bars = _bars(cal[:16])  # 缺 4 → 20%
    r = ValueBenchmarkProbe(index_code="000300value", threshold=0.05).run(FakeFetcher(bars, cal))
    assert r.status == WARNING
    assert r.stats["missing_rate"] == 0.20
    assert "降级" in r.summary


def test_probe_warns_when_no_calendar():
    r = ValueBenchmarkProbe(index_code="000300value", threshold=0.05).run(FakeFetcher(_bars(["2023-01-02"]), []))
    assert r.status == WARNING
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `pytest tests/test_probes/test_probe_value_benchmark.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/probes/probe_value_benchmark.py**

```python
"""探针 4：价值风格基准指数覆盖率（spec §12 #4）。"""
from __future__ import annotations

from src.data_pipeline.probes.base import PASSED, WARNING, Fetcher, Probe, ProbeResult


def missing_rate(bars: list[dict], calendar: list[str]) -> float:
    """基准序列相对交易日历的缺失比例。"""
    if not calendar:
        return 1.0
    have = {b["date"] for b in bars}
    missing = sum(1 for d in calendar if d not in have)
    return missing / len(calendar)


# spec §6.1 降级映射
DOWNGRADE = {
    "csi300_value": "csi300",
    "sp500_value": "sp500",
    "hsi_composite": "hsi",
}


class ValueBenchmarkProbe(Probe):
    name = "value_benchmark_coverage"
    description = "价值风格基准指数 10 年日线序列完整性"

    def __init__(self, index_code: str = "csi300_value", threshold: float = 0.05, years: int = 10):
        self.index_code = index_code
        self.threshold = threshold
        self.years = years

    def _compute(self, fetcher: Fetcher) -> ProbeResult:
        bars = fetcher.fetch("benchmark_daily", index_code=self.index_code, years=self.years)
        calendar = fetcher.fetch("trading_calendar", market="a_share", years=self.years)

        if not calendar:
            return ProbeResult(
                status=WARNING,
                summary=f"基准 {self.index_code}：缺交易日历，无法评估缺失率，默认降级为宽基",
                stats={"index_code": self.index_code, "total_bars": len(bars), "expected_bars": 0, "missing_rate": 1.0},
            )

        rate = missing_rate(bars, calendar)
        stats = {
            "index_code": self.index_code,
            "total_bars": len(bars),
            "expected_bars": len(calendar),
            "missing_rate": rate,
        }
        if rate <= self.threshold:
            return ProbeResult(
                status=PASSED,
                summary=f"基准 {self.index_code} 缺失率 {rate:.1%} ≤ {self.threshold:.0%}，可用",
                stats=stats,
            )
        fallback = DOWNGRADE.get(self.index_code, "宽基")
        return ProbeResult(
            status=WARNING,
            summary=f"基准 {self.index_code} 缺失率 {rate:.1%} > {self.threshold:.0%}，降级为 {fallback}",
            stats=stats,
        )
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `pytest tests/test_probes/test_probe_value_benchmark.py -v`
Expected: PASS（4 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/probes/probe_value_benchmark.py tests/test_probes/test_probe_value_benchmark.py
git commit -m "feat: 探针4 — 价值风格基准指数覆盖率检测"
```

---

## Task 8: 探针 5 — 财务数据字段完整性

> spec §12 探针 5：三地各抽 20 支大/中/小盘股，拉最近 5 年 ROE/FCF/营收/净利润/总市值，统计字段级缺失率。单市场单字段缺失率 ≤15% 通过；否则该股该期从因子体系移除并记 data_quality_log。

**Files:**
- Create: `src/data_pipeline/probes/probe_fundamental_fields.py`
- Create: `tests/test_probes/test_probe_fundamental_fields.py`

**Interfaces:**
- Consumes: `Fetcher.fetch("fundamental_sample", market, n_stocks, years) -> list[dict]`，每条 `{market, code, field, value, date}`
- Produces: `FundamentalFieldsProbe`，stats 含 `per_market_field: dict[(market,field)] -> missing_rate`，`overall_pass: bool`

- [ ] **Step 1: 写失败测试 tests/test_probes/test_probe_fundamental_fields.py**

```python
from src.data_pipeline.probes.base import PASSED, WARNING
from src.data_pipeline.probes.probe_fundamental_fields import (
    FundamentalFieldsProbe,
    field_missing_rates,
)

FIELDS = ["roe", "fcf", "revenue", "net_profit", "market_cap"]


def _rows(market, present, missing):
    rows = []
    for f in FIELDS:
        for _ in range(present):
            rows.append({"market": market, "code": "C", "field": f, "value": 1.0})
        for _ in range(missing):
            rows.append({"market": market, "code": "C", "field": f, "value": None})
    return rows


def test_field_missing_rates():
    rates = field_missing_rates(_rows("a_share", present=17, missing=3), FIELDS)
    # 每个字段 17 present + 3 missing → 15% 缺失
    for f in FIELDS:
        assert abs(rates[("a_share", f)] - 0.15) < 1e-9


class FakeFetcher:
    def __init__(self, rows):
        self.rows = rows

    def fetch(self, kind, **kwargs):
        assert kind == "fundamental_sample"
        return self.rows


def test_probe_passes_all_under_threshold():
    rows = _rows("a_share", 17, 3) + _rows("us", 18, 2) + _rows("hk", 19, 1)
    r = FundamentalFieldsProbe(threshold=0.15).run(FakeFetcher(rows))
    assert r.status == PASSED
    assert r.stats["overall_pass"] is True


def test_probe_warns_when_one_field_over():
    rows = _rows("a_share", 17, 3) + _rows("us", 10, 10)  # us 50% 缺失
    r = FundamentalFieldsProbe(threshold=0.15).run(FakeFetcher(rows))
    assert r.status == WARNING
    assert r.stats["overall_pass"] is False
    assert ("us", "roe") in r.stats["per_market_field"]


def test_probe_warns_on_empty():
    r = FundamentalFieldsProbe(threshold=0.15).run(FakeFetcher([]))
    assert r.status == WARNING
    assert r.stats["overall_pass"] is False
```

- [ ] **Step 2: 运行测试，确认失败**

Run: `pytest tests/test_probes/test_probe_fundamental_fields.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 写 src/data_pipeline/probes/probe_fundamental_fields.py**

```python
"""探针 5：财务数据字段完整性（spec §12 #5）。"""
from __future__ import annotations

from collections import defaultdict

from src.data_pipeline.probes.base import PASSED, WARNING, Fetcher, Probe, ProbeResult

DEFAULT_FIELDS = ["roe", "fcf", "revenue", "net_profit", "market_cap"]


def field_missing_rates(rows: list[dict], fields: list[str]) -> dict[tuple[str, str], float]:
    """按 (market, field) 统计缺失率。"""
    counts: dict[tuple[str, str], list[int, int]] = defaultdict(lambda: [0, 0])  # [total, missing]
    for r in rows:
        key = (r["market"], r["field"])
        counts[key][0] += 1
        if r.get("value") is None:
            counts[key][1] += 1
    return {k: (m / t if t else 1.0) for k, (t, m) in counts.items()}


class FundamentalFieldsProbe(Probe):
    name = "fundamental_field_completeness"
    description = "三地财务关键字段缺失率"

    def __init__(self, threshold: float = 0.15, fields: list[str] | None = None, per_market_n: int = 20):
        self.threshold = threshold
        self.fields = fields or DEFAULT_FIELDS
        self.per_market_n = per_market_n

    def _compute(self, fetcher: Fetcher) -> ProbeResult:
        rows = fetcher.fetch(
            "fundamental_sample",
            markets=["a_share", "us", "hk"],
            n_stocks=self.per_market_n,
            years=5,
        )
        if not rows:
            return ProbeResult(
                status=WARNING,
                summary="未取到财务样本",
                stats={"overall_pass": False, "per_market_field": {}},
            )
        rates = field_missing_rates(rows, self.fields)
        over = {k: v for k, v in rates.items() if v > self.threshold}
        overall_pass = not over
        if overall_pass:
            return ProbeResult(
                status=PASSED,
                summary=f"三地财务字段缺失率均 ≤ {self.threshold:.0%}",
                stats={"overall_pass": True, "per_market_field": rates},
            )
        over_desc = ", ".join(f"{m}:{f}={v:.0%}" for (m, f), v in over.items())
        return ProbeResult(
            status=WARNING,
            summary=(
                f"部分字段缺失率超阈值：{over_desc}；"
                f"超阈值股该期从因子体系移除并记 data_quality_log"
            ),
            stats={"overall_pass": False, "per_market_field": rates},
        )
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `pytest tests/test_probes/test_probe_fundamental_fields.py -v`
Expected: PASS（4 个测试）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/probes/probe_fundamental_fields.py tests/test_probes/test_probe_fundamental_fields.py
git commit -m "feat: 探针5 — 财务数据字段完整性检测"
```

---

## Task 9: 探针运行器与 data_quality.db 持久化

> spec §12 末段：探针批量执行，结果纳入 data_quality.db，Dashboard Tab 1 数据源健康度面板可查。

**Files:**
- Create: `src/data_pipeline/probes/runner.py`
- Create: `src/data_pipeline/probes/schema.sql`
- Create: `tests/test_probes/test_runner.py`

**Interfaces:**
- Consumes: 上述 5 个 Probe 类 + `Fetcher`
- Produces: `run_all_probes(fetcher, db_path) -> list[ProbeResult]`，结果写入 `data_quality.db` 表 `probe_results(name, status, summary, stats_json, run_at)`
- Produces: `init_quality_db(db_path)` — 建表 DDL

- [ ] **Step 1: 写 src/data_pipeline/probes/schema.sql**

```sql
CREATE TABLE IF NOT EXISTS probe_results (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL,           -- passed/failed/warning
    summary     TEXT NOT NULL,
    stats_json  TEXT NOT NULL,
    run_at      TEXT NOT NULL            -- ISO8601 字符串，由调用方传入
);

CREATE INDEX IF NOT EXISTS idx_probe_results_name_run_at
    ON probe_results(name, run_at);
```

- [ ] **Step 2: 写失败测试 tests/test_probes/test_runner.py**

```python
import json
import sqlite3

from src.data_pipeline.probes.base import PASSED, WARNING, ProbeResult
from src.data_pipeline.probes.runner import (
    init_quality_db,
    run_all_probes,
    list_recent_results,
)
from src.storage import sqlite as sq


class StubProbe:
    def __init__(self, name, result):
        self.name = name
        self._result = result

    def run(self, fetcher):
        return self._result


def _probes():
    return [
        StubProbe("p1", ProbeResult(PASSED, "ok1", {"a": 1})),
        StubProbe("p2", ProbeResult(WARNING, "warn2", {"b": 2})),
    ]


class FakeFetcher:
    def fetch(self, kind, **kwargs):
        return []


def test_init_quality_db_creates_table(quality_db):
    init_quality_db(quality_db)
    conn = sq.get_connection(quality_db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")}
    conn.close()
    assert "probe_results" in tables


def test_run_all_probes_persists(quality_db):
    init_quality_db(quality_db)
    results = run_all_probes(FakeFetcher(), quality_db, probes=_probes(), run_at="2026-06-27T22:00:00")
    assert len(results) == 2
    rows = list_recent_results(quality_db, run_at="2026-06-27T22:00:00")
    assert len(rows) == 2
    names = {r["name"] for r in rows}
    assert names == {"p1", "p2"}
    stats = json.loads(rows[0]["stats_json"])
    assert stats in ({"a": 1}, {"b": 2})


def test_run_all_probes_writes_one_row_per_probe(quality_db):
    init_quality_db(quality_db)
    run_all_probes(FakeFetcher(), quality_db, probes=_probes(), run_at="t1")
    run_all_probes(FakeFetcher(), quality_db, probes=_probes(), run_at="t2")
    conn = sq.get_connection(quality_db)
    n = conn.execute("SELECT COUNT(*) FROM probe_results;").fetchone()[0]
    conn.close()
    assert n == 4  # 两次 × 2 探针
```

- [ ] **Step 3: 运行测试，确认失败**

Run: `pytest tests/test_probes/test_runner.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 4: 写 src/data_pipeline/probes/runner.py**

```python
"""探针批量运行器 + data_quality.db 持久化。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from src.data_pipeline.probes.base import Fetcher, Probe, ProbeResult
from src.data_pipeline.probes.probe_announcement_date import AnnouncementDateProbe
from src.data_pipeline.probes.probe_fundamental_fields import FundamentalFieldsProbe
from src.data_pipeline.probes.probe_hk_delisting import HkDelistingProbe
from src.data_pipeline.probes.probe_us_delisting import UsDelistingProbe
from src.data_pipeline.probes.probe_value_benchmark import ValueBenchmarkProbe
from src.storage import sqlite as sq

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def init_quality_db(db_path: Path) -> None:
    """建 probe_results 表（幂等）。"""
    script = SCHEMA_PATH.read_text(encoding="utf-8")
    sq.execute_script(db_path, script)


def default_probes() -> list[Probe]:
    """spec §12 五项探针的默认实例。"""
    return [
        AnnouncementDateProbe(),
        HkDelistingProbe(),
        UsDelistingProbe(),
        ValueBenchmarkProbe(),
        FundamentalFieldsProbe(),
    ]


def run_all_probes(
    fetcher: Fetcher,
    db_path: Path,
    *,
    probes: list[Probe] | None = None,
    run_at: str,
) -> list[ProbeResult]:
    """运行全部探针并写一行结果到 data_quality.db。返回结果列表。"""
    probes = probes if probes is not None else default_probes()
    init_quality_db(db_path)
    results = [p.run(fetcher) for p in probes]

    def _insert(conn):
        for p, r in zip(probes, results):
            conn.execute(
                "INSERT INTO probe_results(name, status, summary, stats_json, run_at) "
                "VALUES (?, ?, ?, ?, ?);",
                (p.name, r.status, r.summary, json.dumps(r.stats, ensure_ascii=False), run_at),
            )

    sq.run_write(db_path, _insert)
    return results


def list_recent_results(db_path: Path, *, run_at: str | None = None) -> list[dict]:
    """读取探针结果。run_at 指定时只返回该批次。"""
    conn = sq.get_connection(db_path)
    try:
        if run_at:
            cur = conn.execute(
                "SELECT name, status, summary, stats_json, run_at FROM probe_results "
                "WHERE run_at = ? ORDER BY name;",
                (run_at,),
            )
        else:
            cur = conn.execute(
                "SELECT name, status, summary, stats_json, run_at FROM probe_results "
                "ORDER BY run_at DESC, name;"
            )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `pytest tests/test_probes/test_runner.py -v`
Expected: PASS（3 个测试）

- [ ] **Step 6: 运行全部测试套件，确认全绿**

Run: `pytest -m "not integration" -v`
Expected: 所有非集成测试 PASS（含 test_config、test_storage_sqlite、test_probes/*）

- [ ] **Step 7: 提交**

```bash
git add src/data_pipeline/probes/runner.py src/data_pipeline/probes/schema.sql tests/test_probes/test_runner.py
git commit -m "feat: 探针运行器与 data_quality.db 持久化"
```

---

## Task 10: 真实数据集成运行（标记为 integration，需联网）

> 此任务验证五项探针对真实 akshare/yfinance 的端到端可用性，作为 §12 硬门槛的实际确认。联网测试默认 `pytest -m "not integration"` 跳过，仅在本地手动运行。

**Files:**
- Create: `src/data_pipeline/probes/integration_fetcher.py`
- Create: `tests/test_probes/test_integration_real_sources.py`

**Interfaces:**
- Produces: `RealFetcher` — 实现 `Fetcher` 协议，封装 akshare/yfinance 真实调用（fetch 方法的各 kind 分支）
- Produces: 集成测试 `@pytest.mark.integration`，运行 `run_all_probes(RealFetcher(), db_path, run_at=...)` 并断言无 FAILED

- [ ] **Step 1: 写 src/data_pipeline/probes/integration_fetcher.py**

```python
"""真实数据源 Fetcher：封装 akshare/yfinance（spec §12 集成运行用）。

注意：此模块仅在联网集成运行时使用。字段名与返回结构按 akshare/yfinance
当前版本约定，Phase 2 fetchers 正式落地时若字段变更需同步更新。
"""
from __future__ import annotations

from typing import Any


class RealFetcher:
    """实现 Fetcher 协议，按 kind 分发到 akshare/yfinance。"""

    def fetch(self, kind: str, **kwargs: Any) -> Any:
        if kind == "a_share_announcement_dates":
            return self._a_share_announcement_dates(**kwargs)
        if kind == "hk_delisted_list":
            return self._hk_delisted_list(**kwargs)
        if kind == "hk_known_delisting_samples":
            return self._hk_known_samples(**kwargs)
        if kind == "us_delisted_source_available":
            return self._us_delisting_source(**kwargs)
        if kind == "benchmark_daily":
            return self._benchmark_daily(**kwargs)
        if kind == "trading_calendar":
            return self._trading_calendar(**kwargs)
        if kind == "fundamental_sample":
            return self._fundamental_sample(**kwargs)
        raise ValueError(f"unknown kind: {kind}")

    # --- 各 kind 实现：调用真实源（联网） ---
    def _a_share_announcement_dates(self, years: int, sample_size: int, **_):
        import akshare as ak
        df = ak.stock_zh_a_spot_em()  # 取成分股列表做样本
        codes = df["代码"].head(sample_size).tolist()
        rows = []
        for code in codes:
            try:
                fin = ak.stock_financial_report_sina(stock=f"sz{code}" if code.startswith("0") else f"sh{code}")
                for _, r in fin.head(20).iterrows():
                    rows.append({
                        "code": code,
                        "report_period": str(r.get("报告日", "")),
                        "announcement_date": str(r.get("公告日", "")) or None,
                    })
            except Exception:
                continue
            if len(rows) >= sample_size * 10:
                break
        return rows

    def _hk_delisted_list(self, **_):
        import akshare as ak
        try:
            df = ak.stock_hk_spot_em()
            return [{"code": str(r["代码"]), "delist_date": "", "reason": ""} for _, r in df.iterrows()]
        except Exception:
            return []

    def _hk_known_samples(self, **_):
        # 已知退市案例（人工维护示例，需按实际补录）
        return [{"code": c} for c in ["03692", "02333", "01378", "00358", "01876"]]

    def _us_delisting_source(self, **_):
        # yfinance 不提供退市列表；返回不可用，触发降级路径
        return {"available": False, "source": "", "sample_count": 0}

    def _benchmark_daily(self, index_code: str, years: int, **_):
        import akshare as ak
        try:
            df = ak.index_zh_a_hist(symbol="000300", period="daily", start_date="20150101", end_date="20260627")
            return [{"date": str(r["日期"]), "close": float(r["收盘"])} for _, r in df.iterrows()]
        except Exception:
            return []

    def _trading_calendar(self, market: str, years: int, **_):
        import akshare as ak
        try:
            df = ak.tool_trade_date_hist_sina()
            return [str(d) for d in df["trade_date"].tolist()]
        except Exception:
            return []

    def _fundamental_sample(self, markets, n_stocks, years, **_):
        import akshare as ak
        rows = []
        for market in markets:
            try:
                df = ak.stock_zh_a_spot_em().head(n_stocks)
                for _, r in df.iterrows():
                    for f, col in [("roe", "ROE"), ("revenue", "营收"), ("net_profit", "净利润"), ("market_cap", "总市值")]:
                        rows.append({"market": market, "code": str(r["代码"]), "field": f, "value": r.get(col)})
                    rows.append({"market": market, "code": str(r["代码"]), "field": "fcf", "value": None})
            except Exception:
                continue
        return rows
```

- [ ] **Step 2: 写集成测试 tests/test_probes/test_integration_real_sources.py**

```python
import pytest

from src.data_pipeline.probes.integration_fetcher import RealFetcher
from src.data_pipeline.probes.runner import init_quality_db, run_all_probes


@pytest.mark.integration
def test_real_probes_run_without_hard_failure(quality_db):
    """spec §12 硬门槛：五项探针对真实源运行，不应出现 FAILED（FAILED 表示取数完全失败）。

    WARNING 是可接受的（触发降级路径），FAILED 不可接受。
    需联网运行：pytest tests/test_probes/test_integration_real_sources.py -m integration -v
    """
    init_quality_db(quality_db)
    results = run_all_probes(RealFetcher(), quality_db, run_at="2026-06-27T22:00:00")
    assert len(results) == 5
    failed = [r for r in results if r.status == "failed"]
    assert not failed, f"探针出现 FAILED：{[r.summary for r in failed]}"
```

- [ ] **Step 3: 运行集成测试（需联网，手动确认）**

Run: `pytest tests/test_probes/test_integration_real_sources.py -m integration -v`
Expected: 联网成功时 PASS（5 项探针无 FAILED）。若某探针因 akshare 字段名变更抛异常，会落到 WARNING（被 Probe.run 捕获），仍可接受；若返回空导致 FAILED，需对照降级方案调整 RealFetcher 字段映射后重跑。

- [ ] **Step 4: 确认默认测试套件仍跳过集成测试**

Run: `pytest -m "not integration" -v`
Expected: 所有非集成测试 PASS，集成测试被跳过（deselected）

- [ ] **Step 5: 提交**

```bash
git add src/data_pipeline/probes/integration_fetcher.py tests/test_probes/test_integration_real_sources.py
git commit -m "test: 真实数据源集成探针（@integration，需联网手动运行）"
```

- [ ] **Step 6: 手动运行集成探针并记录结论**

在本地联网运行 Task 10 Step 3 的命令。根据结果在仓库根创建 `docs/superpowers/specs/2026-06-27-probe-findings.md`，逐项记录：
- 探针 1：A 股披露日覆盖率实测值 → 是否需启用"报告期+固定滞后"降级
- 探针 2：港股退市命中数 → 是否需人工补录
- 探针 3：美股退市源 → 确认降级（仅 A 股修正）
- 探针 4：沪深300价值缺失率 → 是否降级沪深300
- 探针 5：财务字段缺失率 → 哪些字段超阈值

```bash
git add docs/superpowers/specs/2026-06-27-probe-findings.md
git commit -m "docs: 记录五项数据探针实测结论与降级决策"
```

> **Phase 1 完成条件：** 全部非集成测试通过；集成探针已手动运行并记录结论；§12 硬门槛确认。此后方可进入 Phase 2（Data Pipeline fetchers）。
