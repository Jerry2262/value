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
    # NOTE(brief-fix): brief 原写 partition_date="2024-01-10"，但 as_of="2024-01-03"；
    # store.read_parquet(as_of=) 会按 partition_date<=as_of 过滤分区，2024-01-10 > 2024-01-03
    # → 整个分区被排除 → df 为空 → set(df["date"]) == set() != {"2024-01-01"} → 测试失败。
    # 最小修复：把行情分区日改为 "2024-01-03"（= as_of，分区可读），保留测试原意——
    # 验证行级 date<=as_of 过滤把 2024-01-05 排除、保留 2024-01-01。与 test_slicer.py
    # 中 pe_panel 的同类 brief-fix 一致。
    store.write_parquet_partition(quote, "market", "2024-01-03", "a_share")
    df = pit_quote_as_of("2024-01-03", "a_share")
    assert set(df["date"]) == {"2024-01-01"}
    assert "2024-01-05" not in set(df["date"])
