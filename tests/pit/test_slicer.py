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
    # NOTE(brief-fix): brief 原写 partition_date="2025-12-31"，但 test_slice_quote_panel
    # 以 as_of="2025-06-30" 查询；indexer 的 read_parquet(as_of=) 会按 partition_date<=as_of
    # 过滤分区，2025-12-31 > 2025-06-30 → 整个分区被排除 → C 也返回空 → 测试失败。
    # 最小修复：把行情分区日改为 "2025-06-30"（≤ 该测试的 as_of），保留 fixture 原意
    # （C 有 6 年 06-30 行情，X 无数据）。PE 测试不读行情，不受影响。
    store.write_parquet_partition(pd.DataFrame(quote_rows, columns=QUOTE_COLUMNS),
                                  "market", "2025-06-30", "a_share")

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


def test_pe_percentile_pit_gate_excludes_unannounced(pe_panel):
    """as_of=2024-03-01：2023 年报 report_period(2023-12-31) 在 5 年窗口内
    （window_start=2019-03-01）但 announcement_date(2024-04-30) > as_of
    → PIT gate 必须排除。可见仅 2020-2022 年报 → PE=[10,20,15], 当前=15(2022), 分位=2/3。

    此测试隔离 announcement_date PIT gate——这是 window 过滤抓不到的唯一情形
    （report_period <= as_of 但 announcement_date > as_of）。若 pe_percentile 绕过
    pit_fundamental_as_of 直接读 raw，会错误纳入 2023 年报，series 变为
    [10,20,15,30]、当前=30、分位=4/4=1.0。除 pct 断言外，用 visible report_periods
    直接锁定 2023 年报被排除（最严格的隔离断言）。
    """
    pct = pe_percentile("2024-03-01", "a_share", "C", lookback_years=5)
    assert pct == pytest.approx(2 / 3)
    # 关键：验证 2023 年报被 PIT gate 排除（而非被 window 排除）
    from src.pit.indexer import pit_fundamental_as_of
    visible = pit_fundamental_as_of("2024-03-01", "a_share", code="C")
    assert "2023-12-31" not in set(visible["report_period"])  # 未披露
    assert set(visible["report_period"]) == {"2020-12-31", "2021-12-31", "2022-12-31"}


def test_pe_percentile_none_when_insufficient(isolated_data_dir):
    """数据不足 → None。"""
    pct = pe_percentile("2026-05-01", "a_share", "NOPE", lookback_years=5)
    assert pct is None
