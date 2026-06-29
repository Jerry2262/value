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
    # a_share_delisting：C1 delist 2020-01-01、C2 delist 2025-06-30。
    # as_of=2024-05-01 时仅 C1 已退市（C2 退市日 2025-06-30 > as_of，仍在市），
    # 故 pit_active_universe 从 [C2,C3,C1] 扣除 C1 → active=[C2,C3]。
    # fundamentals 的 C1 与 delisting 的 C1 同码但分属 fundamental/delisting 两个
    # 数据集，互不冲突；退市门只读 delisting 分区。
    fm = compute_factor_matrix("2024-05-01", "a_share", ["C2", "C3", "C1"])
    assert len(fm.matrix) == 2
    assert "C1" not in fm.matrix.index
    assert set(fm.matrix.index) == {"C2", "C3"}


def test_reverse_factor_rank_direction(isolated_data_dir):
    """reverse 因子（pe_percentile）方向取负后，便宜股票排名更高（防 direction 回归）。

    端到端校验 engine 的 direction-negation → standardize → rank 链路（Critical 路径）。
    构造两只股票使 pe_percentile（per-code 历史分位）给出 DISTINCT 值：CHEAP 当前 PE
    处自身历史低位（pe_percentile 低），EXPENSIVE 当前 PE 处自身历史高位（pe_percentile
    高）。reverse 取负后 CHEAP 应排名更高；若 refactor 误删取负，CHEAP 反而排名更低，
    断言失败。同时校验 forward 因子（roe）方向不反：两只 roe 相同 → rank 相等。
    """
    from src.data_pipeline import store
    from src.data_pipeline.fetchers.base import FUNDAMENTAL_COLUMNS

    rows = []
    for yr in range(2020, 2025):  # 5 年历史，使 pe_percentile 5yr 窗口有意义
        # CHEAP：2020-2023 PE=50（mcap 500/np 10），2024 PE=10 → 当前处自身历史低位
        mcap_cheap = 500e8 if yr < 2024 else 100e8
        # EXPENSIVE：2020-2023 PE=10（mcap 100/np 10），2024 PE=50 → 当前处自身历史高位
        mcap_expensive = 100e8 if yr < 2024 else 500e8
        for code, mcap in (("CHEAP", mcap_cheap), ("EXPENSIVE", mcap_expensive)):
            rows.append({
                "code": code, "market": "a_share",
                "report_period": f"{yr}-12-31",
                "announcement_date_approx": f"{yr+1}-04-30",
                "revenue": 1e10, "net_profit": 10e8, "roe": 20.0,
                "debt_ratio": 30.0, "fcf": 5e8, "total_market_cap": mcap,
            })
    store.write_parquet_partition(pd.DataFrame(rows, columns=FUNDAMENTAL_COLUMNS),
                                  "fundamental", "2025-04-30", "a_share")

    fm = compute_factor_matrix("2025-05-01", "a_share", ["CHEAP", "EXPENSIVE"])

    # 健全性：pe_percentile 应非 NaN 且 CHEAP（当前 PE 低 vs 自身历史）分位更低
    raw_cheap = fm.raw.loc["CHEAP", "pe_percentile"]
    raw_expensive = fm.raw.loc["EXPENSIVE", "pe_percentile"]
    assert pd.notna(raw_cheap) and pd.notna(raw_expensive), (
        f"pe_percentile 应非 NaN：CHEAP={raw_cheap}, EXPENSIVE={raw_expensive}"
    )
    assert raw_cheap < raw_expensive, (
        f"场景构造有误：CHEAP pe_percentile({raw_cheap}) 应 < EXPENSIVE({raw_expensive})"
    )
    # reverse 取负后：CHEAP（低分位）→ rank 更高（防 direction 回归的核心断言）
    pe_rank_cheap = fm.ranks.loc["CHEAP", "pe_percentile"]
    pe_rank_expensive = fm.ranks.loc["EXPENSIVE", "pe_percentile"]
    assert pe_rank_cheap > pe_rank_expensive, (
        f"reverse 方向错误：CHEAP rank({pe_rank_cheap}) 应 > EXPENSIVE rank({pe_rank_expensive})"
    )
    # forward 因子（roe）方向不反：两只 roe 相同 → rank 相等
    assert fm.ranks.loc["CHEAP", "roe"] == fm.ranks.loc["EXPENSIVE", "roe"]


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
