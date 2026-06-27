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


def test_cash_flow_quality_zero_profit_returns_nan(isolated_data_dir):
    """net_profit=0 → cash_flow_quality 为 NaN（防 ±inf 污染因子）。

    直接覆盖 quality.py 中 `np_ != 0` 守卫：回归若删去守卫将产生 ±inf。
    共享 fixture fundamentals_for_factors 不含 np=0 行，故自建分区。
    """
    from src.data_pipeline.fetchers.base import FUNDAMENTAL_COLUMNS
    from src.data_pipeline import store

    fund = pd.DataFrame([{
        "code": "Z", "market": "a_share", "report_period": "2023-12-31",
        "announcement_date_approx": "2024-04-30",
        "revenue": 1e9, "net_profit": 0.0, "roe": 0.0,
        "debt_ratio": 50.0, "fcf": 1e8, "total_market_cap": 1e10,
    }], columns=FUNDAMENTAL_COLUMNS)
    store.write_parquet_partition(fund, "fundamental", "2024-04-30", "a_share")
    s = compute_factor("cash_flow_quality", "2024-05-01", "a_share", ["Z"])
    assert pd.isna(s["Z"])
