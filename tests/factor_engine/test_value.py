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
