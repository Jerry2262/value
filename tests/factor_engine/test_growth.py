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
