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
