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
