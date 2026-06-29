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
