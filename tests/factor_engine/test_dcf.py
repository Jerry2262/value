import math

import pandas as pd
import pytest

from src.factor_engine.computers.dcf import dcf_intrinsic_value, is_financial


def test_is_financial_by_industry_map():
    industry_map = {"C1": "40", "C2": "10", "C3": "40"}
    assert is_financial("C1", industry_map) is True
    assert is_financial("C2", industry_map) is False


def test_is_financial_no_map_returns_false():
    """无 industry_map → 无法判断 → 视为非金融（不跳过 DCF）。"""
    assert is_financial("C1", None) is False


def test_dcf_returns_value_range(fundamentals_for_factors):
    """C1 有 4 年 FCF 历史 → DCF 返回 (下限, 上限) 非空。"""
    result = dcf_intrinsic_value("C1", "2024-05-01", "a_share")
    assert result is not None
    low, high = result
    assert low > 0
    assert high >= low


def test_dcf_financial_skipped(fundamentals_for_factors):
    """金融股（industry_map gics=40）→ None。"""
    industry_map = {"C1": "40"}
    result = dcf_intrinsic_value("C1", "2024-05-01", "a_share", industry_map=industry_map)
    assert result is None


def test_dcf_insufficient_fcf_returns_none(fundamentals_for_factors):
    """FCF 历史不足 → None。"""
    # C3 有 3 年 FCF（2021-2023），仍可算；用 as_of 早到只有 1 年
    result = dcf_intrinsic_value("C3", "2022-05-01", "a_share")
    # 2022-05-01 时 C3 只有 2021 年报 → 不足
    assert result is None


def test_dcf_discount_rate_bounds(fundamentals_for_factors):
    """折现率下限（10%）应给出更高内在价值，上限（12%）更低。"""
    # 注：brief 原文漏写 fundamentals_for_factors 形参，无 fixture 时数据目录为空、
    # pit_fundamental_as_of 返回空 → dcf 返回 None → assert r_low is not None 失败。
    # 此处补回形参（minimal fix），使 C1 的 4 年 FCF 数据可见。
    r_low = dcf_intrinsic_value("C1", "2024-05-01", "a_share", discount_rate=0.10)
    r_high = dcf_intrinsic_value("C1", "2024-05-01", "a_share", discount_rate=0.12)
    assert r_low is not None and r_high is not None
    # 下限折现率 → 更高价值
    assert r_low[0] > r_high[0]
