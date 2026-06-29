"""DCF 估值（spec §4.4）：金融股跳过，保守参数，5 年平均 FCF。

输出内在价值区间（下限=高折现率，上限=低折现率）。

折现率区间修正（brief bug）：brief 原代码以模块常量 DISCOUNT_RATE_HIGH/LOW 计算
(下限, 上限)，完全忽略传入的 discount_rate 参数——导致 test_dcf_discount_rate_bounds
（分别传 0.10 / 0.12）得到相同结果，r_low[0] > r_high[0] 不成立。

修正方式（minimal）：以传入 discount_rate 为中心、±DISCOUNT_RATE_SPREAD 构造区间。
默认 discount_rate=0.11（= (0.10+0.12)/2）时复现 spec §4.4 的 10-12% 区间，行为与
brief 一致；显式传入 discount_rate 时区间随之平移，使"低折现率 → 更高内在价值"成立。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.pit.indexer import pit_fundamental_as_of

# spec §4.4 DCF 参数
DISCOUNT_RATE_LOW = 0.10   # 区间上界价值用低折现率（spec 下限）
DISCOUNT_RATE_HIGH = 0.12  # 区间下界价值用高折现率（spec 上限）
# 区间半宽 = (HIGH-LOW)/2 = 0.01；以 discount_rate 为中心 ± 此值构造 (下限, 上限)。
DISCOUNT_RATE_SPREAD = (DISCOUNT_RATE_HIGH - DISCOUNT_RATE_LOW) / 2
TERMINAL_GROWTH_MAX = 0.03
FCF_LOOKBACK = 5
FINANCIAL_GICS = "40"


def is_financial(code: str, industry_map: dict | None = None) -> bool:
    """判断是否金融股（GICS 40）。无 industry_map → False（不跳过）。"""
    if industry_map is None:
        return False
    return industry_map.get(code) == FINANCIAL_GICS


def _dcf_value(avg_fcf: float, discount_rate: float, terminal_growth: float) -> float:
    """简化 DCF：永续增长模型 V = FCF / (r - g)。"""
    if discount_rate <= terminal_growth:
        return float("nan")
    return avg_fcf / (discount_rate - terminal_growth)


def dcf_intrinsic_value(
    code: str,
    as_of: str,
    market: str,
    discount_rate: float = 0.11,
    terminal_growth: float = 0.02,
    industry_map: dict | None = None,
) -> tuple[float, float] | None:
    """返回 (内在价值下限, 内在价值上限)。金融股/FCF 不足 → None。

    spec §4.4：5 年平均 FCF，折现率 10-12%，终值增长 ≤3%。
    下限 = 高折现率价值；上限 = 低折现率价值。区间以传入 discount_rate 为中心、
    ±DISCOUNT_RATE_SPREAD 构造（默认 0.11 → 0.10/0.12，复现 spec 区间）。
    """
    if is_financial(code, industry_map):
        return None
    fund = pit_fundamental_as_of(as_of, market, code=code)
    if fund.empty:
        return None
    # pit_fundamental_as_of 按 announcement_date_approx 排序去重，未必按 report_period；
    # 此处按 report_period 排序后再 tail，确保取到最近 N 年（robustness）。
    fund = fund.sort_values("report_period")
    fcfs = fund["fcf"].dropna()
    if len(fcfs) < 2:  # 至少 2 年 FCF
        return None
    # 取最近 FCF_LOOKBACK 年平均
    recent = fcfs.tail(FCF_LOOKBACK)
    avg_fcf = float(recent.mean())
    if avg_fcf <= 0:
        return None
    g = min(terminal_growth, TERMINAL_GROWTH_MAX)
    low = _dcf_value(avg_fcf, discount_rate + DISCOUNT_RATE_SPREAD, g)   # 高折现率 → 下限
    high = _dcf_value(avg_fcf, discount_rate - DISCOUNT_RATE_SPREAD, g)  # 低折现率 → 上限
    if np.isnan(low) or np.isnan(high):
        return None
    return (low, high)
