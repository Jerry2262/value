"""PIT Slicer：面向因子/回测的高层切片 API（spec §3.4）。

封装 indexer，提供多股票面板、最新财报、PE 分位等。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.pit.indexer import pit_fundamental_as_of, pit_quote_as_of


def slice_quote_panel(as_of: str, market: str, codes: list[str]) -> pd.DataFrame:
    """多股票行情面板（date <= as_of，仅 codes）。"""
    if not codes:
        return pd.DataFrame()
    frames = [pit_quote_as_of(as_of, market, code=c) for c in codes]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def slice_latest_fundamental(as_of: str, market: str, codes: list[str]) -> pd.DataFrame:
    """每只股票截至 as_of 最新一期财报（report_period 最大）。"""
    if not codes:
        return pd.DataFrame()
    frames = [pit_fundamental_as_of(as_of, market, code=c) for c in codes]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()
    all_f = pd.concat(frames, ignore_index=True)
    # 每只股票取 report_period 最大的一期
    idx = all_f.groupby("code")["report_period"].idxmax()
    return all_f.loc[idx].reset_index(drop=True)


def pe_ratio_at(as_of: str, market: str, code: str) -> float | None:
    """T 日 PE（总市值法：total_market_cap / net_profit，spec §4.4）。

    用 T 日最新可见财报的 net_profit。net_profit<=0 或缺失 → None。
    """
    fund = pit_fundamental_as_of(as_of, market, code=code)
    if fund.empty:
        return None
    latest = fund.loc[fund["report_period"].idxmax()]
    np_ = latest.get("net_profit")
    mcap = latest.get("total_market_cap")
    if pd.isna(np_) or pd.isna(mcap) or np_ <= 0:
        return None
    return float(mcap) / float(np_)


def pe_percentile(
    as_of: str, market: str, code: str, lookback_years: int = 5
) -> float | None:
    """PE 历史分位（PIT，spec §3.4/§4.4）。

    用 as_of 前 lookback_years 年内每年一期的 PE 序列，计算当前 PE 在序列中的分位。
    PIT 约束：只用 announcement_date_approx <= as_of 的财报，不含未来。
    数据不足（<2 个历史点或无当前 PE）→ None。

    PIT 安全要点（已通过 test_slicer.py 验证）：
    - 输入 fund 已由 pit_fundamental_as_of 行级过滤 announcement_date_approx<=as_of，
      故 as_of 后披露的财报（如 as_of=2024-05-01 时的 2024 年报，披露日 2025-04-30）
      不会进入序列——这是 PIT 的第一道门。
    - 窗口 = [as_of - lookback_years, as_of]，按 report_period 过滤；早于窗口的年报
      被排除（如 as_of=2026-05-01, lookback=5 时 2020 年报因 report_period=2020-12-31
      < window_start=2021-05-01 被排除）。
    - 每年去重（drop_duplicates subset=['year'], keep='last'）：同年有多个 report_period
      时取最新一期，避免重复计数。
    - 当前 PE = 序列中 report_period 最大的一期（即窗口内最新年报），与序列同源，
      保证分位 ∈ (0, 1]。
    """
    fund = pit_fundamental_as_of(as_of, market, code=code)
    if fund.empty:
        return None
    fund = fund.copy()
    fund["report_period_dt"] = pd.to_datetime(fund["report_period"], errors="coerce")
    as_of_dt = pd.to_datetime(as_of)
    window_start = as_of_dt - pd.DateOffset(years=lookback_years)
    # PIT 窗口：report_period <= as_of 且 >= window_start
    in_window = fund[
        (fund["report_period_dt"] <= as_of_dt)
        & (fund["report_period_dt"] >= window_start)
    ].copy()
    # 每年取一期（report_period 末月），按年去重取最新
    in_window["year"] = in_window["report_period_dt"].dt.year
    in_window = in_window.sort_values("report_period_dt")
    in_window = in_window.drop_duplicates(subset=["year"], keep="last")
    # 计算 PE = total_market_cap / net_profit
    in_window["pe"] = in_window["total_market_cap"] / in_window["net_profit"]
    # net_profit==0 → pe=inf；dropna 不剔除 inf，且 inf>0 为 True 会污染分位，故先替换为 NaN
    valid = in_window.replace([np.inf, -np.inf], np.nan)
    valid = valid.dropna(subset=["pe"])
    valid = valid[valid["pe"] > 0]
    if len(valid) < 2:
        return None
    # 当前 PE = 最新一期（report_period 最大）
    current_pe = valid.sort_values("report_period_dt").iloc[-1]["pe"]
    series = valid["pe"].values
    # 分位 = 序列中 <= current_pe 的比例
    pct = float((series <= current_pe).sum()) / len(series)
    return pct
