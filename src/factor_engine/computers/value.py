"""价值因子：pe/pb 分位、股息率、FCF yield（spec §4.2 价值因子）。"""
from __future__ import annotations

import pandas as pd

from src.factor_engine.registry import register
from src.pit.slicer import pe_percentile, slice_latest_fundamental


@register("pe_percentile")
def compute_pe_percentile(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """PE 历史分位（PIT，Phase 3 slicer）。direction=reverse（越低越好）。

    调用 pit.slicer.pe_percentile（lookback_years=5），None → NaN。
    """
    out = {}
    for code in codes:
        pct = pe_percentile(as_of, market, code, lookback_years=5)
        out[code] = pct if pct is not None else float("nan")
    return pd.Series(out)


@register("pb_percentile")
def compute_pb_percentile(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """PB 历史分位。direction=reverse（spec §4.2）。

    v1 占位：返回全 NaN。PB = total_market_cap / net_assets，但
    FUNDAMENTAL_COLUMNS（Phase 2 数据契约）当前不含净资产/book-value 字段，
    故无法 PIT 计算。TODO 后续 Phase 补净资产字段后实现真实 PB 分位。
    """
    out = {code: float("nan") for code in codes}
    return pd.Series(out)


@register("dividend_yield")
def compute_dividend_yield(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """股息率。direction=forward。spec §4.2 min_consistency=3（至少连续3年分红）。

    v1 占位：返回全 NaN。需分红数据，Phase 2 未建分红 fetcher。
    TODO 后续 Phase 补分红 fetcher 后实现。
    """
    out = {code: float("nan") for code in codes}
    return pd.Series(out)


@register("fcf_yield")
def compute_fcf_yield(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """FCF yield = fcf / total_market_cap（最新一期财报）。direction=forward。"""
    fund = slice_latest_fundamental(as_of, market, codes)
    out = {}
    if fund.empty:
        return pd.Series({c: float("nan") for c in codes})
    fund = fund.set_index("code")
    for code in codes:
        if code in fund.index:
            row = fund.loc[code]
            fcf = row.get("fcf")
            mcap = row.get("total_market_cap")
            if pd.notna(fcf) and pd.notna(mcap) and mcap != 0:
                out[code] = float(fcf) / float(mcap)
            else:
                out[code] = float("nan")
        else:
            out[code] = float("nan")
    return pd.Series(out)
