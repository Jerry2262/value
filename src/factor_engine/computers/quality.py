"""质量因子：ROE、毛利稳定性、现金流质量、杠杆率（spec §4.2 质量因子）。"""
from __future__ import annotations

import pandas as pd

from src.factor_engine.registry import register
from src.pit.slicer import slice_latest_fundamental


@register("roe")
def compute_roe(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """ROE = 最新一期财报的 roe 字段。direction=forward。"""
    fund = slice_latest_fundamental(as_of, market, codes)
    out = {}
    if fund.empty:
        return pd.Series({c: float("nan") for c in codes})
    fund = fund.set_index("code")
    for code in codes:
        if code in fund.index:
            v = fund.loc[code, "roe"]
            out[code] = float(v) if pd.notna(v) else float("nan")
        else:
            out[code] = float("nan")
    return pd.Series(out)


@register("gross_margin_stability")
def compute_gross_margin_stability(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """毛利率稳定性。需毛利率字段（FUNDAMENTAL_COLUMNS 无 gross_margin）→ v1 全 NaN。

    direction=forward。理想实现：3 年毛利率标准差越小越好（direction forward 取负或用 1/std）。
    """
    out = {code: float("nan") for code in codes}
    return pd.Series(out)


@register("cash_flow_quality")
def compute_cash_flow_quality(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """现金流质量 = 经营现金流 / 净利润（最新一期）。direction=forward。"""
    fund = slice_latest_fundamental(as_of, market, codes)
    out = {}
    if fund.empty:
        return pd.Series({c: float("nan") for c in codes})
    fund = fund.set_index("code")
    for code in codes:
        if code in fund.index:
            row = fund.loc[code]
            fcf = row.get("fcf")
            np_ = row.get("net_profit")
            if pd.notna(fcf) and pd.notna(np_) and np_ != 0:
                out[code] = float(fcf) / float(np_)
            else:
                out[code] = float("nan")
        else:
            out[code] = float("nan")
    return pd.Series(out)


@register("leverage")
def compute_leverage(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """杠杆率 = 1 - 资产负债率（最新一期）。direction=forward（越低杠杆越好）。"""
    fund = slice_latest_fundamental(as_of, market, codes)
    out = {}
    if fund.empty:
        return pd.Series({c: float("nan") for c in codes})
    fund = fund.set_index("code")
    for code in codes:
        if code in fund.index:
            debt_ratio = fund.loc[code, "debt_ratio"]
            if pd.notna(debt_ratio):
                out[code] = 1.0 - float(debt_ratio) / 100.0  # debt_ratio 是百分比
            else:
                out[code] = float("nan")
        else:
            out[code] = float("nan")
    return pd.Series(out)
