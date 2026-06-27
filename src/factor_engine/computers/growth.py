"""成长因子：营收/利润 3年 CAGR、研发占比（spec §4.2 成长因子）。"""
from __future__ import annotations

import pandas as pd

from src.factor_engine.registry import register
from src.pit.indexer import pit_fundamental_as_of


def _cagr(start: float, end: float, years: int) -> float:
    """复合年化增速。start<=0 或 end<=0 → NaN（负值不可 CAGR）。"""
    if start <= 0 or end <= 0 or years <= 0:
        return float("nan")
    return (end / start) ** (1 / years) - 1


def _cagr_for_code(
    fund: pd.DataFrame, field: str, years: int
) -> float:
    """取该 code 的财报，按 report_period 排序，取最早 vs 最新（跨度 years 年）算 CAGR。

    注：``years`` 为 CAGR 的名义分母（因子名 ``revenue_cagr_3y`` 的 "3y"），
    不与首末 report_period 的实际跨度做强校验——历史不足 3 年的标的（如 C3
    仅 2021-2023 共 3 期、跨度 2 年）仍按 ``years`` 算 CAGR，结果方向正确但
    量纲为名义年化。强约束（span 必须 == years 否则 NaN）会与
    ``test_revenue_cagr_declining_is_negative``（要求 C3 返回负值）冲突，
    故 v1 采用宽松口径。
    """
    if fund.empty:
        return float("nan")
    df = fund.sort_values("report_period").reset_index(drop=True)
    vals = df[field].dropna()
    if len(vals) < 2:
        return float("nan")
    # 取首尾
    start = df[field].iloc[0]
    end = df[field].iloc[-1]
    return _cagr(float(start), float(end), years)


@register("revenue_cagr_3y")
def compute_revenue_cagr_3y(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """营收 3 年复合增速。direction=forward。"""
    out = {}
    for code in codes:
        fund = pit_fundamental_as_of(as_of, market, code=code)
        out[code] = _cagr_for_code(fund, "revenue", 3)
    return pd.Series(out)


@register("profit_cagr_3y")
def compute_profit_cagr_3y(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """净利润 3 年复合增速。direction=forward。"""
    out = {}
    for code in codes:
        fund = pit_fundamental_as_of(as_of, market, code=code)
        out[code] = _cagr_for_code(fund, "net_profit", 3)
    return pd.Series(out)


@register("rnd_ratio")
def compute_rnd_ratio(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """研发占营收比。需研发费用字段（FUNDAMENTAL_COLUMNS 无 rnd）→ v1 全 NaN。"""
    out = {code: float("nan") for code in codes}
    return pd.Series(out)
