"""成长因子：营收/利润 3年 CAGR、研发占比（spec §4.2 成长因子）。"""
from __future__ import annotations

import pandas as pd

from src.factor_engine.registry import register
from src.pit.indexer import pit_fundamental_as_of


def _cagr(start: float, end: float, years: float) -> float:
    """复合年化增速。start<=0 或 end<=0 → NaN（负值不可 CAGR）。"""
    if start <= 0 or end <= 0 or years <= 0:
        return float("nan")
    return (end / start) ** (1 / years) - 1


def _year_fraction(first_dt: pd.Timestamp, last_dt: pd.Timestamp) -> float:
    """首末 ``report_period`` 间的实际年距（ACT/ACT 口径，精确到日）。

    整年用 ``pd.DateOffset(years=n)`` 对齐到同月同日；不足整年的余数按所在
    日历年的实际天数（365 或 366）年化。对同月同日的端点（如年报 12-31）
    返回精确整年数，避免 ``days/365.25`` 在无闰日跨度上因闰年均摊而偏离
    整年（例如 2020-12-31→2023-12-31 共 1095 天，``/365.25`` 得 2.998 年，
    使 3 年 CAGR 偏离名义值超 1e-4）。ACT/ACT 对含/不含闰日的跨度均精确。
    """
    if first_dt == last_dt:
        return 0.0
    if last_dt < first_dt:
        first_dt, last_dt = last_dt, first_dt
    n = last_dt.year - first_dt.year
    anniv = first_dt + pd.DateOffset(years=n)
    if anniv > last_dt:
        n -= 1
        anniv = first_dt + pd.DateOffset(years=n)
    rem_days = (last_dt - anniv).days
    year_len = 366 if last_dt.is_leap_year else 365
    return float(n) + rem_days / year_len


def _cagr_for_code(
    fund: pd.DataFrame, field: str, min_years: float = 1.0
) -> float:
    """取该 code 的财报，按 report_period 排序，用首尾非空值与实际年距算 CAGR。

    计算 "up-to-3y" 实际跨度 CAGR：分母为首末 ``report_period`` 的真实年距
    （``_year_fraction``，ACT/ACT 口径），而非名义 3 年。这避免短历史
    标的（如 C3 仅 2021→2023 跨度 2 年）按 3 年分母算 CAGR 导致的失真
    （量纲被低估）。``min_years`` 为有效年距下限，跨度不足则返回 NaN
    （过短无法有意义地年化）。首尾取 ``dropna`` 后的端点，规避端点 NaN。
    """
    if fund.empty:
        return float("nan")
    df = fund.dropna(subset=[field]).sort_values("report_period").reset_index(drop=True)
    if len(df) < 2:
        return float("nan")
    # 取首尾非空值
    start = float(df[field].iloc[0])
    end = float(df[field].iloc[-1])
    # 实际年距（ACT/ACT，精确到日）
    first_dt = pd.to_datetime(df["report_period"].iloc[0])
    last_dt = pd.to_datetime(df["report_period"].iloc[-1])
    span_years = _year_fraction(first_dt, last_dt)
    if span_years < min_years:
        return float("nan")
    return _cagr(start, end, span_years)


@register("revenue_cagr_3y")
def compute_revenue_cagr_3y(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """营收 3 年复合增速。direction=forward。"""
    out = {}
    for code in codes:
        fund = pit_fundamental_as_of(as_of, market, code=code)
        out[code] = _cagr_for_code(fund, "revenue")
    return pd.Series(out)


@register("profit_cagr_3y")
def compute_profit_cagr_3y(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """净利润 3 年复合增速。direction=forward。"""
    out = {}
    for code in codes:
        fund = pit_fundamental_as_of(as_of, market, code=code)
        out[code] = _cagr_for_code(fund, "net_profit")
    return pd.Series(out)


@register("rnd_ratio")
def compute_rnd_ratio(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """研发占营收比。需研发费用字段（FUNDAMENTAL_COLUMNS 无 rnd）→ v1 全 NaN。"""
    out = {code: float("nan") for code in codes}
    return pd.Series(out)
