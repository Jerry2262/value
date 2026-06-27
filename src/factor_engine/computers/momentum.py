"""动量因子：12-1月动量（spec §4.2，权重 0，仅反向校验不进综合得分）。"""
from __future__ import annotations

import pandas as pd

from src.factor_engine.registry import register
from src.pit.slicer import slice_quote_panel


@register("momentum_12m1m")
def compute_momentum_12m1m(as_of: str, market: str, codes: list[str], **kwargs) -> pd.Series:
    """12-1月动量 = (T-1月收盘 / T-13月收盘) - 1。跳过最近1月（避免短期反转）。

    direction=forward。权重永久 0、in_composite=False（spec §4.2）。
    """
    panel = slice_quote_panel(as_of, market, codes)
    out = {}
    if panel.empty:
        return pd.Series({c: float("nan") for c in codes})
    as_of_ts = pd.to_datetime(as_of)
    t_minus_1m = as_of_ts - pd.DateOffset(months=1)
    t_minus_13m = as_of_ts - pd.DateOffset(months=13)
    for code in codes:
        df = panel[panel["code"] == code].copy()
        if df.empty:
            out[code] = float("nan")
            continue
        df["date_ts"] = pd.to_datetime(df["date"])
        df = df.sort_values("date_ts")
        # 取 <= t-1m 的最近一行 与 <= t-13m 的最近一行
        recent = df[df["date_ts"] <= t_minus_1m]
        old = df[df["date_ts"] <= t_minus_13m]
        if recent.empty or old.empty:
            out[code] = float("nan")
            continue
        p_recent = float(recent.iloc[-1]["close"])
        p_old = float(old.iloc[-1]["close"])
        if p_old <= 0:
            out[code] = float("nan")
            continue
        out[code] = p_recent / p_old - 1
    return pd.Series(out)
