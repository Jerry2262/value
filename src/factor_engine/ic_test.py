"""因子 IC/ICIR 分市场检验（spec §5.3 v1）。"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.factor_engine.registry import compute_factor


def ic(factor_values: pd.Series, forward_returns: pd.Series) -> float:
    """单期 IC = Spearman 秩相关（因子值与前瞻收益）。

    spec §5.3：IC 衡量因子选股能力。NaN 对齐后丢弃；<3 个有效点 → NaN。
    """
    df = pd.DataFrame({"f": factor_values, "r": forward_returns}).dropna()
    if len(df) < 3:
        return float("nan")
    # Spearman = Pearson on ranks
    return float(df["f"].rank().corr(df["r"].rank()))


def icir(ic_series: pd.Series) -> float:
    """ICIR = mean(IC) / std(IC)。std=0 → NaN。"""
    valid = ic_series.dropna()
    if len(valid) < 2:
        return float("nan")
    std = valid.std()
    # 用 isclose 而非 == 0：等值序列（如 [0.1, 0.1, 0.1]）的样本 std 因浮点
    # 残差约为 1e-17 而非精确 0，== 0 会漏判 → 返回巨大伪 ICIR。spec：std=0 → NaN。
    if np.isnan(std) or np.isclose(std, 0.0):
        return float("nan")
    return float(valid.mean() / std)


def factor_ic_report(
    factor_name: str,
    market: str,
    as_of_dates: list[str],
    codes: list[str],
    return_lookup: dict[str, pd.Series],
    factor_lookup: dict[str, pd.Series] | None = None,
) -> dict:
    """分市场多期 IC 报告（spec §5.3）。

    factor_lookup 给定时直接用（测试用）；否则调 compute_factor 实算。
    return_lookup: as_of_date → 前瞻收益 Series。
    返回 {mean_ic, icir, ic_series}。
    """
    ic_vals = {}
    for as_of in as_of_dates:
        if factor_lookup is not None:
            factor_values = factor_lookup.get(as_of, pd.Series(dtype=float))
        else:
            factor_values = compute_factor(factor_name, as_of, market, codes)
        forward_returns = return_lookup.get(as_of, pd.Series(dtype=float))
        ic_vals[as_of] = ic(factor_values, forward_returns)
    ic_series = pd.Series(ic_vals)
    return {
        "mean_ic": float(ic_series.mean()) if not ic_series.dropna().empty else float("nan"),
        "icir": icir(ic_series),
        "ic_series": ic_series,
    }
