"""分层标准化（spec §4.2）：截尾 → 市场内 Z → 行业内 Z → 可选市值中性 + 分位秩。"""
from __future__ import annotations

import numpy as np
import pandas as pd


def winsorize_mad(s: pd.Series, n_mad: float = 5.0) -> pd.Series:
    """MAD 法截尾：超出 median ± n_mad×MAD 的值钳到边界（spec §4.2 第0层）。

    NaN 保留；若 MAD == 0（无离散度）则原样返回拷贝。
    """
    valid = s.dropna()
    if valid.empty:
        return s.copy()
    median = valid.median()
    mad = (valid - median).abs().median()
    if mad == 0 or np.isnan(mad):
        return s.copy()  # 无离散度，不截
    lower = median - n_mad * mad
    upper = median + n_mad * mad
    return s.clip(lower=lower, upper=upper)


def zscore(s: pd.Series) -> pd.Series:
    """Z-score 标准化（NaN 保留）。

    在非 NaN 子集上计算 mean/std；若 std == 0 → 全 0。
    """
    valid = s.dropna()
    if valid.empty:
        return s.copy()
    std = valid.std()
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=s.index)  # 无离散度 → 全 0
    return (s - valid.mean()) / std


def _zscore_within_group(s: pd.Series, groups: pd.Series) -> pd.Series:
    """按 groups 分组做 Z-score。

    NaN 组（无行业归属的标的）被 pandas groupby 默认丢弃，保持 NaN。
    """
    out = pd.Series(np.nan, index=s.index, dtype=float)
    for g, idx in groups.groupby(groups).groups.items():
        out.loc[idx] = zscore(s.loc[idx])
    return out


def standardize_factor(
    raw: pd.Series,
    market: str,
    industry_map: dict | None,
    code_market: dict,
    code_industry: dict | None,
    sector_neutral: bool = True,
    size_neutral: bool = False,
    code_size: dict | None = None,
) -> pd.Series:
    """分层标准化（spec §4.2）。

    第0层：MAD 截尾
    第1层：市场内 Z（调用方已按 market 过滤，故直接对传入的 raw 做 Z）
    第2层：行业内 Z（若 sector_neutral 且 code_industry 提供）
    第3层：市值中性（v1 默认关闭，YAGNI）
    """
    s = winsorize_mad(raw)
    # 第1层：市场内 Z（调用方已按 market 过滤，此处对当前序列做 Z）
    s = zscore(s)
    # 第2层：行业内 Z
    if sector_neutral and code_industry is not None:
        groups = pd.Series({c: code_industry.get(c) for c in s.index})
        s = _zscore_within_group(s, groups)
    # 第3层：市值中性（v1 默认关闭，YAGNI）
    if size_neutral and code_size is not None:
        # 按 size 分位回归取残差（v1 简化：跳过，仅留接口）
        pass
    return s


def percentile_rank(s: pd.Series) -> pd.Series:
    """市场内分位秩（spec §4.2 跨市场排序用）。NaN 保留。

    返回每个值在该序列中的分位（0=最小，1=最大）。
    n>1: (rank-1)/(n-1)；n==1: 0.5。
    """
    valid = s.dropna()
    if valid.empty:
        return s.copy()
    ranks = valid.rank(method="average")
    n = len(valid)
    out = pd.Series(np.nan, index=s.index, dtype=float)
    if n > 1:
        out.loc[valid.index] = (ranks - 1) / (n - 1)
    else:
        out.loc[valid.index] = 0.5
    return out
