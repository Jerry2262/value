"""Factor Engine 编排：universe → 逐因子计算 → 分层标准化 → 矩阵+分位秩（spec §5.2）。"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.factor_engine.registry import (
    compute_factor,
    get_factor_spec,
    list_in_composite_factors,
)
from src.factor_engine.standardizer import percentile_rank, standardize_factor
from src.pit.indexer import pit_active_universe, pit_delisted_before

# 触发所有 computer 注册
import src.factor_engine.computers.value  # noqa: F401
import src.factor_engine.computers.growth  # noqa: F401
import src.factor_engine.computers.quality  # noqa: F401
import src.factor_engine.computers.momentum  # noqa: F401


@dataclass
class FactorMatrix:
    """因子矩阵输出。"""
    matrix: pd.DataFrame          # codes × in_composite_factors，标准化值
    ranks: pd.DataFrame           # codes × factors，市场内分位秩
    momentum: pd.Series           # 动量反向校验值（不进 matrix）
    raw: pd.DataFrame = field(default_factory=pd.DataFrame)  # 原始值


def compute_factor_matrix(
    as_of: str,
    market: str,
    codes: list[str],
    industry_map: dict | None = None,
    code_industry: dict | None = None,
) -> FactorMatrix:
    """计算因子矩阵：扣退市 → 逐因子计算 → 分层标准化 → 输出矩阵+分位秩。

    spec §5.2/§5.3。动量权重 0 不进 matrix，单独输出供反向校验。
    """
    # 扣除退市（spec §5.3）
    active = pit_active_universe(as_of, market, codes) if codes else []
    if not active:
        return FactorMatrix(
            matrix=pd.DataFrame(),
            ranks=pd.DataFrame(),
            momentum=pd.Series(dtype=float),
            raw=pd.DataFrame(),
        )

    code_market = {c: market for c in active}
    in_composite = list_in_composite_factors()

    raw_data = {}
    rank_data = {}
    for name in in_composite:
        spec = get_factor_spec(name)
        raw_series = compute_factor(name, as_of, market, active)
        # 方向处理：reverse 因子取负（使"越好"统一为越大）
        if spec.get("direction") == "reverse":
            raw_series = -raw_series
        raw_data[name] = raw_series
        # 分层标准化
        standardized = standardize_factor(
            raw_series, market=market, industry_map=industry_map,
            code_market=code_market, code_industry=code_industry,
            sector_neutral=spec.get("params", {}).get("sector_neutral", True),
        )
        # 分位秩（spec §4.2 跨市场排序用）
        rank_data[name] = percentile_rank(standardized)

    raw_df = pd.DataFrame(raw_data, index=active)
    matrix = pd.DataFrame(
        {n: standardize_factor(
            raw_data[n], market=market, industry_map=industry_map,
            code_market=code_market, code_industry=code_industry,
            sector_neutral=get_factor_spec(n).get("params", {}).get("sector_neutral", True),
        ) for n in in_composite},
        index=active,
    )
    ranks = pd.DataFrame(rank_data, index=active)

    # 动量（不进 matrix，单独输出）
    momentum = compute_factor("momentum_12m1m", as_of, market, active)

    return FactorMatrix(matrix=matrix, ranks=ranks, momentum=momentum, raw=raw_df)
