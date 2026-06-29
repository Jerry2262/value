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
from src.pit.indexer import pit_active_universe

# 触发所有 computer 注册
import src.factor_engine.computers.value  # noqa: F401
import src.factor_engine.computers.growth  # noqa: F401
import src.factor_engine.computers.quality  # noqa: F401
import src.factor_engine.computers.momentum  # noqa: F401


@dataclass
class FactorMatrix:
    """因子矩阵输出。

    字段语义：
    - ``raw``：原始未取负值。reverse 因子保留真实方向（如 pe_percentile 仍为真实
      历史分位 0~1），供 Phase 5 评分器 §4.4 红绿灯（绝对估值档位判断）使用。
    - ``matrix`` / ``ranks``：方向调整后（reverse 取负）的标准化值与分位秩，
      "越大越好"统一，直接用于复合得分加权。

    v1 NaN 占位交接（Phase 5 评分器须知）：以下 4 个 in_composite 因子因数据源
    缺失，v1 返回全 NaN 列——pb_percentile（缺净资产字段）、dividend_yield（缺
    分红 fetcher）、gross_margin_stability（缺毛利率字段）、rnd_ratio（缺研发费用
    字段）。Phase 5 的 scorer 必须以 NaN-aware 方式合成复合得分：先丢弃全 NaN 列
    并重新归一化权重（或用 nan-aware 加权均值）。朴素 ``sum(w_i * x_i)`` 会在任一
    权重落在全 NaN 列时得到全 NaN 复合得分，不可直接使用。
    """
    matrix: pd.DataFrame          # codes × in_composite_factors，方向调整后标准化值
    ranks: pd.DataFrame           # codes × factors，方向调整后市场内分位秩
    momentum: pd.Series           # 动量反向校验值（不进 matrix）
    raw: pd.DataFrame = field(default_factory=pd.DataFrame)  # 原始未取负值


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
    standardized_data = {}
    rank_data = {}
    for name in in_composite:
        spec = get_factor_spec(name)
        raw_series = compute_factor(name, as_of, market, active)
        # raw 存原始未取负值（供 Phase 5 §4.4 红绿灯绝对估值判断）
        raw_data[name] = raw_series
        # 方向处理：reverse 因子取负（使"越大越好"统一），仅用于 matrix/ranks
        adj = -raw_series if spec.get("direction") == "reverse" else raw_series
        # 分层标准化（计算一次，复用于 matrix 与 ranks）
        standardized = standardize_factor(
            adj, market=market, industry_map=industry_map,
            code_market=code_market, code_industry=code_industry,
            sector_neutral=spec.get("params", {}).get("sector_neutral", True),
        )
        standardized_data[name] = standardized
        # 分位秩（spec §4.2 跨市场排序用）
        rank_data[name] = percentile_rank(standardized)

    raw_df = pd.DataFrame(raw_data, index=active)
    matrix = pd.DataFrame(standardized_data, index=active)
    ranks = pd.DataFrame(rank_data, index=active)

    # 动量（不进 matrix，单独输出）
    momentum = compute_factor("momentum_12m1m", as_of, market, active)

    return FactorMatrix(matrix=matrix, ranks=ranks, momentum=momentum, raw=raw_df)
