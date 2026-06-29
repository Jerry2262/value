"""因子注册表：YAML 定义 + @register 装饰器注册 compute 函数（spec §5.1）。"""
from __future__ import annotations

from typing import Callable

import pandas as pd

from src import config

# factor_key -> compute 函数
FACTOR_REGISTRY: dict[str, Callable] = {}


def register(name: str):
    """装饰器：注册一个因子 compute 函数。

    用法：
        @register("pe_percentile")
        def compute(as_of, market, codes, **kwargs) -> pd.Series: ...
    """
    def decorator(fn: Callable) -> Callable:
        FACTOR_REGISTRY[name] = fn
        return fn
    return decorator


def get_factor_spec(name: str) -> dict:
    """从 YAML 取因子定义。"""
    factors = config.load_factor_configs()
    if name not in factors:
        raise KeyError(f"因子 {name} 未在 config/factors/*.yaml 中定义")
    return factors[name]


def list_in_composite_factors() -> list[str]:
    """返回 in_composite=True 的因子键（动量 excluded）。"""
    factors = config.load_factor_configs()
    return [k for k, spec in factors.items() if spec.get("in_composite") is True]


def compute_factor(
    name: str, as_of: str, market: str, codes: list[str], **kwargs
) -> pd.Series:
    """调用注册的 compute 函数，返回 Series（index=code）。"""
    if name not in FACTOR_REGISTRY:
        raise KeyError(f"因子 {name} 未注册 compute 函数")
    return FACTOR_REGISTRY[name](as_of, market, codes, **kwargs)
