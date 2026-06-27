import pandas as pd
import pytest

from src.factor_engine.registry import (
    FACTOR_REGISTRY,
    compute_factor,
    get_factor_spec,
    list_in_composite_factors,
    register,
)


def test_register_adds_to_registry():
    @register("test_factor_temp")
    def _compute(as_of, market, codes, **kwargs):
        return pd.Series({c: 1.0 for c in codes})

    assert "test_factor_temp" in FACTOR_REGISTRY
    # 清理
    del FACTOR_REGISTRY["test_factor_temp"]


def test_compute_factor_calls_registered():
    @register("test_factor_temp2")
    def _compute(as_of, market, codes, **kwargs):
        return pd.Series({c: i for i, c in enumerate(codes)})

    s = compute_factor("test_factor_temp2", "2024-01-01", "a_share", ["A", "B"])
    assert s["A"] == 0
    assert s["B"] == 1
    del FACTOR_REGISTRY["test_factor_temp2"]


def test_compute_factor_unknown_raises():
    with pytest.raises(KeyError):
        compute_factor("nonexistent_factor", "2024-01-01", "a_share", ["A"])


def test_get_factor_spec_from_yaml():
    spec = get_factor_spec("pe_percentile")
    assert spec["category"] == "value"
    assert spec["direction"] == "reverse"
    assert spec["weight"] == 0.15
    assert spec["in_composite"] is True


def test_get_factor_spec_momentum_weight_zero():
    spec = get_factor_spec("momentum_12m1m")
    assert spec["weight"] == 0.0
    assert spec["in_composite"] is False


def test_list_in_composite_factors_excludes_momentum():
    in_composite = list_in_composite_factors()
    assert "momentum_12m1m" not in in_composite
    assert "pe_percentile" in in_composite
    assert "roe" in in_composite
    # 11 个 in-composite 因子（4 value + 3 growth + 4 quality）；动量 excluded
    # NOTE(brief-fix): brief 原文为 `len == 10` / "3 quality"，与 spec §5（4 quality：
    # ROE / 毛利率稳定性 / 现金流质量 / 杠杆率）及 config/factors/quality.yaml
    # （4 因子，in_composite 权重和恰为 1.00）冲突。spec + YAML 为权威，此处修正为 11。
    assert len(in_composite) == 11
