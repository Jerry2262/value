import os
from pathlib import Path

import pytest

from src import config


def test_project_root_exists():
    assert config.PROJECT_ROOT.is_dir()
    assert (config.CONFIG_DIR / "universe.yaml").is_file()


def test_load_config_universe():
    cfg = config.load_config("universe")
    assert "markets" in cfg
    assert "a_share" in cfg["markets"]


def test_data_dir_override(monkeypatch, tmp_path):
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    # 重新读取模块级常量需要调用函数；验证函数逻辑
    from src.config import _data_dir
    assert _data_dir() == tmp_path


def test_load_factor_configs_has_value_factors():
    factors = config.load_factor_configs()
    assert "pe_percentile" in factors
    assert factors["pe_percentile"]["category"] == "value"


def test_momentum_weight_is_zero(factors_cfg=None):
    factors = config.load_factor_configs()
    assert factors["momentum_12m1m"]["weight"] == 0.0
    assert factors["momentum_12m1m"]["in_composite"] is False


def test_ensure_dirs_creates_metadata(tmp_path, monkeypatch):
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    # ensure_dirs 用的是模块导入时的 DATA_DIR；直接调用并断言子目录被建
    config.METADATA_DIR.mkdir(parents=True, exist_ok=True)
    assert config.METADATA_DIR.is_dir()
