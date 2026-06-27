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


def test_ensure_dirs_creates_metadata(isolated_data_dir):
    # isolated_data_dir reloads src.config so DATA_DIR/METADATA_DIR point at
    # the tmp dir; calling ensure_dirs() must create the runtime subdirs there
    # (not the real project data/).
    import src.config as cfg
    cfg.ensure_dirs()
    assert (cfg.DATA_DIR / "metadata").is_dir()
    assert (cfg.DATA_DIR / "raw").is_dir()
    assert (cfg.DATA_DIR / "processed").is_dir()
    assert (cfg.DATA_DIR / "pit").is_dir()
