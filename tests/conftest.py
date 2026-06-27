import os
from pathlib import Path

import pytest


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    """把 VALUE_DATA_DIR 指向临时目录，避免污染真实 data/。"""
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    # 重新导入 config 以刷新模块级常量
    import importlib
    import src.config as cfg
    importlib.reload(cfg)
    yield tmp_path
    importlib.reload(cfg)  # 恢复


@pytest.fixture
def quality_db(isolated_data_dir):
    """返回临时 data_quality.db 路径。"""
    return Path(isolated_data_dir) / "metadata" / "data_quality.db"
