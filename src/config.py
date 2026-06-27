"""配置加载与项目路径。"""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"


def _data_dir() -> Path:
    override = os.environ.get("VALUE_DATA_DIR")
    return Path(override).resolve() if override else PROJECT_ROOT / "data"


DATA_DIR = _data_dir()
METADATA_DIR = DATA_DIR / "metadata"


def load_config(name: str) -> dict:
    """加载 config/ 下的 YAML 配置（name 不含扩展名或含均可）。"""
    if not name.endswith(".yaml"):
        name = name + ".yaml"
    path = CONFIG_DIR / name
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_factor_configs() -> dict:
    """加载 config/factors/ 下全部因子定义，合并为 {factor_key: {...}}。"""
    merged: dict = {}
    factor_dir = CONFIG_DIR / "factors"
    for f in sorted(factor_dir.glob("*.yaml")):
        with f.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        for key, spec in (data.get("factors") or {}).items():
            merged[key] = spec
    return merged


def ensure_dirs() -> None:
    """创建运行时目录（不纳入 git）。"""
    for d in (DATA_DIR, METADATA_DIR, DATA_DIR / "raw", DATA_DIR / "processed", DATA_DIR / "pit"):
        d.mkdir(parents=True, exist_ok=True)


# 模块导入时加载 .env
load_dotenv(PROJECT_ROOT / ".env")
