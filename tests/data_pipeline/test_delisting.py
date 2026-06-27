import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import DELISTING_COLUMNS
from src.data_pipeline.fetchers.delisting import (
    AShareDelistingFetcher,
    load_manual_delisting,
)


def test_a_share_delisting_normalizes(mocker):
    raw = pd.DataFrame({
        "公司代码": ["000001", "000002"],
        "退市日期": ["2020-01-01", "2021-06-30"],
        "退市原因": ["强制退市", "吸收合并"],
    })
    mocker.patch("akshare.stock_info_sh_name_code", return_value=raw)  # 占位接口名
    df = AShareDelistingFetcher().fetch()
    assert list(df.columns) == DELISTING_COLUMNS
    assert (df["market"] == "a_share").all()
    assert len(df) == 2


def test_load_manual_delisting_hk(tmp_path):
    """港美股退市人工补录 CSV（spec §3.5 非重点，人工维护）。"""
    csv = tmp_path / "hk_delist.csv"
    csv.write_text("code,delist_date,reason\n00700,2020-01-01,私有化\n00358,2019-06-30,收购\n", encoding="utf-8")
    df = load_manual_delisting("hk", csv)
    assert list(df.columns) == DELISTING_COLUMNS
    assert (df["market"] == "hk").all()
    assert len(df) == 2
    assert df.loc[df["code"] == "00700", "reason"].iloc[0] == "私有化"


def test_load_manual_delisting_missing_file(tmp_path):
    """CSV 不存在 → 返回空 DataFrame（不报错，降级为标注）。"""
    df = load_manual_delisting("us", tmp_path / "nonexistent.csv")
    assert list(df.columns) == DELISTING_COLUMNS
    assert len(df) == 0
