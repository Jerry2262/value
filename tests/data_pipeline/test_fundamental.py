import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import FUNDAMENTAL_COLUMNS
from src.data_pipeline.fetchers.fundamental import (
    AShareFundamentalFetcher,
    approx_announcement_date,
)


def test_approx_announcement_date_annual():
    """年报报告期 12-31 → +4月 → 次年 4-30。"""
    assert approx_announcement_date("2023-12-31", "annual") == "2024-04-30"


def test_approx_announcement_date_interim():
    """中报报告期 06-30 → +2月 → 08-31。"""
    assert approx_announcement_date("2023-06-30", "interim") == "2023-08-31"


def test_approx_announcement_date_q1():
    """一季报报告期 03-31 → +1月 → 04-30。"""
    assert approx_announcement_date("2023-03-31", "q1") == "2023-04-30"


def test_approx_announcement_date_q3():
    assert approx_announcement_date("2023-09-30", "q3") == "2023-10-31"


def test_approx_announcement_date_invalid_month():
    """非标准报告期 → 返回 None（该期不计入 PIT）。"""
    assert approx_announcement_date("2023-05-15", "annual") is None


def test_a_share_fundamental_uses_financial_abstract(mocker):
    """财务字段须用 stock_financial_abstract（探针实测：禁用 stock_zh_a_spot_em）。"""
    raw = pd.DataFrame({
        "股票代码": ["600519", "600519"],
        "报告期": ["2023-12-31", "2023-06-30"],
        "营业收入": [1.5e11, 7e10],
        "净利润": [7e10, 3.5e10],
        "净资产收益率": [30.0, 29.0],
        "资产负债率": [20.0, 22.0],
        "经营现金流": [5e10, 2.5e10],
    })
    mocker.patch("akshare.stock_financial_abstract", return_value=raw)
    df = AShareFundamentalFetcher().fetch("600519")
    assert list(df.columns) == FUNDAMENTAL_COLUMNS
    assert len(df) == 2
    assert (df["code"] == "600519").all()
    assert (df["market"] == "a_share").all()
    # 公告日降级：年报 2023-12-31 → 2024-04-30
    assert df.loc[df["report_period"] == "2023-12-31", "announcement_date_approx"].iloc[0] == "2024-04-30"
    # roe 是数值
    assert df["roe"].iloc[0] == 30.0


def test_a_share_fundamental_retry_exhausts(mocker):
    mocker.patch("src.data_pipeline.fetchers.fundamental.time.sleep")
    mocker.patch("akshare.stock_financial_abstract", side_effect=RuntimeError("net"))
    from src.data_pipeline.fetchers.base import FetcherError
    with pytest.raises(FetcherError):
        AShareFundamentalFetcher().fetch("600519")
