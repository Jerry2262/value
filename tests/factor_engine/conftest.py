import pandas as pd
import pytest

from src.data_pipeline.fetchers.base import FUNDAMENTAL_COLUMNS, QUOTE_COLUMNS
from src.data_pipeline import store


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("VALUE_DATA_DIR", str(tmp_path))
    import importlib
    import src.config as cfg
    importlib.reload(cfg)
    yield tmp_path
    importlib.reload(cfg)


@pytest.fixture
def fundamentals_for_factors(isolated_data_dir):
    """3 只股票 × 4 年财报，用于因子计算。

    C1: 优质成长（高 ROE、稳毛利、营收增长）
    C2: 价值（低 PE、高股息、稳毛利）
    C3: 弱质（低 ROE、负 FCF、高杠杆）
    """
    rows = []
    # C1: revenue 100→120→140→160, net_profit 20→24→28→32, roe 30→30→30→30,
    #     debt_ratio 30, fcf 15, total_market_cap 600
    for yr, (rev, np_, roe) in enumerate(
        [(100, 20, 30), (120, 24, 30), (140, 28, 30), (160, 32, 30)], start=2020
    ):
        rows.append({"code": "C1", "market": "a_share", "report_period": f"{yr}-12-31",
                     "announcement_date_approx": f"{yr+1}-04-30",
                     "revenue": rev * 1e8, "net_profit": np_ * 1e8, "roe": roe,
                     "debt_ratio": 30.0, "fcf": 15e8, "total_market_cap": 600e8})
    # C2: revenue 稳定 80, net_profit 8, roe 15, debt 40, fcf 10, mcap 160 (低 PE=20)
    for yr in range(2020, 2024):
        rows.append({"code": "C2", "market": "a_share", "report_period": f"{yr}-12-31",
                     "announcement_date_approx": f"{yr+1}-04-30",
                     "revenue": 80e8, "net_profit": 8e8, "roe": 15.0,
                     "debt_ratio": 40.0, "fcf": 10e8, "total_market_cap": 160e8})
    # C3: revenue 50→45→40, net_profit 2→1→0.5, roe 5, debt 75, fcf -2, mcap 50
    for yr, (rev, np_) in enumerate([(50, 2), (45, 1), (40, 0.5)], start=2021):
        rows.append({"code": "C3", "market": "a_share", "report_period": f"{yr}-12-31",
                     "announcement_date_approx": f"{yr+1}-04-30",
                     "revenue": rev * 1e8, "net_profit": np_ * 1e8, "roe": 5.0,
                     "debt_ratio": 75.0, "fcf": -2e8, "total_market_cap": 50e8})
    store.write_parquet_partition(pd.DataFrame(rows, columns=FUNDAMENTAL_COLUMNS),
                                  "fundamental", "2024-04-30", "a_share")


@pytest.fixture
def quotes_for_momentum(isolated_data_dir):
    """C1/C2/C3 行情用于动量计算（12-1月动量需 ≥13 个月数据）。

    按月写分区（partition_date = 当月日期），模拟月度快照：使 PIT 分区级过滤
    （store.read_parquet 的 partition_date <= as_of）在任意 as_of 下保留正确可见
    月份。原版单分区 "2024-12-31" 在 as_of=2024-12-15 会被整块过滤致空面板
    （brief bug 修正：分区日须 <= as_of 才可见）。
    """
    # 简化：每月一行，C1 涨、C2 平、C3 跌
    for m in range(1, 25):  # 2023-01 ~ 2024-12
        date = f"2023-{m:02d}-15" if m <= 12 else f"2024-{m-12:02d}-15"
        base_c1 = 10 + m * 0.5
        rows = [
            {"date": date, "code": "C1", "market": "a_share",
             "open": base_c1, "high": base_c1, "low": base_c1, "close": base_c1,
             "volume": 1000, "adj_factor": 1.0},
            {"date": date, "code": "C2", "market": "a_share",
             "open": 20, "high": 20, "low": 20, "close": 20,
             "volume": 1000, "adj_factor": 1.0},
            {"date": date, "code": "C3", "market": "a_share",
             "open": 30 - m * 0.3, "high": 30 - m * 0.3, "low": 30 - m * 0.3,
             "close": 30 - m * 0.3, "volume": 1000, "adj_factor": 1.0},
        ]
        store.write_parquet_partition(pd.DataFrame(rows, columns=QUOTE_COLUMNS),
                                      "market", date, "a_share")
