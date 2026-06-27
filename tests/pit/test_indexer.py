import pandas as pd
import pytest

from src.pit.indexer import (
    pit_active_universe,
    pit_delisted_before,
    pit_fundamental_as_of,
    pit_quote_as_of,
)


def test_pit_quote_filters_future_rows(multi_partition_quote):
    """as_of=2026-06-25 → 只含 date<=2026-06-25 的行（06-26 被过滤）。"""
    df = pit_quote_as_of("2026-06-25", "a_share")
    dates = set(df["date"])
    assert dates == {"2026-06-23", "2026-06-24"}
    assert "2026-06-26" not in dates


def test_pit_quote_dedup_across_partitions(multi_partition_quote):
    """跨分区去重：06-24 在两分区都有，取最新分区版本（close=10.5）。"""
    df = pit_quote_as_of("2026-06-27", "a_share")
    row_24 = df[df["date"] == "2026-06-24"].iloc[0]
    assert row_24["close"] == 10.5  # part2 修正值，非 part1 的 10.0
    assert len(df) == 3  # 06-23, 06-24(去重), 06-26


def test_pit_quote_code_filter(multi_partition_quote):
    df = pit_quote_as_of("2026-06-27", "a_share", code="C")
    assert (df["code"] == "C").all()


def test_pit_quote_empty_when_no_data(isolated_data_dir):
    df = pit_quote_as_of("2026-06-27", "us")
    assert df.empty


def test_pit_fundamental_filters_by_announcement_date(multi_partition_fundamental):
    """as_of=2024-05-15 → 只含 announcement_date_approx<=2024-05-15（2023年报已披露，2024中报未披露）。"""
    df = pit_fundamental_as_of("2024-05-15", "a_share")
    periods = set(df["report_period"])
    assert periods == {"2023-12-31"}
    assert "2024-06-30" not in periods


def test_pit_fundamental_dedup_takes_latest_disclosure(multi_partition_fundamental):
    """2023 年报在两分区都有（part1 roe=30, part2 roe=31），取较新披露版本（31.0）。"""
    df = pit_fundamental_as_of("2024-05-15", "a_share")
    row_2023 = df[df["report_period"] == "2023-12-31"].iloc[0]
    assert row_2023["roe"] == 31.0


def test_pit_fundamental_includes_both_after_later_disclosure(multi_partition_fundamental):
    """as_of=2024-09-01 → 2023年报 + 2024中报都可见。"""
    df = pit_fundamental_as_of("2024-09-01", "a_share")
    assert set(df["report_period"]) == {"2023-12-31", "2024-06-30"}


def test_pit_delisted_before(a_share_delisting):
    """as_of=2024-01-01 → C1 已退市，C2 未退市。"""
    df = pit_delisted_before("2024-01-01", "a_share")
    codes = set(df["code"])
    assert codes == {"C1"}


def test_pit_active_universe(a_share_delisting):
    """as_of=2024-01-01：C1 已退市剔除，C2/C3 在市。"""
    active = pit_active_universe("2024-01-01", "a_share", all_codes=["C1", "C2", "C3"])
    assert set(active) == {"C2", "C3"}


def test_pit_active_universe_all_active_later(a_share_delisting):
    """as_of=2026-01-01：C1/C2 都已退市，只剩 C3。"""
    active = pit_active_universe("2026-01-01", "a_share", all_codes=["C1", "C2", "C3"])
    assert set(active) == {"C3"}
