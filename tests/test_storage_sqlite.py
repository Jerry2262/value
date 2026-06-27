import threading
from pathlib import Path

import pytest

from src.storage import sqlite as sq


def test_get_connection_enables_wal(tmp_path):
    db = tmp_path / "t.db"
    conn = sq.get_connection(db)
    mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
    assert mode.lower() == "wal"
    conn.close()


def test_run_write_commits(tmp_path):
    db = tmp_path / "t.db"
    sq.execute_script(db, "CREATE TABLE x(v INTEGER);")

    def insert(conn):
        conn.execute("INSERT INTO x(v) VALUES (42);")
        return conn.execute("SELECT COUNT(*) FROM x;").fetchone()[0]

    count = sq.run_write(db, insert)
    assert count == 1


def test_run_write_rolls_back_on_error(tmp_path):
    db = tmp_path / "t.db"
    sq.execute_script(db, "CREATE TABLE x(v INTEGER);")

    def boom(conn):
        conn.execute("INSERT INTO x(v) VALUES(1);")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        sq.run_write(db, boom)

    conn = sq.get_connection(db)
    assert conn.execute("SELECT COUNT(*) FROM x;").fetchone()[0] == 0
    conn.close()


def test_writes_are_serialized(tmp_path):
    """并发写不应产生交错损坏（WAL + 全局写锁）。"""
    db = tmp_path / "t.db"
    sq.execute_script(db, "CREATE TABLE x(v INTEGER);")

    errors = []

    def writer(n):
        try:
            for _ in range(50):
                sq.run_write(db, lambda c: c.execute("INSERT INTO x(v) VALUES(?);", (n,)))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    conn = sq.get_connection(db)
    assert conn.execute("SELECT COUNT(*) FROM x;").fetchone()[0] == 200
    conn.close()
