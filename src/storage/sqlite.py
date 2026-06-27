"""SQLite 连接管理：WAL 模式 + 外键 + 应用层串行写锁。"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Callable, TypeVar

T = TypeVar("T")

_write_lock = threading.Lock()


def get_connection(db_path: Path) -> sqlite3.Connection:
    """返回启用 WAL + 外键的连接。调用方负责 close。"""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def run_write(db_path: Path, fn: Callable[[sqlite3.Connection], T]) -> T:
    """串行化执行写事务：加全局写锁，开事务，执行 fn，提交/回滚。

    注：若 fn 内部已自行结束事务（例如 sqlite3.Connection.executescript 会先
    隐式 COMMIT），则通过 conn.in_transaction 判定后跳过显式 COMMIT/ROLLBACK，
    避免 "no transaction is active" 错误。
    """
    with _write_lock:
        conn = get_connection(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE;")
            result = fn(conn)
            if conn.in_transaction:
                conn.execute("COMMIT;")
            return result
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK;")
            raise
        finally:
            conn.close()


def execute_script(db_path: Path, script: str) -> None:
    """执行建表 DDL 脚本（一次性，用于初始化 schema）。"""
    def _do(conn: sqlite3.Connection) -> None:
        conn.executescript(script)

    run_write(db_path, _do)
