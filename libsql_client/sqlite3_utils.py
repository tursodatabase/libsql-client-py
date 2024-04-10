from __future__ import annotations

import math
import sqlite3
from typing import Any
from typing import cast
from typing import List
from typing import Optional

from .client import _normalize_value
from .client import Client
from .client import InArgs
from .client import InStatement
from .client import InValue
from .client import LibsqlError
from .client import Statement
from .client import Transaction
from .config import _Config
from .result import ResultSet
from .result import Row


def _create_sqlite3_client(config: _Config) -> Sqlite3Client:
    assert config.scheme == "file"
    if config.authority not in ("", "localhost"):
        raise LibsqlError(
            f"Invalid authority in file URL: {config.authority!r}", "URL_INVALID"
        )

    client = Sqlite3Client(config.path)
    db = client._connect()
    try:
        _execute_stmt(db, "SELECT 1 AS check_that_the_database_can_be_opened")
    finally:
        db.close()

    return client


class Sqlite3Client(Client):
    _path: str
    _closed: bool

    def __init__(self, path: str):
        self._path = path
        self._closed = False

    async def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet:
        db = self._connect()
        try:
            return _execute_stmt(db, stmt, args)
        finally:
            db.close()

    async def batch(self, stmts: List[InStatement]) -> List[ResultSet]:
        db = self._connect()
        try:
            _execute_stmt(db, "BEGIN")
            result_sets = []
            for stmt in stmts:
                result_set = _execute_stmt(db, stmt)
                result_sets.append(result_set)
            _execute_stmt(db, "COMMIT")
            return result_sets
        finally:
            db.close()

    def transaction(self) -> Sqlite3Transaction:
        db = self._connect()
        try:
            _execute_stmt(db, "BEGIN")
            return Sqlite3Transaction(db)
        except Exception:
            db.close()
            raise

    async def close(self) -> None:
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def _connect(self) -> sqlite3.Connection:
        if self._closed:
            raise LibsqlError("The client was closed", "CLIENT_CLOSED")
        return sqlite3.connect(
            self._path,
            isolation_level=None,
            check_same_thread=False,
            timeout=0,
        )


class Sqlite3Transaction(Transaction):
    database: Optional[sqlite3.Connection]

    def __init__(self, database: sqlite3.Connection):
        self.database = database

    async def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet:
        db = self._connection()
        return _execute_stmt(db, stmt, args)

    async def rollback(self) -> None:
        if self.database is None:
            return
        _execute_stmt(self.database, "ROLLBACK")
        self.close()

    async def commit(self) -> None:
        db = self._connection()
        _execute_stmt(db, "COMMIT")
        self.close()

    def close(self) -> None:
        db, self.database = self.database, None
        if db is not None:
            db.close()

    @property
    def closed(self) -> bool:
        return self.database is None

    def _connection(self) -> sqlite3.Connection:
        if self.database is None:
            raise LibsqlError("The transaction was closed", "TRANSACTION_CLOSED")
        return self.database


def _execute_stmt(
    db: sqlite3.Connection, in_stmt: InStatement, in_args: InArgs = None
) -> ResultSet:
    stmt = Statement.convert(in_stmt, in_args)
    sql_args: Any
    if stmt.args is None:
        sql_args = ()
    elif isinstance(stmt.args, dict):
        sql_args = {
            _strip_arg_name(key): _value_to_sql(value)
            for key, value in stmt.args.items()
        }
    else:
        sql_args = [_value_to_sql(value) for value in stmt.args]

    cursor = None
    try:
        cursor = db.execute(stmt.sql, sql_args)
        sql_rows = cursor.fetchall()
    except sqlite3.Error as e:
        if cursor is not None:
            cursor.close()

        if hasattr(e, "sqlite_errorname"):
            code = e.sqlite_errorname
        else:
            code = "SQLITE"
        raise LibsqlError(str(e), code) from e

    try:
        columns = tuple(cast(str, desc[0]) for desc in cursor.description or ())
        column_idxs = {column: idx for idx, column in enumerate(columns)}
        rows = [Row(column_idxs, sql_row) for sql_row in sql_rows]
        rows_affected = cursor.rowcount
        last_insert_rowid = cursor.lastrowid
        return ResultSet(columns, rows, rows_affected, last_insert_rowid)
    finally:
        cursor.close()


def _strip_arg_name(name: str) -> str:
    if len(name) >= 1 and name[0] in (":", "$", "@"):
        return name[1:]
    return name


def _value_to_sql(value: InValue) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Only finite floats (not Infinity or NaN) are supported")
    return _normalize_value(value)
