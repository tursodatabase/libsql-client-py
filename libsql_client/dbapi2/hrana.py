from __future__ import annotations

import asyncio
import collections.abc
import sqlite3.dbapi2
import sys
from typing import Awaitable
from typing import Callable
from typing import Iterable
from typing import List
from typing import Optional
from typing import overload
from typing import Tuple
from typing import Type
from typing import TYPE_CHECKING
from typing import TypeVar

import aiohttp
from typing_extensions import ParamSpec

from ._async_executor import AsyncExecutor
from .types import Autocommit
from .types import Connection
from .types import Cursor
from .types import IsolationLevel
from .types import LEGACY_TRANSACTION_CONTROL
from .types import OperationalError
from .types import RawExecuteResult
from .types import SqlParameters
from ..client import LibsqlError
from ..config import _expand_config
from ..hrana.client import _config_to_url
from ..hrana.conn import HranaConn
from ..hrana.conn import HranaStream
from ..hrana.convert import _error_from_proto
from ..hrana.convert import _value_to_proto
from ..hrana.proto import Batch
from ..hrana.proto import BatchResult
from ..hrana.proto import BatchStep
from ..hrana.proto import Error as ErrorResult
from ..hrana.proto import Stmt
from ..hrana.proto import StmtResult


# TODO: we're creating a whole sync client for each connection, but we
# could create a single thread for all connections of the same URL,
# with a single HranaClient/HranaConn, and just get a HranaStream.
# But this requires us to change ClientSync and other stuff, will
# do it later.

P = ParamSpec("P")
T = TypeVar("T")


def _create_hrana_connection(
    session: aiohttp.ClientSession,
    url: str,
    auth_token: str = None,
) -> HranaConn:
    config = _expand_config(url, auth_token=auth_token, tls=None)

    try:
        if config.scheme not in ("ws", "wss"):
            raise LibsqlError(
                "Only 'libsql', 'ws' and 'wss' URLs are supported,"
                f" got {config.scheme!r}",
                "URL_INVALID",
            )
        url = _config_to_url(config)
    except LibsqlError as e:
        if e.code == "URL_INVALID":
            sqlite_err = OperationalError(str(e))
            sqlite_err.sqlite_errorcode = 14  # type: ignore
            sqlite_err.sqlite_errorname = "SQLITE_CANTOPEN"  # type: ignore
            raise sqlite_err
        raise

    return HranaConn(session, url, config.auth_token)


def _conv_stmt_plain_to_stored(stmt: Stmt, sql_id: int) -> Stmt:
    del stmt["sql"]
    stmt["sql_id"] = sql_id
    return stmt


def _conv_stmt(
    sql: str,
    parameters: SqlParameters,
    want_rows: bool,
) -> Stmt:
    stmt: Stmt = {
        "sql": sql,
        "want_rows": want_rows,
    }
    if isinstance(parameters, collections.abc.Mapping):
        stmt["named_args"] = [
            {"name": k, "value": _value_to_proto(v)} for k, v in parameters.items()
        ]
    elif parameters:
        # SupportsLenAndGetItem is only __len__ + __getitem__, no __iter__
        # so create the list manually :-(
        args = []
        for i in range(len(parameters)):
            args.append(_value_to_proto(parameters[i]))
        stmt["args"] = args

    return stmt


def _conv_stmts(
    sql: str,
    parameters: Iterable[SqlParameters],
    want_rows: bool,
) -> List[Stmt]:
    return [_conv_stmt(sql, p, want_rows) for p in parameters]


_aiohttp_error_map = (
    (aiohttp.InvalidURL, "SQLITE_CANTOPEN"),
    (aiohttp.ClientConnectionError, "SQLITE_CANTOPEN"),
    (aiohttp.ClientResponseError, "SQLITE_CANTOPEN"),
    (aiohttp.ClientPayloadError, "SQLITE_IOERR"),
)


def _get_aiohttp_client_error_code(
    error: aiohttp.ClientError,
) -> Tuple[int, str]:
    for error_cls, error_name in _aiohttp_error_map:
        if isinstance(error, error_cls):
            error_code = getattr(sqlite3.dbapi2, error_name)
            return error_code, error_name
    return 0, ""


def _conv_stmt_result(
    result: Optional[StmtResult],
    error: Optional[BaseException],
) -> RawExecuteResult:
    if isinstance(error, aiohttp.ClientError):
        code, name = _get_aiohttp_client_error_code(error)
        error = OperationalError(str(error))
        if code:
            error.sqlite_errorcode = code  # type: ignore
            error.sqlite_errorname = name  # type: ignore

    return RawExecuteResult([result], [error])


def _conv_batch(stmts: List[Stmt]) -> Batch:
    steps: List[BatchStep] = []

    for i, stmt in enumerate(stmts):
        if i == 0:
            steps.append({"stmt": stmt})
        else:
            steps.append(
                {
                    "condition": {"type": "ok", "step": i - 1},
                    "stmt": stmt,
                }
            )

    return {"steps": steps}


def _conv_batch_result(resp: BatchResult) -> RawExecuteResult:
    def conv_err(e: Optional[ErrorResult]) -> Optional[BaseException]:
        if e is None:
            return None
        return _error_from_proto(e)

    errors = [conv_err(e) for e in resp["step_errors"]]
    return RawExecuteResult(resp["step_results"], errors)


if TYPE_CHECKING:
    if sys.version_info[:2] >= (3, 9):

        @overload
        def run_in_executor(
            fn: Callable[P, Awaitable[asyncio.Future[T]]],
        ) -> Callable[P, T]:
            ...

    @overload
    def run_in_executor(fn: Callable[P, Awaitable[T]]) -> Callable[P, T]:
        ...

    @overload
    def run_in_executor(fn: Callable[P, T]) -> Callable[P, T]:
        ...


def run_in_executor(fn: Callable[P, T]) -> Callable[P, T]:
    """ConnectionHrana method decorator that runs code in the executor thread.

    This will execute the decorated method body inside the
    :py:class:`AsyncExecutor` thread by doing a ``AsyncExecutor.submit()``
    and then ``future.result(timeout)``.

    The method itself will block until the executor runs.

    :meta private:
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = args[0]
        assert isinstance(self, ConnectionHrana)
        assert self._executor is not None

        future = self._executor.submit(fn, *args, **kwargs)

        return future.result(timeout=self._timeout)

    return wrapper


class ConnectionHrana(Connection):
    """Implement :py:class:`sqlite3.Connection` for remote servers
    using the `Hrana Protocol
    <https://github.com/libsql/sqld/blob/main/docs/HRANA_2_SPEC.md>`_.
    """

    _executor: Optional[AsyncExecutor]  # TODO: share
    _session: Optional[aiohttp.ClientSession]  # TODO: share (per url)
    _conn: Optional[HranaConn]  # TODO: share (per url)
    _stream: Optional[HranaStream]
    cursor_factory: Type["CursorHrana"]
    auth_token: str = ""

    def __init__(
        self,
        database: str,
        auth_token: str = None,
        timeout: float = 5.0,
        detect_types: int = 0,
        isolation_level: Optional[IsolationLevel] = "",
        check_same_thread: bool = True,
        cached_statements: int = 128,
        uri: bool = False,
        autocommit: Autocommit = LEGACY_TRANSACTION_CONTROL,
    ) -> None:
        assert uri
        self.auth_token = auth_token
        super().__init__(
            database=database,
            timeout=timeout,
            detect_types=detect_types,
            isolation_level=isolation_level,
            check_same_thread=check_same_thread,
            cached_statements=cached_statements,
            autocommit=autocommit,
        )

    def _raw_init(self) -> None:
        self.cursor_factory = CursorHrana
        try:
            self._executor = self._acquire_executor()
            self._session = self._acquire_session()
            self._conn = self._acquire_connection(self._database)
            self._stream = self._create_stream()
        except Exception:
            self._raw_close()
            raise

    def _acquire_executor(self) -> AsyncExecutor:
        return AsyncExecutor()  # TODO: share

    def _dispose_executor(self, executor: AsyncExecutor) -> None:
        executor.shutdown()  # TODO: share

    @run_in_executor
    def _acquire_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession()  # TODO: share

    @run_in_executor
    async def _dispose_session(self, session: aiohttp.ClientSession) -> None:
        await session.close()  # TODO: share

    @run_in_executor
    async def _acquire_connection(self, url: str) -> HranaConn:
        assert self._session is not None
        # TODO: share (per url)
        conn = _create_hrana_connection(self._session, url, self.auth_token)
        try:
            await conn.wait_connected()
            return conn
        except Exception as error:
            await conn.close()
            exc = _conv_stmt_result(None, error).errors[0]
            assert exc is not None  # make mypy happy
            raise exc

    @run_in_executor
    async def _dispose_connection(self, conn: HranaConn) -> None:
        await conn.close()  # TODO: share (per url)

    @run_in_executor
    def _create_stream(self) -> HranaStream:
        assert self._conn is not None
        stream = self._conn.open_stream()
        self._inf("created stream: %s", stream)
        return stream

    @run_in_executor
    def _destroy_stream(self, stream: HranaStream) -> None:
        self._inf("closing stream: %s", stream)
        stream.close()

    @run_in_executor
    def _raw_execute(self, stmt: Stmt) -> asyncio.Future[StmtResult]:
        assert self._stream is not None
        return self._stream.execute(stmt)

    @run_in_executor
    def _raw_store_sql(self, sql: str) -> int:
        assert self._conn is not None
        return self._conn.store_sql(sql)

    @run_in_executor
    def _raw_close_sql(self, sql_id: int) -> None:
        assert self._conn is not None
        return self._conn.close_sql(sql_id)

    @run_in_executor
    def _raw_execute_script(self, sql_script: str) -> asyncio.Future[None]:
        assert self._stream is not None
        return self._stream.sequence(sql_script)

    @run_in_executor
    def _raw_batch(self, batch: Batch) -> asyncio.Future[BatchResult]:
        assert self._stream is not None
        return self._stream.batch(batch)

    def _raw_close(self) -> None:
        # use of getattr as the object may fail init
        stream = getattr(self, "_stream", None)
        if stream is not None:
            self._destroy_stream(stream)
            self._stream = None

        conn = getattr(self, "_conn", None)
        if conn is not None:
            self._dispose_connection(conn)
            self._conn = None

        session = getattr(self, "_session", None)
        if session is not None:
            self._dispose_session(session)
            self._session = None

        executor = getattr(self, "_executor", None)
        if executor is not None:
            self._dispose_executor(executor)
            self._executor = None


class CursorHrana(Cursor):
    """Implement :py:class:`sqlite3.Cursor` for remote servers
    using the `Hrana Protocol
    <https://github.com/libsql/sqld/blob/main/docs/HRANA_2_SPEC.md>`_.
    """

    connection: ConnectionHrana

    def _raw_execute_one(self, stmt: Stmt) -> RawExecuteResult:
        try:
            if self.connection._trace_callback is not None:
                self.connection._trace(stmt["sql"])
            result = self.connection._raw_execute(stmt)
            return _conv_stmt_result(result, None)
        except Exception as error:
            return _conv_stmt_result(None, error)

    def _raw_execute_multiple(self, stmts: List[Stmt]) -> RawExecuteResult:
        sql_id = None
        try:
            sql = stmts[0]["sql"]
            sql_id = self.connection._raw_store_sql(sql)
            assert sql_id is not None  # keep mypy happy

            stored_stmts: List[Stmt] = [
                _conv_stmt_plain_to_stored(stmt, sql_id) for stmt in stmts
            ]

            result = self.connection._raw_batch(_conv_batch(stored_stmts))
            if self.connection._trace_callback is not None:
                for stmt in stmts:
                    self.connection._trace(sql)
            return _conv_batch_result(result)
        except Exception as error:
            return _conv_stmt_result(None, error)
        finally:
            if sql_id is not None:
                try:
                    self.connection._raw_close_sql(sql_id)
                except Exception:
                    pass

    def _raw_execute(
        self,
        sql: str,
        parameters: Iterable[SqlParameters],
        *,
        want_rows: bool = True,
    ) -> RawExecuteResult:
        stmts = _conv_stmts(sql, parameters, want_rows)
        if len(stmts) == 1:
            return self._raw_execute_one(stmts[0])
        elif len(stmts) > 1:
            return self._raw_execute_multiple(stmts)
        else:
            return RawExecuteResult([], [])

    def _raw_execute_script(self, sql_script: str) -> None:
        self.connection._raw_execute_script(sql_script)

    def _raw_close(self) -> None:
        pass  # TODO: should we do anything specific?
