from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
import collections
import collections.abc
import functools
import logging
import os
import re
import sqlite3.dbapi2
from sqlite3.dump import _iterdump as sqlite3_iterdump
import sys
import threading
from typing import Any
from typing import Callable
from typing import cast
from typing import ClassVar
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import NoReturn
from typing import Optional
from typing import overload
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union
from weakref import WeakSet

from typing_extensions import Literal
from typing_extensions import ParamSpec
from typing_extensions import Self

from . import _reexports
from ._reexports import DatabaseError
from ._reexports import DataError
from ._reexports import IntegrityError
from ._reexports import InterfaceError
from ._reexports import InternalError
from ._reexports import NotSupportedError
from ._reexports import OperationalError
from ._reexports import ProgrammingError
from ._reexports import SQLITE_LIMIT_SQL_LENGTH
from ._utils import iter_sql_statements
from ._utils import log_obj
from ..client import LibsqlError
from ..hrana import proto
from ..hrana.convert import _value_from_proto

if sys.version_info[:2] >= (3, 11):
    from ._reexports import Blob

_logger = logging.getLogger(__name__)
_log_obj = functools.partial(log_obj, _logger)

P = ParamSpec("P")
T = TypeVar("T")


if TYPE_CHECKING:
    from sqlite3.dbapi2 import _Parameters as SqlParameters
    from sqlite3.dbapi2 import _SqliteData as SqlData
else:
    SqlParameters = Any
    SqlData = Any

PathLike = Union[str, bytes, os.PathLike]
IsolationLevel = Literal["", "DEFERRED", "EXCLUSIVE", "IMMEDIATE"]
isolation_level_set: Set[IsolationLevel] = {
    "",
    "DEFERRED",
    "EXCLUSIVE",
    "IMMEDIATE",
}

SqlNativeType = Union[None, int, float, str, bytes]
ConnectionTypes = Union[sqlite3.dbapi2.Connection, "Connection"]
ConnectFactory = Callable[..., ConnectionTypes]
CursorFactory = Callable[["Connection"], "Cursor"]
RowFactory = Callable[["Cursor", Iterable[Any]], Any]
TextFactory = Callable[[bytes], Any]
ConverterCallback = Callable[[Any], Any]

AuthorizerCallback = Callable[..., int]  # SQLITE_OK, SQLITE_DENY, or SQLITE_IGNORE
ProgressHandler = Callable[[], int]
TraceCallback = Callable[[str], None]
BackupProgressCallback = Callable[[int, int, int], None]

RawResults = proto.StmtResult
RawColumn = proto.Col
RawRow = List[proto.Value]

LEGACY_TRANSACTION_CONTROL: Literal[-1] = -1
Autocommit = Union[bool, Literal[-1]]


def _get_local_default_limits() -> Dict[int, int]:
    if sys.version_info[:2] < (3, 11):
        return {}
    # while this doesn't make much sense, let's keep it compatible and
    # allows tests to pass, such as checking if SQLITE_LIMIT_SQL_LENGTH
    # is being enforced
    with sqlite3.dbapi2.Connection(":memory:") as con:
        categories = (
            "SQLITE_LIMIT_LENGTH",
            "SQLITE_LIMIT_SQL_LENGTH",
            "SQLITE_LIMIT_COLUMN",
            "SQLITE_LIMIT_EXPR_DEPTH",
            "SQLITE_LIMIT_COMPOUND_SELECT",
            "SQLITE_LIMIT_VDBE_OP",
            "SQLITE_LIMIT_FUNCTION_ARG",
            "SQLITE_LIMIT_ATTACHED",
            "SQLITE_LIMIT_LIKE_PATTERN_LENGTH",
            "SQLITE_LIMIT_VARIABLE_NUMBER",
            "SQLITE_LIMIT_TRIGGER_DEPTH",
            "SQLITE_LIMIT_WORKER_THREADS",
        )
        defaults = {}
        for category in categories:
            cat_id = getattr(_reexports, category)
            defaults[cat_id] = con.getlimit(cat_id)
        return defaults


_default_limits = _get_local_default_limits()


def check_valid_autocommit(value: Any) -> Autocommit:
    ":meta private:"
    if value is LEGACY_TRANSACTION_CONTROL or isinstance(value, bool):
        return value
    raise ValueError(
        "autocommit must be True, False, or " "sqlite3.LEGACY_TRANSACTION_CONTROL"
    )


_callback_tracebacks: bool = False


def enable_callback_tracebacks(flag: bool) -> None:
    """See :py:func:`sqlite3.enable_callback_tracebacks`"""
    global _callback_tracebacks
    _callback_tracebacks = flag


class RawExecuteResult:
    """Iterable over pairs of ``result, error``.

    :meta private:
    """

    results: List[Optional[RawResults]]
    errors: List[Optional[BaseException]]
    _idx: int
    __slots__ = ("results", "errors", "_idx")

    def __init__(
        self,
        results: List[Optional[RawResults]],
        errors: List[Optional[BaseException]],
    ) -> None:
        assert len(results) == len(errors)
        self.results = results
        self.errors = errors
        self._idx = 0

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Tuple[Optional[RawResults], Optional[BaseException]]:
        idx = self._idx
        self._idx += 1
        try:
            return (self.results[idx], self.errors[idx])
        except IndexError:
            raise StopIteration


class Statement:
    ":meta private:"
    sql: str
    tokens: Sequence[str]
    is_dml: bool
    is_ddl: bool
    __slots__ = ("sql", "tokens", "is_dml", "is_ddl")

    _dml_statements: ClassVar[Set[str]] = {
        "INSERT",
        "UPDATE",
        "DELETE",
        "REPLACE",
    }
    _ddl_statements: ClassVar[Set[str]] = {
        "CREATE",
        "ALTER",
        "DROP",
    }

    @overload
    def __init__(self, sql: str) -> None:
        ...

    @overload
    def __init__(self, sql: str, _allow_instantiation: "Connection") -> None:
        ...

    def __init__(self, *args: object, **kwargs: object) -> None:
        # NOTE do this to enable test_dbapi.py
        # ModuleTests.test_disallow_instantiation(), it will call
        # the connection to get a Statement, but the statement itself
        # must not be instantiated directly.
        # Test instantiates the result of: type(cx("select 1"))
        con = kwargs.get("_allow_instantiation")
        if not isinstance(con, Connection):
            # matches check_disallow_instantiation() body
            tp = self.__class__
            mod = tp.__module__
            name = tp.__name__
            qualname = f"{mod}.{name}"
            msg = f"cannot create '{qualname}' instances"
            raise TypeError(msg)

        assert len(args) == 1
        sql = args[0]
        if not isinstance(sql, str):
            raise TypeError("sql must be a str")

        if "\0" in sql:
            raise ProgrammingError("the query contains a null character")
        if sys.version_info[:2] >= (3, 11):
            if len(sql) > con.getlimit(SQLITE_LIMIT_SQL_LENGTH):
                raise DataError("query string is too large")

        self.sql = sql
        # see pysqlite_statement_create()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/statement.c#L32
        # however we need to parse the statement with iter_sql_statements()
        # as we can't use sqlite3_prepare_v2()
        itr = iter(iter_sql_statements(sql))
        self.tokens = next(itr)
        assert self.tokens
        next_stmt = next(itr, None)
        if next_stmt and next_stmt[0].upper() != "END":
            msg = "You can only execute one statement at a time."
            raise ProgrammingError(msg)
        self.is_dml = self._check_is_dml(self.tokens)
        self.is_ddl = self._check_is_ddl(self.tokens)

    @classmethod
    def _check_is_dml(cls, tokens: Sequence[str]) -> bool:
        # NOTE: this matches C pysqlite_statement_create()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/statement.c#L75-L82
        # but it's not fully correct as the language allows: WITH ...
        # See https://www.sqlite.org/lang_insert.html
        # https://www.sqlite.org/lang_update.html
        # https://www.sqlite.org/lang_delete.html
        # Syntax:
        #    WITH RECURSIVE table_name ( col_name, other_col)
        #    AS NOT MATERIALIZED ( SELECT ... )
        return tokens[0].upper() in cls._dml_statements

    @classmethod
    def _check_is_ddl(cls, tokens: Sequence[str]) -> bool:
        return tokens[0].upper() in cls._ddl_statements

    @property
    def is_readonly(self) -> bool:
        # mimics sqlite3_stmt_readonly()
        return not self.is_dml and not self.is_ddl


def check_thread(fn: Callable[P, T]) -> Callable[P, T]:
    """Decorator that checks :py:class:`Connection` thread usage.

    Otherwise raises ``ProgrammingError``.

    :meta private:
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = args[0]
        assert len(args) > 0 and isinstance(self, Connection)
        self._do_check_thread()
        return fn(*args, **kwargs)

    return wrapper


def check_connection(fn: Callable[P, T]) -> Callable[P, T]:
    """Decorator that checks :py:class:`Connection` being valid.

        Otherwise raises ``ProgrammingError``.

    :meta private:
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = args[0]
        assert len(args) > 0 and isinstance(self, Connection)
        self._do_check_connection()
        return fn(*args, **kwargs)

    return wrapper


class Connection(metaclass=ABCMeta):
    """Implement :py:class:`sqlite3.Connection` for remote servers.

    :meta private:
    """

    _closed: bool
    _thread: Optional[int]
    _database: str
    _timeout: float
    _detect_types: int
    _isolation_level: Optional[IsolationLevel]
    _check_same_thread: bool
    _cached_statement_getter: Callable[[str], Statement]
    _in_transaction: bool
    _cursors: WeakSet
    _autocommit: Autocommit
    _trace_callback: Optional[TraceCallback]
    _limits: Dict[int, int]
    row_factory: Optional[RowFactory]
    text_factory: Optional[TextFactory]
    cursor_factory: CursorFactory

    def __init__(
        self,
        database: str,
        timeout: float,
        detect_types: int,
        isolation_level: Optional[IsolationLevel],
        check_same_thread: bool,
        cached_statements: int,
        autocommit: Autocommit,
    ) -> None:
        check_valid_autocommit(autocommit)  # required by test
        self._closed = False
        self._thread = threading.current_thread().ident
        self._database = database
        self._timeout = timeout
        self._detect_types = detect_types
        self._check_same_thread = check_same_thread
        self._cached_statement_getter = functools.lru_cache(cached_statements)(
            self.__call__
        )
        self._in_transaction = False
        self._cursors = WeakSet()
        self.row_factory = None
        self.text_factory = None
        self.cursor_factory = Cursor
        self._autocommit = autocommit
        self._trace_callback = None
        self._limits = _default_limits.copy()
        self.isolation_level = isolation_level
        self._raw_init()  # after this point we can call the DB
        if autocommit is False:
            self.cursor()._query_execute("BEGIN", want_rows=False)

    @abstractmethod
    def _raw_init(self) -> None:
        raise NotImplementedError()

    def __del__(self) -> None:
        self._dbg("destroying")
        if getattr(self, "_closed", False):  # __init__() may have failed
            try:
                # do not check threads as the main thread reference may be
                # gone before the executor executor thread
                self._check_same_thread = False
                self.close()
            except Exception as e:
                self._err("failed to close connection: %s", e, exc_info=e)
        self._inf("destroyed")

    def _do_check_thread(self) -> None:
        # class may not be properly initialized, as in some unit tests
        check = getattr(self, "_check_same_thread", None)
        if check:
            current_ident = threading.current_thread().ident
            if self._thread != current_ident:
                msg = (
                    "SQLite objects created in a thread can only be used "
                    "in that same thread. "
                    "The object was created in thread id "
                    "%s and this is thread id %s." % (self._thread, current_ident)
                )
                raise ProgrammingError(msg)
        elif check is None:
            raise ProgrammingError("Base Connection.__init__ not called")

    def _do_check_connection(self) -> None:
        # class may not be properly initialized, as in some unit tests
        closed = getattr(self, "_closed", None)
        if closed:
            raise ProgrammingError("Cannot operate on a closed database.")
        elif closed is None:
            raise ProgrammingError("Base Connection.__init__ not called")

    @check_connection
    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc_value: Any,
        traceback: Any,
    ) -> Literal[False]:
        # https://docs.python.org/3/library/sqlite3.html#how-to-use-the-connection-context-manager
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/connection.c#L2262
        if exc_type is None:
            self.commit()
        else:
            try:
                self.rollback()
            except Exception as e:  # TODO: is this explicitly needed?
                raise e from exc_value
        return False

    @check_thread
    @check_connection
    def __call__(self, sql: str) -> Statement:
        return Statement(sql, _allow_instantiation=self)

    def __repr__(self) -> str:
        return "%s<%s>" % (self.__class__.__name__, id(self))

    @property
    def _log_prefix(self) -> str:
        idx = self._database.find("?")
        db = self._database if idx < 1 else self._database[:idx]
        return "%r: database=%r " % (self, db)

    _log = functools.partial(_log_obj)
    _dbg = functools.partialmethod(_log, logging.DEBUG)
    _inf = functools.partialmethod(_log, logging.INFO)
    _err = functools.partialmethod(_log, logging.ERROR)

    @property
    def autocommit(self) -> Autocommit:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: Autocommit) -> None:
        check_valid_autocommit(value)
        if value is True:
            if self._in_transaction:
                self.cursor()._query_execute("COMMIT", want_rows=False)
        elif value is False:
            if not self._in_transaction:
                self.cursor()._query_execute("BEGIN", want_rows=False)
        self._autocommit = value

    @property
    @check_connection
    def total_changes(self) -> int:
        # I don't think we have a way to reliably implement it,
        # but maybe incrementing based on cursors's rowcount will do
        raise NotSupportedError()

    @property
    @check_connection
    def in_transaction(self) -> bool:
        # In C this is is retrieved from sqlite3_get_autocommit()
        # here we try to emulate this behavior, see Cursor.execute()
        # see pysqlite_connection_get_in_transaction()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/connection.c#L1691
        # see Connection._update_in_transaction()
        return self._in_transaction

    def _sqlite3_get_autocommit(self) -> bool:
        # We don't have this, so we emulate based in in_transaction,
        # which in C is implemented on top of sqlite3_get_autocommit(),
        # here we reverse that logic.
        # see pysqlite_connection_get_in_transaction()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/connection.c#L1691
        return not self._in_transaction

    @property
    @check_connection
    def isolation_level(self) -> Optional[IsolationLevel]:
        return self._isolation_level

    @isolation_level.setter
    @check_connection
    def isolation_level(self, level: Optional[IsolationLevel]) -> None:
        if level is not None:
            # required by RegressionTests.test_set_isolation_level()
            if not isinstance(level, str):
                raise TypeError("level must be a str")
            normalized_level = str(level).upper()
            if normalized_level not in isolation_level_set:
                raise ValueError(f"unknown level: {normalized_level!r}")
            level = cast(Optional[IsolationLevel], normalized_level)

        self._isolation_level = level
        if level is None:
            # Execute a COMMIT to re-enable autocommit mode
            self.commit()

    @overload
    def cursor(self, factory: CursorFactory) -> "Cursor":
        ...

    @overload
    def cursor(self) -> "Cursor":
        ...

    @check_thread
    @check_connection
    def cursor(
        self,
        *args: CursorFactory,
        **kwargs: CursorFactory,
    ) -> "Cursor":
        if args:
            factory = args[0]
        elif "factory" in kwargs:
            factory = kwargs["factory"]
        else:
            factory = self.cursor_factory

        if not callable(factory):
            raise TypeError("factory must be callable")

        cursor = factory(self)
        # While isinstance() should do,
        # RowFactoryTests.test_fake_cursor_class() tests for a hack
        # messing up with __class__
        cur_tp = type(cursor)
        if not issubclass(cur_tp, Cursor):
            raise TypeError("factory must return a cursor")
        return cursor

    def _add_cursor(self, cursor: "Cursor") -> None:
        self._cursors.add(cursor)

    def _remove_cursor(self, cursor: "Cursor") -> None:
        self._cursors.discard(cursor)

    def _get_statement(self, sql: str) -> Statement:
        return self._cached_statement_getter(sql)

    def _begin_transaction(self) -> None:
        # see begin_transaction()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/cursor.c#L476
        assert self.isolation_level is not None
        self.cursor()._query_execute(
            f"BEGIN {self.isolation_level}",
            want_rows=False,
        )

    def _begin_transaction_if_needed(self, statement: Statement) -> None:
        # _pysqlite_query_execute()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/cursor.c#L867-L877
        if (
            self._autocommit is LEGACY_TRANSACTION_CONTROL
            and self.isolation_level is not None
            and statement.is_dml
            and self._sqlite3_get_autocommit()
        ):
            self._begin_transaction()

    _begin_transaction_tokens: ClassVar[Set[str]] = {"BEGIN"}
    _end_transaction_tokens: ClassVar[Set[str]] = {"END", "COMMIT", "ROLLBACK"}

    def _update_in_transaction(self, statement: Statement) -> None:
        # we cannot query sqlite3_get_autocommit(), then emulate
        t = statement.tokens[0].upper()
        if t in self._begin_transaction_tokens:
            self._in_transaction = True
        elif t in self._end_transaction_tokens:
            self._in_transaction = False

    @check_thread
    @check_connection
    def commit(self) -> None:
        "See :py:meth:`sqlite3.Connection.commit`"
        # See pysqlite_connection_commit_impl()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/connection.c#L634
        if self._autocommit is LEGACY_TRANSACTION_CONTROL:
            if not self._sqlite3_get_autocommit():
                self.cursor()._query_execute("COMMIT", want_rows=False)
        elif self._autocommit is False:
            c = self.cursor()
            c._query_execute("COMMIT", want_rows=False)
            c._query_execute("BEGIN", want_rows=False)

    @check_thread
    @check_connection
    def rollback(self) -> None:
        "See :py:meth:`sqlite3.Connection.rollback`"
        # See pysqlite_connection_rollback_impl()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/connection.c#L668
        if self._autocommit is LEGACY_TRANSACTION_CONTROL:
            if not self._sqlite3_get_autocommit():
                self.cursor()._query_execute("ROLLBACK", want_rows=False)
        elif self._autocommit is False:
            c = self.cursor()
            c._query_execute("ROLLBACK", want_rows=False)
            c._query_execute("BEGIN", want_rows=False)

    @abstractmethod
    def _raw_close(self) -> None:
        raise NotImplementedError()

    @check_thread
    def close(self) -> None:
        "See :py:meth:`sqlite3.Connection.close`"
        if self._closed:
            return
        if self._autocommit is False and not self._sqlite3_get_autocommit():
            self.cursor()._query_execute("ROLLBACK", want_rows=False)
        self._dbg("closing %d cursors", len(self._cursors))
        for cursor in tuple(self._cursors):  # force a copy
            cursor.close()
        self._dbg("doing low level raw-close")
        self._raw_close()
        self._dbg("finished low level raw-close")
        self._closed = True

    def interrupt(self) -> None:
        "Does nothing"
        pass

    def execute(
        self,
        sql: str,
        parameters: SqlParameters = (),
    ) -> "Cursor":
        "See :py:meth:`sqlite3.Connection.execute`"
        return self.cursor().execute(sql, parameters)

    def executemany(
        self,
        sql: str,
        parameters: Iterable[SqlParameters],
    ) -> "Cursor":
        "See :py:meth:`sqlite3.Connection.executemany`"
        return self.cursor().executemany(sql, parameters)

    def executescript(self, sql_script: str) -> "Cursor":
        "See :py:meth:`sqlite3.Connection.executescript`"
        return self.cursor().executescript(sql_script)

    @check_connection
    def iterdump(self) -> Iterator[str]:
        "See :py:meth:`sqlite3.Connection.iterdump`"
        return sqlite3_iterdump(self)

    def _get_converter(self, key: str) -> Optional[ConverterCallback]:
        # See _pysqlite_get_converter()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/cursor.c#L186-L207
        upcase_key = key.upper()
        return sqlite3.converters.get(upcase_key)

    @check_thread
    @check_connection
    def set_trace_callback(
        self,
        trace_callback: Optional[TraceCallback],
    ) -> None:
        "See :py:meth:`sqlite3.Connection.set_trace_callback`"
        self._trace_callback = trace_callback

    def _trace(self, sql: str) -> None:
        if self._trace_callback is None:
            return
        try:
            self._trace_callback(sql)
        except Exception as e:
            if _callback_tracebacks is False:
                return

            if sys.version_info[:2] >= (3, 8):
                # this class is in sys.pyi but doesn't exist in runtime
                class UnraisableHookArgs:
                    exc_type = type(e)
                    exc_value = e
                    exc_traceback = e.__traceback__
                    err_msg = f"trace callback failed: {sql}"
                    object = self._trace_callback  # noqa: A003

                sys.unraisablehook(UnraisableHookArgs())  # type: ignore
            else:
                self._dbg("trace callback failed: %r", sql, exc_info=e)

    # BEGIN: methods that are not supported when operating as RPC

    if sys.version_info[:2] >= (3, 11):

        def blobopen(
            self,
            table: str,
            column: str,
            row: str,
            *,
            readonly: bool = False,
            name: str = "main",
        ) -> Blob:
            "Unsupported by libsql_client.dbapi2"
            raise NotSupportedError()

    def create_function(
        self,
        name: str,
        narg: int,
        func: Optional[Callable[..., SqlNativeType]],
        *,
        deterministic: bool = False,
    ) -> None:
        "Unsupported by libsql_client.dbapi2"
        # maybe one day support with WASM
        raise NotSupportedError()

    def create_aggregate(
        self,
        name: str,
        n_arg: int,
        aggregate_class: Optional[Callable[..., Any]],
    ) -> None:
        "Unsupported by libsql_client.dbapi2"
        # maybe one day support with WASM
        raise NotSupportedError()

    if sys.version_info[:2] >= (3, 11):

        def create_window_function(
            self,
            name: str,
            num_params: int,
            aggregate_class: Optional[Callable[..., Any]],
        ) -> None:
            "Unsupported by libsql_client.dbapi2"
            # maybe one day support with WASM
            raise NotSupportedError()

    def create_collation(
        self,
        name: str,
        _callable: Optional[Callable[[str, str], int]],
    ) -> None:
        "Unsupported by libsql_client.dbapi2"
        # maybe one day support with WASM
        raise NotSupportedError()

    def set_authorizer(
        self,
        authorizer_callback: Optional[AuthorizerCallback],
    ) -> None:
        "Unsupported by libsql_client.dbapi2"
        # maybe one day support with WASM
        raise NotSupportedError()

    def set_progress_handler(
        self,
        progress_handler: Optional[ProgressHandler],
        n: int,
    ) -> None:
        "Unsupported by libsql_client.dbapi2"
        # not likely to be supported
        raise NotSupportedError()

    def enable_load_extension(self, enabled: bool) -> None:
        "Unsupported by libsql_client.dbapi2"
        # not likely to be supported
        raise NotSupportedError()

    def load_extension(self, path: str) -> None:
        "Unsupported by libsql_client.dbapi2"
        # not likely to be supported
        raise NotSupportedError()

    def backup(
        self,
        target: "Connection",
        *,
        pages: int = -1,
        progress: Optional[BackupProgressCallback] = None,
        name: str = "main",
        sleep: float = 0.250,
    ) -> None:
        "Unsupported by libsql_client.dbapi2"
        # not likely to be supported
        raise NotSupportedError()

    if sys.version_info[:2] >= (3, 11):

        def getlimit(self, category: int) -> int:
            "Unsupported by libsql_client.dbapi2"
            try:
                return self._limits[category]
            except KeyError:
                raise ProgrammingError(f"unknown category: {category}")

        def setlimit(self, category: int, limit: int) -> int:
            "Unsupported by libsql_client.dbapi2"
            old = self.getlimit(category)
            if limit >= 0:
                self._limits[category] = limit
            return old

        def serialize(self, *, name: str = "main") -> bytes:
            # not likely to be supported
            "Unsupported by libsql_client.dbapi2"
            raise NotSupportedError()

        def deserialize(self, data: bytes, *, name: str = "main") -> None:
            # not likely to be supported
            # maybe use DatabaseError since it's declared in the original doc
            "Unsupported by libsql_client.dbapi2"
            raise NotSupportedError()

    # END: methods that are not supported when operating as RPC


CursorColumnDescription = Tuple[
    str,  # name
    None,  # type_code: sqlite also uses None in here
    None,  # display_size
    None,  # internal_size
    None,  # precision
    None,  # scale
    None,  # null_ok
]
CursorDescription = Tuple[CursorColumnDescription, ...]
RowCastMap = Tuple[Optional[ConverterCallback], ...]


def tuple_row_factory(
    cursor: "Cursor",
    cells: Iterable[Any],
) -> Tuple[Any, ...]:
    ":meta private:"
    return tuple(cells)


class Row(collections.abc.Sequence):
    """Implement :py:class:`sqlite3.Row`"""

    _description: CursorDescription
    _data: Tuple[Any, ...]
    __slots__ = ("_description", "_data")

    def __init__(self, cursor: "Cursor", data: Iterable[Any]) -> None:
        # While isinstance() should do,
        # RowFactoryTests.test_fake_cursor_class() tests for a hack
        # messing up with __class__
        cur_tp = type(cursor)
        if not issubclass(cur_tp, Cursor):
            cls_name = self.__class__.__name__
            cur_name = Cursor.__name__
            got_name = type(cursor).__name__
            msg = f"{cls_name} argument 1 must be {cur_name}, not {got_name}"
            raise TypeError(msg)

        description = getattr(cursor, "description", None)
        if not description:
            raise TypeError("cursor lacks description")

        self._description = description
        self._data = tuple(data)

    def __hash__(self) -> int:
        return hash(self._description) ^ hash(self._data)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Row):
            return False
        if self._description != other._description:
            return False
        return self._data == other._data

    def __getitem__(self, idx: Union[int, str, slice]) -> Any:
        if isinstance(idx, (int, slice)):
            return self._data[idx]
        elif isinstance(idx, str):
            idx_lower = idx.lower() if idx.isascii() else None
            # similar to equal_ignore_case() in row.c,
            # RowFactoryTests.test_sqlite_row_index_unicode checks this
            for i, d in enumerate(self._description):
                d_name = d[0]
                if d_name == idx:
                    return self._data[i]
                if not isinstance(d_name, str):
                    continue
                if idx_lower is None or not d_name.isascii():
                    continue
                if idx_lower == d_name.lower():
                    return self._data[i]

            raise IndexError("No item with that key")
        else:
            raise IndexError("Index must be int or string")

    def keys(self) -> List[str]:
        return [d[0] for d in self._description]


def check_cursor(fn: Callable[P, T]) -> Callable[P, T]:
    """Decorator that checks :py:class:`Cursor` being valid

    Otherwise raises ``ProgrammingError``.

    :meta private:
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        self = args[0]
        assert len(args) > 0 and isinstance(self, Cursor)
        self._do_check_cursor()
        return fn(*args, **kwargs)

    return wrapper


# https://github.com/python/cpython/blob/main/Modules/_sqlite/util.c#L28-L67C2
_exc_code_map = {
    "SQLITE_INTERNAL": InternalError,
    "SQL_INPUT_ERROR": OperationalError,
    "SQLITE_NOTFOUND": InternalError,
    "SQLITE_NOMEM": MemoryError,
    "SQLITE_ERROR": OperationalError,
    "SQLITE_PERM": OperationalError,
    "SQLITE_ABORT": OperationalError,
    "SQLITE_BUSY": OperationalError,
    "SQLITE_LOCKED": OperationalError,
    "SQLITE_READONLY": OperationalError,
    "SQLITE_INTERRUPT": OperationalError,
    "SQLITE_IOERR": OperationalError,
    "SQLITE_FULL": OperationalError,
    "SQLITE_CANTOPEN": OperationalError,
    "SQLITE_PROTOCOL": OperationalError,
    "SQLITE_EMPTY": OperationalError,
    "SQLITE_SCHEMA": OperationalError,
    "SQLITE_CORRUPT": DatabaseError,
    "SQLITE_TOOBIG": DataError,
    "SQLITE_CONSTRAINT": IntegrityError,
    "SQLITE_MISMATCH": IntegrityError,
    "SQLITE_MISUSE": InterfaceError,
    "SQLITE_RANGE": InterfaceError,
    "SQLITE_UNKNOWN": OperationalError,
    "PROGRAMMING_ERROR": ProgrammingError,
    "ARGS_INVALID": ProgrammingError,
    "UNICODE_ERROR": OperationalError,
    "VALUE_ERROR": OperationalError,
    "UNKNOWN": OperationalError,
}


def _raise_converted_exception(exc: BaseException) -> NoReturn:
    if not isinstance(exc, LibsqlError):
        raise exc

    code = exc.code
    cls = None
    while code and cls is None:
        cls = _exc_code_map.get(code)
        if cls is None and code.startswith("SQLITE_"):
            # fallback to mapped error, example:
            # SQLITE_CONSTRAINT_UNIQUE -> SQLITE_CONSTRAINT
            code = code.rsplit("_", 1)[0]
            if code == "SQLITE":
                break
        else:
            break

    if cls is None:
        cls = DatabaseError

    dbapi2_exc = cls(str(exc))

    sqlite_errorcode = getattr(_reexports, exc.code, None)
    dbapi2_exc.sqlite_errorname = exc.code  # type: ignore
    dbapi2_exc.sqlite_errorcode = sqlite_errorcode  # type: ignore

    raise dbapi2_exc from exc


def _get_value_bytes(value: proto.Value) -> Optional[bytes]:
    # mimics https://www.sqlite.org/c3ref/column_blob.html
    # usage in _pysqlite_fetch_one_row(), converter != Py_None branch at
    # https://github.com/python/cpython/blob/main/Modules/_sqlite/cursor.c
    v = _value_from_proto(value)
    if v is None:
        return None  # blob == NULL in their code
    if isinstance(v, bytes):
        return v
    if isinstance(v, str):
        return v.encode()
    if isinstance(v, int):
        return str(v).encode()  # ASCII rendering of the integer
    if isinstance(v, float):
        # https://www.sqlite.org/lang_expr.html#castexpr
        return str(v).encode()  # converts to text

    raise NotImplementedError(f"cannot handle value type {type(v)}")


def _get_value_converted(
    value: proto.Value,
    text_factory: Optional[TextFactory],
) -> Any:
    v = _value_from_proto(value)
    if not isinstance(v, str) or text_factory is None or text_factory is str:
        return v
    b = v.encode()
    if text_factory is bytes:
        return b
    return text_factory(b)


class Cursor(metaclass=ABCMeta):
    ":meta private:"
    _connection: Connection
    _description: Optional[CursorDescription]
    _row_cast_map: Optional[RowCastMap]
    arraysize: int
    _lastrowid: Optional[int]
    _rowcount: int
    row_factory: Optional[RowFactory]
    _rows: Optional[List[RawRow]]
    _rows_iter_idx: int
    _raw_results: Optional[RawExecuteResult]
    _closed: bool
    _locked: bool

    __slots__ = (
        "_connection",
        "_description",
        "_row_cast_map",
        "arraysize",
        "_lastrowid",
        "_rowcount",
        "row_factory",
        "_rows",
        "_rows_iter_idx",
        "_raw_results",
        "_closed",
        "_locked",
    )

    def __init__(self, connection: Connection) -> None:
        if not isinstance(connection, Connection):
            # be safe, pass CursorTests.test_cursor_wrong_class
            raise TypeError("connection must be instance of 'Connection'")

        self._connection = connection
        self._connection._add_cursor(self)
        self.arraysize = 1
        self.row_factory = None
        self._lastrowid = None  # not reset by _reset_result()
        self._closed = False
        self._locked = False
        self._reset_result()

    def __del__(self) -> None:
        self._dbg("destroying")
        if getattr(self, "_closed", False):  # __init__() may have failed
            try:
                self.close()
            except Exception as e:
                self._err("failed to close cursor: %s", e, exc_info=e)
        self._dbg("destroyed")

    def __repr__(self) -> str:
        return "%s<%s>" % (self.__class__.__name__, id(self))

    @property
    def _log_prefix(self) -> str:
        return "%r: connection=%r " % (self, self._connection)

    _log = functools.partial(_log_obj)
    _dbg = functools.partialmethod(_log, logging.DEBUG)
    _inf = functools.partialmethod(_log, logging.INFO)
    _err = functools.partialmethod(_log, logging.ERROR)

    def _do_check_cursor(self) -> None:
        # class may not be properly initialized, as in some unit tests
        closed = getattr(self, "_closed", None)
        if closed:
            raise ProgrammingError("Cannot operate on a closed cursor.")
        elif closed is None:
            raise ProgrammingError("Base Cursor.__init__ not called.")
        self._connection._do_check_thread()
        self._connection._do_check_connection()
        self._do_check_locked()

    def _do_check_locked(self) -> None:
        # class may not be properly initialized, as in some unit tests
        locked = getattr(self, "_locked", None)
        if locked:
            raise ProgrammingError("Recursive use of cursors not allowed")
        elif locked is None:
            raise ProgrammingError("Base Cursor.__init__ not called.")

    def _reset_result(self, statement: Optional[Statement] = None) -> None:
        is_dml = statement.is_dml if statement else False
        self._description = None
        self._row_cast_map = None
        self._rowcount = 0 if is_dml else -1
        self._rows = None
        self._rows_iter_idx = 0
        self._raw_results = None

    @property
    def connection(self) -> Connection:
        "Returns :py:class:`Connection`"
        return self._connection

    @property
    def description(self) -> Optional[CursorDescription]:
        "See :py:attr:`sqlite3.Cursor.description`"
        return self._description

    @property
    def lastrowid(self) -> Optional[int]:
        "See :py:attr:`sqlite3.Cursor.lastrowid`"
        return self._lastrowid

    @property
    def rowcount(self) -> int:
        "See :py:attr:`sqlite3.Cursor.rowcount`"
        return self._rowcount

    @abstractmethod
    def _raw_execute(
        self,
        sql: str,
        parameters: Iterable[SqlParameters],
        *,
        want_rows: bool = True,
    ) -> RawExecuteResult:
        # each system should implement this one
        raise NotImplementedError()

    def _apply_raw_results(self, raw_results: RawResults) -> None:
        row_id = raw_results["last_insert_rowid"]
        self._lastrowid = int(row_id) if row_id else None
        if self._rowcount >= 0:  # -1 if not is_dml
            self._rowcount += raw_results["affected_row_count"]
        self._apply_raw_results_description(raw_results)
        self._apply_raw_results_rows(raw_results)

    def _apply_raw_results_description(self, raw_results: RawResults) -> None:
        columns = raw_results["cols"]
        if self._description is not None or len(columns) == 0:
            return

        description: List[CursorColumnDescription] = []
        row_cast_map: List[Optional[ConverterCallback]] = []
        for column in columns:
            desc, cast_cb = self._get_column_description_and_cast(column)
            description.append(desc)
            row_cast_map.append(cast_cb)

        self._description = tuple(description)
        if row_cast_map.count(None) == len(row_cast_map):
            self._row_cast_map = None
        else:
            self._row_cast_map = tuple(row_cast_map)

    def _apply_raw_results_rows(self, raw_results: RawResults) -> None:
        self._rows = raw_results["rows"]
        self._rows_iter_idx = 0

    def _convert_row(self, row: RawRow) -> Any:
        row_factory = (
            self.row_factory or self.connection.row_factory or tuple_row_factory
        )

        return row_factory(self, self._get_cells(row))

    def _get_cells(self, row: RawRow) -> Iterable[Any]:
        # See _pysqlite_fetch_one_row()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/cursor.c#L319
        # https://docs.python.org/3/library/sqlite3.html#how-to-convert-sqlite-values-to-custom-python-types
        cells = []
        _row_cast_map = self._row_cast_map
        text_factory = self.connection.text_factory
        for i, v in enumerate(row):
            if _row_cast_map is not None:
                converter = _row_cast_map[i]
            else:
                converter = None

            if converter is not None:
                item = _get_value_bytes(v)
                if item:
                    converted = converter(item)
                else:
                    converted = None
            else:
                converted = _get_value_converted(v, text_factory)

            cells.append(converted)

        return cells

    _row_colname_parser_re = re.compile(
        r"^(?P<name>[^[]+)\s*\[(?P<type_name>[^]]+)\]",
    )

    _row_decltype_parser_re = re.compile(r"^(?P<type_name>[^ (]+)")

    def _get_column_description_and_cast(
        self,
        col: RawColumn,
    ) -> Tuple[CursorColumnDescription, Optional[ConverterCallback]]:
        name = col.get("name") or ""
        converter: Optional[ConverterCallback] = None

        detect_types = self.connection._detect_types
        get_converter = self.connection._get_converter

        if detect_types & sqlite3.PARSE_COLNAMES:
            m = self._row_colname_parser_re.match(name)
            if m is not None:
                name = m.group("name").strip()
                type_name = m.group("type_name")
                converter = get_converter(type_name)

        if not converter and detect_types & sqlite3.PARSE_DECLTYPES:
            decltype = col.get("decltype") or ""
            m = self._row_decltype_parser_re.match(decltype)
            if m is not None:
                type_name = m.group("type_name")
                converter = get_converter(type_name)

        return ((name, None, None, None, None, None, None), converter)

    _no_adapt_types = (int, float, str, bytes, bytearray, memoryview)

    def _adapt_param_value(self, obj: Any) -> SqlData:
        # see need_adapt(), but we can't optimize on BaseTypeAdapted
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/cursor.c#L614-L627
        try:
            t = type(obj)

            # See pysqlite_microprotocols_adapt()
            # https://github.com/python/cpython/blob/main/Modules/_sqlite/microprotocols.c
            # https://peps.python.org/pep-0246/
            proto = sqlite3.PrepareProtocol
            key = (t, proto)
            adapter = sqlite3.adapters.get(key)
            if adapter is not None:
                return adapter(obj)

            # try to have the protocol adapt this object
            adapter = getattr(proto, "___adapt__", None)
            if adapter is not None:
                adapted = adapter(obj)
                if adapted is not None:
                    return adapted

            # and finally try to have the object adapt itself
            adapter = getattr(obj, "__conform__", None)
            if adapter is not None:
                adapted = adapter(proto)
                if adapted is not None:
                    return adapted

            if obj is None:
                return obj
            if not isinstance(obj, self._no_adapt_types):
                memoryview(obj)  # TypeError if not bytes-like
                return obj

            return obj
        except TypeError:
            raise ProgrammingError("can't adapt")

    def _adapt_params(self, parameters: SqlParameters) -> SqlParameters:
        if isinstance(parameters, collections.abc.Mapping):
            return {k: self._adapt_param_value(v) for k, v in parameters.items()}
        elif parameters:
            # SupportsLenAndGetItem is only __len__ + __getitem__, no __iter__
            # so create the list manually :-(
            return [
                self._adapt_param_value(parameters[i]) for i in range(len(parameters))
            ]
        return parameters

    @check_cursor
    def _query_execute(
        self,
        sql: str,
        parameters: Iterable[SqlParameters] = ((),),
        *,
        multiple: bool = False,
        want_rows: bool = True,
    ) -> Self:
        if not isinstance(sql, str):
            # required by test CursorTests.test_execute_many_wrong_sql_arg()
            raise TypeError("sql must be a str")
        if not sql:
            return self

        params = tuple(self._adapt_params(p) for p in parameters)

        # See _pysqlite_query_execute()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/cursor.c#L781
        try:
            self._dbg("execute: %r, %r", sql, params)
            statement = self._connection._get_statement(sql)
            if multiple and statement.is_readonly:
                msg = "executemany() can only execute DML statements."
                raise ProgrammingError(msg)
            self._reset_result(statement)
            self._connection._begin_transaction_if_needed(statement)

            # Locking is not that meaningful with libsql_client since
            # we don't callback to python during execution as done by
            # create_function() and the likes. Nevertheless keep it here
            # NOTE: _begin_transaction_if_needed() will call this function
            # so do not lock before this point!
            self._locked = True
            self._raw_results = self._raw_execute(
                sql,
                params,
                want_rows=want_rows,
            )
            for result, error in self._raw_results:
                if error is not None:
                    self._rowcount = -1  # as per cursor.c
                    _raise_converted_exception(error)
                assert result is not None
                self._apply_raw_results(result)

            self._connection._update_in_transaction(statement)
            return self
        finally:
            self._locked = False

    def execute(
        self,
        sql: str,
        parameters: SqlParameters = (),
    ) -> Self:
        "See :py:meth:`sqlite3.Cursor.execute`"
        if not isinstance(parameters, collections.abc.Mapping) and not (
            hasattr(parameters, "__len__") and hasattr(parameters, "__getitem__")
        ):
            raise ProgrammingError("parameters are of unsupported type")
        self._query_execute(sql, (parameters,), multiple=False, want_rows=True)
        return self

    def executemany(
        self,
        sql: str,
        parameters: Iterable[SqlParameters],
    ) -> Self:
        "See :py:meth:`sqlite3.Cursor.executemany`"
        self._query_execute(sql, parameters, multiple=True, want_rows=False)
        return self

    @abstractmethod
    def _raw_execute_script(self, sql_script: str) -> None:
        raise NotImplementedError()

    @check_cursor
    def executescript(self, sql_script: str) -> Self:
        "See :py:meth:`sqlite3.Cursor.executescript`"
        # See pysqlite_cursor_executescript_impl()
        # https://github.com/python/cpython/blob/main/Modules/_sqlite/cursor.c#L1046-L1060
        if self._connection._autocommit is LEGACY_TRANSACTION_CONTROL:
            if not self._connection._sqlite3_get_autocommit():
                self._query_execute("COMMIT", want_rows=False)

        self._raw_execute_script(sql_script)
        return self

    def __iter__(self) -> Self:
        return self

    @check_cursor
    def __next__(self) -> Any:
        # TODO: we're just doing a single raw_results in a batch,
        # seems to match the behavior when calling executemany() ("multiple")
        if self._rows is None:
            raise StopIteration

        idx = self._rows_iter_idx
        self._rows_iter_idx += 1
        try:
            row = self._rows[idx]
        except IndexError:
            raise StopIteration

        try:
            self._locked = True
            return self._convert_row(row)
        finally:
            self._locked = False

    def fetchone(self) -> Any:
        "See :py:meth:`sqlite3.Cursor.fetchone`"
        return next(self, None)

    def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        "See :py:meth:`sqlite3.Cursor.fetchmany`"
        if size is None or size < 1:
            assert self.arraysize > 0
            size = self.arraysize

        result = []
        for i, row in enumerate(self):
            if i == size:
                break
            result.append(row)

        return result

    def fetchall(self) -> List[Any]:
        "See :py:meth:`sqlite3.Cursor.fetchall`"
        return list(self)

    @abstractmethod
    def _raw_close(self) -> None:
        raise NotImplementedError()

    def close(self) -> None:
        "See :py:meth:`sqlite3.Cursor.close`"
        # class may not be properly initialized, as in some unit tests
        closed = getattr(self, "_closed", None)
        if closed:
            return
        self._do_check_locked()
        self._connection._do_check_thread()
        self._connection._do_check_connection()
        self.connection._remove_cursor(self)
        self._closed = True
        self._reset_result()
        self._raw_close()

    def setinputsizes(self, sizes: Any) -> None:
        "Does nothing"
        # Required by the DB-API. Does nothing in sqlite3.
        pass

    def setoutputsize(self, size: int, column: Any = None) -> None:
        "Does nothing"
        # Required by the DB-API. Does nothing in sqlite3.
        pass
