from __future__ import annotations

import sqlite3.dbapi2
from typing import Any
from typing import Mapping
from typing import Optional
from urllib.parse import urlparse

from ._reexports import *
from .hrana import ConnectionHrana as Connection
from .hrana import CursorHrana as Cursor
from .types import ConnectFactory
from .types import Connection as BaseConnection
from .types import ConnectionTypes
from .types import Cursor as BaseCursor
from .types import enable_callback_tracebacks
from .types import IsolationLevel
from .types import LEGACY_TRANSACTION_CONTROL
from .types import PathLike
from .types import Row

"""
This module implements `Python Database API Specification v2.0
<https://peps.python.org/pep-0249/>`_
mimicking as much as possible :py:mod:`sqlite3` in order to provide a
drop-in replacement. Whenever explicitly undocumented, please refer its
documentation.

"""
__docformat__ = "reStructuredText en"


_connection_handlers: Mapping[str, ConnectFactory] = {
    "file": sqlite3.dbapi2.connect,
    "libsql": Connection,
    "ws": Connection,
    "wss": Connection,
}

_uri_forced_databases_prefixes = ("libsql://", "ws://", "wss://")


def connect(
    database: PathLike,
    timeout: float = 5.0,
    detect_types: int = 0,
    isolation_level: Optional[IsolationLevel] = "",
    check_same_thread: bool = True,
    factory: Optional[ConnectFactory] = None,
    cached_statements: int = 128,
    uri: bool = False,
    **kwargs: Any,
) -> ConnectionTypes:
    """
    Open a connection to an SQLite (local) or sqld (remote) database.

    In addition to :py:func:`sqlite3.connect`, this function allows
    connecting to remote servers using the following protocols:

    - ``libsql://`` alias for ``wss://``
    - ``ws://`` insecure web socket
    - ``wss://`` secure web socket

    If local files or ``:memory:`` is used, then the standard
    :py:class:`sqlite3.Connection` is returned.

    For remote URL, :py:class:`libsql_client.dbapi2.hrana.ConnectionHrana` is
    returned, it should match the behavior of
    :py:class:`sqlite3.Connection` whenever possible.
    """
    if not uri and isinstance(database, str):
        for prefix in _uri_forced_databases_prefixes:
            if database.startswith(prefix):
                uri = True
                break

    handler: ConnectFactory
    if not uri:
        handler = sqlite3.dbapi2.connect
        if factory is not None:
            kwargs["factory"] = factory
    else:
        if factory is not None:
            handler = factory
        else:
            assert isinstance(database, str)
            u = urlparse(database)
            try:
                handler = _connection_handlers[u.scheme]
            except KeyError as e:
                raise ValueError(f"unsupported uri scheme: {u.scheme}") from e

    return handler(
        database,
        timeout=timeout,
        detect_types=detect_types,
        isolation_level=isolation_level,
        check_same_thread=check_same_thread,
        cached_statements=cached_statements,
        uri=uri,
        **kwargs,
    )
