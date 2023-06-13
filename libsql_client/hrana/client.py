from __future__ import annotations

import asyncio
from typing import List
from typing import Optional
from typing import Set
import urllib.parse

import aiohttp

from . import proto
from .conn import HranaConn
from .conn import HranaStream
from .convert import _batch_results_from_proto
from .convert import _batch_to_proto
from .convert import _result_set_from_proto
from .convert import _stmt_to_proto
from ..client import Client
from ..client import InArgs
from ..client import InStatement
from ..client import LibsqlError
from ..client import Transaction
from ..config import _Config
from ..result import ResultSet


def _create_hrana_client(config: _Config) -> HranaClient:
    assert config.scheme in ("ws", "wss")
    url = _config_to_url(config)
    return HranaClient(url, config.auth_token)


def _config_to_url(config: _Config) -> str:
    if config.scheme == "ws" and config.tls:
        raise LibsqlError(
            "A 'ws:' URL cannot opt into TLS by using ?tls=1", "URL_INVALID"
        )
    elif config.scheme == "wss" and not config.tls:
        raise LibsqlError(
            "A 'wss:' URL cannot opt out of TLS by using ?tls=0", "URL_INVALID"
        )

    return urllib.parse.urlunparse(
        (
            config.scheme,
            config.authority,
            config.path,
            "",
            "",
            "",
        )
    )


class HranaClient(Client):
    _session: aiohttp.ClientSession
    _conn: HranaConn
    _close_tasks: Set[asyncio.Task[None]]
    _url: str
    _auth_token: Optional[str]
    _closed: bool

    def __init__(self, url: str, auth_token: Optional[str]):
        self._session = aiohttp.ClientSession()
        self._close_tasks = set()
        self._url = url
        self._auth_token = auth_token
        self._conn = self._open_conn()
        self._closed = False

    async def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet:
        with self._open_stream() as stream:
            proto_stmt = _stmt_to_proto(stmt, args)
            proto_result_fut = stream.execute(proto_stmt)
        return _result_set_from_proto(await proto_result_fut)

    async def batch(self, stmts: List[InStatement]) -> List[ResultSet]:
        with self._open_stream() as stream:
            proto_batch = _batch_to_proto(stmts)
            proto_result_fut = stream.batch(proto_batch)
        return _batch_results_from_proto(await proto_result_fut, len(stmts))

    def transaction(self) -> HranaTransaction:
        stream = self._open_stream()
        return HranaTransaction(stream)

    def _open_stream(self) -> HranaStream:
        if self._closed:
            raise LibsqlError("The client is closed", "CLIENT_CLOSED")

        if self._conn.exception is not None:
            close_task = asyncio.create_task(self._conn.close())
            self._close_tasks.add(close_task)
            close_task.add_done_callback(self._close_tasks.discard)
            self._conn = self._open_conn()

        return self._conn.open_stream()

    def _open_conn(self) -> HranaConn:
        return HranaConn(self._session, self._url, self._auth_token)

    async def close(self) -> None:
        await self._conn.close()
        if len(self._close_tasks) > 0:
            await asyncio.wait(
                list(self._close_tasks), return_when=asyncio.ALL_COMPLETED
            )
        await self._session.close()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed


class HranaTransaction(Transaction):
    _stream: HranaStream
    _begin_fut: asyncio.Future[proto.StmtResult]

    def __init__(self, stream: HranaStream):
        self._stream = stream
        self._begin_fut = stream.execute(
            {
                "sql": "BEGIN",
                "want_rows": False,
            }
        )

    async def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet:
        await self._begin_fut
        if self._stream.closed:
            raise LibsqlError("The transaction is closed", "TRANSACTION_CLOSED")

        proto_stmt = _stmt_to_proto(stmt, args)
        proto_result = await self._stream.execute(proto_stmt)
        return _result_set_from_proto(proto_result)

    async def rollback(self) -> None:
        await self._begin_fut
        if self._stream.closed:
            return

        fut = self._stream.execute(
            {
                "sql": "ROLLBACK",
                "want_rows": False,
            }
        )
        self._stream.close()
        await fut

    async def commit(self) -> None:
        await self._begin_fut
        if self._stream.closed:
            raise LibsqlError("The transaction is closed", "TRANSACTION_CLOSED")

        fut = self._stream.execute(
            {
                "sql": "COMMIT",
                "want_rows": False,
            }
        )
        self._stream.close()
        await fut

    def close(self) -> None:
        self._stream.close()

    @property
    def closed(self) -> bool:
        return self._stream.closed
