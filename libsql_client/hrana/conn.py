from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
from typing import Any
from typing import Callable
from typing import cast
from typing import Dict
from typing import Optional
from typing import TypeVar
from typing import Union

import aiohttp

from . import proto
from .convert import _error_from_proto
from .id_alloc import IdAlloc
from ..client import LibsqlError


@dataclass
class _ResponseState:
    type: str
    future: asyncio.Future[proto.Response]


@dataclass
class _StreamState:
    stream_id: int
    closed: Optional[BaseException]


class HranaConn:
    _connect_task: asyncio.Task[aiohttp.ClientWebSocketResponse]
    _receive_task: Optional[asyncio.Task[None]]
    _send_task: Optional[asyncio.Task[None]]

    _socket: Optional[aiohttp.ClientWebSocketResponse]
    _send_msg_queue: asyncio.Queue[str]

    _recvd_hello: bool
    _finished_handshake: asyncio.Event
    _response_map: Dict[int, _ResponseState]
    _request_id_alloc: IdAlloc
    _stream_id_alloc: IdAlloc
    _sql_id_alloc: IdAlloc

    exception: Optional[BaseException]

    def __init__(
        self, session: aiohttp.ClientSession, url: str, auth_token: Optional[str] = None
    ):
        self._connect_task = asyncio.create_task(self._do_connect(session, url))
        self._connect_task.add_done_callback(self._done_connect)
        self._receive_task = None
        self._send_task = None

        self._socket = None
        self._send_msg_queue = asyncio.Queue()

        self._recvd_hello = False
        self._finished_handshake = asyncio.Event()
        self._response_map = {}
        self._request_id_alloc = IdAlloc()
        self._stream_id_alloc = IdAlloc()
        self._sql_id_alloc = IdAlloc()

        self.exception = None

        self._send({"type": "hello", "jwt": auth_token})

    async def wait_connected(self) -> None:
        await self._finished_handshake.wait()
        if self.exception:
            raise self.exception

    async def _do_connect(
        self, session: aiohttp.ClientSession, url: str
    ) -> aiohttp.ClientWebSocketResponse:
        return await session.ws_connect(
            url,
            protocols=["hrana2"],
            autoclose=False,
            autoping=True,
        )

    def _done_connect(
        self, task: asyncio.Task[aiohttp.ClientWebSocketResponse]
    ) -> None:
        e: Optional[BaseException]
        if task.cancelled():
            e = LibsqlError("The connect task was cancelled", "CLIENT_CLOSED")
        else:
            e = task.exception()
        if e is not None:
            self._set_exception(e)

        if self.exception is not None:
            return

        socket = task.result()
        receive_task = asyncio.create_task(self._do_receive(socket))
        send_task = asyncio.create_task(self._do_send(socket))

        receive_task.add_done_callback(self._done_receive)
        send_task.add_done_callback(self._done_send)

        self._socket = socket
        self._receive_task = receive_task
        self._send_task = send_task

    async def _do_receive(self, socket: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            msg = await socket.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    self._receive(msg.data)
                except Exception:
                    await socket.close(
                        code=3007, message="Could not handle message".encode()
                    )
                    raise
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await socket.close(
                    code=3003, message="Only text messages are accepted".encode()
                )
                raise LibsqlError(
                    "Received a binary WebSocket message", "HRANA_PROTO_ERROR"
                )
            elif msg.type == aiohttp.WSMsgType.PING:
                await socket.pong(msg.data)
            elif msg.type == aiohttp.WSMsgType.PONG:
                pass
            elif msg.type == aiohttp.WSMsgType.CLOSE:
                code = cast(aiohttp.WSCloseCode, msg.data)
                reason = cast(str, msg.extra)
                raise LibsqlError(
                    f"WebSocket was closed with code {code}: {reason!r}",
                    "HRANA_WEBSOCKET_ERROR",
                )
            elif msg.type == aiohttp.WSMsgType.CLOSING:
                pass
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                raise LibsqlError("WebSocket was closed", "HRANA_WEBSOCKET_ERROR")
            elif msg.type == aiohttp.WSMsgType.ERROR:
                raise msg.data
            else:
                raise LibsqlError(
                    f"Received unexpected WebSocket message {msg.type!r}",
                    "HRANA_PROTO_ERROR",
                )

    async def _do_send(self, socket: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            msg_str = await self._send_msg_queue.get()
            await socket.send_str(msg_str)

    def _done_receive(self, task: asyncio.Task[None]) -> None:
        e: Optional[BaseException]
        if task.cancelled():
            e = LibsqlError("The receive task was cancelled", "CLIENT_CLOSED")
        else:
            e = task.exception()

        if e is not None:
            if self._send_task is not None:
                self._send_task.cancel()
            self._set_exception(e)

    def _done_send(self, task: asyncio.Task[None]) -> None:
        e: Optional[BaseException]
        if task.cancelled():
            e = LibsqlError("The send task was cancelled", "CLIENT_CLOSED")
        else:
            e = task.exception()

        if e is not None:
            self._set_exception(e)

        if self._receive_task is not None:
            self._receive_task.cancel()

    def _send(self, msg: proto.ClientMsg) -> None:
        assert self.exception is None
        self._send_msg_queue.put_nowait(json.dumps(msg))

    def _set_exception(self, e: BaseException) -> None:
        if self.exception is not None:
            return
        self.exception = e
        self._finished_handshake.set()

        for task in (self._connect_task, self._receive_task, self._send_task):
            if task is not None:
                task.cancel()

        for request_id, response_state in self._response_map.items():
            response_state.future.set_exception(e)
            self._request_id_alloc.free(request_id)
        self._response_map.clear()

    def send_request(self, request: proto.Request) -> asyncio.Future[proto.Response]:
        future = asyncio.get_running_loop().create_future()

        if self.exception is not None:
            future.set_exception(self.exception)
            return future

        request_id = self._request_id_alloc.alloc()
        self._response_map[request_id] = _ResponseState(request["type"], future)
        self._send({"type": "request", "request_id": request_id, "request": request})
        return future

    def _receive(self, text: str) -> None:
        if self.exception is not None:
            return

        try:
            msg = json.loads(text)
        except ValueError as e:
            raise LibsqlError(
                "Server message is not valid JSON", "HRANA_PROTO_ERROR"
            ) from e

        if msg["type"] in ("hello_ok", "hello_error"):
            if self._recvd_hello:
                raise LibsqlError(
                    "Received a duplicated error response", "HRANA_PROTO_ERROR"
                )
            self._recvd_hello = True
            self._finished_handshake.set()

            if msg["type"] == "hello_error":
                raise _error_from_proto(msg["error"])
            return
        elif not self._recvd_hello:
            raise LibsqlError(
                "Received a non-hello message before hello response",
                "HRANA_PROTO_ERROR",
            )

        if msg["type"] == "response_ok":
            request_id = int(msg["request_id"])
            response_state = self._response_map.pop(request_id, None)
            if response_state is None:
                raise LibsqlError(
                    "Received unexpected OK response", "HRANA_PROTO_ERROR"
                )
            self._request_id_alloc.free(request_id)

            try:
                if response_state.type != msg["response"]["type"]:
                    raise LibsqlError(
                        "Received unexpected type of response", "HRANA_PROTO_ERROR"
                    )
                response_state.future.set_result(msg["response"])
            except Exception as e:
                response_state.future.set_exception(e)
                raise
        elif msg["type"] == "response_error":
            request_id = int(msg["request_id"])
            response_state = self._response_map.pop(request_id, None)
            if response_state is None:
                raise LibsqlError(
                    "Received unexpected error response", "HRANA_PROTO_ERROR"
                )
            self._request_id_alloc.free(request_id)

            response_state.future.set_exception(_error_from_proto(msg["error"]))
        else:
            raise LibsqlError("Received unexpected message type", "HRANA_PROTO_ERROR")

    def open_stream(self) -> HranaStream:
        stream_id = self._stream_id_alloc.alloc()
        stream_state = _StreamState(stream_id, None)

        def open_done(fut: asyncio.Future[proto.Response]) -> None:
            e: Optional[BaseException]
            if fut.cancelled():
                e = asyncio.CancelledError("Stream opening was cancelled")
            else:
                e = fut.exception()
            if e is not None:
                self._close_stream(stream_state, e)

        open_fut = self.send_request(
            {
                "type": "open_stream",
                "stream_id": stream_id,
            }
        )
        open_fut.add_done_callback(open_done)

        return HranaStream(self, stream_state)

    def _close_stream(self, stream_state: _StreamState, e: BaseException) -> None:
        if stream_state.closed is not None or self.exception is not None:
            return
        stream_state.closed = e

        def close_done(fut: asyncio.Future[proto.Response]) -> None:
            self._stream_id_alloc.free(stream_state.stream_id)
            if not fut.cancelled():
                fut.exception()

        close_fut = self.send_request(
            {
                "type": "close_stream",
                "stream_id": stream_state.stream_id,
            }
        )
        close_fut.add_done_callback(close_done)

    async def close(self) -> None:
        self._set_exception(LibsqlError("Client was manually closed", "CLIENT_CLOSED"))
        if self._socket is not None:
            await self._socket.close()

    def store_sql(self, sql: str) -> int:
        sql_id = self._sql_id_alloc.alloc()

        def store_sql_done(fut: asyncio.Future[proto.Response]) -> None:
            e: Optional[BaseException]
            if fut.cancelled():
                e = asyncio.CancelledError("store_sql was cancelled")
            else:
                e = fut.exception()
            if e is not None:
                self._sql_id_alloc.free(sql_id)

        store_sql_fut = self.send_request(
            {
                "type": "store_sql",
                "sql_id": sql_id,
                "sql": sql,
            }
        )
        store_sql_fut.add_done_callback(store_sql_done)
        return sql_id

    def close_sql(self, sql_id: int) -> None:
        if self.exception is not None:
            return

        def close_sql_done(fut: asyncio.Future[proto.Response]) -> None:
            self._sql_id_alloc.free(sql_id)
            if not fut.cancelled():
                fut.exception()

        close_sql_fut = self.send_request(
            {
                "type": "close_sql",
                "sql_id": sql_id,
            }
        )
        close_sql_fut.add_done_callback(close_sql_done)


class HranaStream:
    _conn: HranaConn
    _state: _StreamState

    def __init__(self, conn: HranaConn, state: _StreamState):
        self._conn = conn
        self._state = state

    def execute(self, stmt: proto.Stmt) -> asyncio.Future[proto.StmtResult]:
        if self._state.closed is not None:
            raise LibsqlError(
                "Stream was closed", "STREAM_CLOSED"
            ) from self._state.closed

        request: proto.ExecuteReq = {
            "type": "execute",
            "stream_id": self._state.stream_id,
            "stmt": stmt,
        }
        response_fut = self._conn.send_request(request)

        def get_result(response: proto.Response) -> proto.StmtResult:
            return cast(proto.ExecuteResp, response)["result"]

        return _map_future(response_fut, get_result)

    def sequence(self, stmt: Union[str, int]) -> asyncio.Future[None]:
        if self._state.closed is not None:
            raise LibsqlError(
                "Stream was closed", "STREAM_CLOSED"
            ) from self._state.closed

        request: proto.SequenceReq
        if isinstance(stmt, str):
            request = {
                "type": "sequence",
                "stream_id": self._state.stream_id,
                "sql": stmt,
            }
        else:
            request = {
                "type": "sequence",
                "stream_id": self._state.stream_id,
                "sql_id": stmt,
            }

        response_fut = self._conn.send_request(request)

        def get_result(response: proto.Response) -> None:
            return None

        return _map_future(response_fut, get_result)

    def batch(self, batch: proto.Batch) -> asyncio.Future[proto.BatchResult]:
        if self._state.closed is not None:
            raise LibsqlError(
                "Stream was closed", "STREAM_CLOSED"
            ) from self._state.closed

        request: proto.BatchReq = {
            "type": "batch",
            "stream_id": self._state.stream_id,
            "batch": batch,
        }
        response_fut = self._conn.send_request(request)

        def get_result(response: proto.Response) -> proto.BatchResult:
            return cast(proto.BatchResp, response)["result"]

        return _map_future(response_fut, get_result)

    def close(self) -> None:
        e = LibsqlError("Stream was manually closed", "STREAM_CLOSED")
        self._conn._close_stream(self._state, e)

    @property
    def closed(self) -> bool:
        return self._state.closed is not None

    def __enter__(self) -> HranaStream:
        return self

    def __exit__(self, _exc_type: Any, _exc_value: Any, _traceback: Any) -> None:
        self.close()


T = TypeVar("T")
R = TypeVar("R")


def _map_future(fut: asyncio.Future[T], f: Callable[[T], R]) -> asyncio.Future[R]:
    ret: asyncio.Future[R] = asyncio.get_running_loop().create_future()

    def done(fut: asyncio.Future[T]) -> None:
        if fut.cancelled():
            ret.cancel()
            return
        e = fut.exception()
        if e is None:
            ret.set_result(f(fut.result()))
        else:
            ret.set_exception(e)

    fut.add_done_callback(done)
    return ret
