from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, TypeVar, cast
import aiohttp
import asyncio
import json
import urllib.parse

from ..client import Client, InArgs, InStatement, LibsqlError, Transaction
from ..config import _Config
from ..result import ResultSet
from . import proto
from .convert import (
    _stmt_to_proto, _result_set_from_proto,
    _batch_to_proto, _batch_results_from_proto,
    _error_from_proto,
)
from .id_alloc import IdAlloc

def _create_hrana_client(config: _Config) -> HranaClient:
    assert config.scheme in ("ws", "wss")
    url = urllib.parse.urlunparse((
        config.scheme, config.authority, config.path,
        "", "", "",
    ))
    return HranaClient(url, auth_token=config.auth_token)

@dataclass
class _ResponseState:
    type: str
    future: asyncio.Future[proto.Response]

@dataclass
class _StreamState:
    stream_id: int
    closed: Optional[BaseException]

class HranaClient(Client):
    _socket_task: asyncio.Task[None]
    _send_msg_queue: asyncio.Queue[Optional[str]]
    _closed: Optional[BaseException]

    _recvd_hello: bool
    _response_map: Dict[int, _ResponseState]
    _request_id_alloc: IdAlloc
    _stream_id_alloc: IdAlloc

    def __init__(self, url: str, *, auth_token: Optional[str] = None):
        self._socket_task = asyncio.create_task(self._run_socket_task(url))
        self._socket_task.add_done_callback(self._socket_task_done)
        self._send_msg_queue = asyncio.Queue()
        self._closed = None

        self._recvd_hello = False
        self._response_map = {}
        self._request_id_alloc = IdAlloc()
        self._stream_id_alloc = IdAlloc()

        self._send({"type": "hello", "jwt": auth_token})

    async def _run_socket_task(self, url: str) -> None:
        async with aiohttp.ClientSession() as session:
            socket = await session.ws_connect(
                url,
                protocols=["hrana1"],
                autoclose=False,
                autoping=True,
            )
            await self._run_socket(socket)

    async def _run_socket(self, socket: aiohttp.ClientWebSocketResponse) -> None:
        async def do_receive() -> None:
            while True:
                msg = await socket.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        self._receive(msg.data)
                    except Exception as e:
                        await socket.close(code=3007, message="Could not handle message".encode())
                        raise
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    await socket.close(code=3003, message="Only text messages are accepted".encode())
                    raise LibsqlError("Received a binary WebSocket message", "HRANA_PROTO_ERROR")
                elif msg.type == aiohttp.WSMsgType.PING:
                    await socket.pong(msg.data)
                elif msg.type == aiohttp.WSMsgType.PONG:
                    pass
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    code = cast(aiohttp.WSCloseCode, msg.data)
                    reason = cast(str, msg.extra)
                    raise LibsqlError(
                        f"WebSocket was closed with code {code}: {reason!r}",
                        "WEBSOCKET_CLOSED",
                    )
                elif msg.type in (aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise msg.data
                else:
                    raise LibsqlError(
                        f"Received unexpected WebSocket message {msg.type!r}",
                        "HRANA_PROTO_ERROR",
                    )

        async def do_send() -> None:
            while True:
                msg_str = await self._send_msg_queue.get()
                if msg_str is None:
                    break
                await socket.send_str(msg_str)
            await socket.close()

        try:
            receiver = asyncio.create_task(do_receive())
            sender = asyncio.create_task(do_send())
            await asyncio.gather(receiver, sender)
            await socket.close()
        except Exception as e:
            await socket.close(code=3000, message="Client encountered an internal error".encode())
            raise
        finally:
            receiver.cancel()
            sender.cancel()

    def _socket_task_done(self, fut: asyncio.Future[None]) -> None:
        if fut.cancelled():
            self._set_closed(LibsqlError("Client task was cancelled", "CLIENT_CLOSED"))
        e = fut.exception()
        if e is not None:
            self._set_closed(e)
        else:
            self._set_closed(LibsqlError("Client task has terminated", "CLIENT_CLOSED"))

    def _send(self, msg: proto.ClientMsg) -> None:
        assert self._closed is None
        self._send_msg_queue.put_nowait(json.dumps(msg))

    def _set_closed(self, e: BaseException) -> None:
        if self._closed is not None:
            return
        self._closed = e
        self._send_msg_queue.put_nowait(None)

        for request_id, response_state in self._response_map.items():
            response_state.future.set_exception(e)
            self._request_id_alloc.free(request_id)
        self._response_map.clear()

    def _send_request(self, request: proto.Request) -> asyncio.Future[proto.Response]:
        future = asyncio.get_running_loop().create_future()

        if self._closed is not None:
            future.set_exception(LibsqlError("Client is closed", "CLIENT_CLOSED"))
            return future

        request_id = self._request_id_alloc.alloc()
        self._response_map[request_id] = _ResponseState(request["type"], future)
        self._send({"type": "request", "request_id": request_id, "request": request})
        return future

    def _receive(self, text: str) -> None:
        if self._closed is not None:
            return

        try:
            msg = json.loads(text)
        except ValueError as e:
            raise LibsqlError("Server message is not valid JSON", "HRANA_PROTO_ERROR") from e

        if msg["type"] in ("hello_ok", "hello_error"):
            if self._recvd_hello:
                raise LibsqlError("Received a duplicated error response", "HRANA_PROTO_ERROR")
            self._recvd_hello = True

            if msg["type"] == "hello_error":
                raise _error_from_proto(msg["error"])
            return
        elif not self._recvd_hello:
            raise LibsqlError("Received a non-hello message before hello response", "HRANA_PROTO_ERROR")

        if msg["type"] == "response_ok":
            request_id = int(msg["request_id"])
            response_state = self._response_map.pop(request_id, None)
            if response_state is None:
                raise LibsqlError("Received unexpected OK response", "HRANA_PROTO_ERROR")
            self._request_id_alloc.free(request_id)

            try:
                if response_state.type != msg["response"]["type"]:
                    raise LibsqlError("Received unexpected type of response", "HRANA_PROTO_ERROR")
                response_state.future.set_result(msg["response"])
            except Exception as e:
                response_state.future.set_exception(e)
                raise
        elif msg["type"] == "response_error":
            request_id = int(msg["request_id"])
            response_state = self._response_map.pop(request_id, None)
            if response_state is None:
                raise LibsqlError("Received unexpected error response", "HRANA_PROTO_ERROR")
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

        open_fut = self._send_request({
            "type": "open_stream",
            "stream_id": stream_id,
        })
        open_fut.add_done_callback(open_done)

        return HranaStream(self, stream_state)

    def _close_stream(self, stream_state: _StreamState, e: BaseException) -> None:
        if stream_state.closed is not None or self._closed is not None:
            return
        stream_state.closed = e

        def close_done(_fut: asyncio.Future[proto.Response]) -> None:
            self._stream_id_alloc.free(stream_state.stream_id)

        close_fut = self._send_request({
            "type": "close_stream",
            "stream_id": stream_state.stream_id,
        })
        close_fut.add_done_callback(close_done)

    async def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet:
        with self.open_stream() as stream:
            proto_stmt = _stmt_to_proto(stmt, args)
            proto_result_fut = stream.execute(proto_stmt)
        return _result_set_from_proto(await proto_result_fut)

    async def batch(self, stmts: List[InStatement]) -> List[ResultSet]:
        with self.open_stream() as stream:
            proto_batch = _batch_to_proto(stmts)
            proto_result_fut = stream.batch(proto_batch)
        return _batch_results_from_proto(await proto_result_fut, len(stmts))

    def transaction(self) -> "HranaTransaction":
        stream = self.open_stream()
        return HranaTransaction(stream)

    async def close(self) -> None:
        self._set_closed(LibsqlError("Client was manually closed", "CLIENT_CLOSED"))
        await self._socket_task

    @property
    def closed(self) -> bool:
        return self._closed is not None

class HranaStream:
    _client: HranaClient
    _state: _StreamState

    def __init__(self, client: HranaClient, state: _StreamState):
        self._client = client
        self._state = state

    def execute(self, stmt: proto.Stmt) -> asyncio.Future[proto.StmtResult]:
        if self._state.closed is not None:
            raise LibsqlError("Stream was closed", "STREAM_CLOSED") from self._state.closed

        request: proto.ExecuteReq = {
            "type": "execute",
            "stream_id": self._state.stream_id,
            "stmt": stmt,
        }
        response_fut = self._client._send_request(request)

        def get_result(response: proto.Response) -> proto.StmtResult:
            return cast(proto.ExecuteResp, response)["result"]
        return _map_future(response_fut, get_result)

    def batch(self, batch: proto.Batch) -> asyncio.Future[proto.BatchResult]:
        if self._state.closed is not None:
            raise LibsqlError("Stream was closed", "STREAM_CLOSED") from self._state.closed

        request: proto.BatchReq = {
            "type": "batch",
            "stream_id": self._state.stream_id,
            "batch": batch,
        }
        response_fut = self._client._send_request(request)

        def get_result(response: proto.Response) -> proto.BatchResult:
            return cast(proto.BatchResp, response)["result"]
        return _map_future(response_fut, get_result)

    def close(self) -> None:
        e = LibsqlError("Stream was manually closed", "STREAM_CLOSED")
        self._client._close_stream(self._state, e)

    @property
    def closed(self) -> bool:
        return self._state.closed is not None

    def __enter__(self) -> HranaStream:
        return self

    def __exit__(self, _exc_type: Any, _exc_value: Any, _traceback: Any) -> None:
        self.close()

class HranaTransaction(Transaction):
    _stream: HranaStream
    _begin_fut: asyncio.Future[proto.StmtResult]

    def __init__(self, stream: HranaStream):
        self._stream = stream
        self._begin_fut = stream.execute({
            "sql": "BEGIN",
            "want_rows": False,
        })

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

        fut = self._stream.execute({
            "sql": "ROLLBACK",
            "want_rows": False,
        })
        self._stream.close()
        await fut

    async def commit(self) -> None:
        await self._begin_fut
        if self._stream.closed:
            raise LibsqlError("The transaction is closed", "TRANSACTION_CLOSED")

        fut = self._stream.execute({
            "sql": "COMMIT",
            "want_rows": False,
        })
        self._stream.close()
        await fut

    def close(self) -> None:
        self._stream.close()

    @property
    def closed(self) -> bool:
        return self._stream.closed

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
