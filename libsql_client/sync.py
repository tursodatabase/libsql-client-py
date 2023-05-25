from __future__ import annotations

import asyncio
import collections
import concurrent
from dataclasses import dataclass
import threading
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Deque
from typing import List
from typing import Optional
from typing import TypeVar

from .client import Client
from .client import InArgs
from .client import InStatement
from .client import LibsqlError
from .client import Transaction
from .create_client import create_client
from .result import ResultSet

T = TypeVar("T")


def create_client_sync(*args: Any, **kwargs: Any) -> ClientSync:
    executor = _AsyncExecutor()
    try:
        client: Client = executor.submit_func(lambda: create_client(*args, **kwargs))
        return ClientSync(executor, client)
    except Exception:
        executor.close()
        raise


class ClientSync:
    _executor: _AsyncExecutor
    _client: Client

    def __init__(self, executor: _AsyncExecutor, client: Client):
        self._executor = executor
        self._client = client

    def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet:
        return self._executor.submit_coro(self._client.execute(stmt, args))

    def batch(self, stmts: List[InStatement]) -> List[ResultSet]:
        return self._executor.submit_coro(self._client.batch(stmts))

    def transaction(self) -> TransactionSync:
        transaction: Transaction = self._executor.submit_func(self._client.transaction)
        return TransactionSync(self._executor, transaction)

    def close(self) -> None:
        self._executor.close_with_coro(self._client.close)

    @property
    def closed(self) -> bool:
        return self._executor.is_closed()

    def __enter__(self) -> ClientSync:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class TransactionSync:
    _executor: _AsyncExecutor
    _transaction: Transaction

    def __init__(self, executor: _AsyncExecutor, transaction: Transaction):
        self._executor = executor
        self._transaction = transaction

    def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet:
        return self._executor.submit_coro(self._transaction.execute(stmt, args))

    def rollback(self) -> None:
        return self._executor.submit_coro(self._transaction.rollback())

    def commit(self) -> None:
        return self._executor.submit_coro(self._transaction.commit())

    def close(self) -> None:
        self._executor.submit_func_unless_closed(self._transaction.close, lambda: None)

    @property
    def closed(self) -> bool:
        return self._executor.submit_func_unless_closed(
            lambda: self._transaction.closed, lambda: True
        )

    def __enter__(self) -> TransactionSync:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


@dataclass
class _QueueItem:
    coroutine: Coroutine[Any, Any, Any]
    future: concurrent.futures.Future


class _AsyncExecutor:
    _thread: threading.Thread
    _loop: asyncio.AbstractEventLoop

    _lock: threading.Lock
    _closed: bool
    _queue: Deque[Optional[_QueueItem]]
    _waker: Optional[asyncio.Future[None]]

    def __init__(self) -> None:
        self._thread = threading.Thread(target=self._run, name="libsql_client")
        self._loop = asyncio.new_event_loop()

        self._lock = threading.Lock()
        self._closed = False
        self._queue = collections.deque()
        self._waker = None

        self._thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._run_on_loop())
        _cancel_all_tasks(self._loop)
        self._loop.run_until_complete(self._loop.shutdown_asyncgens())
        self._loop.close()

    async def _run_on_loop(self) -> None:
        while True:
            item = await self._dequeue_item()
            if item is None:
                break
            try:
                item.future.set_result(await item.coroutine)
            except Exception as e:
                item.future.set_exception(e)

        with self._lock:
            self._closed = True
            for item in self._queue:
                if item is not None:
                    item.future.set_exception(
                        LibsqlError("Client is closed", "CLIENT_CLOSED")
                    )
            self._queue.clear()

    async def _dequeue_item(self) -> Optional[_QueueItem]:
        while True:
            with self._lock:
                if len(self._queue) > 0:
                    return self._queue.popleft()
                assert self._waker is None
                waker = self._loop.create_future()
                self._waker = waker
            await waker

    def _enqueue_item_with_lock(self, item: Optional[_QueueItem]) -> None:
        self._queue.append(item)
        waker, self._waker = self._waker, None
        if waker is not None:
            waker_: asyncio.Future[None] = waker

            def resolve_waker() -> None:
                waker_.set_result(None)

            self._loop.call_soon_threadsafe(resolve_waker)

    def submit_coro(self, coro: Coroutine[Any, Any, T]) -> T:
        with self._lock:
            if self._closed:
                raise LibsqlError("Client is closed", "CLIENT_CLOSED")
            fut: concurrent.futures.Future = concurrent.futures.Future()
            self._enqueue_item_with_lock(_QueueItem(coro, fut))
        return fut.result()

    def submit_func(self, func: Callable[[], T]) -> T:
        async def coro() -> T:
            return func()

        return self.submit_coro(coro())

    def submit_func_unless_closed(
        self, on_open: Callable[[], T], on_closed: Callable[[], T]
    ) -> T:
        async def on_open_coro() -> T:
            return on_open()

        with self._lock:
            if self._closed:
                return on_closed()
            fut: concurrent.futures.Future = concurrent.futures.Future()
            self._enqueue_item_with_lock(_QueueItem(on_open_coro(), fut))
        return fut.result()

    def close_with_coro(
        self, coro_func: Callable[[], Coroutine[Any, Any, None]]
    ) -> None:
        with self._lock:
            if self._closed:
                return
            fut: concurrent.futures.Future = concurrent.futures.Future()
            self._enqueue_item_with_lock(_QueueItem(coro_func(), fut))
            self._enqueue_item_with_lock(None)
        self._thread.join()
        fut.result()

    def close(self) -> None:
        async def noop() -> None:
            return None

        self.close_with_coro(noop)

    def is_closed(self) -> bool:
        with self._lock:
            return self._closed


# this is copied from CPython's Lib/asyncio/runners.py
def _cancel_all_tasks(loop: asyncio.AbstractEventLoop) -> None:
    to_cancel = asyncio.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    loop.run_until_complete(asyncio.gather(*to_cancel, return_exceptions=True))

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler(
                {
                    "message": "unhandled exception during _AsyncExecutor shutdown",
                    "exception": task.exception(),
                    "task": task,
                }
            )
