from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import logging
import queue
import sys
import threading
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import NamedTuple
from typing import Optional
from typing import overload
from typing import TypeVar

from typing_extensions import ParamSpec

from ._utils import log_obj
from ._utils import log_prefix

_logger = logging.getLogger(__name__)
_log_obj = functools.partial(log_obj, _logger)
_log_prefix = functools.partial(log_prefix, _logger)

P = ParamSpec("P")
T = TypeVar("T")


class LoopControl(NamedTuple):
    loop: asyncio.AbstractEventLoop
    stop_event: asyncio.Event


# NOTE: keep outside of class, do not touch self, not even keep a reference
def _thread_main(log_prefix: str, q: queue.Queue[LoopControl]) -> None:
    loop: Optional[asyncio.AbstractEventLoop] = None

    def dbg(
        msg: str,
        *args: object,
        exc_info: Optional[BaseException] = None,
    ) -> None:
        nonlocal loop
        loop_str = f" {loop!r}" if loop else ""
        prefix = f"{log_prefix}{loop_str}: "
        _log_prefix(prefix, logging.DEBUG, msg, *args, exc_info=exc_info)

    async def main() -> None:
        nonlocal loop
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        try:
            dbg("started main()")
            q.put_nowait(LoopControl(loop, stop_event))
            await stop_event.wait()
            dbg("finished main()")
        except Exception as e:
            dbg("failed main(): %s", e, exc_info=e)
        finally:
            loop = None

    dbg("started thread")
    asyncio.run(main())
    dbg("finished thread")


class AsyncExecutor(threading.Thread):
    _lock: threading.Lock
    _control: Optional[LoopControl]
    __slots__ = ("_control", "_lock")

    def __init__(self) -> None:
        self._control = None
        q: queue.Queue[LoopControl] = queue.Queue(1)
        log_prefix = f"<{self.__class__.__name__} at {id(self):x}>"
        super().__init__(daemon=True, target=_thread_main, args=(log_prefix, q))
        self._inf("created")
        self.start()
        self._lock = threading.Lock()
        self._control = q.get()
        self._inf("thread ready")

    def __del__(self) -> None:
        self._inf("destroyed")
        # This is really unlikely, since run() will be in the thread,
        # and thus will hold a reference in there, but let's check it anyway
        assert self._control is None, "Thread is still running"

    def __repr__(self) -> str:
        addr = hex(id(self))
        s = "started" if self.is_alive() else "stopped"
        if self._control is not None:
            s += f" loop={self._control.loop}"
        return f"<{self.__class__.__name__} at {addr} name={self.name!r} {s}>"

    _log = functools.partial(_log_obj)
    _dbg = functools.partialmethod(_log, logging.DEBUG)
    _inf = functools.partialmethod(_log, logging.INFO)
    _err = functools.partialmethod(_log, logging.ERROR)

    def shutdown(self) -> None:
        self._dbg("shuting down thread...")
        with self._lock:
            if self._control is None:
                raise RuntimeError("thread already down")

            loop, stop_event = self._control
            self._control = None

        async def run_in_main_loop() -> None:
            self._dbg("notifying _stop_event")
            stop_event.set()

        self._dbg("thread will stop")
        coro = run_in_main_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        future.result()
        self.join()
        self._inf("thread did stop")

    if sys.version_info[:2] >= (3, 9):

        @overload
        def submit(
            self,
            fn: Callable[P, Awaitable[asyncio.Future[T]]],
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> concurrent.futures.Future[T]:
            ...

        @overload
        def submit(
            self,
            fn: Callable[P, Awaitable[T]],
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> concurrent.futures.Future[T]:
            ...

        @overload
        def submit(
            self,
            fn: Callable[P, T],
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> concurrent.futures.Future[T]:
            ...

    else:

        @overload
        def submit(
            self,
            fn: Callable[P, Awaitable[asyncio.Future]],
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> concurrent.futures.Future:
            ...

        @overload
        def submit(
            self,
            fn: Callable[P, Awaitable[T]],
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> concurrent.futures.Future:
            ...

        @overload
        def submit(
            self,
            fn: Callable[P, T],
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> concurrent.futures.Future:
            ...

    def submit(
        self,
        fn: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Any:  # variants are covered by overloads
        with self._lock:
            return self._unlocked_submit(fn, *args, **kwargs)

    def _unlocked_submit(
        self,
        fn: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Any:  # variants are covered by overloads
        if self._control is None:
            raise RuntimeError("already down")

        async def run_in_main_loop() -> Any:
            try:
                self._dbg("calling: %s, args=%s, kwargs=%s", fn, args, kwargs)
                r = fn(*args, **kwargs)
                while asyncio.iscoroutine(r) or asyncio.isfuture(r):
                    r = await r
                self._dbg(
                    "finished: %s, args=%s, kwargs=%s, result=%s",
                    fn,
                    args,
                    kwargs,
                    r,
                )
                return r
            except Exception as e:
                self._dbg(
                    "failed: %s, args=%s, kwargs=%s, exc=%s",
                    fn,
                    args,
                    kwargs,
                    e,
                    exc_info=e,
                )
                raise

        coro = run_in_main_loop()
        return asyncio.run_coroutine_threadsafe(coro, self._control.loop)
