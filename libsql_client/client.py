from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union

from .result import ResultSet
from .result import Value

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer
else:
    ReadableBuffer = bytes

InValue = Union[Value, bool, datetime, ReadableBuffer]
InArgs = Union[List[InValue], Tuple[InValue, ...], Dict[str, InValue], None]
InStatement = Union["Statement", str, Tuple[str], Tuple[str, InArgs]]


class Statement:
    sql: str
    args: InArgs

    def __init__(self, sql: str, args: InArgs = None):
        self.sql = sql
        self.args = args

    @staticmethod
    def convert(stmt: InStatement, args: InArgs = None) -> Statement:
        if isinstance(stmt, tuple):
            if len(stmt) == 1:
                return Statement(stmt[0], args)
            if len(stmt) > 2:
                raise TypeError(
                    "Statement must be a 1-tuple or 2-tuple, "
                    f"but got a {len(stmt)}-tuple"
                )
            if args:
                raise TypeError(
                    "Cannot pass additional args to a statement passed as tuple"
                )
            return Statement(stmt[0], stmt[1])  # type: ignore[misc]
        if isinstance(stmt, Statement):
            if args:
                raise TypeError("Cannot pass additional args to a Statement instance")
            return stmt
        return Statement(stmt, args)


class LibsqlError(RuntimeError):
    code: str
    explanation: str

    def __init__(self, message: str, code: str):
        super(RuntimeError, self).__init__(f"{code}: {message}")
        self.code = code
        self.explanation = message


TClient = TypeVar("TClient", bound="Client")


class Client(ABC):
    @abstractmethod
    async def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet:
        pass

    @abstractmethod
    async def batch(self, stmts: List[InStatement]) -> List[ResultSet]:
        pass

    @abstractmethod
    def transaction(self) -> Transaction:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass

    @property
    @abstractmethod
    def closed(self) -> bool:
        pass

    async def __aenter__(self: TClient) -> TClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


TTransaction = TypeVar("TTransaction", bound="Transaction")


class Transaction(ABC):
    @abstractmethod
    async def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet:
        ...

    @abstractmethod
    async def rollback(self) -> None:
        ...

    @abstractmethod
    async def commit(self) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    @property
    @abstractmethod
    def closed(self) -> bool:
        ...

    def __enter__(self: TTransaction) -> TTransaction:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def _normalize_value(in_value: InValue) -> Value:
    if isinstance(in_value, datetime):
        return int(in_value.timestamp() * 1000)
    elif isinstance(in_value, bool):
        return int(in_value)
    elif isinstance(in_value, (str, int, float)) or in_value is None:
        return in_value
    return bytes(memoryview(in_value))
