from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Dict, List, NamedTuple, Optional, Protocol, Tuple, TypeVar, Union

from .result import ResultSet, Value

InValue = Union[Value, bool, datetime]
InArgs = Union[List[InValue], Tuple[InValue, ...], Dict[str, InValue], None]
InStatement = Union["Statement", str, Tuple[str], Tuple[str, InArgs]]

class Statement:
    sql: str
    args: InArgs

    def __init__(self, sql: str, args: InArgs = None):
        self.sql = sql
        self.args = args

    @staticmethod
    def convert(stmt: InStatement, args: InArgs = None) -> "Statement":
        if isinstance(stmt, tuple):
            if len(stmt) > 2:
                raise TypeError(f"Statement must be a 2-tuple, but got a {len(stmt)}-tuple")
            if args:
                raise TypeError("Cannot pass additional args to a statement passed as tuple")
            return Statement(stmt[0], stmt[1] if len(stmt) >= 2 else None)
        if isinstance(stmt, Statement):
            if args:
                raise TypeError("Cannot pass additional args to a Statement instance")
            return stmt
        return Statement(stmt, args)

class LibsqlError(RuntimeError):
    code: str

    def __init__(self, message: str, code: str):
        super(RuntimeError, self).__init__(f"{code}: {message}")
        self.code = code

TClient = TypeVar("TClient", bound="Client")

class Client(ABC):
    @abstractmethod
    async def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet: pass

    @abstractmethod
    async def batch(self, stmts: List[InStatement]) -> List[ResultSet]: pass

    @abstractmethod
    async def transaction(self) -> "Transaction": pass

    @abstractmethod
    def close(self) -> None: pass

    @property
    @abstractmethod
    def closed(self) -> bool: pass

    def __enter__(self: TClient) -> TClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

TTransaction = TypeVar("TTransaction", bound="Transaction")

class Transaction(ABC):
    @abstractmethod
    async def execute(self, stmt: InStatement, args: InArgs = None) -> ResultSet: ...

    @abstractmethod
    async def rollback(self) -> None: ...

    @abstractmethod
    async def commit(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    @property
    @abstractmethod
    def closed(self) -> bool: ...

    def __enter__(self: TTransaction) -> TTransaction:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
