from collections.abc import Sequence
from typing import Dict, Iterator, List, Tuple, Union, overload

Value = Union[None, str, int, float, bytes]

class ResultSet:
    """Result of an SQL statement.

    The result is composed of columns and rows. Every row is represented as a `Row` object and the length of
    every row is equal to the number of columns.
    """

    _columns: Tuple[str, ...]
    _rows: List["Row"]
    _rows_affected: int
    __slots__ = ["_columns", "_rows", "_rows_affected"]

    def __init__(self, columns: Tuple[str, ...], rows: List["Row"], rows_affected: int):
        self._columns = columns
        self._rows = rows
        self._rows_affected = rows_affected

    def __iter__(self) -> Iterator["Row"]:
        return self._rows.__iter__()

    def __len__(self) -> int:
        return len(self._rows)

    def __getitem__(self, key: Union[int, slice]) -> "Row":
        return self._rows[key]

    @property
    def columns(self) -> Tuple[str, ...]:
        return self._columns

    @property
    def rows(self) -> List["Row"]:
        return self._rows

    @property
    def rows_affected(self) -> int:
        return self._rows_affected

class Row(Sequence):
    """A row returned by an SQL statement.

    The row values can be accessed with an index or by name.
    """

    _column_idxs: Dict[str, int]
    _values: Tuple[Value, ...]
    __slots__ = ["_column_idxs", "_values"]

    def __init__(self, column_idxs: Dict[str, int], values: Tuple[Value, ...]) -> None:
        self._column_idxs = column_idxs
        self._values = values

    @overload
    def __getitem__(self, key: int) -> Value: pass
    @overload
    def __getitem__(self, key: str) -> Value: pass
    @overload
    def __getitem__(self, key: slice) -> Tuple[Value, ...]: pass

    def __getitem__(self, key: Union[int, str, slice]) -> Union[Value, Tuple[Value, ...]]:
        """Access a value by index or by name."""
        tuple_key: Union[int, slice]
        if isinstance(key, str):
            tuple_key = self._column_idxs[key]
        else:
            tuple_key = key
        return self._values[tuple_key]

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return repr(self._values)

    def astuple(self) -> Tuple[Value, ...]:
        return self._values

    def asdict(self) -> Dict[str, Value]:
        return {key: self._values[idx] for key, idx in self._column_idxs.items()}

    _asdict = asdict

    @property
    def _fields(self) -> Tuple[str, ...]:
        return tuple(self._column_idxs.keys())
