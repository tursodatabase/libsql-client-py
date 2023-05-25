from __future__ import annotations

import base64
import math
from typing import List

from . import proto
from ..client import _normalize_value
from ..client import InArgs
from ..client import InStatement
from ..client import InValue
from ..client import LibsqlError
from ..client import Statement
from ..result import ResultSet
from ..result import Row
from ..result import Value


def _stmt_to_proto(in_stmt: InStatement, in_args: InArgs = None) -> proto.Stmt:
    stmt = Statement.convert(in_stmt, in_args)
    args: List[proto.Value] = []
    named_args: List[proto.NamedArg] = []
    if stmt.args is None:
        pass
    elif isinstance(stmt.args, dict):
        named_args = [
            {"name": key, "value": _value_to_proto(value)}
            for key, value in stmt.args.items()
        ]
    else:
        args = [_value_to_proto(value) for value in stmt.args]
    return {"sql": stmt.sql, "args": args, "named_args": named_args, "want_rows": True}


def _result_set_from_proto(proto_res: proto.StmtResult) -> ResultSet:
    columns = tuple(proto_col["name"] or "" for proto_col in proto_res["cols"])
    column_idxs = {column: idx for idx, column in enumerate(columns)}
    rows = []
    for proto_row in proto_res["rows"]:
        values = tuple(_value_from_proto(proto_val) for proto_val in proto_row)
        rows.append(Row(column_idxs, values))
    rows_affected = proto_res["affected_row_count"]
    last_insert_rowid_str = proto_res.get("last_insert_rowid")
    last_insert_rowid = (
        int(last_insert_rowid_str) if last_insert_rowid_str is not None else None
    )
    return ResultSet(columns, rows, rows_affected, last_insert_rowid)


def _batch_to_proto(in_stmts: List[InStatement]) -> proto.Batch:
    steps: List[proto.BatchStep] = []
    steps.append(
        {
            "stmt": {"sql": "BEGIN", "want_rows": False},
        }
    )

    for in_stmt in in_stmts:
        steps.append(
            {
                "condition": {
                    "type": "ok",
                    "step": len(steps) - 1,
                },
                "stmt": _stmt_to_proto(in_stmt),
            }
        )

    steps.append(
        {
            "condition": {
                "type": "ok",
                "step": len(steps) - 1,
            },
            "stmt": {"sql": "COMMIT", "want_rows": False},
        }
    )
    steps.append(
        {
            "condition": {
                "type": "not",
                "cond": {
                    "type": "ok",
                    "step": len(steps) - 1,
                },
            },
            "stmt": {"sql": "ROLLBACK", "want_rows": False},
        }
    )

    return {"steps": steps}


def _batch_results_from_proto(
    proto_res: proto.BatchResult, stmt_count: int
) -> List[ResultSet]:
    if len(proto_res["step_results"]) != stmt_count + 3:
        raise LibsqlError(
            "Server did not return the expected number of batch results",
            "HRANA_PROTO_ERROR",
        )
    if len(proto_res["step_errors"]) != stmt_count + 3:
        raise LibsqlError(
            "Server did not return the expected number of batch errors",
            "HRANA_PROTO_ERROR",
        )

    for proto_err in proto_res["step_errors"]:
        if proto_err is not None:
            raise _error_from_proto(proto_err)

    result_sets = []
    for stmt_res in proto_res["step_results"][1:-2]:
        if stmt_res is None:
            raise LibsqlError(
                "Server did not return a result in batch", "HRANA_PROTO_ERROR"
            )
        result_sets.append(_result_set_from_proto(stmt_res))
    return result_sets


def _error_from_proto(proto_err: proto.Error) -> LibsqlError:
    message = proto_err["message"]
    code = proto_err.get("code") or "UNKNOWN"
    return LibsqlError(message, code)


def _value_to_proto(in_value: InValue) -> proto.Value:
    value = _normalize_value(in_value)
    if value is None:
        return {"type": "null"}
    elif isinstance(value, str):
        return {"type": "text", "value": value}
    elif isinstance(value, int):
        if value < _MIN_INTEGER or value > _MAX_INTEGER:
            raise OverflowError(
                "Integer exceeds the range of SQLite integers (64 bits, signed)"
            )
        return {"type": "integer", "value": str(value)}
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Only finite floats (not Infinity or NaN) are supported")
        return {"type": "float", "value": value}
    else:
        try:
            data = base64.b64encode(value).decode()
            return {"type": "blob", "base64": data}
        except TypeError:
            raise TypeError(f"Unsupported value of type {type(value)}")


_MIN_INTEGER = -(2**63)
_MAX_INTEGER = 2**63 - 1


def _value_from_proto(value: proto.Value) -> Value:
    if value["type"] == "null":
        return None
    elif value["type"] == "text":
        return str(value["value"])
    elif value["type"] == "integer":
        return int(value["value"])
    elif value["type"] == "float":
        return float(value["value"])
    elif value["type"] == "blob":
        return base64.b64decode(value["base64"] + "====")
    else:
        raise LibsqlError(f"Unknown value type {value['type']!r}", "HRANA_PROTO_ERROR")
