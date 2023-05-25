from __future__ import annotations

import logging
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence


def log_prefix(
    logger: logging.Logger,
    prefix: str,
    level: int,
    msg: str,
    *args: object,
    exc_info: Optional[BaseException] = None,
) -> None:
    logger.log(level, prefix + msg, *args, exc_info=exc_info)


def log_obj(
    logger: logging.Logger,
    obj: object,
    level: int,
    msg: str,
    *args: object,
    exc_info: Optional[BaseException] = None,
) -> None:
    prefix = getattr(obj, "_log_prefix", None)
    if prefix is None:
        prefix = f"{obj!r}: "
    log_prefix(logger, prefix, level, msg, *args, exc_info=exc_info)


_lstrip_sql_whitespace_chars = {" ", "\t", "\f", "\n", "\r"}


def lstrip_sql(sql: str) -> Optional[str]:  # noqa: C901
    # See statement.c, lstrip_sql() function at:
    # https://github.com/python/cpython/blob/main/Modules/_sqlite/statement.c#L134
    # using the same names here, however in C pos is both the index (offset)
    # and the value (ch = *pos)
    pos = 0
    end_pos = len(sql)
    while pos < end_pos:
        ch = sql[pos]
        if ch in _lstrip_sql_whitespace_chars:
            pos += 1  # C uses for() with trailing ++
            continue

        if ch == "-":
            # Skip line comments.
            # NOTE: in C pos[end_pos] == 0, so "pos[1] == '-'" is enough,
            # Here we must compare pos + 1 < end_pos
            if pos + 1 < end_pos and sql[pos + 1] == "-":
                pos += 2
                # NOTE: in C pos[end_pos] == 0, so they do "pos[0]".
                # Here we compare pos + 1 < end_pos
                while pos < end_pos and sql[pos] != "\n":
                    pos += 1
                if pos >= end_pos:
                    return None

                pos += 1  # C uses for() with trailing ++
                continue

            return sql[pos:]  # not a line comment

        if ch == "/":
            # Skip C style comments.
            # NOTE: in C pos[end_pos] == 0, so "pos[1] == '*'" is enough,
            # Here we must compare pos + 1 < end_pos
            if pos + 1 < end_pos and sql[pos + 1] == "*":
                pos += 2
                # NOTE: in C pos[end_pos] == 0, so they do "pos[0]".
                # Here we compare pos + 1 < end_pos. To avoid a messy
                # long line, the condition is moved into an explicit "if"
                # that "break"s.
                while pos < end_pos:
                    if sql[pos] == "*":
                        if pos + 1 < end_pos and sql[pos + 1] == "/":
                            break
                    pos += 1

                if pos >= end_pos:
                    return None

                pos += 2  # C uses for() with trailing ++
                continue

            return sql[pos:]  # not a C style comment

        return sql[pos:]

    return None


_iter_sql_delim_chars = {";", ",", "(", ")", "[", "]"}
_iter_sql_stop_chars = _lstrip_sql_whitespace_chars.union(
    _iter_sql_delim_chars,
)
_iter_sql_quote_chars = {'"', "'"}


def _iter_sql_get_quoted_end(sql: str, pos: int, end_pos: int) -> int:
    """Find the end of the quoted string starting at ``pos``.

    The returned position includes the position of the quote character,
    that matches the one at ``pos``.

    >>> def test_quote(ts):
    ...     end = _iter_sql_get_quoted_end(ts, 0, len(ts))
    ...     return (end, ts[:end])

    >>> test_quote("'abc'")
    (5, "'abc'")
    >>> test_quote('"abc "')
    (6, '"abc "')

    >>> test_quote("'abc' def")
    (5, "'abc'")
    >>> test_quote('"abc "def')
    (6, '"abc "')

    It also handles escaping by double quotes, see
    https://www.sqlite.org/faq.html#q14

    >>> test_quote('"abc""def" ghi')
    (10, '"abc""def"')
    >>> test_quote('"abc "def" ghi')
    (6, '"abc "')
    """
    if pos + 1 >= end_pos:
        return pos

    ch = sql[pos]
    while pos + 1 < end_pos:
        pos += 1
        if sql[pos] == ch:
            pos += 1
            if pos == end_pos or sql[pos] != ch:
                break

            # escaped by double quoting: https://www.sqlite.org/faq.html#q14
            pos += 1

    return pos


def iter_sql_tokens(sql: str) -> Iterable[str]:
    """Calls lstrip_sql() to get the start of the next token and yield it.

    It will handle stop chars (;,[]()) as their own tokens, as well as handle
    double and single quotes, as well as quote escaping with \\

    >>> list(iter_sql_tokens("--COMMENT\\nBEGIN X"))
    ['BEGIN', 'X']
    >>> list(iter_sql_tokens(
    ... "CREATE TABLE x (id INTEGER /* COMMENT */, name TEXT)"))
    ['CREATE', 'TABLE', 'x', '(', 'id', 'INTEGER', ',', 'name', 'TEXT', ')']
    >>> list(iter_sql_tokens("SELECT; INSERT; DELETE"))
    ['SELECT', ';', 'INSERT', ';', 'DELETE']
    >>> print("\\n".join(iter_sql_tokens(
    ... 'INSERT INTO t (a,b) VALUES("s -- c /* c */", \\'x\\')'
    ... )))
    INSERT
    INTO
    t
    (
    a
    ,
    b
    )
    VALUES
    (
    "s -- c /* c */"
    ,
    'x'
    )
    >>> list(iter_sql_tokens('SELECT"abc"'))
    ['SELECT', '"abc"']
    """
    while sql:
        sql = lstrip_sql(sql) or ""
        if not sql:
            break

        pos = 0
        end_pos = len(sql)
        while pos < end_pos:
            ch = sql[pos]
            if ch in _iter_sql_quote_chars:
                if pos > 0:
                    break
                pos = _iter_sql_get_quoted_end(sql, pos, end_pos)
                break
            elif ch in _iter_sql_stop_chars:
                break
            else:
                pos += 1

        if pos > 0:
            yield sql[:pos]
            sql = sql[pos:]
        else:
            yield sql[0]
            sql = sql[1:]


def iter_sql_statements(sql: str) -> Iterable[Sequence[str]]:
    """Yields sql statements based on ";" tokens.

    >>> list(iter_sql_statements("BEGIN"))
    [['BEGIN']]
    >>> list(iter_sql_statements("BEGIN;INSERT INTO x;COMMIT"))
    [['BEGIN', ';'], ['INSERT', 'INTO', 'x', ';'], ['COMMIT']]

    """
    stmt: List[str] = []
    for token in iter_sql_tokens(sql):
        stmt.append(token)
        if token == ";":
            yield stmt
            stmt = []

    if stmt:
        yield stmt
