import pytest

import libsql_client


@pytest.mark.asyncio
async def test_query_value(client):
    rs = await client.execute("SELECT 42")
    assert len(rs.columns) == 1
    assert len(rs.rows) == 1
    row = rs.rows[0]
    assert len(row) == 1
    assert row[0] == 42


@pytest.mark.asyncio
async def test_query_row(client):
    rs = await client.execute("SELECT 1 AS one, 'two' AS two, 0.5 AS three")
    assert rs.columns == ("one", "two", "three")
    assert len(rs.rows) == 1
    row = rs.rows[0]
    assert len(row) == 3
    assert (row[0], row[1], row[2]) == (1, "two", 0.5)
    assert (row["one"], row["two"], row["three"]) == (1, "two", 0.5)


@pytest.mark.asyncio
async def test_query_multiple_rows(client):
    rs = await client.execute("VALUES (1, 'one'), (2, 'two'), (3, 'three')")
    assert len(rs.columns) == 2
    assert len(rs.rows) == 3
    assert tuple(rs.rows[0]) == (1, "one")
    assert tuple(rs.rows[1]) == (2, "two")
    assert tuple(rs.rows[2]) == (3, "three")


@pytest.mark.asyncio
async def test_statement_without_args(client):
    rs = await client.execute(libsql_client.Statement("SELECT 42"))
    assert rs[0][0] == 42


@pytest.mark.asyncio
async def test_statement_with_args(client):
    rs = await client.execute(libsql_client.Statement("SELECT ?", [10]))
    assert rs[0][0] == 10


@pytest.mark.asyncio
async def test_error_no_such_column(client):
    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        await client.execute("SELECT foo")
    assert "no such column: foo" in str(excinfo.value)


@pytest.mark.asyncio
async def test_error_syntax(client):
    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        await client.execute("SELECT")
    assert "unexpected end of input" in str(excinfo.value) or "incomplete input" in str(
        excinfo.value
    )


@pytest.mark.asyncio
async def test_error_multiple_statements(url, client):
    if url.startswith("file:"):
        return pytest.skip(
            "sqlite3 library does not provide a robust way "
            "to recognize that the user attempted to execute multiple statements"
        )

    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        await client.execute("SELECT 1; SELECT 2")
    assert "one statement" in str(excinfo.value)


@pytest.mark.asyncio
async def test_rows_affected_insert(client):
    await client.batch(
        [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (a)",
        ]
    )
    rs = await client.execute("INSERT INTO t VALUES (1), (2)")
    assert rs.rows_affected == 2


@pytest.mark.asyncio
async def test_rows_affected_delete(client):
    await client.batch(
        [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (a)",
            "INSERT INTO t VALUES (1), (2), (3), (4), (5)",
        ]
    )
    rs = await client.execute("DELETE FROM t WHERE a >= 3")
    assert rs.rows_affected == 3


@pytest.mark.asyncio
async def test_last_insert_rowid(client):
    await client.batch(
        [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (a)",
            "INSERT INTO t VALUES ('one'), ('two')",
        ]
    )
    insert_rs = await client.execute("INSERT INTO t VALUES ('three')")
    assert insert_rs.last_insert_rowid is not None
    select_rs = await client.execute(
        "SELECT a FROM t WHERE ROWID = ?", [insert_rs.last_insert_rowid]
    )
    assert select_rs[0].astuple() == ("three",)
