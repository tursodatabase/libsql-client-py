import asyncio
import libsql_client
import pytest


@pytest.mark.asyncio
async def test_multiple_queries(client):
    rss = await client.batch(
        [
            "SELECT 1+1",
            ("SELECT 1 AS one, 2 AS two",),
            libsql_client.Statement("SELECT ?", ["boomerang"]),
            ("VALUES (?), (?)", ["big", "ben"]),
        ]
    )

    assert [len(rs) for rs in rss] == [1, 1, 1, 2]

    assert rss[0][0].astuple() == (2,)
    assert rss[1][0].asdict() == {"one": 1, "two": 2}
    assert rss[2][0].astuple() == ("boomerang",)
    assert rss[3][0].astuple() == ("big",)
    assert rss[3][1].astuple() == ("ben",)


@pytest.mark.asyncio
async def test_statements_are_executed_sequentially(client):
    rss = await client.batch(
        [
            "DROP TABLE IF EXISTS t",  # 0
            "CREATE TABLE t (a, b)",  # 1
            "INSERT INTO t VALUES (1, 'one')",  # 2
            "SELECT * FROM t ORDER BY a",  # 3
            "INSERT INTO t VALUES (2, 'two')",  # 4
            "SELECT * FROM t ORDER BY a",  # 5
            "DROP TABLE t",  # 6
        ]
    )

    assert len(rss) == 7
    assert [r.astuple() for r in rss[3].rows] == [(1, "one")]
    assert [r.astuple() for r in rss[5].rows] == [(1, "one"), (2, "two")]


@pytest.mark.asyncio
async def test_statements_are_executed_in_transaction(client):
    await client.batch(
        [
            "DROP TABLE IF EXISTS t1",
            "DROP TABLE IF EXISTS t2",
            "CREATE TABLE t1 (a)",
            "CREATE TABLE t2 (a)",
        ]
    )

    async def step(i):
        rss = await client.batch(
            [
                ("INSERT INTO t1 VALUES (?)", [i]),
                ("INSERT INTO t2 VALUES (?)", [i * 10]),
                "SELECT SUM(a) FROM t1",
                "SELECT SUM(a) FROM t2",
            ]
        )
        sum1 = int(rss[2][0][0])
        sum2 = int(rss[3][0][0])
        assert sum2 == sum1 * 10

    n = 100
    await asyncio.gather(*(asyncio.create_task(step(i)) for i in range(n)))

    rs1 = await client.execute("SELECT SUM(a) FROM t1")
    assert rs1[0][0] == n * (n - 1) / 2
    rs2 = await client.execute("SELECT SUM(a) FROM t2")
    assert rs2[0][0] == 10 * n * (n - 1) / 2


@pytest.mark.asyncio
async def test_error(client):
    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        await client.batch(
            [
                "SELECT 1+1",
                "SELECT foobar",
            ]
        )
    assert "no such column: foobar" in str(excinfo.value)


@pytest.mark.asyncio
async def test_error_rollback(client):
    await client.batch(
        [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (a)",
            "INSERT INTO t VALUES ('one')",
        ]
    )

    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        await client.batch(
            [
                "INSERT INTO t VALUES ('two')",
                "SELECT foobar",
                "INSERT INTO t VALUES ('three')",
            ]
        )

    rs = await client.execute("SELECT COUNT(*) FROM t")
    assert rs[0][0] == 1
