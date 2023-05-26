import libsql_client
import pytest


async def _assert_closed(t):
    assert t.closed
    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        await t.execute("SELECT 1")
    assert excinfo.value.code == "TRANSACTION_CLOSED"


@pytest.mark.asyncio
async def test_multiple_queries(transaction_client):
    with transaction_client.transaction() as t:
        rs = await t.execute("SELECT 1")
        assert rs[0][0] == 1

        rs = await t.execute("SELECT 1 AS one, 2 AS two, 3 AS three")
        assert rs[0].asdict() == {"one": 1, "two": 2, "three": 3}


@pytest.mark.asyncio
async def test_commit(transaction_client):
    await transaction_client.batch(
        [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (a)",
        ]
    )

    with transaction_client.transaction() as t:
        await t.execute("INSERT INTO t VALUES ('one')")
        assert not t.closed
        await t.commit()
        await _assert_closed(t)

    rs = await transaction_client.execute("SELECT COUNT(*) FROM t")
    assert rs[0][0] == 1


@pytest.mark.asyncio
async def test_rollback(transaction_client):
    await transaction_client.batch(
        [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (a)",
        ]
    )

    with transaction_client.transaction() as t:
        await t.execute("INSERT INTO t VALUES ('one')")
        assert not t.closed
        await t.rollback()
        await _assert_closed(t)

    rs = await transaction_client.execute("SELECT COUNT(*) FROM t")
    assert rs[0][0] == 0


@pytest.mark.asyncio
async def test_close(transaction_client):
    await transaction_client.batch(
        [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (a)",
        ]
    )

    t = transaction_client.transaction()
    await t.execute("INSERT INTO t VALUES ('one')")
    assert not t.closed
    t.close()
    await _assert_closed(t)

    rs = await transaction_client.execute("SELECT COUNT(*) FROM t")
    assert rs[0][0] == 0


@pytest.mark.asyncio
async def test_context_manager(transaction_client):
    with transaction_client.transaction() as t:
        assert not t.closed
    assert t.closed


@pytest.mark.asyncio
async def test_close_twice(transaction_client):
    t = transaction_client.transaction()
    t.close()
    t.close()
    assert t.closed


@pytest.mark.asyncio
async def test_error_does_not_rollback(transaction_client):
    await transaction_client.batch(
        [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (a)",
        ]
    )

    with transaction_client.transaction() as t:
        await t.execute("INSERT INTO t VALUES ('one')")
        with pytest.raises(libsql_client.LibsqlError) as excinfo:
            await t.execute("SELECT foobar")
        await t.execute("INSERT INTO t VALUES ('two')")
        await t.commit()

    rs = await transaction_client.execute("SELECT COUNT(*) FROM t")
    assert rs[0][0] == 2


@pytest.mark.asyncio
async def test_transaction_not_supported(http_url):
    async with libsql_client.create_client(http_url) as c:
        with pytest.raises(libsql_client.LibsqlError) as excinfo:
            c.transaction()
        assert excinfo.value.code == "TRANSACTIONS_NOT_SUPPORTED"
