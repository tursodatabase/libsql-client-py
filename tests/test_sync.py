import libsql_client
import pytest


@pytest.fixture
def client_sync(url):
    with libsql_client.create_client_sync(url) as client:
        yield client


@pytest.fixture
def transaction_client_sync(transaction_url):
    with libsql_client.create_client_sync(transaction_url) as client:
        yield client


def test_execute(client_sync):
    rs = client_sync.execute("SELECT 1 AS one, 'two' AS two")
    assert rs.columns == ("one", "two")
    assert len(rs.rows) == 1
    assert rs.rows[0].astuple() == (1, "two")


def test_execute_error(client_sync):
    with pytest.raises(libsql_client.LibsqlError):
        client_sync.execute("SELECT foo")


def test_batch(client_sync):
    rss = client_sync.batch(
        [
            "SELECT 1+1",
            ("SELECT ? AS one, ? AS two", [10, "two"]),
        ]
    )
    assert [len(rs) for rs in rss] == [1, 1]
    assert rss[0][0].astuple() == (2,)
    assert rss[1][0].astuple() == (10, "two")


def test_transaction_commit(transaction_client_sync):
    transaction_client_sync.batch(
        [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (a)",
        ]
    )

    with transaction_client_sync.transaction() as transaction:
        transaction.execute("INSERT INTO t VALUES ('one'), ('two')")
        rs = transaction.execute("SELECT COUNT(*) FROM t")
        assert rs[0][0] == 2
        transaction.commit()
    assert transaction.closed

    rs = transaction_client_sync.execute("SELECT COUNT(*) FROM t")
    assert rs[0][0] == 2


def test_transaction_rollback(transaction_client_sync):
    transaction_client_sync.batch(
        [
            "DROP TABLE IF EXISTS t",
            "CREATE TABLE t (a)",
        ]
    )

    with transaction_client_sync.transaction() as transaction:
        transaction.execute("INSERT INTO t VALUES ('one'), ('two')")
        rs = transaction.execute("SELECT COUNT(*) FROM t")
        assert rs[0][0] == 2
        transaction.rollback()
    assert transaction.closed

    rs = transaction_client_sync.execute("SELECT COUNT(*) FROM t")
    assert rs[0][0] == 0


def test_close_twice(client_sync):
    client_sync.close()
    client_sync.close()


def test_close_transaction_twice(transaction_client_sync):
    with transaction_client_sync.transaction() as transaction:
        transaction.close()


def test_close_transaction_after_client(transaction_client_sync):
    transaction = transaction_client_sync.transaction()
    transaction_client_sync.close()
    transaction.close()
