import libsql_client
import pytest
import sys


@pytest.mark.asyncio
async def test_blob(client):
    rs = await client.execute(f"SELECT X'deadbeef'")
    assert rs.rows[0][0] == b"\xde\xad\xbe\xef"


@pytest.mark.asyncio
async def test_columns(client):
    rs = await client.execute("SELECT 1 AS a, 2 AS b, 3 AS c")
    assert rs.columns == ("a", "b", "c")
    row = rs.rows[0]
    assert row["a"] == 1
    assert row["b"] == 2
    assert row["c"] == 3


@pytest.mark.asyncio
async def test_rows(client):
    rs = await client.execute("VALUES (1, 'one'), (2, 'two'), (3, 'three')")
    assert len(rs.rows) == 3
    assert len(rs) == 3

    assert rs.rows[0][0] == 1
    assert rs.rows[0][1] == "one"
    assert rs.rows[1][0] == 2
    assert rs.rows[1][1] == "two"
    assert rs.rows[2][0] == 3
    assert rs.rows[2][1] == "three"

    for i in range(3):
        assert rs[i] is rs.rows[i]
    assert list(rs) == rs.rows


@pytest.mark.asyncio
async def test_row_repr(client):
    rs = await client.execute("SELECT 42, 0.5, 'brontosaurus', NULL")
    assert repr(rs.rows[0]) == "(42, 0.5, 'brontosaurus', None)"


@pytest.mark.asyncio
async def test_row_slice(client):
    rs = await client.execute("SELECT 'one', 'two', 'three', 'four', 'five'")
    assert rs.rows[0][1:3] == ("two", "three")


@pytest.mark.asyncio
async def test_row_tuple(client):
    rs = await client.execute("SELECT 'one', 'two', 'three'")
    assert tuple(rs.rows[0]) == ("one", "two", "three")


@pytest.mark.asyncio
async def test_row_astuple(client):
    rs = await client.execute("SELECT 'one', 'two', 'three'")
    assert rs.rows[0].astuple() == ("one", "two", "three")


@pytest.mark.asyncio
async def test_row_asdict(client):
    rs = await client.execute("SELECT 1 AS one, 2 AS two, 3 AS three")
    assert rs.rows[0].asdict() == {"one": 1, "two": 2, "three": 3}
    assert rs.rows[0].asdict() == rs.rows[0]._asdict()


try:
    import pandas
except ImportError:
    pandas = None
pandas_only = pytest.mark.skipif(pandas is None, reason="pandas not installed")


@pytest.mark.asyncio
@pandas_only
async def test_pandas_from_records(client):
    rs = await client.execute("SELECT 1, 'two', 3.0")
    data_frame = pandas.DataFrame.from_records(rs.rows)
    assert data_frame.shape == (1, 3)


@pytest.mark.asyncio
@pandas_only
async def test_pandas_ctor(client):
    rs = await client.execute("SELECT 1 AS one, 'two' AS two, 3.0 AS three")
    data_frame = pandas.DataFrame(rs)
    assert data_frame.shape == (1, 3)
    assert tuple(data_frame.columns) == ("one", "two", "three")
