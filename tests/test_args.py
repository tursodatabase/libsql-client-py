import pytest


@pytest.mark.asyncio
async def test_pos_args_implicit_index(client):
    rs = await client.execute("SELECT ?, ?", ["one", "two"])
    assert tuple(rs.rows[0]) == ("one", "two")


@pytest.mark.asyncio
async def test_pos_args_explicit_index(client):
    rs = await client.execute("SELECT ?2, ?3, ?1", ["one", "two", "three"])
    assert tuple(rs.rows[0]) == ("two", "three", "one")


@pytest.mark.asyncio
async def test_pos_args_explicit_index_with_holes(client):
    rs = await client.execute("SELECT ?3, ?1", ["one", "two", "three"])
    assert tuple(rs.rows[0]) == ("three", "one")


@pytest.mark.asyncio
async def test_pos_args_implicit_and_explicit_index(client):
    rs = await client.execute("SELECT ?2, ?, ?3", ["one", "two", "three"])
    assert tuple(rs.rows[0]) == ("two", "three", "three")


@pytest.mark.asyncio
@pytest.mark.parametrize("sign", [":", "@", "$"])
async def test_named_args(client, sign):
    rs = await client.execute(
        f"SELECT {sign}b, {sign}a", {"a": "one", f"{sign}b": "two"}
    )
    assert tuple(rs.rows[0]) == ("two", "one")


@pytest.mark.asyncio
@pytest.mark.parametrize("sign", [":", "@", "$"])
async def test_named_args_used_multiple_times(client, sign):
    rs = await client.execute(
        f"SELECT {sign}b, {sign}a, {sign}b || {sign}a", {"a": "one", f"{sign}b": "two"}
    )
    assert tuple(rs.rows[0]) == ("two", "one", "twoone")


@pytest.mark.asyncio
@pytest.mark.parametrize("sign", [":", "@", "$"])
async def test_named_args_and_pos_args(client, sign):
    rs = await client.execute(
        f"SELECT {sign}b, {sign}a, ?1", {"a": "one", f"{sign}b": "two"}
    )
    assert tuple(rs.rows[0]) == ("two", "one", "two")
