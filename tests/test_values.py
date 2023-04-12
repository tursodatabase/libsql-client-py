from datetime import datetime
import math
import pytest

async def _roundtrip(client, arg):
    rs = await client.execute("SELECT ?", [arg])
    return rs[0][0]
    assert rs[0][0] == expected

@pytest.mark.asyncio
async def test_string(client):
    assert await _roundtrip(client, "boomerang") == "boomerang"

@pytest.mark.asyncio
async def test_string_with_weird_characters(client):
    assert await _roundtrip(client, "a\n\r\t ") == "a\n\r\t "

@pytest.mark.asyncio
async def test_string_with_unicode(client):
    s = "žluťoučký kůň úpěl ďábelské ódy"
    assert await _roundtrip(client, s) == s

@pytest.mark.asyncio
async def test_int_zero(client):
    x = await _roundtrip(client, 0)
    assert isinstance(x, int) and x == 0

@pytest.mark.asyncio
async def test_float_zero(client):
    x = await _roundtrip(client, 0.0)
    assert isinstance(x, float) and x == 0

@pytest.mark.asyncio
async def test_integer(client):
    x = await _roundtrip(client, -2023)
    assert isinstance(x, int) and x == -2023

@pytest.mark.asyncio
async def test_big_integer(client):
    with pytest.raises(OverflowError):
        await _roundtrip(client, 2**100 + 42)

@pytest.mark.asyncio
async def test_float(client):
    assert await _roundtrip(client, 12.345) == 12.345

@pytest.mark.asyncio
async def test_infinity(client):
    assert await _roundtrip(client, math.inf) == math.inf
    assert await _roundtrip(client, -math.inf) == -math.inf

@pytest.mark.asyncio
async def test_nan(client):
    assert await _roundtrip(client, math.nan) is None

@pytest.mark.asyncio
async def test_bytes(client):
    b = bytes(range(256))
    assert await _roundtrip(client, b) == b

@pytest.mark.asyncio
async def test_none(client):
    assert await _roundtrip(client, None) is None

@pytest.mark.asyncio
async def test_bool(client):
    assert await _roundtrip(client, True) == 1
    assert await _roundtrip(client, False) == 0

@pytest.mark.asyncio
async def test_datetime(client):
    d = datetime.fromisoformat("2023-04-01T12:34:56+02:00")
    ts = 1680345296000
    assert await _roundtrip(client, d) == ts
