import libsql_client
import pytest


@pytest.fixture(params=[".close_ws", ".close_tcp"])
def trigger_net_error(request):
    return request.param


@pytest.mark.asyncio
async def test_execute(ws_client, trigger_net_error):
    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        await ws_client.execute(trigger_net_error)
    assert excinfo.value.code == "HRANA_WEBSOCKET_ERROR"

    rs = await ws_client.execute("SELECT 42")
    assert rs[0].astuple() == (42,)


@pytest.mark.asyncio
async def test_batch(ws_client, trigger_net_error):
    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        await ws_client.batch(["SELECT 42", trigger_net_error, "SELECT 24"])
    assert excinfo.value.code == "HRANA_WEBSOCKET_ERROR"

    rs = await ws_client.execute("SELECT 42")
    assert rs[0].astuple() == (42,)


@pytest.mark.asyncio
async def test_transaction(ws_client, trigger_net_error):
    txn = ws_client.transaction()

    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        await txn.execute(trigger_net_error)
    assert excinfo.value.code == "HRANA_WEBSOCKET_ERROR"

    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        await txn.commit()
    assert excinfo.value.code == "HRANA_WEBSOCKET_ERROR"

    rs = await ws_client.execute("SELECT 42")
    assert rs[0].astuple() == (42,)
