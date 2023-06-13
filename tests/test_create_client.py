import pytest

import libsql_client


@pytest.mark.asyncio
async def test_closed(url):
    client = libsql_client.create_client(url)
    assert not client.closed
    await client.close()
    assert client.closed


@pytest.mark.asyncio
async def test_context_manager(url):
    async with libsql_client.create_client(url) as client:
        assert not client.closed
    assert client.closed


@pytest.mark.asyncio
async def test_close_twice(url):
    client = libsql_client.create_client(url)
    await client.close()
    await client.close()
    assert client.closed


def test_error_url_scheme_not_supported():
    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        libsql_client.create_client("ftp://localhost")
    assert excinfo.value.code == "URL_SCHEME_NOT_SUPPORTED"
    assert "ftp" in str(excinfo.value)


def test_error_url_param_not_supported():
    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        libsql_client.create_client("ws://localhost?foo=bar")
    assert excinfo.value.code == "URL_PARAM_NOT_SUPPORTED"
    assert "foo" in str(excinfo.value)


def test_error_url_scheme_incompatible_with_tls():
    urls = [
        "ws://localhost?tls=1",
        "wss://localhost?tls=0",
        "http://localhost?tls=1",
        "https://localhost?tls=0",
    ]
    for url in urls:
        with pytest.raises(libsql_client.LibsqlError) as excinfo:
            libsql_client.create_client(url)
        assert excinfo.value.code == "URL_INVALID"
        assert "tls" in str(excinfo.value)


def test_error_invalid_value_of_tls():
    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        libsql_client.create_client("libsql://localhost?tls=foo")
    assert excinfo.value.code == "URL_INVALID"
    assert "foo" in str(excinfo.value)


def test_missing_port_in_libsql_url_without_tls():
    with pytest.raises(libsql_client.LibsqlError) as excinfo:
        libsql_client.create_client("libsql://localhost?tls=0")
    assert excinfo.value.code == "URL_INVALID"
    assert "port" in str(excinfo.value)
