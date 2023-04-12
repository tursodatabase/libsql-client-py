import libsql_client
import pytest

def test_closed(url):
    client = libsql_client.create_client(url)
    assert not client.closed
    client.close()
    assert client.closed

def test_context_manager(url):
    with libsql_client.create_client(url) as client:
        assert not client.closed
    assert client.closed

def test_close_twice(url):
    client = libsql_client.create_client(url)
    client.close()
    client.close()
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
