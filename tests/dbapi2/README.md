# tests.dbapi2

These tests are a copy of the official C Python standard library
test suite `Lib/test/test_sqlite3` with minimal modifications in
order to validate `libsql_client.dbapi2` conformance.

It's based on https://github.com/python/cpython/releases/tag/v3.11.3

## libsql_client_helpers.py

This file re-exports all of `libsql_client.dbapi2` replacing its
`connect()` function, changing all `":memory:"` databases
with `$URL` environment variable, which defaults to
`ws://localhost:8080`, which is the default address for
https://github.com/libsql/hrana-test-server

Since this DB may be persistent, say a real
https://github.com/libsql/sqld, then we list all user-schema
(table, indexes) and drop them before proceeding.
