# Python SDK for libSQL

This is the source repository of the Python SDK for libSQL. You can either connect to a local SQLite database or to a remote libSQL server ([sqld][sqld]).

[sqld]: https://github.com/libsql/sqld

## Installation

```
pip install libsql-client
```

## Getting Started

Connecting to a local SQLite database:

```python
import asyncio
import libsql_client

async def main():
    url = "file:local.db"
    async with libsql_client.create_client(url) as client:
        result_set = await client.execute("SELECT * from users")
        print(len(result_set.rows), "rows")
        for row in result_set.rows:
            print(row)

asyncio.run(main())
```

To connect to a remote libSQL server ([sqld][sqld]), just change the URL:

```python
url = "ws://localhost:8080"
```

## Supported URLs

The client can connect to the database using different methods depending on the scheme (protocol) of the passed URL:

* `file:` connects to a local SQLite database (using the builtin `sqlite3` package)
  * `file:/absolute/path` or `file:///absolute/path` is an absolute path on local filesystem
  * `file:relative/path` is a relative path on local filesystem
  * (`file://path` is not a valid URL)
* `ws:` or `wss:` connect to sqld using WebSockets (the Hrana protocol).
* `http:` or `https:` connect to sqld using HTTP. The `transaction()` API is not available in this case.
* `libsql:` is equivalent to `wss:`.

## Contributing to this package

First, please install Python and [Poetry][poetry]. To install all dependencies for local development to a
virtual environment, run:

[poetry]: https://python-poetry.org/

```
poetry install --with dev
```

To run the tests, use:

```
poetry run pytest
```

To check types with MyPy, use:

```
poetry run mypy
```

## License

This project is licensed under the MIT license.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in `libsql-client` by you, shall be licensed as MIT, without any additional terms or conditions.
