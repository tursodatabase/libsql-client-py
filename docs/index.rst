=====================
Python SDK for libSQL
=====================

This is the Python SDK for libSQL. You can either connect to a local SQLite database or to a remote libSQL
server (`sqld <https://github.com/libsql/sqld>`_).

------------
Installation
------------

::

   pip install libsql-client

---------------
Getting Started
---------------

Connecting to a local SQLite database::

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

To connect to a remote libSQL server, just change the URL::

   url = "ws://localhost:8080"

-----------------
Python DB API 2.0
-----------------

In addition to the ``libsql_client`` API that is shared among all languages,
we offer a synchronous client that implements
`DB API 2.0 (PEP-249) <https://peps.python.org/pep-0249>`_ and is targeted
at 1:1 compatibility with :py:mod:`sqlite3`. You can just replace your import
and it will keep working::

   # original code:
   #    import sqlite3
   # just change the import:
   from libsql_client import dbapi2 as sqlite3

   # the rest of your code remains the same:
   database = os.getenv("DATABASE")
   con = sqlite3.connect(database)
   conn.executescript("""
       CREATE TABLE IF NOT EXISTS users (
           id INTEGER PRIMARY KEY,
           email TEXT NOT NULL UNIQUE
       );
   """)
   cursor = conn.executemany(
       "INSERT INTO users (email) VALUES (?)",
       [(f"sql{i}@libsql.org",) for i in range(10)]
   )
   print(f"inserted rowid: {cursor.lastrowid}, affected: {cursor.rowcount}")
   for row in cursor.execute("SELECT * FROM users"):
       print(row)


Alternatively you don't need to change your code and just use our wrapper
based on ``$PYTHONPATH`` injection. With this every usage of ``sqlite3``
will be redirected to ``libsql_client.dbapi2``:

.. code-block:: console

   $ python -m libsql_client.dbapi2 my-app-using-sqlite my-arg-1 ...


-----------------
Table of Contents
-----------------

.. toctree::
   :maxdepth: 1

   reference
   dbapi2
