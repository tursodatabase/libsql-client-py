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
Table of Contents
-----------------

.. toctree::
   :maxdepth: 0

   reference
