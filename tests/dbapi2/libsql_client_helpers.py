import os
import re

from libsql_client.dbapi2 import *


dburi = os.getenv("URL", "ws://localhost:8080")
_orig_connect = connect
_system_schema_re = re.compile("^(_|sqlite_|libsql_)")


def drop_user_schemas(conn):
    try:
        cur = conn.execute("SELECT name, type FROM sqlite_schema")
    except Exception as e:
        print(f"WARNING: could not list sqlite_schema: {e}")
        return

    for name, kind in cur.fetchall():
        if _system_schema_re.match(name):
            continue
        try:
            conn.execute(f"drop {kind} '{name}'")
        except Exception as e:
            print(f"WARNING: could not drop {kind} {name}: {e}")


def connect(database, *args, **kwargs):
    drop_schema = False
    if database == ":memory:":
        # most of the test suite uses :memory: for testing, so redirect
        # those to our test server, but drop all user schema
        database = dburi
        kwargs["uri"] = True
        drop_schema = True
    elif os.path.isdir(database):
        # tests that open a folder expect to fail
        database = "wss://127.0.0.1:1"
        kwargs["uri"] = True
    else:
        # keep intact, but they will be using sqlite3.connect and not
        # our code, nevertheless this helps to test our compatibility
        pass

    conn = _orig_connect(database, *args, **kwargs)
    if drop_schema:
        drop_user_schemas(conn)
    return conn
