from libsql_client import dbapi2
import datetime
import os
import logging


def show_rows(cursor, pfx="|"):
    table = []

    table.append([repr(col[0]) for col in cursor.description])
    sizes = [len(c) for c in table[0]]

    for row in cursor:
        r = [repr(cell) for cell in row]
        table.append(r)
        sizes = [max(prev, len(c)) for prev, c in zip(sizes, r)]

    fmts = ["%%-%ds" % size for size in sizes]
    for i, row in enumerate(table):
        print(pfx + " | ".join(fmt % value for fmt, value in zip(fmts, row)))
        if i == 0:
            print(pfx + "-+-".join("-" * size for size in sizes))
    print()


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "WARNING"))
    url = os.getenv("URL", "file:local.db")
    conn = dbapi2.connect(
        url,
        uri=True,
        detect_types=dbapi2.PARSE_COLNAMES | dbapi2.PARSE_DECLTYPES,
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL UNIQUE
        )
        """
    )

    print("\n# iterdump")
    for stmt in conn.iterdump():
        print(f"   {stmt}")

    print("\n# execute")

    print("\n>>> execute: no arguments, multiple values (rowcount)")
    cursor = conn.execute(
        """
        INSERT INTO users (email) VALUES
            ('alice@libsql.org'),
            ('bob@example.com')
        """
    )
    print(f"inserted rowid: {cursor.lastrowid}, affected: {cursor.rowcount}")

    print("\n>>> execute: with positional arguments")
    cursor = conn.execute("INSERT INTO users (email) VALUES (?)", ("rob@other.com",))
    print(f"inserted rowid: {cursor.lastrowid}, affected: {cursor.rowcount}")

    print("\n>>> execute: with named arguments")
    cursor = conn.execute(
        "INSERT INTO users (email) VALUES (@email)",
        {"email": "sql@other.com"},
    )
    print(f"inserted rowid: {cursor.lastrowid}, affected: {cursor.rowcount}")

    print("\n# executemany")

    print("\n>>> executemany: with positional arguments")
    cursor = conn.executemany(
        "INSERT INTO users (email) VALUES (?)",
        (
            ("sql2@sql.com",),
            ("sql3@sql.com",),
        ),
    )
    print(f"inserted rowid: {cursor.lastrowid}, affected: {cursor.rowcount}")

    print("\n# executescript")
    conn.executescript(
        """
        INSERT INTO users (email) VALUES ('script1@server.com');
        INSERT INTO users (email) VALUES ('script2@server.com');
        INSERT INTO users (email) VALUES ('script3@server.com');
        """,
    )

    print("\n# transactions")
    try:
        with conn as cursor:
            cursor.execute("BEGIN")
            cursor.execute("INSERT INTO users (email) VALUES ('bug!')")
            cursor.execute("INSERT INTO users (email) VALUES ('bug!')")
    except dbapi2.IntegrityError as e:
        print("Got expected exception, should rollback transaction:", e)

    print("\n# fetching: rows and description")
    cursor = conn.execute("SELECT * from users")

    show_rows(cursor)

    # example based on:
    # https://docs.python.org/3/library/sqlite3.html#default-adapters-and-converters
    print("\n# Type converters:")
    cursor.execute("create table test(d date, ts timestamp)")
    today = datetime.date.today()
    now = datetime.datetime.now()

    print("\n>>> based on column decltype:")
    cursor.execute("insert into test(d, ts) values (?, ?)", (today, now))
    cursor.execute("select d, ts from test")
    show_rows(cursor)
    conn.execute("DROP TABLE test")

    print("\n>>> based on column names: 'd [date]', 'ts [timestamp]'")
    cursor.execute(
        """
        select current_date as "d [date]",
        current_timestamp as "ts [timestamp]"
    """
    )
    show_rows(cursor)

    # https://docs.python.org/3/library/sqlite3.html#how-to-convert-sqlite-values-to-custom-python-types
    class Point:
        def __init__(self, x, y):
            self.x, self.y = x, y

        def __repr__(self):
            return f"Point({self.x}, {self.y})"

    def adapt_point(point):
        return f"{point.x};{point.y}"

    def convert_point(s):
        x, y = list(map(float, s.split(b";")))
        return Point(x, y)

    # Register the adapter and converter
    dbapi2.register_adapter(Point, adapt_point)
    dbapi2.register_converter("point", convert_point)
    p = Point(4.0, -3.2)
    cur = conn.execute("CREATE TABLE test(p point)")

    print("\n>>> inserting custom type: Point")
    cur.execute("INSERT INTO test(p) VALUES(?)", (p,))
    cur.execute("SELECT p FROM test")
    show_rows(cur)
    conn.execute("DROP TABLE test")

    cur = conn.execute("CREATE TABLE test(p)")
    cur.execute("INSERT INTO test(p) VALUES(?)", (p,))
    cur.execute('SELECT p AS "p [point]" FROM test')
    show_rows(cur)
    conn.execute("DROP TABLE test")

    conn.close()


main()
