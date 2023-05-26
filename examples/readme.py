import asyncio
import libsql_client
import os


async def main():
    url = os.getenv("URL", "file:local.db")
    async with libsql_client.create_client(url) as client:
        await client.batch(
            [
                """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """,
                """
            INSERT INTO users (email) VALUES
                ('alice@libsql.org'),
                ('bob@example.com')
            """,
            ]
        )

        result_set = await client.execute("SELECT * from users")
        print(len(result_set.rows), "rows")
        for row in result_set.rows:
            print(row)


asyncio.run(main())
