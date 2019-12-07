# violet: An asyncio background job library
# Copyright 2019, elixi.re Team and the violet contributors
# SPDX-License-Identifier: LGPL-3.0

from typing import Any
import json


async def pg_set_json(con):
    """Set JSON and JSONB codecs for an asyncpg connection."""
    await con.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog",
    )

    await con.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog",
    )


async def execute_with_json(pool, *args, **kwargs) -> Any:
    """Execute SQL with support for json and jsonb."""
    async with pool.acquire() as conn:
        await pg_set_json(conn)
        return await conn.execute(*args, **kwargs)


async def fetch_with_json(pool, *args, **kwargs) -> Any:
    """Execute SQL with support for json and jsonb."""
    async with pool.acquire() as conn:
        await pg_set_json(conn)
        return await conn.fetch(*args, **kwargs)


async def fetchrow_with_json(pool, *args, **kwargs) -> Any:
    """Execute SQL with support for json and jsonb."""
    async with pool.acquire() as conn:
        await pg_set_json(conn)
        return await conn.fetchrow(*args, **kwargs)
