import os
import uuid
import pytest
from wire_rpc.auth.sessions import InMemorySessionStore, SQLiteSessionStore, RedisSessionStore, PostgresSessionStore


@pytest.fixture(params=['memory', 'sqlite', 'redis', 'postgres'])
async def stores(request, tmp_path):
    """Two independent adapters sharing canonical state; services are mandatory when configured."""
    kind = request.param
    first: InMemorySessionStore | SQLiteSessionStore | RedisSessionStore | PostgresSessionStore
    second: InMemorySessionStore | SQLiteSessionStore | RedisSessionStore | PostgresSessionStore
    namespace = 'test-' + uuid.uuid4().hex
    if kind == 'memory':
        first = InMemorySessionStore(max_sessions=2)
        yield first, first
    elif kind == 'sqlite':
        path = str(tmp_path / 'sessions.sqlite')
        first, second = SQLiteSessionStore(path, max_sessions=2), SQLiteSessionStore(path, max_sessions=2)
        await first.startup(); await second.startup()
        try:
            yield first, second
        finally:
            await first.shutdown(); await second.shutdown()
    elif kind == 'redis':
        url = os.getenv('WIRE_TEST_REDIS_URL')
        if not url:
            pytest.skip('Set WIRE_TEST_REDIS_URL to run real Redis contracts')
        from redis.asyncio import Redis
        client = Redis.from_url(url, socket_timeout=5)
        first = RedisSessionStore(client, namespace=namespace, max_sessions=2)
        second = RedisSessionStore(client, namespace=namespace, max_sessions=2)
        try:
            yield first, second
        finally:
            await client.delete(*first._keys)
            await client.aclose()
    else:
        url = os.getenv('WIRE_TEST_POSTGRES_DSN')
        if not url:
            pytest.skip('Set WIRE_TEST_POSTGRES_DSN to run real PostgreSQL contracts')
        import asyncpg
        pool = await asyncpg.create_pool(url, min_size=1, max_size=4, command_timeout=5)
        first = PostgresSessionStore(pool, namespace=namespace, max_sessions=2)
        second = PostgresSessionStore(pool, namespace=namespace, max_sessions=2)
        await first.startup(); await second.startup()
        try:
            yield first, second
        finally:
            await pool.execute('DELETE FROM wire_rpc_sessions WHERE namespace=$1', namespace)
            await pool.close()
