import uuid
from collections.abc import Iterator
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer
import pytest
from wire_rpc.auth.sessions import InMemorySessionStore, SQLiteSessionStore, RedisSessionStore, PostgresSessionStore


@pytest.fixture(scope='session')
def redis_url() -> Iterator[str]:
    with RedisContainer('redis:7') as container:
        host = container.get_container_host_ip()
        if ':' in host:
            host = f'[{host}]'
        yield f'redis://{host}:{container.get_exposed_port(6379)}/0'


@pytest.fixture(scope='session')
def postgres_dsn() -> Iterator[str]:
    with PostgresContainer('postgres:17', driver=None) as container:
        yield container.get_connection_url()


@pytest.fixture(params=[
    'memory', 'sqlite',
    pytest.param('redis', marks=[pytest.mark.integration, pytest.mark.timeout(180)]),
    pytest.param('postgres', marks=[pytest.mark.integration, pytest.mark.timeout(180)]),
])
async def stores(request, tmp_path):
    """Two independent adapters sharing canonical state; containers are mandatory for shared backends."""
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
        url = request.getfixturevalue('redis_url')
        from redis.asyncio import Redis
        client = Redis.from_url(url, socket_timeout=5)
        first = RedisSessionStore(client, namespace=namespace, max_sessions=2)
        second = RedisSessionStore(client, namespace=namespace, max_sessions=2)
        try:
            yield first, second
        finally:
            try:
                await client.delete(*first._keys)
            finally:
                await client.aclose()
    else:
        url = request.getfixturevalue('postgres_dsn')
        import asyncpg
        pool = await asyncpg.create_pool(url, min_size=1, max_size=4, command_timeout=5)
        first = PostgresSessionStore(pool, namespace=namespace, max_sessions=2)
        second = PostgresSessionStore(pool, namespace=namespace, max_sessions=2)
        try:
            await first.startup(); await second.startup()
            yield first, second
        finally:
            try:
                await pool.execute('DELETE FROM wire_rpc_sessions WHERE namespace=$1', namespace)
            finally:
                await pool.close()
