from collections.abc import Iterator
from unittest.mock import Mock

import pytest
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer


class MemoryWriter:
    """Only the StreamWriter API used by the transport; no actual socket."""

    def __init__(self):
        self.data = bytearray()
        self.closed = False

    def write(self, data):
        if self.closed:
            raise ConnectionError("closed")
        self.data.extend(data)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    def is_closing(self):
        return self.closed

    async def wait_closed(self):
        pass


@pytest.fixture
def writer():
    return MemoryWriter()


@pytest.fixture
def request_factory():
    def make(data):
        request = Mock()

        async def read():
            return data

        request.read = read
        return request
    return make


# Shared Docker backends; one container per test session, used by auth and e2e suites.
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
