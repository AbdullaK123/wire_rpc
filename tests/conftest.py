from unittest.mock import Mock

import pytest


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
