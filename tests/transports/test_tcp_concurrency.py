import asyncio

import pytest

from wire_rpc.transports.tcp import TcpClientTransport, TcpServerTransport
from wire_rpc.transports.tcp._tcp_connection import TcpConnection


@pytest.fixture(params=[TcpClientTransport, TcpServerTransport])
async def transport(request, writer):
    instance = request.param(keep_alive=None)
    instance._connection = TcpConnection(asyncio.StreamReader(), writer)
    if isinstance(instance, TcpServerTransport):
        instance._connected.set()
    yield instance
    await instance.close()


async def test_concurrent_receivers_cannot_split_one_frames_header_and_body(transport):
    partial_header = asyncio.Event()
    second_started = asyncio.Event()

    class Reader(asyncio.StreamReader):
        async def readexactly(self, n):
            if n == 3:
                partial_header.set()
            return await super().readexactly(n)

    reader = Reader()
    transport._connection.reader = reader
    reader.feed_data(b"\x00")
    first = asyncio.create_task(transport.recv())
    await partial_header.wait()

    async def receive_second():
        second_started.set()
        return await transport.recv()

    second = asyncio.create_task(receive_second())
    await second_started.wait()
    reader.feed_data(b"\x00\x00\x01A\x00\x00\x00\x01B")
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert results == [b"A", b"B"], "overlapping reads must preserve complete frame ownership instead of consuming another receiver's header"


async def test_second_writer_cannot_advance_past_a_blocked_first_frame(transport):
    entered = asyncio.Event()
    release = asyncio.Event()
    second_started = asyncio.Event()
    writer = transport._connection.writer

    async def drain():
        entered.set()
        await release.wait()

    writer.drain = drain
    first = asyncio.create_task(transport.send(b"A"))
    await entered.wait()

    async def send_second():
        second_started.set()
        await transport.send(b"B")

    second = asyncio.create_task(send_second())
    await second_started.wait()
    try:
        assert writer.data == b"\x00\x00\x00\x01A", "a backpressured peer must not accumulate writes from callers bypassing the connection's write lock"
    finally:
        release.set()
        await asyncio.gather(first, second)
    assert writer.data == b"\x00\x00\x00\x01A\x00\x00\x00\x01B", "concurrent writes must arrive as two complete ordered frames"
