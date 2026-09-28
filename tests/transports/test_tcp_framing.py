import asyncio

import pytest

from wire_rpc.transports.errors import InvalidFrameSizeError
from wire_rpc.transports.tcp import TcpClientTransport, TcpServerTransport
from wire_rpc.transports.tcp._tcp_connection import TcpConnection


@pytest.mark.parametrize("factory", [TcpClientTransport, TcpServerTransport])
@pytest.mark.parametrize("length", [0, 9, 2**32 - 1])
async def test_invalid_header_closes_connection_before_payload_can_be_reinterpreted(factory, length, writer):
    transport = factory(max_frame_size=8, keep_alive=None)
    reader = asyncio.StreamReader()
    reader.feed_data(length.to_bytes(4, "big"))
    transport._connection = TcpConnection(reader, writer)
    if isinstance(transport, TcpServerTransport):
        transport._connected.set()
    try:
        with pytest.raises(InvalidFrameSizeError):
            await transport.recv()
        assert writer.closed, "invalid frame boundaries must kill the stream before attacker-controlled payload becomes another header"
    finally:
        await transport.close()


@pytest.mark.parametrize("factory", [TcpClientTransport, TcpServerTransport])
async def test_cancellation_after_partial_header_cannot_leave_reusable_desynchronized_stream(factory, writer):
    waiting = asyncio.Event()

    class Reader(asyncio.StreamReader):
        async def readexactly(self, n):
            if n == 3:
                waiting.set()
            return await super().readexactly(n)

    reader = Reader()
    reader.feed_data(b"\x00")
    transport = factory(keep_alive=None)
    transport._connection = TcpConnection(reader, writer)
    if isinstance(transport, TcpServerTransport):
        transport._connected.set()
    task = asyncio.create_task(transport.recv())
    await waiting.wait()
    task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await task
        assert writer.closed, "cancelling after consuming header bytes must prevent subsequent reads from using the wrong frame boundary"
    finally:
        await transport.close()
