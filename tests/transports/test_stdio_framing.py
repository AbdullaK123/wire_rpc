import asyncio

import pytest

from wire_rpc.transports.errors import InvalidFrameSizeError
from wire_rpc.transports.stdio import StdIoTransport, StdIoServerTransport


@pytest.fixture(params=[StdIoTransport, StdIoServerTransport])
async def transport(request, writer):
    instance = request.param()
    reader = asyncio.StreamReader()
    if isinstance(instance, StdIoTransport):
        instance.stdout = reader
        instance.stdin = writer
    else:
        instance.reader = reader
        instance.writer = writer
    return instance, reader, writer


@pytest.mark.parametrize("length", [0, 16 * 1024 * 1024 + 1, 2**32 - 1])
async def test_stdio_invalid_header_is_rejected_before_waiting_for_an_unbounded_body(transport, length):
    instance, reader, writer = transport
    reader.feed_data(length.to_bytes(4, "big"))
    # EOF proves the implementation rejects the header rather than trying to
    # consume an attacker-selected body and only discovering truncation later.
    reader.feed_eof()
    with pytest.raises(InvalidFrameSizeError):
        await instance.recv()
    assert writer.closed, "a subprocess with invalid framing must not retain a reusable protocol channel"


@pytest.mark.parametrize("data", [b"", b"x" * (16 * 1024 * 1024 + 1)], ids=["empty", "oversize"])
async def test_stdio_invalid_outbound_frame_does_not_write_a_partial_header(transport, data):
    instance, reader, writer = transport
    with pytest.raises(InvalidFrameSizeError):
        await instance.send(data)
    assert writer.data == b"", "local validation must run before any bytes corrupt the peer's frame stream"


async def test_stdio_truncated_body_closes_channel_instead_of_allowing_another_read(transport):
    instance, reader, writer = transport
    reader.feed_data(b"\x00\x00\x00\x05ab")
    reader.feed_eof()
    with pytest.raises(asyncio.IncompleteReadError):
        await instance.recv()
    assert writer.closed, "truncated subprocess output is a fatal protocol error, not a recoverable message boundary"
