import asyncio

import pytest

from wire_rpc.transports.http import HttpServerTransport


@pytest.fixture
async def transport():
    transport = HttpServerTransport("127.0.0.1", 0)
    await transport.connect()
    yield transport
    await transport.close()


async def test_cancelled_http_caller_cannot_leak_its_response_to_next_caller(transport, request_factory):
    abandoned = asyncio.create_task(transport._handle(request_factory(b"private request")))
    await transport.recv()
    abandoned.cancel()
    with pytest.raises(asyncio.CancelledError):
        await abandoned
    # A response completed after disconnect must be discarded, not queued for B.
    await transport.send(b"private response")
    next_call = asyncio.create_task(transport._handle(request_factory(b"next request")))
    try:
        await transport.recv()
        await transport.send(b"next response")
        response = await next_call
        assert response.body == b"next response", "a disconnected user's response must never be delivered to another HTTP caller"
    finally:
        next_call.cancel()
        await asyncio.gather(next_call, return_exceptions=True)


async def test_second_recv_cannot_overwrite_an_unanswered_request(transport, request_factory):
    first = asyncio.create_task(transport._handle(request_factory(b"first")))
    await transport.recv()
    second = asyncio.create_task(transport._handle(request_factory(b"second")))
    try:
        with pytest.raises(RuntimeError):
            await transport.recv()
    finally:
        first.cancel()
        second.cancel()
        await asyncio.gather(first, second, return_exceptions=True)


async def test_unsolicited_send_cannot_poison_a_future_http_response(transport):
    with pytest.raises(RuntimeError):
        await transport.send(b"unowned response")
