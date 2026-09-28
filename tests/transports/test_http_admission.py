import asyncio

import pytest
from aiohttp import web

from wire_rpc.transports.http import HttpServerTransport


async def test_capacity_is_reserved_before_authentication_await(request_factory):
    entered = asyncio.Event()
    release = asyncio.Event()

    class Auth:
        async def verify(self, request):
            entered.set()
            await release.wait()
            return "user"

    transport = HttpServerTransport("127.0.0.1", 0, auth=Auth(), max_pending_requests=1)
    first = asyncio.create_task(transport._handle(request_factory(b"first")))
    await entered.wait()
    try:
        with pytest.raises(web.HTTPServiceUnavailable):
            await transport._handle(request_factory(b"second"))
        assert len(transport._pending) == 1, "slow authentication must not allow unbounded pending request allocation"
    finally:
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        await transport.close()


async def test_cancelled_queued_request_is_not_dispatched_and_releases_capacity(request_factory):
    transport = HttpServerTransport("127.0.0.1", 0, max_pending_requests=1)
    first = asyncio.create_task(transport._handle(request_factory(b"abandoned")))
    await transport._ready.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    second = asyncio.create_task(transport._handle(request_factory(b"live")))
    try:
        data = await transport.recv()
        assert data == b"live", "a request cancelled before dispatch must not execute a stale mutation"
        await transport.send(b"response")
        await second
        assert not transport._queue, "cancelled queue entries must not accumulate beyond configured admission limits"
    finally:
        second.cancel()
        await asyncio.gather(second, return_exceptions=True)
        await transport.close()
