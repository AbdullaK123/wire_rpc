from tests.helpers import UnusedAuthenticator
import asyncio
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from wire_rpc.transports.http import HttpServerTransport


async def test_close_does_not_shutdown_owned_authentication_twice():
    class Auth(UnusedAuthenticator):
        startup = AsyncMock()
        shutdown = AsyncMock()
        login = AsyncMock()
        logout = AsyncMock()

    auth = Auth()
    transport = HttpServerTransport("127.0.0.1", 0, auth=auth)
    await transport.connect()
    await transport.close()
    await transport.shutdown()
    assert auth.shutdown.await_count == 1, "double shutdown can release an authentication store already used or closed elsewhere in app teardown"


async def test_close_wakes_receiver_with_no_requests():
    transport = HttpServerTransport("127.0.0.1", 0)
    task = asyncio.create_task(transport.recv())
    try:
        await transport.close()
        with pytest.raises(ConnectionError):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_close_releases_request_waiting_for_application_response(request_factory):
    transport = HttpServerTransport("127.0.0.1", 0)
    task = asyncio.create_task(transport._handle(request_factory(b"pending")))
    await transport.recv()
    await transport.close()
    with pytest.raises(web.HTTPServiceUnavailable):
        await task
    assert not transport._pending, "shutdown must release outstanding HTTP requests instead of retaining response futures"

