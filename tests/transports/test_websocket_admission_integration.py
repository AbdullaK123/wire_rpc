"""Adversarial checks against real sockets, without sleeps or fixed ports."""

import asyncio

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from wire_rpc.transports.http import HttpServerTransport
from wire_rpc.transports.tcp import TcpMulticastServerTransport
from wire_rpc.transports.websocket import MulticastWsServerTransport


async def test_websocket_capacity_rejects_upgrade_before_a_second_socket_is_registered():
    transport = MulticastWsServerTransport(max_connections=1)
    app = web.Application()
    app.router.add_get("/ws", transport._handle_ws)
    async with TestServer(app) as server, aiohttp.ClientSession() as client:
        first = await client.ws_connect(server.make_url("/ws"))
        try:
            with pytest.raises(aiohttp.WSServerHandshakeError) as error:
                await client.ws_connect(server.make_url("/ws"))
            assert error.value.status == 503, "capacity exhaustion must return a controlled refusal instead of admitting excess sockets or leaking a 500"
            assert len(transport._clients) == 1, "a failed upgrade must not replace or unregister the established peer"
        finally:
            await first.close()

