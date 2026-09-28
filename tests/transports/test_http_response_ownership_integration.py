from tests.helpers import listening_port
"""Adversarial checks against real sockets, without sleeps or fixed ports."""

import asyncio

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from wire_rpc.transports.http import HttpServerTransport
from wire_rpc.transports.tcp import TcpMulticastServerTransport
from wire_rpc.transports.websocket import MulticastWsServerTransport


async def test_http_cancelled_dispatch_does_not_cross_wire_response_between_clients():
    transport = HttpServerTransport("127.0.0.1", 0)
    await transport.connect()
    port = listening_port(transport._runner)
    try:
        async with aiohttp.ClientSession() as client:
            abandoned = asyncio.create_task(client.post(f"http://127.0.0.1:{port}/rpc", data=b"private"))
            assert await transport.recv() == b"private", "the cancellation scenario must target the request currently owned by dispatch"
            abandoned.cancel()
            with pytest.raises(asyncio.CancelledError):
                await abandoned
            # The response must remain owned by A regardless of whether aiohttp
            # has observed its disconnection when the application finishes.
            await transport.send(b"private reply")
            successor = asyncio.create_task(client.post(f"http://127.0.0.1:{port}/rpc", data=b"public"))
            assert await transport.recv() == b"public", "subsequent requests must remain dispatchable after peer cancellation"
            await transport.send(b"public reply")
            async with await successor as response:
                assert await response.read() == b"public reply", "real HTTP clients must not receive another caller's abandoned response"
    finally:
        await transport.close()

