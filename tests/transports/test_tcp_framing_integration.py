"""Adversarial checks against real sockets, without sleeps or fixed ports."""

import asyncio

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from wire_rpc.transports.http import HttpServerTransport
from wire_rpc.transports.tcp import TcpMulticastServerTransport
from wire_rpc.transports.websocket import MulticastWsServerTransport


async def test_tcp_oversized_header_disconnects_peer_without_waiting_for_body():
    transport = TcpMulticastServerTransport(host="127.0.0.1", port=0, max_frame_size=8, keep_alive=None)
    await transport.connect()
    port = transport._server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write((9).to_bytes(4, "big"))
        await writer.drain()
        assert await reader.read() == b"", "an oversized frame must terminate the actual socket without allocating or waiting for its body"
    finally:
        writer.close()
        await writer.wait_closed()
        await transport.close()

