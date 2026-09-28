import asyncio
from unittest.mock import Mock

import pytest
from aiohttp import web

from wire_rpc.transports import websocket as module


@pytest.fixture
def sockets(monkeypatch):
    created = []

    class Socket:
        def __init__(self, **kwargs):
            self.prepared = asyncio.Event()
            self.finished = asyncio.Event()
            created.append(self)

        async def prepare(self, request):
            self.prepared.set()

        def __aiter__(self):
            return self

        async def __anext__(self):
            await self.finished.wait()
            raise StopAsyncIteration

        async def close(self):
            self.finished.set()

    monkeypatch.setattr(module.web, "WebSocketResponse", Socket)
    return created


async def test_same_principal_connections_cannot_overwrite_each_others_routing_or_cleanup(sockets):
    class Auth:
        async def verify(self, request):
            return "same-user"

    transport = module.MulticastWsServerTransport(auth=Auth())
    first = asyncio.create_task(transport._handle_ws(Mock()))
    # Event-loop callbacks mark task progress without wall-clock sleeps.
    checkpoint = asyncio.Event()
    asyncio.get_running_loop().call_soon(checkpoint.set)
    await checkpoint.wait()
    second = asyncio.create_task(transport._handle_ws(Mock()))
    checkpoint.clear()
    asyncio.get_running_loop().call_soon(checkpoint.set)
    await checkpoint.wait()
    try:
        assert len(transport._clients) == 2, "two tabs logged in as one user need distinct routing IDs to avoid misdelivering private responses"
        sockets[0].finished.set()
        await first
        assert list(transport._clients.values()) == [sockets[1]], "closing one tab must not unregister the user's other live connection"
    finally:
        for socket in sockets:
            socket.finished.set()
        await asyncio.gather(first, second, return_exceptions=True)


@pytest.mark.parametrize("factory", [module.WsServerTransport, module.MulticastWsServerTransport])
async def test_pending_authentication_counts_toward_connection_capacity(factory, sockets):
    entered = asyncio.Event()
    release = asyncio.Event()

    class Auth:
        calls = 0

        async def verify(self, request):
            self.calls += 1
            entered.set()
            await release.wait()
            return "user"

    auth = Auth()
    kwargs = {"max_connections": 1} if factory is module.MulticastWsServerTransport else {}
    transport = factory(auth=auth, **kwargs)
    first = asyncio.create_task(transport._handle_ws(Mock()))
    await entered.wait()
    second = asyncio.create_task(transport._handle_ws(Mock()))
    checkpoint = asyncio.Event()
    asyncio.get_running_loop().call_soon(checkpoint.set)
    await checkpoint.wait()
    try:
        assert second.done(), "capacity exhaustion must reject before another authentication or upgrade can reserve server resources"
        with pytest.raises(web.HTTPServiceUnavailable):
            await second
        assert auth.calls == 1, "unauthenticated peers must count toward admission limits to prevent handshake floods"
    finally:
        first.cancel()
        second.cancel()
        await asyncio.gather(first, second, return_exceptions=True)



async def test_unicast_replacement_cannot_receive_a_disconnected_peers_queued_replies(sockets):
    transport = module.WsServerTransport()
    first = asyncio.create_task(transport._handle_ws(Mock()))
    await transport._connected.wait()
    sockets[0].finished.set()
    await first
    # The byte-only unicast API has no connection ID to associate old queued
    # requests or in-flight handlers with their original peer.
    second = asyncio.create_task(transport._handle_ws(Mock()))
    checkpoint = asyncio.Event()
    asyncio.get_running_loop().call_soon(checkpoint.set)
    await checkpoint.wait()
    try:
        assert second.done(), "replacing a unicast peer without response correlation could deliver the previous peer's private reply"
        with pytest.raises(web.HTTPServiceUnavailable):
            await second
    finally:
        second.cancel()
        await asyncio.gather(second, return_exceptions=True)
