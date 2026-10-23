"""Closing a TCP server must not wait on the clients it is about to close."""
import asyncio
import time

import pytest

from wire_rpc.transports.tcp import (
    TcpClientTransport,
    TcpMulticastServerTransport,
    TcpServerTransport,
)


def _port(server) -> int:
    return server._server.sockets[0].getsockname()[1]


async def _timed_close(server) -> float:
    started = time.monotonic()
    async with asyncio.timeout(10):
        await server.close()
    return time.monotonic() - started


async def test_single_client_server_closes_promptly_with_an_idle_client_connected():
    server = TcpServerTransport(host='127.0.0.1', port=0, keep_alive=None, shutdown_timeout=5)
    await server.__aenter__()
    client = TcpClientTransport(host='127.0.0.1', port=_port(server), keep_alive=None)
    await client.connect()
    try:
        await client.send(b'hi')
        assert await server.recv() == b'hi'
        # No recv is pending now, so nothing else will close the connection for us.
        elapsed = await _timed_close(server)
        assert elapsed < 1, ('close must close its own client before waiting for asyncio.Server, '
                             f'whose wait_closed() waits for every connection; took {elapsed:.1f}s')
        with pytest.raises((ConnectionError, asyncio.IncompleteReadError, TimeoutError)):
            async with asyncio.timeout(2):
                await client.recv()
    finally:
        await client.close()


async def test_multicast_server_closes_promptly_with_idle_clients_connected():
    server = TcpMulticastServerTransport(host='127.0.0.1', port=0, keep_alive=None, shutdown_timeout=5)
    await server.__aenter__()
    clients = [TcpClientTransport(host='127.0.0.1', port=_port(server), keep_alive=None) for _ in range(3)]
    try:
        for client in clients:
            await client.connect()
        async with asyncio.timeout(5):
            while server.stats['connections'] < len(clients):
                await asyncio.sleep(0.01)
        elapsed = await _timed_close(server)
        assert elapsed < 1, ('close must not wait the full shutdown_timeout on readers that only end '
                             f'when their connection closes; took {elapsed:.1f}s')
    finally:
        for client in clients:
            await client.close()


async def test_server_close_is_bounded_even_if_a_connection_never_drops(monkeypatch):
    server = TcpServerTransport(host='127.0.0.1', port=0, keep_alive=None, shutdown_timeout=0.5)
    await server.__aenter__()
    asyncio_server = server._server
    assert asyncio_server is not None
    never = asyncio.get_running_loop().create_future()

    async def wait_forever():
        await never
    monkeypatch.setattr(asyncio_server, 'wait_closed', wait_forever)
    elapsed = await _timed_close(server)
    assert elapsed < 2, f'waiting for asyncio.Server must be bounded by shutdown_timeout; took {elapsed:.1f}s'
    never.cancel()


async def test_cancelling_one_closer_does_not_cancel_another():
    from wire_rpc.transports.tcp._tcp_connection import TcpConnection
    accepted: asyncio.Future = asyncio.get_running_loop().create_future()
    server = await asyncio.start_server(lambda r, w: accepted.set_result((r, w)), '127.0.0.1', 0)
    _, client_writer = await asyncio.open_connection('127.0.0.1', server.sockets[0].getsockname()[1])
    try:
        reader, writer = await accepted
        connection = TcpConnection(reader=reader, writer=writer, close_timeout=0.5)
        # A full send buffer to a peer that never reads keeps the graceful close pending.
        writer.write(b'x' * 50_000_000)
        first = asyncio.create_task(connection.close())
        await asyncio.sleep(0.05)
        second = asyncio.create_task(connection.close())
        await asyncio.sleep(0.05)
        first.cancel()
        results = await asyncio.gather(first, second, return_exceptions=True)
        assert isinstance(results[0], asyncio.CancelledError), 'the task that was cancelled must see it'
        assert results[1] is None, ('StreamWriter.wait_closed() shares one future between callers; cancelling one '
                                    f'caller must not cancel the others (got {results[1]!r})')
        assert writer.transport.is_closing(), 'the second closer must still finish closing the connection'
    finally:
        client_writer.close()
        server.close()
