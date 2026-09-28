import asyncio
from wire_rpc.transports.tcp import TcpMulticastServerTransport
from wire_rpc.transports.tcp._tcp_connection import TcpConnection


async def test_backpressured_peer_cannot_delay_broadcast_to_fast_peer(writer):
    slow_entered=asyncio.Event(); slow_release=asyncio.Event(); fast_delivered=asyncio.Event()
    slow=type(writer)(); fast=type(writer)()
    async def slow_drain():
        slow_entered.set(); await slow_release.wait()
    async def fast_drain():
        fast_delivered.set()
    slow.drain=slow_drain; fast.drain=fast_drain
    transport=TcpMulticastServerTransport(keep_alive=None)
    transport._clients={'slow':TcpConnection(asyncio.StreamReader(),slow),'fast':TcpConnection(asyncio.StreamReader(),fast)}
    task=asyncio.create_task(transport.broadcast(b'x'))
    try:
        await slow_entered.wait(); await fast_delivered.wait()
        assert not slow_release.is_set(), 'one non-reading peer must not serialize broadcasts to every other client'
        assert fast.data == b'\x00\x00\x00\x01x', 'the independent recipient must receive the complete frame while the slow peer is blocked'
    finally:
        slow_release.set(); await task; await transport.close()
