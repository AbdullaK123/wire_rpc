import asyncio
import pytest
from wire_rpc.transports.http import HttpServerTransport
from wire_rpc.transports.websocket import WsServerTransport,MulticastWsServerTransport
from wire_rpc.transports.tcp import TcpServerTransport,TcpMulticastServerTransport


@pytest.mark.parametrize('factory',[HttpServerTransport,WsServerTransport,MulticastWsServerTransport,TcpServerTransport,TcpMulticastServerTransport])
async def test_occupied_port_fails_context_entry_instead_of_hiding_startup_failure(factory):
    occupied=await asyncio.start_server(lambda r,w:w.close(),'127.0.0.1',0)
    port=occupied.sockets[0].getsockname()[1]
    transport=factory(host='127.0.0.1',port=port)
    try:
        with pytest.raises(OSError):
            async with transport:
                pytest.fail('a failed bind must surface before the application can wait for requests')
    finally:
        occupied.close(); await occupied.wait_closed(); await transport.close()


@pytest.mark.parametrize('factory',[WsServerTransport,MulticastWsServerTransport,TcpMulticastServerTransport])
async def test_close_wakes_blocked_receive_and_releases_queue_payloads(factory):
    transport=factory()
    task=asyncio.create_task(transport.recv())
    await transport.close()
    with pytest.raises(ConnectionError):
        await task
    assert transport.stats['bytes'] == 0, 'shutdown must free queued payloads even when no peer ever connected'
