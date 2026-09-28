from tests.helpers import listening_port
import asyncio
from pathlib import Path
from wire_rpc.transports.websocket import MulticastWsServerTransport, WsClientTransport


async def test_repeated_socket_lifecycles_do_not_accumulate_tasks_or_file_descriptors():
    before_tasks=asyncio.all_tasks()
    fd_path=Path('/proc/self/fd')
    before_fds=len(list(fd_path.iterdir())) if fd_path.exists() else None
    for _ in range(20):
        server=MulticastWsServerTransport(host='127.0.0.1',port=0)
        await server.connect()
        port=listening_port(server._runner)
        client=WsClientTransport(f'ws://127.0.0.1:{port}/ws')
        try:
            await client.connect(); await client.send(b'queued and abandoned')
            await server.recv()
        finally:
            await client.close(); await server.close()
        assert server.stats['bytes'] == 0 and server.stats['connections'] == 0, 'every disconnect cycle must release queued payloads and connection ownership'
    checkpoint=asyncio.Event(); asyncio.get_running_loop().call_soon(checkpoint.set); await checkpoint.wait()
    assert not (asyncio.all_tasks()-before_tasks), 'repeated connect/close cycles must not leave background reader or cleanup tasks running'
    if before_fds is not None:
        assert len(list(fd_path.iterdir())) <= before_fds, 'repeated socket lifecycles must not leak file descriptors until the process exhausts its limit'
