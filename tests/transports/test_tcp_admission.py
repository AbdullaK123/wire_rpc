import asyncio

from wire_rpc.transports.tcp import TcpServerTransport


async def test_second_client_cannot_replace_connection_receiving_private_replies(writer):
    transport = TcpServerTransport(keep_alive=None)
    second = type(writer)()
    try:
        await transport._handle_client(asyncio.StreamReader(), writer)
        original = transport._connection
        await transport._handle_client(asyncio.StreamReader(), second)
        assert transport._connection is original, "a second peer must never redirect replies away from the established client"
        assert second.closed, "rejected peers must not retain open sockets"
        assert not writer.closed, "rejecting a second peer must preserve the original client's connection"
    finally:
        await transport.close()
        second.close()


async def test_authentication_in_progress_reserves_the_only_connection_slot(writer):
    entered = asyncio.Event()
    release = asyncio.Event()

    class Auth:
        calls = 0

        async def verify(self, connection):
            self.calls += 1
            entered.set()
            await release.wait()
            return "user"

    auth = Auth()
    transport = TcpServerTransport(auth=auth, keep_alive=None)
    second = type(writer)()
    first_task = asyncio.create_task(transport._handle_client(asyncio.StreamReader(), writer))
    await entered.wait()
    # Releasing via the event loop allows the vulnerable second auth call to
    # finish too, without a wall-clock race or hanging the test.
    asyncio.get_running_loop().call_soon(release.set)
    try:
        await transport._handle_client(asyncio.StreamReader(), second)
        await first_task
        assert auth.calls == 1, "concurrent handshakes must not bypass single-client admission or amplify authentication work"
        assert second.closed, "capacity must be reserved before the first authentication await"
    finally:
        release.set()
        await first_task
        await transport.close()
        writer.close()
        second.close()
