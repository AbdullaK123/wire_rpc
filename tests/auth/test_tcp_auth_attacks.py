import asyncio
import hmac
from unittest.mock import AsyncMock, Mock
import pytest
from wire_rpc.auth import TcpTokenAuth, TcpTokenClientAuth, HmacChallengeAuth, HmacChallengeClientAuth
from wire_rpc.auth.tcp import _read


def frame(data):
    return len(data).to_bytes(4, 'big') + data


def reader_for(data):
    reader = asyncio.StreamReader()
    reader.feed_data(data); reader.feed_eof()
    return reader


@pytest.mark.parametrize('data', [b'\xff'*4, b'\0'*4, frame(b'wrong-version token'), frame(b'WRPC-TOKEN-1 \xff')])
async def test_malformed_token_handshakes_close_before_credential_backend(data, writer):
    validator = Mock(validate=AsyncMock(return_value='alice'))
    auth = TcpTokenAuth(validator, require_tls=False)
    assert await auth.verify((reader_for(data), writer)) is None, 'invalid handshake framing and encoding must never establish a TCP identity'
    assert validator.validate.await_count == 0 and writer.closed, 'malformed frames must close the unauthenticated connection before any backend work'


async def test_network_token_client_refuses_to_transmit_credentials_without_tls(writer):
    writer.get_extra_info = lambda name: None
    auth = TcpTokenClientAuth('VERY_SECRET')
    with pytest.raises(PermissionError):
        await auth.authenticate((reader_for(b''), writer))
    assert writer.data == b'' and writer.closed, 'client-side TLS enforcement must happen before a bearer credential is written to the wire'


async def test_hmac_proof_captured_from_one_connection_cannot_authenticate_another(monkeypatch, writer):
    import wire_rpc.auth.tcp as module
    auth = HmacChallengeAuth({'service':('principal',b's'*32)}, require_tls=False)
    nonces = iter([b'a'*32, b'b'*32])
    monkeypatch.setattr(module.secrets, 'token_bytes', lambda size: next(nonces))
    proof = hmac.digest(b's'*32, b'WRPC-HMAC-1\x00'+b'a'*32+b'\x00service', 'sha256')
    packet = frame(b'service') + frame(proof)
    first = await auth.verify((reader_for(packet), writer))
    other = type(writer)()
    second = await auth.verify((reader_for(packet), other))
    assert first == 'principal' and second is None, 'a valid proof must be bound to one freshly issued challenge rather than reusable as a bearer secret'
    assert other.closed, 'replayed authentication must release the connection slot immediately'


async def test_hmac_proof_is_bound_to_client_identifier_even_when_two_keys_share_secret(monkeypatch, writer):
    import wire_rpc.auth.tcp as module
    auth = HmacChallengeAuth({'a':('alice',b's'*32), 'b':('bob',b's'*32)}, require_tls=False)
    monkeypatch.setattr(module.secrets, 'token_bytes', lambda size: b'n'*size)
    proof = hmac.digest(b's'*32, b'WRPC-HMAC-1\x00'+b'n'*32+b'\x00a', 'sha256')
    assert await auth.verify((reader_for(frame(b'b')+frame(proof)), writer)) is None, 'changing the claimed client ID must invalidate a captured proof even if configuration reuses secret material'


async def test_hmac_unknown_key_does_not_bypass_proof_check_or_return_peer_address(writer):
    auth = HmacChallengeAuth({'service':('principal',b's'*32)}, require_tls=False)
    assert await auth.verify((reader_for(frame(b'unknown')+frame(b'x'*32)),writer)) is None, 'unknown key identifiers must never fall back to a network address identity'


@pytest.mark.parametrize('kind', ['token', 'hmac'])
async def test_authenticated_tcp_connection_returns_configured_identity_after_fragmented_handshake(kind):
    received = asyncio.get_running_loop().create_future()
    server_auth: TcpTokenAuth | HmacChallengeAuth
    client_auth: TcpTokenClientAuth | HmacChallengeClientAuth
    if kind == 'token':
        server_auth = TcpTokenAuth(Mock(validate=AsyncMock(return_value='service-principal')), require_tls=False)
        client_auth = TcpTokenClientAuth('key.'+'x'*40, require_tls=False)
    else:
        server_auth = HmacChallengeAuth({'key':('service-principal',b'x'*32)}, require_tls=False)
        client_auth = HmacChallengeClientAuth('key', b'x'*32, require_tls=False)
    async def accept(reader, writer):
        try:
            principal = await server_auth.verify((reader, writer))
            # Verify authentication consumed exactly its frames, not the first RPC payload.
            payload = await _read(reader, 16)
            received.set_result((principal, payload))
        except Exception as exc:
            received.set_exception(exc)
        finally:
            writer.close(); await writer.wait_closed()
    server = await asyncio.start_server(accept, '127.0.0.1', 0)
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1',server.sockets[0].getsockname()[1])
        try:
            await client_auth.authenticate((reader,writer))
            packet = frame(b'RPC')
            for byte in packet:
                writer.write(bytes([byte])); await writer.drain()
            assert await received == ('service-principal',b'RPC'), 'handshake framing must preserve the next RPC payload and bind identity to credentials rather than ephemeral peer ports'
        finally:
            writer.close(); await writer.wait_closed()
    finally:
        server.close(); await server.wait_closed()


async def test_revoked_session_is_rechecked_before_existing_tcp_connection_dispatches():
    from wire_rpc.auth.credentials import SessionTokenValidator
    from wire_rpc.auth.sessions import InMemorySessionStore
    from wire_rpc.transports.tcp import TcpClientTransport, TcpMulticastServerTransport
    store = InMemorySessionStore()
    token = await store.create('alice')
    server = TcpMulticastServerTransport(host='127.0.0.1', port=0, keep_alive=None,
        auth=TcpTokenAuth(SessionTokenValidator(store), require_tls=False))
    await server.connect()
    assert server._server is not None, 'the revocation test needs a real bound TCP listener'
    client = TcpClientTransport(host='127.0.0.1', port=server._server.sockets[0].getsockname()[1],
        keep_alive=None, auth=TcpTokenClientAuth(token, require_tls=False))
    try:
        await client.connect()
        await client.send(b'first')
        peer, _ = await server.recv()
        assert await server.get_principal(peer) == 'alice', 'the connection must initially be authenticated for the revocation regression to be meaningful'
        connection = server._clients[peer]
        await store.destroy(token)
        with pytest.raises(PermissionError):
            await server.get_principal(peer)
        assert connection.writer.is_closing(), 'revoking a token must close its established TCP channel rather than preserve handshake-time authorization forever'
    finally:
        await client.close(); await server.close()
