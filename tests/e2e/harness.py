"""Bring up any supported server/client pairing for end-to-end tests."""
import asyncio
import contextlib
import datetime
import hashlib
import ipaddress
import pathlib
import ssl
import sys
import threading
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from tests.e2e.handlers import install
from tests.helpers import listening_port
from wire_rpc import App, MulticastApp
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec, MsgSpecMsgPackCodec
from wire_rpc.codecs.pydantic import PydanticCodec
from wire_rpc.transports import ThreadTransport
from wire_rpc.transports.http import HttpClientTransport, HttpServerTransport
from wire_rpc.transports.stdio import StdIoTransport
from wire_rpc.transports.tcp import (
    TcpClientTransport,
    TcpMulticastServerTransport,
    TcpServerTransport,
)
from wire_rpc.transports.websocket import (
    MulticastWsServerTransport,
    WsClientTransport,
    WsServerTransport,
)

CODECS = {'json': MsgSpecJsonCodec, 'msgpack': MsgSpecMsgPackCodec, 'pydantic': PydanticCodec}
TEXT_ONLY = {'http', 'ws', 'ws-multi'}
MULTICAST = {'tcp-multi', 'ws-multi'}
KINDS = ['tcp', 'tcp-multi', 'http', 'ws', 'ws-multi', 'stdio', 'thread']
STDIO_SERVER = pathlib.Path(__file__).with_name('stdio_server.py')

# Every (transport, codec) pairing the transports accept: 18 in total.
TRANSPORT_CODECS = [(kind, codec) for kind in KINDS for codec in CODECS
                    if not (kind in TEXT_ONLY and codec == 'msgpack')]


@dataclass
class TlsMaterial:
    """A throwaway CA with a server certificate for 127.0.0.1 and a client certificate."""
    server: ssl.SSLContext           # plain TLS server; does not ask for client certificates
    server_mtls: ssl.SSLContext      # requires and verifies a client certificate
    client: ssl.SSLContext           # trusts the CA, presents no certificate
    client_with_cert: ssl.SSLContext  # trusts the CA and presents the client certificate
    client_fingerprint: str


def make_tls(directory: pathlib.Path) -> TlsMaterial:
    now = datetime.datetime.now(datetime.UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'wire-e2e-ca')])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - datetime.timedelta(minutes=1)).not_valid_after(now + datetime.timedelta(days=1))
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .add_extension(x509.KeyUsage(digital_signature=False, content_commitment=False, key_encipherment=False,
                                       data_encipherment=False, key_agreement=False, key_cert_sign=True,
                                       crl_sign=True, encipher_only=False, decipher_only=False), critical=True)
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
          .sign(ca_key, hashes.SHA256()))
    ca_path = directory / 'ca.pem'
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    fingerprint = ''
    for name, usage in [('server', ExtendedKeyUsageOID.SERVER_AUTH), ('client', ExtendedKeyUsageOID.CLIENT_AUTH)]:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        builder = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
                   .issuer_name(ca_name).public_key(key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(now - datetime.timedelta(minutes=1)).not_valid_after(now + datetime.timedelta(days=1))
                   .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                   .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
                   .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False))
        if name == 'server':
            builder = builder.add_extension(
                x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
        cert = builder.sign(ca_key, hashes.SHA256())
        (directory / f'{name}.pem').write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        (directory / f'{name}.key').write_bytes(key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        if name == 'client':
            fingerprint = hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest()
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(directory / 'server.pem', directory / 'server.key')
    server_mtls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_mtls.load_cert_chain(directory / 'server.pem', directory / 'server.key')
    server_mtls.load_verify_locations(ca_path)
    server_mtls.verify_mode = ssl.CERT_REQUIRED
    client = ssl.create_default_context(cafile=str(ca_path))
    client_with_cert = ssl.create_default_context(cafile=str(ca_path))
    client_with_cert.load_cert_chain(directory / 'client.pem', directory / 'client.key')
    return TlsMaterial(server, server_mtls, client, client_with_cert, fingerprint)


class _LoopThread:
    """A second thread with its own event loop, for the ThreadTransport server end."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()

    def submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()


@dataclass
class Server:
    """A running server plus everything a test needs to reach it."""
    kind: str
    codec_name: str
    app: App | MulticastApp | None
    transport: Any
    port: int | None
    tls: bool
    notes: list[object] = field(default_factory=list)
    _thread_client_end: ThreadTransport | None = None
    principal: str | None = None

    @property
    def codec(self):
        return CODECS[self.codec_name]()

    def wire(self, *, auth=None, headers=None, cookie_jar=None, ssl_context=None,
             max_message_size: int | None = None):
        """A new, unconnected client transport for this server."""
        if self.kind in ('tcp', 'tcp-multi'):
            assert self.port is not None
            extra = {} if max_message_size is None else {'max_frame_size': max_message_size}
            return TcpClientTransport(host='127.0.0.1', port=self.port, keep_alive=None, auth=auth,
                                      ssl_context=ssl_context, read_timeout=5, **extra)
        if self.kind == 'http':
            scheme = 'https' if self.tls else 'http'
            extra = {} if max_message_size is None else {'max_body_size': max_message_size}
            return HttpClientTransport(f'{scheme}://127.0.0.1:{self.port}/rpc', request_timeout=5,
                                       headers=headers, cookie_jar=cookie_jar, ssl_context=ssl_context,
                                       allow_insecure_credentials=not self.tls, **extra)
        if self.kind in ('ws', 'ws-multi'):
            scheme = 'wss' if self.tls else 'ws'
            extra = {} if max_message_size is None else {'max_msg_size': max_message_size}
            return WsClientTransport(f'{scheme}://127.0.0.1:{self.port}/ws', receive_timeout=5,
                                     headers=headers, cookie_jar=cookie_jar, ssl_context=ssl_context,
                                     allow_insecure_credentials=not self.tls, **extra)
        if self.kind == 'stdio':
            args = [sys.executable, str(STDIO_SERVER), self.codec_name]
            if self.principal is not None:
                args.append(self.principal)
            return StdIoTransport(*args, read_timeout=5, max_frame_size=max_message_size or 16 * 1024 * 1024)
        if self.kind == 'thread':
            end, self._thread_client_end = self._thread_client_end, None
            assert end is not None, 'a ThreadTransport pair has exactly one client end'
            return end
        raise AssertionError(self.kind)

    @property
    def base_url(self) -> str:
        return f"{'https' if self.tls else 'http'}://127.0.0.1:{self.port}"


def _server_transport(kind, *, auth, ssl_context, max_message_size):
    if kind == 'tcp':
        extra = {} if max_message_size is None else {'max_frame_size': max_message_size}
        return TcpServerTransport(host='127.0.0.1', port=0, keep_alive=None, auth=auth, ssl_context=ssl_context,
                                  shutdown_timeout=1, read_timeout=5, **extra)
    if kind == 'tcp-multi':
        extra = {} if max_message_size is None else {'max_frame_size': max_message_size}
        return TcpMulticastServerTransport(host='127.0.0.1', port=0, keep_alive=None, auth=auth,
                                           ssl_context=ssl_context, shutdown_timeout=1, read_timeout=5, **extra)
    if kind == 'http':
        extra = {} if max_message_size is None else {'max_body_size': max_message_size}
        return HttpServerTransport('127.0.0.1', 0, auth=auth, ssl_context=ssl_context, request_timeout=5,
                                   shutdown_timeout=1, **extra)
    if kind == 'ws':
        extra = {} if max_message_size is None else {'max_msg_size': max_message_size}
        return WsServerTransport('127.0.0.1', 0, auth=auth, ssl_context=ssl_context, close_timeout=1, **extra)
    if kind == 'ws-multi':
        extra = {} if max_message_size is None else {'max_msg_size': max_message_size}
        return MulticastWsServerTransport('127.0.0.1', 0, auth=auth, ssl_context=ssl_context, close_timeout=1,
                                          **extra)
    raise AssertionError(kind)


def _bound_port(kind, transport) -> int | None:
    if kind in ('tcp', 'tcp-multi'):
        server = transport._server
        return None if server is None else server.sockets[0].getsockname()[1]
    runner = transport._runner
    return None if runner is None or not runner.addresses else listening_port(runner)


@contextlib.asynccontextmanager
async def serve(kind: str, codec_name: str = 'json', *, auth=None, ssl_context=None,
                max_message_size: int | None = None, principal: str | None = None,
                app_options: dict | None = None,
                setup: Callable[[App | MulticastApp], None] | None = None) -> AsyncIterator[Server]:
    """Run a server of ``kind`` with the shared handlers until the block exits."""
    codec = CODECS[codec_name]()
    options = dict(app_options or {})
    if kind == 'stdio':
        # Each client wire launches its own child process running the server.
        yield Server(kind, codec_name, None, None, None, False, principal=principal)
        return
    if kind == 'thread':
        limits = {} if max_message_size is None else {'max_message_size': max_message_size}
        client_end, server_end = ThreadTransport.pair(connect_timeout=5, **limits)
        app = App(server_end, codec=codec, **options)
        notes = install(app)
        if setup:
            setup(app)
        worker = _LoopThread()
        running = worker.submit(app._run())
        try:
            yield Server(kind, codec_name, app, server_end, None, False, notes, client_end)
        finally:
            worker.submit(app.stop()).result(5)
            # The app exits once its client end closes or stop() lands.
            with contextlib.suppress(Exception):
                await asyncio.wait_for(asyncio.wrap_future(running), 5)
            worker.stop()
        return
    transport = _server_transport(kind, auth=auth, ssl_context=ssl_context, max_message_size=max_message_size)
    app = (MulticastApp if kind in MULTICAST else App)(transport, codec=codec, **options)
    notes = install(app, multicast=kind in MULTICAST)
    if setup:
        setup(app)
    task = asyncio.create_task(app._run())
    try:
        for _ in range(500):
            if task.done():
                task.result()
                raise AssertionError('server exited before it started listening')
            port = _bound_port(kind, transport)
            if port is not None:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError('server did not start listening within 5 seconds')
        yield Server(kind, codec_name, app, transport, port, ssl_context is not None, notes)
    finally:
        await app.stop()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, 5)
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
