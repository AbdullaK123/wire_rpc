import asyncio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from wire_rpc.transports.http import HttpClientTransport,HttpServerTransport


async def test_chunked_response_is_bounded_without_trusting_content_length():
    app=web.Application()
    async def response(request):
        stream=web.StreamResponse(); await stream.prepare(request)
        await stream.write(b'12345'); await stream.write_eof(); return stream
    app.router.add_post('/rpc',response)
    async with TestServer(app) as server:
        async with HttpClientTransport(str(server.make_url('/rpc')),max_body_size=4) as client:
            await client.send(b'x')
            with pytest.raises(ConnectionError):
                await client.recv()
            assert client._pending is None, 'oversized chunked bodies must release their response instead of poisoning the client connection pool'


async def test_http_error_status_cannot_be_decoded_as_a_successful_rpc_reply():
    app=web.Application()
    async def response(request):
        return web.Response(status=503,body=b'{"result":"wrong"}')
    app.router.add_post('/rpc',response)
    async with TestServer(app) as server:
        async with HttpClientTransport(str(server.make_url('/rpc'))) as client:
            with pytest.raises(ConnectionError):
                await client.send(b'x')
            assert client._pending is None, 'HTTP failures must not leave an apparently valid pending RPC response'


async def test_http_queue_byte_capacity_rejects_before_retaining_more_payloads(request_factory):
    transport=HttpServerTransport('127.0.0.1',0,max_body_size=10,max_queue_bytes=4)
    first=asyncio.create_task(transport._handle(request_factory(b'1234')))
    await transport._ready.wait()
    try:
        with pytest.raises(web.HTTPServiceUnavailable):
            await transport._handle(request_factory(b'x'))
        assert transport.stats['queued_bytes'] == 4, 'pending request count must not substitute for a concrete queued-byte ceiling'
    finally:
        first.cancel(); await asyncio.gather(first,return_exceptions=True); await transport.close()
