from tests.helpers import UnusedTransport, response_bytes
import asyncio
import pytest
from wire_rpc import App, MulticastApp
from wire_rpc._execution import process
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec


async def test_one_peers_overlapping_requests_cannot_occupy_all_handler_slots():
    codec=MsgSpecJsonCodec(); first_entered=asyncio.Event(); other_served=asyncio.Event(); release=asyncio.Event()
    class Transport:
        async def connect(self):
            pass
        async def close(self):
            pass
        async def broadcast(self, data: bytes):
            raise AssertionError('Request isolation must not broadcast private responses')
        def __init__(self):
            self.queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue(); self.sent=[]
        async def recv(self):
            return await self.queue.get()
        async def send(self,client_id,data):
            self.sent.append((client_id,codec.decode(data,dict)))
            if client_id == 'b':
                other_served.set()
        async def __aenter__(self):
            return self
        async def __aexit__(self,exc_type,exc_val,exc_tb):
            pass
    transport=Transport(); app=MulticastApp(transport,max_concurrency=2)
    @app.method('block')
    async def block(ctx):
        first_entered.set(); await release.wait(); return 'done'
    @app.method('other')
    async def other(ctx):
        return 'independent'
    task=asyncio.create_task(app._listen())
    def packet(method,id):
        return codec.encode({'jsonrpc':'2.0','method':method,'id':id})
    await transport.queue.put(('a',packet('block',1))); await first_entered.wait()
    await transport.queue.put(('a',packet('block',2)))
    await transport.queue.put(('b',packet('other',3)))
    try:
        await other_served.wait()
        rejected=[reply for key,reply in transport.sent if key=='a']
        assert rejected[0]['error']['code'] == -32002, 'overlapping work from a busy peer must be explicitly refused instead of occupying all global handler slots'
        assert not release.is_set(), 'an independent caller must complete while the first peer remains stalled'
    finally:
        release.set(); await app.stop(); await task
    assert app.stats['active'] == 0 and not app._tasks, 'stopping must release active execution and all owned dispatch tasks'


async def test_handler_timeout_is_a_safe_request_error_and_context_is_released(monkeypatch):
    import wire_rpc._execution as execution
    class Deadline:
        async def __aenter__(self):
            raise TimeoutError
        async def __aexit__(self,exc_type,exc_val,exc_tb):
            pass
    monkeypatch.setattr(execution.asyncio,'timeout',lambda delay:Deadline())
    codec=MsgSpecJsonCodec(); app=App(UnusedTransport())
    calls=[]
    @app.method('mutate')
    async def mutate(ctx):
        calls.append('side effect')
    data=codec.encode({'jsonrpc':'2.0','method':'mutate','id':1})
    reply=codec.decode(response_bytes(await process(app,data,app._dispatch)),dict)
    assert reply['error']['code'] == -32000, 'execution deadline exhaustion must return a controlled failure without leaking internals'
    assert calls == [] and app.stats['active'] == 0, 'a deadline reached before dispatch must not execute the handler or leak admission'
