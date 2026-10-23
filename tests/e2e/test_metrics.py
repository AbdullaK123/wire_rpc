"""metrics_middleware records every executed call accurately, through a real transport."""
import asyncio

import pytest

from tests.e2e.harness import serve
from wire_rpc.client import BatchCall, Client, WireRpcError
from wire_rpc.errors import ServerError
from wire_rpc.middleware import CallRecord, metrics_middleware
from wire_rpc.response import WireErrorResponse

pytestmark = pytest.mark.timeout(30)


def instrumented(records: list[CallRecord], *, explode=False):
    def record(call: CallRecord) -> None:
        records.append(call)
        if explode:
            raise RuntimeError('exporter down')

    def setup(app):
        app.middleware(metrics_middleware(record))

        @app.middleware
        async def deny_admin(request, ctx, call_next):
            if request.method == 'admin':
                return WireErrorResponse(error=ServerError(message='Forbidden', code=-32050), id=request.response_id)
            return await call_next(request, ctx)

        @app.method('admin')
        async def admin(ctx) -> str:
            return 'should not run'

        @app.method('nap')
        async def nap(ctx) -> str:
            await asyncio.sleep(0.05)
            return 'rested'

        @app.method('hang')
        async def hang(ctx) -> None:
            await asyncio.Event().wait()
    return setup


@pytest.mark.parametrize('kind', ['thread', 'tcp-multi'])
async def test_outcomes_are_misrecorded(kind):
    records: list[CallRecord] = []
    async with serve(kind, setup=instrumented(records), app_options={'handler_timeout': 0.2}) as server:
        async with Client(server.wire(), call_timeout=5) as client:
            assert await client.call('nap', str) == 'rested'
            with pytest.raises(WireRpcError):
                await client.call('fail', int)
            with pytest.raises(WireRpcError):
                await client.call('admin', str)
            with pytest.raises(WireRpcError):
                await client.call('hang', str)
    outcomes = [(r.method, r.outcome, r.error_code) for r in records]
    assert outcomes == [('nap', 'ok', None), ('fail', 'exception', None), ('admin', 'error', -32050),
                        ('hang', 'cancelled', None)], 'each call must be recorded once with the outcome the client actually saw'
    assert records[0].duration >= 0.05, 'duration must cover the handler, or latency dashboards lie'
    assert 0.15 < records[3].duration < 1.0, 'a timed-out call must be recorded when the deadline cut it off'


async def test_rejected_requests_are_recorded_as_handler_calls():
    records: list[CallRecord] = []
    async with serve('thread', setup=instrumented(records)) as server:
        async with Client(server.wire(), call_timeout=5) as client:
            with pytest.raises(WireRpcError):
                await client.call('no.such.method', int)
            with pytest.raises(WireRpcError):
                await client.call('add', int, {'a': 'x', 'b': 1})
            assert await client.call('add', int, {'a': 1, 'b': 1}) == 2
    assert [r.method for r in records] == ['add'], 'only executed handlers are recorded; rejected requests are counted by app.stats'


async def test_batch_items_are_merged_into_one_record():
    records: list[CallRecord] = []
    async with serve('thread', setup=instrumented(records)) as server:
        async with Client(server.wire(), call_timeout=5) as client:
            await client.batch([BatchCall('add', int, {'a': i, 'b': 0}) for i in range(3)])
    assert [r.method for r in records] == ['add', 'add', 'add'], 'each call inside a batch is its own record'


async def test_broken_exporter_breaks_calls():
    records: list[CallRecord] = []
    async with serve('thread', setup=instrumented(records, explode=True)) as server:
        async with Client(server.wire(), call_timeout=5) as client:
            assert await client.call('add', int, {'a': 2, 'b': 2}) == 4, 'a failing metrics exporter must never fail the RPC it measures'
    assert len(records) == 1
