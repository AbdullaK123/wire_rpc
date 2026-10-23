import asyncio
import sys

from wire_rpc.transports.stdio import StdIoTransport


async def test_child_filling_stderr_cannot_deadlock_rpc_output():
    script = "import os; os.write(2,b'x'*(2*1024*1024)); os.write(1,b'\\x00\\x00\\x00\\x02ok')"
    transport = StdIoTransport(sys.executable,'-c',script)
    async with transport:
        assert await transport.recv() == b'ok', 'a noisy subprocess must not deadlock RPC by filling its stderr pipe'
        assert transport.proc is not None, "connected stdio must retain its child handle so shutdown can reap it"
        await transport.proc.wait()
    assert transport.proc is None, 'process handles must be released after normal exit and repeated close'
    assert transport.stats['stderr_bytes_discarded'] == 2*1024*1024, 'stderr draining must discard bytes instead of accumulating unbounded logs'
    await transport.close()


async def test_close_escalates_to_termination_when_child_ignores_stdin_eof(monkeypatch):
    import asyncio
    class Process:
        returncode = None
        terminated = False
        async def wait(self):
            self.returncode = 0
        def terminate(self):
            self.terminated = True
        def kill(self):
            self.returncode = -9
    class Pipe:
        def close(self):
            pass
    class Reader:
        async def read(self, size):
            return b''
    class ImmediateTimeout:
        async def __aenter__(self):
            raise TimeoutError
        async def __aexit__(self, *args):
            pass
    real_timeout = asyncio.timeout
    monkeypatch.setattr(asyncio, 'timeout', lambda delay: ImmediateTimeout() if delay == 7 else real_timeout(delay))
    transport = StdIoTransport('unused',shutdown_timeout=7)
    process = Process()
    monkeypatch.setattr(transport, 'proc', process)
    monkeypatch.setattr(transport, 'stdin', Pipe())
    monkeypatch.setattr(transport, 'stdout', Reader())
    await transport.close()
    assert process.terminated, 'a child ignoring graceful shutdown must receive termination instead of hanging the parent forever'
    assert transport.proc is None, 'termination must release the process even when no stderr task was started'


# The child imports the shared e2e handlers; when NO_PIPES is set it first makes the
# event loop refuse to watch pipes, as Windows' ProactorEventLoop does for stdin/stdout.
_SERVER_SCRIPT = '''
import asyncio, os, sys
sys.path.insert(0, os.environ["WIRE_ROOT"])
if os.environ.get("NO_PIPES"):
    async def refuse(*args, **kwargs):
        raise NotImplementedError("this event loop cannot watch pipes")
    asyncio.BaseEventLoop.connect_read_pipe = refuse
    asyncio.BaseEventLoop.connect_write_pipe = refuse
from tests.e2e.handlers import install
from wire_rpc import App
from wire_rpc.transports.stdio import StdIoServerTransport
app = App(StdIoServerTransport(read_timeout=10))
install(app)
app.run(log_level="warning")
'''


def _child(monkeypatch, *, no_pipes: bool):
    import pathlib
    monkeypatch.setenv('WIRE_ROOT', str(pathlib.Path(__file__).resolve().parents[2]))
    if no_pipes:
        monkeypatch.setenv('NO_PIPES', '1')
    return StdIoTransport(sys.executable, '-c', _SERVER_SCRIPT, read_timeout=10)


async def test_server_side_works_where_the_event_loop_cannot_watch_pipes(monkeypatch):
    from wire_rpc import Client
    async with Client(_child(monkeypatch, no_pipes=True), call_timeout=10) as client:
        assert await client.call('add', int, {'a': 2, 'b': 3}) == 5, \
            'the stdio server must not depend on connect_read_pipe/connect_write_pipe, which Windows lacks'
        assert await client.call('add', int, {'a': 4, 'b': 5}) == 9, 'the channel must stay usable after the first reply'


async def test_server_side_round_trips_frames_far_larger_than_its_read_buffer(monkeypatch):
    from wire_rpc import Client
    payload = {'blob': 'x' * (3 * 1024 * 1024)}
    async with Client(_child(monkeypatch, no_pipes=False), call_timeout=10) as client:
        assert await client.call('echo', dict, payload) == payload, \
            'stdin reading must pause and resume around a full buffer without losing or reordering bytes'
        assert await client.call('add', int, {'a': 1, 'b': 1}) == 2, 'a large frame must not desynchronize the next one'


async def test_server_side_closes_stdout_when_it_stops(monkeypatch):
    transport = _child(monkeypatch, no_pipes=False)
    async with transport:
        assert transport.stdin is not None and transport.proc is not None
        transport.stdin.close()
        async with asyncio.timeout(10):
            await transport.proc.wait()
        assert transport.proc.returncode == 0, 'the server child must exit cleanly once its stdin reaches EOF'
