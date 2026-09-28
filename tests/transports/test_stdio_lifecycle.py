import sys
import pytest
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
