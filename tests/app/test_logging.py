"""Logging: an access line per request with no payloads, and safe defaults."""
import logging
import subprocess
import sys

import pytest

from tests.e2e.harness import serve
from wire_rpc import Client
from wire_rpc.client import WireRpcError
from wire_rpc.logger import configure_logging, logger

pytestmark = pytest.mark.timeout(30)


async def test_access_log_misreports_outcomes_or_leaks_payloads(caplog):
    caplog.set_level(logging.INFO, logger='wire_rpc')
    async with serve('thread') as server:
        async with Client(server.wire(), call_timeout=5) as client:
            assert await client.call('echo', dict, {'card': 'needle-4111111111111111'})
            await client.notify('note', {'n': 'needle-notify'})
            for method, params in [('no.such.method', None), ('add', {'a': 'needle-bad'}), ('fail', None)]:
                with pytest.raises(WireRpcError):
                    await client.call(method, int, params)
    lines = [r.getMessage() for r in caplog.records if r.name == 'wire_rpc.access']
    outcomes = [line.split()[:2] for line in lines]
    assert outcomes == [['echo', 'ok'], ['note', 'notification'], ['no.such.method', '-32601'],
                        ['add', '-32602'], ['fail', '-32603']], 'every request needs one access line with the outcome the caller saw'
    text = '\n'.join(r.getMessage() for r in caplog.records)
    assert 'needle' not in text and 'SECRET' not in text, 'params, results and exception text must never reach the logs'


async def test_garbage_requests_go_unlogged(caplog):
    caplog.set_level(logging.INFO, logger='wire_rpc')
    async with serve('thread') as server:
        wire = server.wire()
        await wire.connect()
        try:
            await wire.send(b'{not json')
            await wire.recv()
            await wire.send(b'{"jsonrpc": "1.0", "method": "add", "id": 1}')
            await wire.recv()
        finally:
            await wire.close()
    lines = [r.getMessage() for r in caplog.records if r.name == 'wire_rpc.access']
    assert lines[0].startswith('<unparseable> -32700') and lines[1].startswith('<invalid> -32600'), 'rejected traffic must show up in the access log too, or probing goes unseen'


@pytest.fixture
def unconfigured(monkeypatch):
    """Call inside the test to pretend nothing has configured logging; undone afterwards.

    pytest attaches its capture handler to the root logger after fixtures run, so
    clearing it must happen in the test body.
    """
    def clear():
        monkeypatch.setattr(logging.getLogger(), 'handlers', [])
        monkeypatch.setattr(logger, 'handlers', [])
        monkeypatch.setattr(logger, 'level', logging.NOTSET)
    return clear


def test_configure_logging_stacks_handlers(unconfigured):
    unconfigured()
    configure_logging('debug')
    configure_logging('debug')
    assert len(logger.handlers) == 1, 'calling run() twice must not print every line twice'
    assert logger.level == logging.DEBUG
    assert getattr(logger.handlers[0], 'stream', None) is sys.stderr, 'logs must go to stderr; stdout carries stdio frames'


def test_configure_logging_overrides_application_setup(unconfigured):
    unconfigured()
    logging.getLogger().addHandler(logging.NullHandler())
    configure_logging('debug')
    assert logger.handlers == [] and logger.level == logging.NOTSET, 'an application that configured logging must keep full control'


def test_bad_log_level_is_accepted():
    with pytest.raises(ValueError):
        configure_logging('verbose')


def _python(code: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=20)


def test_warnings_are_silent_by_default():
    result = _python('from wire_rpc.logger import logger; logger.warning("listener gone")')
    assert 'listener gone' in result.stderr, 'with no configuration, library warnings must still be visible'
    assert result.stdout == ''


def test_import_configures_logging():
    result = _python('import logging, wire_rpc; print(len(logging.getLogger("wire_rpc").handlers), len(logging.getLogger().handlers))')
    assert result.stdout.split() == ['0', '0'], 'importing a library must not install handlers'


def test_run_logs_to_stdout():
    code = '''
import asyncio, threading
from wire_rpc import App, Client
from wire_rpc.transports import ThreadTransport
client_end, server_end = ThreadTransport.pair()
app = App(server_end)
@app.method("ping")
async def ping(ctx) -> str:
    return "pong"
async def call():
    async with Client(client_end) as client:
        await client.call("ping", str)
    await asyncio.sleep(0.2)
threading.Thread(target=lambda: asyncio.run(call()), daemon=True).start()
app.run()
'''
    result = _python(code)
    assert result.stdout == '', 'run() must never write logs to stdout'
    assert 'wire_rpc.access: ping ok' in result.stderr, 'run() must show the access log by default, like a server runner'


async def test_refused_batches_are_invisible(caplog):
    caplog.set_level(logging.INFO, logger='wire_rpc')
    async with serve('thread', app_options={'max_batch_size': 1}) as server:
        wire = server.wire()
        await wire.connect()
        try:
            for message in (b'[]', b'[{"jsonrpc":"2.0","method":"add","id":1},{"jsonrpc":"2.0","method":"add","id":2}]'):
                await wire.send(message)
                await wire.recv()
        finally:
            await wire.close()
        assert server.app is not None and server.app.stats['invalid'] == 2, 'refused batches must be counted, or monitoring misses them'
    lines = [r.getMessage() for r in caplog.records if r.name == 'wire_rpc.access']
    assert [line.split()[:2] for line in lines] == [['<batch>', '-32600'], ['<batch>', '-32600']]
