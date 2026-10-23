# Testing your service

The easiest way to test a wire_rpc service is to run the real app and talk to it
with the real client. `ThreadTransport` makes that cheap: no ports, no
subprocesses, nothing to clean up.

## A pytest fixture

```python
# conftest.py
import threading

import pytest

from wire_rpc import App, Client
from wire_rpc.transports import ThreadTransport

from notes.server import register_handlers   # your code: adds methods to an app


@pytest.fixture
async def client():
    client_end, server_end = ThreadTransport.pair()
    app = App(server_end)
    register_handlers(app)
    threading.Thread(target=lambda: app.run(log_level=None), daemon=True).start()
    async with Client(client_end, call_timeout=5) as client:
        yield client
    # Closing the client closes the server's end, and the app shuts down.
```

```python
# test_notes.py
import pytest
from wire_rpc.client import WireRpcError


async def test_create_and_fetch(client):
    note_id = await client.call("notes.create", int, {"title": "hi"})
    note = await client.call("notes.get", dict, {"id": note_id})
    assert note["title"] == "hi"


async def test_bad_params_are_rejected(client):
    with pytest.raises(WireRpcError) as error:
        await client.call("notes.create", int, {"title": 42})
    assert error.value.code == -32602
```

These need an async test runner, such as `pytest-asyncio` with
`asyncio_mode = "auto"`.

Structure your server so handlers are registered by a function, like
`register_handlers(app)`, rather than at import time on a module-level app bound
to a real transport. Then tests and production build the app the same way with
different transports.

## Testing with authentication

`ThreadTransport` has no authenticator, so `current_request().principal` is
`None`. To test authorization logic, either:

- test against a real transport with real credentials (TCP on `127.0.0.1` with
  `require_tls=False` on both sides is quick to set up), or
- put the authorization check in a function that takes the principal, and test
  that function directly.

## Testing startup hooks and `ctx`

`app.run()` in the thread runs your startup and shutdown hooks as usual, so a
fixture can point them at a test database through configuration or environment
variables.

## Testing a handler without a transport

Handlers are plain async functions. You can call them directly:

```python
async def test_add_directly():
    assert await add(AddParams(a=2, b=3), ctx=None) == 5
```

That skips validation, middleware and encoding, so keep at least a few tests
that go through a real client.

## Testing against an external client

To check that non-Python clients can talk to you, run the app on a real
transport and use their library, or plain bytes:

```python
import json
import socket

def call_over_tcp(port: int, request: dict) -> dict:
    payload = json.dumps(request).encode()
    with socket.create_connection(("127.0.0.1", port)) as sock:
        sock.sendall(len(payload).to_bytes(4, "big") + payload)
        size = int.from_bytes(sock.recv(4), "big")
        data = b""
        while len(data) < size:
            data += sock.recv(size - len(data))
    return json.loads(data)
```

## Logging in tests

Pass `log_level=None` to `app.run()` in tests, as above, so the server doesn't
install its own log handler. pytest's `caplog` fixture captures the `wire_rpc`
and `wire_rpc.access` loggers as usual.
