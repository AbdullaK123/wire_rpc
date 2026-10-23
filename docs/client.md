# The client

`Client` calls a JSON-RPC server over any transport. It doesn't have to be a
wire_rpc server; anything that speaks JSON-RPC 2.0 over the same transport and
codec works.

```python
from wire_rpc import Client
from wire_rpc.transports.tcp import TcpClientTransport

async with Client(TcpClientTransport(host="127.0.0.1", port=9000)) as client:
    total = await client.call("add", int, {"a": 2, "b": 3})
```

`async with` connects on entry and closes on exit. You can also call
`await client.connect()` and `await client.close()` yourself.

## Options

```python
Client(
    transport,
    codec=MsgSpecJsonCodec(),   # must match the server's codec
    call_timeout=30.0,          # seconds per call, including waiting for the client
    close_timeout=5.0,          # seconds allowed to close the transport
    on_notification=None,       # callback for server notifications; see below
)
```

## Calls

```python
result = await client.call(method, response_type, params)
```

- `response_type` is what the result is converted to: `int`, `str`, `dict`, a
  `msgspec.Struct`, a Pydantic model with `PydanticCodec`, and so on. Use
  `type(None)` for methods that return nothing.
- `params` is any value the codec can encode, usually a dict or a Struct. Omit it
  for methods without params.

If the server returns an error, `call` raises `WireRpcError` with `code`,
`message` and `data`. The client stays usable. See [Errors](errors.md).

### One call at a time

A `Client` sends one request and waits for its reply before sending the next.
Concurrent `call()`s on the same client queue up and run in turn, and
`call_timeout` includes the time spent waiting in that queue.

For parallel work, use several clients:

```python
clients = [Client(make_transport()) for _ in range(4)]
```

To send many small calls quickly, batch them (below). A batch of 100 is 15 to 30
times faster than 100 separate calls; see [Benchmarks](benchmarks.md).

## Notifications

```python
await client.notify("audit.log", {"event": "opened", "note": 42})
```

The server runs the method and sends nothing back, not even an error.
`notify` returns once the message is sent.

## Batches

```python
from wire_rpc.client import BatchCall, BatchNotification, WireRpcError

total, _, note = await client.batch([
    BatchCall("add", int, {"a": 1, "b": 2}),
    BatchNotification("audit.log", {"event": "batched"}),
    BatchCall("notes.get", Note, {"id": 7}),
])
```

`batch` sends everything as one JSON-RPC batch and returns one entry per item, in
the same order:

- the converted result for a `BatchCall` that succeeded,
- a `WireRpcError` for a `BatchCall` that failed. It's **returned, not raised**,
  so one failure doesn't hide the other results,
- `None` for a `BatchNotification`.

```python
for result in results:
    if isinstance(result, WireRpcError):
        log.warning("call failed: %s", result.code)
```

The server runs the items in order, under one shared deadline. If the whole
batch is refused (for example, it has more items than the server's
`max_batch_size`, 100 by default), `batch` raises `WireRpcError` and nothing ran.
A batch of only notifications returns a list of `None` without waiting for a
reply. An empty batch raises `ValueError`.

## Server notifications

Over TCP and WebSocket, a server can push notifications to its clients at any
time. `MulticastApp.broadcast` does this. There are two ways to receive them.

**While making calls,** pass a callback. Notifications that arrive while a call
is waiting for its reply are handed to it, in order:

```python
from wire_rpc.client import Notification

def on_notification(note: Notification) -> None:
    print(note.method, note.params)

async with Client(transport, on_notification=on_notification) as client:
    ...
```

The callback can be a plain function or `async`. If it raises, the error is
logged and the call carries on. Without a callback, these notifications are
dropped. Either way, they're never mistaken for a call's reply.

Notifications that arrive between calls wait in the connection until the next
call reads them.

**On a dedicated listener,** wait for them directly:

```python
async with Client(listener_transport) as listener:
    while True:
        note = await listener.receive_notification()
        handle(note)
```

`receive_notification()` holds the client while it waits, and has no deadline. Use
a separate client for listening, so it doesn't block your calls. Cancelling it
closes the client, because a half-read message can't be resumed. Over HTTP it
raises `RuntimeError`, because an HTTP server can't push.

## Failures and reconnecting

When anything goes wrong with the connection itself, the client closes for good:

- `ConnectionError`: the connection dropped, the reply couldn't be decoded, or
  the reply didn't belong to the request.
- `TimeoutError`: the call didn't finish within `call_timeout`.

After either, create a new `Client` with a new transport. The client never retries
for you, because it can't know whether the server ran the call. After a timeout
the server may have finished the work just after the client gave up, and retrying
a call that changes data could do it twice.

For methods that are safe to repeat (reads, or idempotent writes), retry
yourself:

```python
import asyncio

async def call_idempotent(make_transport, method, response_type, params, attempts=3):
    """Retry only methods that are safe to run more than once."""
    for attempt in range(attempts):
        try:
            async with Client(make_transport(), call_timeout=10) as client:
                return await client.call(method, response_type, params)
        except (ConnectionError, TimeoutError):
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(0.2 * 2 ** attempt)
```

To make a write safe to retry, give it an idempotency key the client chooses and
the server records:

```python
await client.call("payments.create", Payment, {"key": str(uuid4()), "amount": 500})
```

The server stores `key` with the result and returns the stored result if it sees
the same key again.

## Cancellation

Cancelling a task that is waiting for a call to *start* (queued behind another
call) is harmless. Cancelling a call that has already been sent closes the
client, for the same reason a timeout does.

## Authentication and TLS

Credentials belong to the transport, not the client:

```python
# HTTP or WebSocket
HttpClientTransport(url, headers=credential_headers(bearer=token), ssl_context=tls)

# TCP
TcpClientTransport(host, port, auth=TcpTokenClientAuth(token), ssl_context=tls)
```

See [Authentication](authentication/README.md).
