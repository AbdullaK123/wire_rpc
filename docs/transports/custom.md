# Writing a custom transport

A transport is any object with a handful of async methods. It moves whole
encoded messages and never parses them. You might write one for a message queue,
a serial line, a Unix socket with your own framing, or a test double.

## The single-peer interface

`App` and `Client` use a transport with this shape (`wire_rpc.transports.Transport`):

```python
class Transport(Protocol):
    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def recv(self) -> bytes: ...
    async def send(self, data: bytes) -> None: ...
    async def __aenter__(self) -> Self: ...      # usually: await self.connect(); return self
    async def __aexit__(self, *exc) -> None: ... # usually: await self.close()
```

The contract:

- **`recv()` returns exactly one whole message**, never a fragment and never two
  joined together. Framing is the transport's job.
- **`send(data)` sends exactly one whole message.**
- **When the connection is gone, `recv()` raises `ConnectionError`** (or
  `asyncio.IncompleteReadError`). That's how `App` knows to shut down, and how
  `Client` knows the channel is dead. Raise it promptly; don't hang.
- **`close()` is safe to call more than once**, and wakes any pending `recv()`
  with `ConnectionError`.
- **Every wait is bounded.** Use `asyncio.timeout` around reads and writes that
  could otherwise block forever.
- **Enforce a size limit** on incoming messages before allocating them.

`App` calls `recv()` and `send()` alternately from one task. `Client` does the
same from whichever task makes the call, protected by its own lock. You don't
need to support concurrent `recv()` calls.

## The many-peer interface

`MulticastApp` uses `MulticastTransport`, where every message carries the peer it
came from:

```python
class MulticastTransport(Protocol):
    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def recv(self) -> tuple[str, bytes]: ...       # (client_id, message)
    async def send(self, client_id: str, data: bytes) -> None: ...
    async def broadcast(self, data: bytes) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc) -> None: ...
```

`client_id` is any string that identifies a connection for its lifetime. Don't
reuse ids. `MulticastApp` calls `send()` from many tasks at once, so `send()` for
different peers must be safe to run concurrently.

## Optional hooks

wire_rpc looks for these and uses them when present:

| Hook | Used by | Purpose |
|---|---|---|
| `max_message_size: int` (property) | `App`, `MulticastApp` | Caps the app's `max_response_size`, so the app never builds a reply the transport would refuse. |
| `async get_principal(client_id=None) -> str \| None` | `App`, `MulticastApp` | The authenticated identity for `current_request().principal`. Called before every request; raise `PermissionError` to deny it (`-32001`). |
| `requires_text_codec = True` | `App`, `MulticastApp`, `Client` | Rejects binary codecs at construction. Set it if you can only carry text. |
| `async finish_notification()` | `App` | Called instead of `send()` after a notification, when the transport must still finish an exchange (HTTP sends `204`). |
| `async startup()` / `async shutdown()` | `App`, `MulticastApp` | Run before the app's startup hook and after its shutdown hook, for resources like an authenticator's database. |
| `supports_server_push = False` | `Client` | Makes `receive_notification()` raise `RuntimeError` instead of waiting forever. |
| `stats` (property) | you | A dict of counters for your monitoring. |

## Example: an in-memory transport over queues

This connects a client and a server in the same event loop, which is useful in
tests. It shows the whole contract in a few lines:

```python
import asyncio
from typing import Self


class QueueTransport:
    """One end of an in-memory channel; make two with ``QueueTransport.pair()``."""

    max_message_size = 1024 * 1024

    def __init__(self, inbox: asyncio.Queue, outbox: asyncio.Queue):
        self._inbox, self._outbox = inbox, outbox
        self._closed = False

    @classmethod
    def pair(cls) -> tuple["QueueTransport", "QueueTransport"]:
        a, b = asyncio.Queue(maxsize=256), asyncio.Queue(maxsize=256)
        return cls(a, b), cls(b, a)

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            for queue in (self._inbox, self._outbox):
                queue.put_nowait(None)          # wake whoever is waiting on either side

    async def recv(self) -> bytes:
        if self._closed:
            raise ConnectionError("closed")
        message = await self._inbox.get()
        if message is None:
            self._closed = True
            raise ConnectionError("closed")
        return message

    async def send(self, data: bytes) -> None:
        if self._closed:
            raise ConnectionError("closed")
        if not data or len(data) > self.max_message_size:
            raise ValueError("bad message size")
        await asyncio.wait_for(self._outbox.put(bytes(data)), timeout=10)

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()
```

```python
client_end, server_end = QueueTransport.pair()
app = App(server_end)
...
serving = asyncio.create_task(app.serve())
async with Client(client_end) as client:
    print(await client.call("add", int, {"a": 2, "b": 3}))
await serving    # the server end sees ConnectionError and the app shuts down
```

## Testing a transport

The end-to-end suite in `tests/e2e/` runs every built-in transport through the
same attacks: wrong-size messages, concurrent callers, peers that vanish
mid-call, batches and notifications. Adding a branch for your transport to
`tests/e2e/harness.py` gets it the same coverage.
