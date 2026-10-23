# Threads

`ThreadTransport` connects two threads of the same process, each running its own
event loop. Nothing touches the network or the filesystem.

```python
import threading

from wire_rpc import App, Client
from wire_rpc.transports import ThreadTransport

client_end, server_end = ThreadTransport.pair()

app = App(server_end)

@app.method("add")
async def add(params: dict, ctx) -> int:
    return params["a"] + params["b"]

threading.Thread(target=app.run, daemon=True).start()

async with Client(client_end) as client:
    print(await client.call("add", int, {"a": 2, "b": 3}))
```

## How it works

`pair()` creates two connected ends. Give one to each thread. Each end binds to
the event loop of the thread that connects it, which happens when the `App` or
`Client` starts.

Sending puts the message straight into the other end's inbox, handing it across
with `loop.call_soon_threadsafe`, the one asyncio call that's safe to make from
another thread.

**Either thread can start first.** If one end sends before the other has
connected, the send waits up to `connect_timeout` (5 s) for it. If the other end
closes before connecting, the send fails at once instead of waiting.

**Messages are copied** when sent, so the sender can reuse its buffer.

## Limits

| Setting | Default | Meaning |
|---|---|---|
| `max_message_size` | 16 MiB | Largest message; empty messages are rejected too |
| `max_queue_size` | 256 | Unread messages an end will hold; a send to a full end fails instead of waiting |
| `connect_timeout` | 5 s | How long a send waits for the other end to connect |

Pass them to `pair()` so both ends share them.

## When to use it

- **Tests.** A real server and client with no ports and nothing to clean up; see
  [Testing your service](../testing.md).
- **Isolating an event loop.** Run a component on its own thread and loop, for
  example a library that needs a dedicated loop, and talk to it over a typed API.
- **Calling async code from a synchronous program.** Run the server in a
  background thread and a client per request.

It isn't faster than TCP: every message wakes the other thread's event loop,
which costs more than a loopback socket write. See [Benchmarks](../benchmarks.md).

## Errors

- Sending after either end has closed raises `ConnectionError`.
- Sending to a full end raises `ConnectionError("Peer inbox full")`.
- Sending an empty or over-size message raises `InvalidFrameSizeError`.
- Connecting an end twice, or after it has closed, raises `RuntimeError`.
