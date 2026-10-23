# stdio

The stdio transports connect a parent process to a child it launches, over the
child's standard input and output. Each message is a 4-byte big-endian length
followed by the payload, the same framing as TCP. Use them for plugins, language
servers, and worker processes.

```python
from wire_rpc.transports.stdio import StdIoServerTransport, StdIoTransport
```

## The usual setup: parent calls, child serves

**Child** (`worker.py`):

```python
from wire_rpc import App
from wire_rpc.transports.stdio import StdIoServerTransport

app = App(StdIoServerTransport())

@app.method("resize")
async def resize(params: ResizeJob, ctx) -> str: ...

app.run()
```

**Parent:**

```python
import sys
from wire_rpc import Client
from wire_rpc.transports.stdio import StdIoTransport

async with Client(StdIoTransport(sys.executable, "worker.py")) as worker:
    path = await worker.call("resize", str, {"src": "a.png", "width": 640})
```

`StdIoTransport(*cmd)` launches `cmd` when the client connects, and stops it when
the client closes. The child's app shuts down by itself when its stdin closes.

Either side can be the server; it's the transport that's tied to the process
roles. If the *parent* should serve and the *child* call, build an `App` on
`StdIoTransport` and a `Client` on `StdIoServerTransport`.

## Rules for the child

- **Never write to stdout.** stdout carries the RPC messages; a stray `print()`
  corrupts the stream and breaks the connection. `app.run()` sends logs to
  stderr, which is safe.
- **stderr is discarded.** The parent reads the child's stderr continuously so the
  child can't block on a full pipe, but throws it away (it counts the bytes in
  `transport.stats["stderr_bytes_discarded"]`). To keep the child's logs, have it
  log to a file:

  ```python
  import logging
  logging.basicConfig(filename="worker.log", level=logging.INFO)
  app.run()   # sees logging is configured and leaves it alone
  ```

## Stopping the child

When the parent closes the transport:

1. Pending reads and writes are cancelled.
2. The child's stdin is closed, so a wire_rpc child shuts down cleanly.
3. The parent waits up to `shutdown_timeout` (5 s) for it to exit,
4. then sends `SIGTERM` (`terminate()`) and waits up to `kill_timeout` (2 s),
5. then sends `SIGKILL` (`kill()`).

So a child that ignores its closed stdin is stopped within about 9 seconds, and
the parent never leaves a running child behind, even if closing is itself
cancelled. This supervises the direct child only, not processes it launched.

## Timeouts and limits

| Setting | Default | Applies to |
|---|---|---|
| `max_frame_size` | 16 MiB | Each message, up to 4 GiB − 1 |
| `read_timeout` | 30 s | Waiting for each message |
| `write_timeout` | 10 s | Sending a message |
| `shutdown_timeout` (parent) | 5 s | The child's graceful exit |
| `kill_timeout` (parent) | 2 s | Each of the terminate and kill waits |

`read_timeout` also covers the wait for a reply, so raise it if the child has
methods that run longer than 30 seconds.

## Identity

There's no authentication: the parent launched the child, so each trusts the
other. `trusted_principal="..."` sets the principal that handlers see in
`current_request().principal`, for example the name of the plugin or the user the
parent is acting for:

```python
app = App(StdIoServerTransport(trusted_principal="image-plugin"))
```

## Platform notes

Both sides work on Linux, macOS and Windows. `StdIoServerTransport` reads stdin
and writes stdout on two daemon threads rather than through the event loop,
because not every event loop can watch the process's own stdin and stdout
(Windows' can't). The threads feed the event loop, so your handlers still run on
it as usual.
