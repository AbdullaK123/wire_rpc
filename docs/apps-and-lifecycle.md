# Apps and lifecycle

An app ties a transport, a codec and your handlers together, and owns the
lifecycle: startup, serving, and shutdown.

## `App` or `MulticastApp`?

| | `App` | `MulticastApp` |
|---|---|---|
| Peers | One connection | Many connections, each with a `client_id` |
| Concurrency | One request at a time, in order | Up to `max_concurrency` (16) at once, across peers |
| Per-peer ordering | n/a | One request at a time per peer; a second overlapping one gets `-32002` |
| Push to clients | No | `await app.broadcast(method, params)` |
| Transports | `TcpServerTransport`, `WsServerTransport`, `HttpServerTransport`, `StdIoServerTransport`, `ThreadTransport` | `TcpMulticastServerTransport`, `MulticastWsServerTransport` |

Use `App` for one-to-one links: a plugin process, a single service connection, a
test, or HTTP (where each request is independent anyway). Use `MulticastApp` for
a server that many clients connect to at once.

```python
from wire_rpc import App, MulticastApp
from wire_rpc.transports.tcp import TcpMulticastServerTransport

app = MulticastApp(TcpMulticastServerTransport(host="0.0.0.0", port=9000))
```

Both take the same options; see the [configuration reference](reference/configuration.md#app-and-multicastapp).

### Why `App` handles one request at a time

`App` reads a request, runs it to completion, sends the reply, then reads the
next. That keeps replies in order on a single connection and makes handlers easy
to reason about, but a slow handler delays everything behind it. If that
matters, use `MulticastApp`, move slow work out of the handler, or have clients
batch their calls (see [Benchmarks](benchmarks.md)).

### `MulticastApp` and busy peers

Each peer may have one request running at a time. If a client sends a second
request before the first finishes, the second gets `-32002 Peer already has an
active request` immediately rather than queuing. This stops one client from
filling the execution pool with its own backlog. wire_rpc's `Client` never does
this, since it sends one call at a time. Clients that pipeline should use
[batches](client.md#batches) instead.

## Startup and shutdown

```python
@app.on_startup
async def startup():
    pool = await create_pool(DSN)
    return AppState(db=pool)          # becomes ctx for every handler

@app.on_shutdown
async def shutdown(state):
    await state.db.close()
```

Each app has one startup hook and one shutdown hook. Registering a second
replaces the first.

The full sequence when the app runs:

1. **Transport startup.** If the transport or its authenticator has its own
   startup step (for example a session store creating its table), it runs.
2. **Your startup hook.** Its return value becomes `ctx`. Steps 1 and 2 together
   must finish within `startup_timeout` (30 seconds).
3. **Serving.** The transport opens and requests are handled until the transport
   closes or `stop()` is called.
4. **Your shutdown hook** receives `ctx`. It must finish within `shutdown_timeout`
   (10 seconds).
5. **Transport shutdown.** Also within `shutdown_timeout`.

If startup fails, the steps that already succeeded are rolled back: transport
startup is undone, but your shutdown hook doesn't run, because your startup hook
never finished. The error propagates out of `run()` or `serve()`.

An app instance runs once. Calling `run()` or `serve()` a second time raises
`RuntimeError`; build a new app instead.

## Running

### `run()`: a standalone program

```python
if __name__ == "__main__":
    app.run()
```

`run()` creates an event loop, serves until the transport closes or `stop()` is
called, then returns. It also sets up logging to stderr, like starting a server
with Uvicorn. Pass `log_level="warning"` for less output, or `log_level=None` to
leave logging alone. See [Observability](observability.md#logging).

### `serve()`: inside your own event loop

When your program already runs asyncio, for example alongside another server,
use `await app.serve()`:

```python
async def main():
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(app.serve())
        tasks.create_task(other_service())

asyncio.run(main())
```

`serve()` does not configure logging.

### Stopping

```python
await app.stop()
```

`stop()` stops accepting new requests and lets the running app drain:

- `App` gives the request in progress up to `shutdown_timeout` to finish, then
  cancels it.
- `MulticastApp` gives all running requests up to `shutdown_timeout`, then
  cancels whatever is left.

Then the shutdown steps above run, and `run()`/`serve()` returns. `stop()` itself
returns immediately; await the `serve()` task to know when shutdown has finished.

For signal handling in a standalone program:

```python
import signal

async def main():
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(app.stop()))
    await app.serve()
```

### When the transport closes by itself

Some transports serve exactly one peer: `TcpServerTransport`,
`WsServerTransport`, `StdIoServerTransport` and `ThreadTransport`. When that peer
disconnects, the app shuts down and `run()` returns. This is what you want for a
plugin process (exit when the parent goes away) and for tests. For a long-running
server, use a multicast transport or HTTP.

## Broadcasting

`MulticastApp.broadcast` sends a notification to every connected peer:

```python
@app.method("chat.post")
async def post(params: Message, ctx, client_id) -> None:
    await app.broadcast("chat.message", {"from": client_id, "text": params.text})
```

It's a JSON-RPC notification (no `id`), so clients don't reply. Delivery is
best-effort: a peer whose connection fails during the broadcast is dropped, and
the others still receive it. A slow peer doesn't hold up the others.

To send to one peer, encode a notification with the same codec the app uses and
hand it to the transport:

```python
codec = MsgSpecJsonCodec()            # the codec you passed to MulticastApp
app = MulticastApp(transport, codec=codec)

await app.transport.send(client_id, codec.encode(
    {"jsonrpc": "2.0", "method": "you.were.mentioned", "params": {"by": "alice"}}
))
```

Clients receive notifications as described in [The client](client.md#server-notifications).
Broadcasting over HTTP isn't possible, because HTTP servers can't push.

## Counters

`app.stats` returns a snapshot dict: `requests`, `active`, `completed`, `failed`,
`timed_out` and `invalid`. See [Observability](observability.md#counters).
