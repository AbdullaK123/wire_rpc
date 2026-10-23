# wire_rpc user guide

This guide shows how to build a JSON-RPC 2.0 service with wire_rpc and how to call
it. For exact limits and guarantees, see [production contracts](production_contracts.md).

## Install

```sh
uv add wire-rpc                  # core: TCP, HTTP, WebSocket, stdio, threads
uv add "wire-rpc[jwt]"           # JwtTokenValidator
uv add "wire-rpc[password]"      # PasswordCredentialValidator (Argon2)
uv add "wire-rpc[redis]"         # RedisSessionStore
uv add "wire-rpc[postgres]"      # PostgresSessionStore
```

Python 3.12 or newer.

## A server and a client

```python
import msgspec
from wire_rpc import App
from wire_rpc.transports.tcp import TcpServerTransport


class AddParams(msgspec.Struct):
    a: int
    b: int


app = App(TcpServerTransport(host="127.0.0.1", port=9000))


@app.method("add")
async def add(params: AddParams, ctx) -> int:
    return params.a + params.b


app.run()
```

```python
import asyncio
from wire_rpc import Client
from wire_rpc.transports.tcp import TcpClientTransport


async def main():
    async with Client(TcpClientTransport(host="127.0.0.1", port=9000)) as client:
        print(await client.call("add", int, {"a": 2, "b": 3}))  # 5


asyncio.run(main())
```

## Handlers

A handler is `async (ctx)` or `async (params, ctx)`:

- The type hint on `params` decides how incoming params are validated: a
  `msgspec.Struct`, a Pydantic model (with `PydanticCodec`), a `dict`, or a plain
  type. Params that don't match never reach the handler; the caller gets
  `-32602 Invalid params`.
- The return hint decides how the result is encoded.
- `ctx` is whatever your `@app.on_startup` hook returned (see below), such as a
  database pool.
- `current_request()` returns trusted metadata for the call: `request_id`,
  `method`, `principal` (the authenticated identity) and `connection_id`.

```python
from wire_rpc import current_request

@app.method("notes.create")
async def create_note(params: NewNote, ctx) -> NoteId:
    owner = current_request().principal
    return await ctx.db.insert_note(owner, params.text)
```

### Errors

Any exception in a handler becomes `-32603 Internal error` for the caller. The
exception text is never sent, so it can't leak secrets. `PermissionError` becomes
`-32001 Access denied`, and a handler that runs past `handler_timeout` (30 s by
default) becomes `-32000`.

## Organising methods with routers

```python
from wire_rpc import Router

notes = Router("notes")

@notes.method("create")
async def create(params: NewNote, ctx) -> NoteId: ...

@notes.method("list")
async def list_notes(ctx) -> list[Note]: ...

app.include_router(notes)   # registers "notes.create" and "notes.list"
```

Routers nest with `router.include_router(child)`; prefixes join with dots. For a
`MulticastApp`, create routers with `Router("notes", multicast=True)` so their
handlers may take `client_id`.

## Middleware

Middleware wraps every handler call. It receives the request, `ctx` and the next
step, and returns the response:

```python
from wire_rpc import current_request

@app.middleware
async def require_login(request, ctx, call_next):
    if current_request().principal is None:
        raise PermissionError("login required")
    return await call_next(request, ctx)
```

App middleware runs first, then router middleware, in registration order.
Authentication only tells you *who* is calling; check that they may touch the
specific resource inside the handler or middleware.

## Startup, shutdown and `ctx`

```python
@app.on_startup
async def startup():
    return await create_pool(DSN)      # becomes ctx for every handler

@app.on_shutdown
async def shutdown(pool):
    await pool.close()
```

`app.run()` blocks until the transport closes. Inside your own event loop, run
`await app._run()` as a task and call `await app.stop()` to drain and stop.

## One client or many: `App` and `MulticastApp`

`App` serves one peer and handles its requests one at a time. `MulticastApp` serves
many peers, runs up to `max_concurrency` requests at once (16 by default), and can
push notifications to every peer:

```python
from wire_rpc import MulticastApp
from wire_rpc.transports.tcp import TcpMulticastServerTransport

app = MulticastApp(TcpMulticastServerTransport(host="0.0.0.0", port=9000))

@app.method("post")
async def post(params: Message, ctx, client_id) -> None:
    await app.broadcast("message", {"from": client_id, "text": params.text})
```

Multicast handlers may take `client_id` as their last parameter. Each peer has at
most one request running; a second overlapping request from the same peer gets
`-32002`.

## Calling the server

```python
from wire_rpc.client import BatchCall, BatchNotification, Client, WireRpcError

async with Client(transport, call_timeout=10) as client:
    total = await client.call("add", int, {"a": 2, "b": 3})

    await client.notify("audit.log", {"event": "viewed"})      # no reply

    total, _, failed = await client.batch([
        BatchCall("add", int, {"a": 1, "b": 2}),
        BatchNotification("audit.log", {"event": "batched"}),
        BatchCall("might.fail", str),
    ])
    if isinstance(failed, WireRpcError):
        print(failed.code, failed.message)
```

- `call` raises `WireRpcError` (with `code`, `message`, `data`) when the server
  returns an error.
- `batch` returns one entry per item, in order: the result, a `WireRpcError`
  (returned, not raised) for a failed call, or `None` for a notification. A batch
  runs under one deadline, and is 15 to 30 times faster than the same calls sent
  one by one (see [benchmarks](benchmarks.md)).
- One `Client` makes one call at a time; concurrent `call`s queue. For parallel
  work, use several clients.

### Receiving broadcasts

Over TCP and WebSocket, a server can send notifications at any time. Pass a
callback to receive the ones that arrive while a call is waiting, or use a client
dedicated to listening:

```python
from wire_rpc.client import Notification

def on_message(note: Notification) -> None:
    print(note.method, note.params)

async with Client(transport, on_notification=on_message) as client:
    ...

async with Client(listener_transport) as listener:
    while True:
        note = await listener.receive_notification()
```

`receive_notification` holds the client while it waits, so give listening its own
client. HTTP cannot push, so it raises `RuntimeError` there.

### Failures and reconnecting

Any failure on the channel (connection dropped, timeout, unreadable reply) raises
`ConnectionError` or `TimeoutError` and **closes the client for good**. wire_rpc
never retries for you: after a timeout you can't know whether the server ran the
call, and retrying a call that changes data could run it twice.

If a method is safe to repeat (it reads, or it is idempotent), reconnect and retry
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

For calls that change data, make them idempotent first, for example with a
client-chosen request key that the server stores and checks.

## Choosing a transport

| Transport | Server | Client | Use it for |
|---|---|---|---|
| TCP | `TcpServerTransport` (one client), `TcpMulticastServerTransport` | `TcpClientTransport` | Service-to-service; fastest per call |
| WebSocket | `WsServerTransport`, `MulticastWsServerTransport` | `WsClientTransport` | Browsers, and anything that needs server push |
| HTTP | `HttpServerTransport` (`POST /rpc`) | `HttpClientTransport` | Request/response through proxies and load balancers |
| stdio | `StdIoServerTransport` (in the child) | `StdIoTransport` (launches it) | Plugins and worker processes |
| Thread | `ThreadTransport.pair()` | the other end | Two threads in one process, and tests |

HTTP and WebSocket carry text, so use `MsgSpecJsonCodec` (the default) or
`PydanticCodec` with them. `MsgSpecMsgPackCodec` is smaller and faster but works
only over TCP, stdio and threads. Client and server must use the same codec.

Every transport has a maximum message size (1 MiB by default for TCP, HTTP and
WebSocket). Pass `ssl_context=` to TCP, HTTP and WebSocket servers and clients for
TLS.

## Authentication

Server transports take `auth=`. The identity it establishes is
`current_request().principal`.

**Bearer tokens over HTTPS (JWT):**

```python
from wire_rpc.auth import BearerAuth, credential_headers
from wire_rpc.auth.credentials import JwtTokenValidator

validator = JwtTokenValidator({"key-1": PUBLIC_KEY}, algorithm="RS256",
                              issuer="https://auth.example.com", audience="rpc")
server = HttpServerTransport("0.0.0.0", 8443, auth=BearerAuth(validator), ssl_context=server_tls)

client = HttpClientTransport("https://rpc.example.com:8443/rpc",
                             headers=credential_headers(bearer=token), ssl_context=client_tls)
```

**Service-to-service over TCP:** `TcpTokenAuth` / `TcpTokenClientAuth` (a token
checked by any validator), `HmacChallengeAuth` / `HmacChallengeClientAuth` (a
shared secret that never crosses the wire), or `ClientCertificateAuth` (mutual
TLS). Token and HMAC authentication require TLS unless you pass `require_tls=False`.

**Browser login with a cookie:** `CookieSessionAuth` adds `POST /login` and
`POST /logout` to HTTP and WebSocket servers and keeps sessions in a
`SessionStore`: `InMemorySessionStore` for one process, `SQLiteSessionStore` for
one machine, `RedisSessionStore` or `PostgresSessionStore` to share across
processes.

Sessions and tokens are checked again before every call, so revoking one cuts off
an open connection at its next request. More detail:
[authentication](authentication.md).

## Monitoring

- `app.stats` counts requests, active, completed, failed, timed-out and invalid
  requests. Transports expose `stats` with queue and connection counts.
- `metrics_middleware` reports every executed call (method, duration, outcome,
  error code, principal) to a function you provide, for Prometheus,
  OpenTelemetry or logs:

  ```python
  from wire_rpc.middleware import CallRecord, metrics_middleware

  def export(call: CallRecord) -> None:
      rpc_latency.labels(call.method, call.outcome).observe(call.duration)

  app.middleware(metrics_middleware(export))   # register first
  ```

- Logging uses Python's standard `logging` module. `app.run()` prints logs to
  stderr at INFO, like starting a server with Uvicorn: one access line per request
  from `wire_rpc.access`, plus startup, connection and problem messages from
  `wire_rpc`.

  ```
  2026-10-23 14:02:11,512 INFO     wire_rpc: TCP server listening on 0.0.0.0:9000
  2026-10-23 14:02:13,040 INFO     wire_rpc.access: notes.create ok 2.4ms peer=-
  2026-10-23 14:02:13,377 INFO     wire_rpc.access: notes.get -32001 0.6ms peer=-
  ```

  The outcome is `ok`, `notification`, or the JSON-RPC error code. Params,
  results and exception text are never logged. Use `app.run(log_level="warning")`
  for less, `"debug"` for handler registration, or `log_level=None` to leave
  logging entirely to your application. If your application configures logging
  first (for example with `logging.basicConfig`), `run()` leaves it alone.
  Importing wire_rpc configures nothing; when you run the app inside your own
  event loop, only warnings and errors appear until you configure logging.

## Testing your service

`ThreadTransport` runs a real server in a background thread with no sockets, which
makes it a convenient test fixture:

```python
import threading
from wire_rpc import App, Client
from wire_rpc.transports import ThreadTransport

async def test_add():
    client_end, server_end = ThreadTransport.pair()
    app = App(server_end)
    register_handlers(app)
    threading.Thread(target=app.run, daemon=True).start()
    async with Client(client_end) as client:
        assert await client.call("add", int, {"a": 2, "b": 3}) == 5
```
