# wire_rpc

Typed asyncio JSON-RPC 2.0 with JSON/Pydantic/MessagePack codecs and TCP, HTTP,
WebSocket, stdio, and in-process thread transports.

## Quick start

A server registers handlers on an `App`. The type hint on `params` sets how
incoming params are validated, and the return hint sets how the result is encoded.

```python
# server.py
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


if __name__ == "__main__":
    app.run()
```

A client calls a method by name and names the type it expects back:

```python
# client.py
import asyncio
from wire_rpc import Client
from wire_rpc.transports.tcp import TcpClientTransport


async def main():
    async with Client(TcpClientTransport(host="127.0.0.1", port=9000)) as client:
        print(await client.call("add", int, {"a": 2, "b": 3}))  # 5


asyncio.run(main())
```

A server error reaches the client as `WireRpcError`, which carries the JSON-RPC
`code` and `message`. Handler exception details are never sent to the client.

Batches (a JSON array of requests, as in section 6 of the JSON-RPC 2.0 spec)
work on every transport. A batch shares one `handler_timeout`, holds at most
`max_batch_size` calls (100 by default), and its reply always fits
`max_response_size`.

Handlers take `(ctx)` or `(params, ctx)`. `ctx` is whatever the
`@app.on_startup` hook returned, such as a database pool. Trusted request
metadata, including the authenticated principal, comes from `current_request()`.
Group related methods with `Router("prefix")`, and wrap them with
`@app.middleware` or `@router.middleware`.

## Transports

`App` serves one peer, and requests run one at a time. `MulticastApp` serves many
peers, each identified by a `client_id`, and can broadcast notifications to all of them.

| Transport | Use with | Notes |
|---|---|---|
| `TcpServerTransport` / `TcpClientTransport` | `App` / `Client` | Length-prefixed frames, optional TLS. The server serves one connection; the app stops when it closes. |
| `TcpMulticastServerTransport` | `MulticastApp` | Many TCP clients, bounded per-client queues. |
| `HttpServerTransport` / `HttpClientTransport` | `App` / `Client` | One `POST /rpc` per call. Text codecs only. |
| `WsServerTransport` / `WsClientTransport` | `App` / `Client` | One WebSocket connection on `/ws`. Text codecs only. |
| `MulticastWsServerTransport` | `MulticastApp` | Many WebSocket clients, for browsers. |
| `StdIoTransport` / `StdIoServerTransport` | `Client` / `App` | Talk to a child process over its stdin/stdout. |
| `ThreadTransport` | `App` + `Client` | Two threads in one process, each with its own event loop. |

Every transport enforces a maximum message size. Authentication is set per transport with
`auth=`; see the [authentication suite](docs/authentication.md).

### Between threads

`ThreadTransport.pair()` returns two connected ends. Give one to each thread;
each end binds to the event loop of the thread that connects it. Sending copies
the message, and an end that is full (`max_queue_size` unread messages) rejects
new sends instead of growing without limit. Either thread can start first: a
send waits up to `connect_timeout` (5 seconds by default) for the other end to
connect.

```python
import asyncio
import threading

from wire_rpc import App, Client
from wire_rpc.transports import ThreadTransport

client_end, server_end = ThreadTransport.pair()
app = App(server_end)


@app.method("add")
async def add(params: dict, ctx) -> int:
    return params["a"] + params["b"]


threading.Thread(target=app.run, daemon=True).start()


async def main():
    async with Client(client_end) as client:
        print(await client.call("add", int, {"a": 2, "b": 3}))  # 5


asyncio.run(main())
```

## Development and release checks

Requires Python 3.12 or newer, uv, and a running Docker daemon for the full suite.
Testcontainers starts isolated PostgreSQL 17 and Redis 7 services automatically.
Missing Docker or a failed container startup fails the tests; no backend is silently skipped.
To explicitly run only tests without Docker: `uv run --locked pytest -q -W error -m "not integration"`.

```sh
uv sync --locked
uv run --locked pyrefly check
uv run --locked ruff check
uv run --locked pytest -q -W error
uv build --no-sources
uv run --locked python scripts/check_wheel.py
```

Pyrefly checks the package, tests, and release scripts against a Python 3.12
baseline, configured in `[tool.pyrefly]`, and runs in CI.
Ruff enforces pyflakes rules and import order, configured in `[tool.ruff]`;
`uv run ruff check --fix` sorts imports.

Tests are organized by attack surface. Concurrency tests use events and
controlled ordering; socket tests use ephemeral ports. Test timeouts are
deadlock guards. The CI matrix covers Python 3.12–3.14 on Linux, macOS, and Windows.

Releases follow semantic versioning; see the [changelog](CHANGELOG.md) for
the policy and for breaking changes. Report vulnerabilities privately as described
in [SECURITY.md](SECURITY.md).

Read [production contracts](docs/production_contracts.md) before deploying,
including codec compatibility, identity propagation, overload behavior,
cancellation semantics, resource budgets, and platform limitations.

The [execution plan](docs/production_readiness_plan.md) records the follow-up
hardening work. The [initial review](docs/transport_codec_hardening_review.md)
records the first transport/codec regression pass.

The [authentication suite](docs/authentication.md) documents authenticators,
credential validators, session backends, client credentials, TLS policy, and
adversarial backend tests. Optional extras are `password`, `jwt`, `redis`, and
`postgres`; core imports do not require them.
