# Getting started

This page takes you from nothing to a working server and client in a few minutes.

## Install

wire_rpc needs Python 3.12 or newer.

```sh
uv add wire-rpc          # or: pip install wire-rpc
```

The core install includes every transport and codec. Some authentication
features need an extra:

| Extra | Adds | Needed for |
|---|---|---|
| `password` | argon2-cffi | `PasswordCredentialValidator`, `StaticPasswordCredentialValidator` |
| `jwt` | PyJWT | `JwtTokenValidator` |
| `redis` | redis | `RedisSessionStore` |
| `postgres` | asyncpg | `PostgresSessionStore` |

```sh
uv add "wire-rpc[jwt,redis]"
```

## Write a server

Create `server.py`:

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


@app.method("ping")
async def ping(ctx) -> str:
    return "pong"


if __name__ == "__main__":
    app.run()
```

What's happening:

- `App` is the server. It takes a **transport**, which decides how bytes move.
  `TcpServerTransport` listens on a TCP port.
- `@app.method("add")` registers a handler under the JSON-RPC method name `add`.
- The type hint on `params` (`AddParams`) tells wire_rpc how to validate incoming
  params. A call with `{"a": "two"}` never reaches your function; the caller gets
  an *Invalid params* error instead.
- The return hint (`-> int`) is how the result is encoded.
- `ctx` is your application context; see [Apps and lifecycle](apps-and-lifecycle.md).
  It's `None` here.
- `app.run()` starts the server and blocks. It also prints logs to stderr.

Run it:

```sh
python server.py
```

```
2026-10-23 14:02:11,512 INFO     wire_rpc: TCP server listening on 127.0.0.1:9000
```

## Write a client

Create `client.py`:

```python
import asyncio

from wire_rpc import Client
from wire_rpc.transports.tcp import TcpClientTransport


async def main():
    async with Client(TcpClientTransport(host="127.0.0.1", port=9000)) as client:
        print(await client.call("add", int, {"a": 2, "b": 3}))   # 5
        print(await client.call("ping", str))                     # pong


asyncio.run(main())
```

`client.call(method, response_type, params)` sends a request and converts the
result to `response_type`. Run it in a second terminal:

```sh
python client.py
```

The server's terminal shows one access-log line per call:

```
2026-10-23 14:02:13,040 INFO     wire_rpc.access: add ok 0.3ms peer=-
2026-10-23 14:02:13,041 INFO     wire_rpc.access: ping ok 0.1ms peer=-
```

`TcpServerTransport` serves exactly one client connection, so the server exits
when this client disconnects. For a server that many clients use at once, see
[`MulticastApp`](apps-and-lifecycle.md#app-or-multicastapp).

## Handle an error

```python
from wire_rpc.client import WireRpcError

try:
    await client.call("add", int, {"a": "two", "b": 3})
except WireRpcError as error:
    print(error.code, error.message)   # -32602 Invalid params
```

Every error code is listed in [Errors](errors.md).

## Talk to it without wire_rpc

The server speaks plain JSON-RPC 2.0, so anything that can open the transport
can call it. Over HTTP, for example, a server built with `HttpServerTransport`
answers `curl`:

```sh
curl -s http://127.0.0.1:8080/rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc": "2.0", "method": "add", "params": {"a": 2, "b": 3}, "id": 1}'
```

```json
{"result":5,"id":1,"jsonrpc":"2.0"}
```

## Where next

- More about writing handlers: [Handlers](handlers.md).
- Picking a transport: [Transports overview](transports/README.md).
- Adding authentication: [Authentication overview](authentication/README.md).
