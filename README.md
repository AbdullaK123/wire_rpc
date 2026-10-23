# wire_rpc

Typed asyncio JSON-RPC 2.0 for Python, with pluggable transports, codecs and
authentication.

- **Typed handlers.** Write plain `async` functions with type hints; params are
  validated before your code runs and results are checked on the way out.
- **Seven transports, one API.** TCP, WebSocket, HTTP, stdio and in-process
  threads, with one-client and many-client servers. Swap transports without
  touching handlers.
- **Three codecs.** JSON (msgspec), Pydantic, and MessagePack.
- **Full JSON-RPC 2.0.** Calls, notifications and batches, plus server push
  (`broadcast`) on TCP and WebSocket.
- **Authentication built in.** Bearer tokens, JWTs, API keys, cookie login,
  TCP token and HMAC handshakes, and mutual TLS, with sessions in memory, SQLite,
  Redis or PostgreSQL.
- **Safe by default.** Every message, queue and wait is bounded; errors never leak
  exception text; credentials are re-checked before every call and never sent
  without TLS.
- **Observable.** An access log, a metrics hook and counters.

Python 3.12+. [Documentation](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/README.md) ·
[Changelog](https://github.com/AbdullaK123/wire_rpc/blob/main/CHANGELOG.md) ·
[PyPI](https://pypi.org/project/wire-rpc/)

## Install

```sh
pip install wire-rpc
# extras: password, jwt, redis, postgres
pip install "wire-rpc[jwt,redis]"
```

## Quick start

A server:

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


if __name__ == "__main__":
    app.run()
```

A client:

```python
import asyncio

from wire_rpc import Client
from wire_rpc.transports.tcp import TcpClientTransport


async def main():
    async with Client(TcpClientTransport(host="127.0.0.1", port=9000)) as client:
        print(await client.call("add", int, {"a": 2, "b": 3}))   # 5


asyncio.run(main())
```

Batches and notifications:

```python
from wire_rpc.client import BatchCall, BatchNotification

results = await client.batch([
    BatchCall("add", int, {"a": 1, "b": 2}),
    BatchNotification("audit.log", {"event": "added"}),
])
```

## Transports

| Transport | Server | Client | Best for |
|---|---|---|---|
| TCP | `TcpServerTransport`, `TcpMulticastServerTransport` | `TcpClientTransport` | Service-to-service; fastest |
| WebSocket | `WsServerTransport`, `MulticastWsServerTransport` | `WsClientTransport` | Browsers, server push |
| HTTP | `HttpServerTransport` | `HttpClientTransport` | Proxies, load balancers, `curl` |
| stdio | `StdIoServerTransport` | `StdIoTransport` | Plugins and worker processes |
| Threads | `ThreadTransport.pair()` | the other end | In-process, and tests |

## Documentation

- [Getting started](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/getting-started.md)
- [Handlers](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/handlers.md) ·
  [Routers and middleware](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/routers-and-middleware.md) ·
  [Apps and lifecycle](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/apps-and-lifecycle.md) ·
  [Errors](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/errors.md)
- [The client](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/client.md)
- [Transports](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/transports/README.md) ·
  [Codecs](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/codecs.md)
- [Authentication](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/authentication/README.md) ·
  [Security model](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/security.md)
- [Limits](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/limits.md) ·
  [Observability](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/observability.md) ·
  [Deployment](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/deployment.md) ·
  [Benchmarks](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/benchmarks.md)
- [Protocol support](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/protocol.md) ·
  [Configuration reference](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/reference/configuration.md) ·
  [Testing your service](https://github.com/AbdullaK123/wire_rpc/blob/main/docs/testing.md)

## Versioning and security

wire_rpc follows semantic versioning; while it's `0.x`, minor releases may break
compatibility, and every breaking change is listed in the
[changelog](https://github.com/AbdullaK123/wire_rpc/blob/main/CHANGELOG.md) with
what to do. Report vulnerabilities privately as described in
[SECURITY.md](https://github.com/AbdullaK123/wire_rpc/blob/main/SECURITY.md).

## Contributing

Development setup, the test suite, design notes and the release process are in
[dev_notes/](https://github.com/AbdullaK123/wire_rpc/blob/main/dev_notes/README.md).

## License

MIT
