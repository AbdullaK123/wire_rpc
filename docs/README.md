# wire_rpc documentation

wire_rpc is a typed asyncio framework for JSON-RPC 2.0. You write handlers as
plain `async` functions with type hints; wire_rpc validates what comes in,
encodes what goes out, and carries it over the transport you choose: TCP,
WebSocket, HTTP, a child process's stdin/stdout, or a pair of threads.

## Start here

- [Getting started](getting-started.md): install, write a server and a client, and run them.

## Building a server

- [Handlers](handlers.md): signatures, typed params and results, `ctx`, and request metadata.
- [Routers and middleware](routers-and-middleware.md): grouping methods, and code that runs around every call.
- [Apps and lifecycle](apps-and-lifecycle.md): `App` versus `MulticastApp`, startup and shutdown, running inside your own event loop, broadcasting.
- [Errors](errors.md): what callers see when something fails, and every error code.

## Calling a server

- [The client](client.md): calls, notifications, batches, server notifications, timeouts and reconnecting.

## Transports and codecs

- [Transports overview](transports/README.md): how to choose, and what every transport has in common.
  - [TCP](transports/tcp.md) · [WebSocket](transports/websocket.md) · [HTTP](transports/http.md) · [stdio](transports/stdio.md) · [Threads](transports/thread.md)
  - [Writing your own transport](transports/custom.md)
- [Codecs](codecs.md): JSON, MessagePack and Pydantic, and writing your own.

## Security

- [Authentication overview](authentication/README.md): how identity flows from a connection to your handler.
  - [HTTP and WebSocket](authentication/http-and-websocket.md): bearer tokens, API keys, cookie login, client certificates.
  - [TCP](authentication/tcp.md): token and HMAC handshakes, client certificates.
  - [Credential validators](authentication/credentials.md): passwords, API keys, JWTs, session tokens.
  - [Session stores](authentication/sessions.md): in-memory, SQLite, Redis and PostgreSQL.
- [Security model](security.md): TLS, browser origins, what wire_rpc protects and what is yours to handle.

## Running in production

- [Limits](limits.md): every size, count and time limit, and how to tune them.
- [Observability](observability.md): logs, the access log, metrics and counters.
- [Deployment](deployment.md): a checklist, reverse proxies, scaling and process supervision.
- [Benchmarks](benchmarks.md): rough latency and throughput per transport.

## Reference

- [Protocol support](protocol.md): exactly which parts of JSON-RPC 2.0 wire_rpc implements, and how.
- [Configuration reference](reference/configuration.md): every constructor parameter and its default.
- [Testing your service](testing.md): running a real server inside your tests.

---

Working on wire_rpc itself? Design notes, hardening reviews and the test-suite
layout live in [`dev_notes/`](../dev_notes/README.md).
