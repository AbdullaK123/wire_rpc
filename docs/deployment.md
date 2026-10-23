# Deployment

This page covers running a wire_rpc service in production: a checklist, reverse
proxies, scaling, and supervision.

## Checklist

Before a wire_rpc service takes real traffic:

- [ ] **Transport and app.** A multicast transport or HTTP for a server many
      clients use; `App` with a one-client transport only for dedicated links.
- [ ] **TLS** on every network hop, or a TLS-terminating proxy on a network you
      trust. See [Security model](security.md#tls).
- [ ] **Authentication** configured, and **authorization** checked in handlers.
      See [Authentication](authentication/README.md).
- [ ] **A shared session store** (Redis or PostgreSQL) if you run more than one
      process and use sessions.
- [ ] **Limits** sized to your largest legitimate messages and slowest methods;
      timeouts raised consistently on server and client. See [Limits](limits.md).
- [ ] **An origin policy** on anything browsers reach: the same-origin default, or
      an explicit `allowed_origins` behind a TLS-terminating proxy or for pages on
      another origin.
- [ ] **Rate limiting** at your edge, especially for `/login`.
- [ ] **Logs** collected and **metrics** exported. See [Observability](observability.md).
- [ ] **A load test** with your real methods and payloads. The
      [benchmarks](benchmarks.md) measure wire_rpc's overhead, not your workload.
- [ ] **Graceful shutdown** wired to your supervisor's stop signal.
- [ ] **Idempotency** for any write that clients may retry after a timeout.

## Running the process

`app.run()` is all a standalone service needs. Run it under a supervisor that
restarts it if it dies: systemd, a container orchestrator, or similar.

A systemd unit:

```ini
[Unit]
Description=notes RPC
After=network-online.target

[Service]
ExecStart=/srv/notes/.venv/bin/python -m notes.server
Restart=on-failure
User=notes
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

### Graceful shutdown

Handle `SIGTERM` by stopping the app, so in-flight requests get
`shutdown_timeout` to finish:

```python
import asyncio
import signal

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(app.stop()))
    await app.serve()

asyncio.run(main())
```

Give your supervisor a stop timeout longer than the app's shutdown phases:
`shutdown_timeout` for draining, plus up to `shutdown_timeout` each for your
shutdown hook and transport shutdown.

### Blocking work

The event loop must never block. Run CPU-heavy or synchronous work with
`asyncio.to_thread`, a process pool, or a separate worker service. A handler that
blocks can't be cancelled by `handler_timeout` and stalls every other request.

## Reverse proxies

### HTTP

Any HTTP reverse proxy works: forward `POST /rpc` (and `/login`, `/logout` if
you use cookie auth). Make sure the proxy's body size limit and timeouts are at
least as large as wire_rpc's, or the proxy will cut requests off first.

nginx:

```nginx
location /rpc {
    proxy_pass http://127.0.0.1:8080;
    client_max_body_size 1m;
    proxy_read_timeout 35s;
}
```

### WebSocket

Proxies must pass the `Upgrade` header and keep connections open:

```nginx
location /ws {
    proxy_pass http://127.0.0.1:8000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Origin $http_origin;
    proxy_read_timeout 1h;
}
```

wire_rpc pings every 30 seconds, which keeps idle connections alive through most
proxies.

### Origins behind a proxy

The default origin policy accepts browsers from the server's own origin, judged
by the scheme and `Host` of the request wire_rpc receives. Behind a proxy those
usually differ from what the browser sees (`http://127.0.0.1:8000` versus
`https://app.example.com`), so browser requests get `403`. Set
`allowed_origins={"https://app.example.com"}` on servers behind a proxy.

### TLS at the proxy

If the proxy terminates TLS and forwards plain HTTP to wire_rpc:

- Keep the hop from the proxy to wire_rpc on a private network or loopback.
- Authenticators that require TLS see a plain connection. Set
  `require_tls=False` on `BearerAuth` and `ApiKeyAuth`, and use
  `CookieSessionAuth(..., secure=True, require_tls=False)` so cookies stay
  `Secure`. wire_rpc doesn't read `X-Forwarded-Proto`; the proxy is trusted
  because of where it sits, not because of a header.
- Client-certificate authentication needs wire_rpc to see the TLS connection
  itself. It doesn't trust certificate details forwarded in headers.

### TCP

Use a layer-4 (TCP) load balancer, or connect clients directly. With TLS
passthrough, wire_rpc terminates TLS itself. The TCP framing isn't HTTP, so HTTP
proxies can't carry it.

## Scaling

**Run more processes.** Each wire_rpc app runs on one event loop on one CPU core.
To use more cores, run several processes behind a load balancer:

- **HTTP**: any load balancer; every request is independent.
- **WebSocket and TCP**: connections stick to the process they opened on.
  Broadcasts only reach that process's clients, so if clients on different
  processes must see the same broadcasts, fan them out through something shared
  like Redis pub/sub, with each process subscribing and calling
  `app.broadcast`.
- **Sessions**: use `RedisSessionStore` or `PostgresSessionStore`, so a login on
  one process is valid on all of them.

**Within a process,** `MulticastApp` runs up to `max_concurrency` requests at
once, which helps when handlers wait on I/O. `App` runs one at a time.

**On the client side,** batch calls where you can. A batch of 100 is 15 to 30
times faster than 100 separate calls ([Benchmarks](benchmarks.md)).

## Platform notes

- Linux, macOS and Windows are tested in CI for Python 3.12, 3.13 and 3.14.
- `StdIoServerTransport` relies on asyncio pipe support for the process's own
  stdin and stdout; verify it on Windows before depending on it there.
- Use separate Redis namespaces or PostgreSQL namespaces per application, and per
  environment if they share a backend.

## Upgrading

Read the [changelog](../CHANGELOG.md) before upgrading. While wire_rpc is `0.x`,
a minor release can break things, and every breaking change is listed with what
to do about it. Upgrade the client and server together when the changelog says
a wire format changed.
