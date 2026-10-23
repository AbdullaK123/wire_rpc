# Transports

A transport moves whole messages between a client and a server. It never looks
inside them; the codec and the app do that. Swapping transports doesn't change
your handlers.

## Choosing

| Transport | Server classes | Client class | Best for |
|---|---|---|---|
| [TCP](tcp.md) | `TcpServerTransport` (one client), `TcpMulticastServerTransport` (many) | `TcpClientTransport` | Service-to-service links. Lowest overhead per call. |
| [WebSocket](websocket.md) | `WsServerTransport` (one client), `MulticastWsServerTransport` (many) | `WsClientTransport` | Browsers, and anything that needs server push through HTTP infrastructure. |
| [HTTP](http.md) | `HttpServerTransport` | `HttpClientTransport` | Request/response through proxies, load balancers and API gateways; `curl`-friendly. |
| [stdio](stdio.md) | `StdIoServerTransport` | `StdIoTransport` | Plugins and worker processes the parent launches. |
| [Threads](thread.md) | `ThreadTransport.pair()` | the other end of the pair | Two threads in one process, and tests. |

Rough guide:

- Browser involved? **WebSocket** (needs push) or **HTTP** (doesn't).
- Two services you control? **TCP**, with TLS and token or HMAC authentication.
- Many clients that all need updates? A **multicast** TCP or WebSocket server
  with `MulticastApp`.
- Launching a helper process? **stdio**.
- Same process? **Threads**.

Per-call speed for each is in [Benchmarks](../benchmarks.md#results-2026-10-23).

## What every transport has in common

- **A maximum message size.** Larger messages are refused. Defaults: 1 MiB for
  TCP, HTTP and WebSocket; 16 MiB for stdio and threads. The app's
  `max_response_size` is automatically capped to the server transport's limit.
- **Bounded queues.** Incoming messages wait in queues capped by count and bytes.
  A peer that sends faster than the server handles requests is slowed down or
  refused, never buffered without limit.
- **Deadlines on every wait.** Connecting, authenticating, reading, writing and
  closing all have timeouts.
- **Clean shutdown.** Closing a transport wakes everything waiting on it and
  releases queued messages.

Every limit is listed in [Limits](../limits.md), and every parameter in the
[configuration reference](../reference/configuration.md).

## Codecs and transports

HTTP and WebSocket carry text, so they need a text codec: `MsgSpecJsonCodec` (the
default) or `PydanticCodec`. TCP, stdio and threads carry bytes and work with
every codec, including `MsgSpecMsgPackCodec`. Pairing a binary codec with a text
transport raises `ValueError` when you build the `App` or `Client`. See
[Codecs](../codecs.md).

## One-client transports end the app

`TcpServerTransport`, `WsServerTransport`, `StdIoServerTransport` and
`ThreadTransport` each serve exactly one peer. When that peer disconnects, the
app shuts down and `run()` returns. That's right for a plugin, a test, or a
dedicated link; for a long-running server many clients use, pick a multicast
transport or HTTP.

## TLS

TCP, HTTP and WebSocket servers and clients take `ssl_context=`:

```python
import ssl

server_tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
server_tls.load_cert_chain("server.pem", "server.key")

client_tls = ssl.create_default_context(cafile="ca.pem")   # verifies the server
```

Clients verify the server's certificate and hostname by default. Clients that
send credentials refuse to run without that verification unless you opt out for
local testing. For mutual TLS and client-certificate identity, see
[Authentication](../authentication/README.md). You can also terminate TLS at a
reverse proxy; see [Deployment](../deployment.md#reverse-proxies).

## Writing your own

See [Writing a custom transport](custom.md).
