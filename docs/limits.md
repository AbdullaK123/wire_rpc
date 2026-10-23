# Limits

Everything that can grow in wire_rpc has a limit: message sizes, queue lengths,
connection counts and how long anything may wait. The defaults are safe for most
services; this page lists them all and explains when to change them.

## Default limits

### Messages

| What | Default | Set with |
|---|---|---|
| TCP message | 1 MiB | `max_frame_size` on the server and client transports |
| WebSocket message | 1 MiB | `max_msg_size` on the server and client transports |
| HTTP request or response body | 1 MiB | `max_body_size` on the server and client transports |
| stdio message | 16 MiB | `max_frame_size` on both transports |
| Thread message | 16 MiB | `max_message_size` on `ThreadTransport.pair()` |
| Encoded response (or batch reply) | 4 MiB, capped by the transport's message limit; at least 2 KiB | `max_response_size` on the app |
| Calls in one batch | 100 | `max_batch_size` on the app |

### Queues and connections

| What | Default | Set with |
|---|---|---|
| Multicast TCP / WebSocket connections | 64, including ones still authenticating | `max_connections` |
| Queued incoming messages (TCP, WebSocket) | 256 | `recv_queue_size` |
| Queued incoming bytes (TCP, WebSocket) | 16 MiB | `max_queue_bytes` |
| Queued messages per peer (multicast) | 16 | `per_peer_queue_size` |
| Requests running at once (`MulticastApp`) | 16 | `max_concurrency` |
| HTTP requests admitted at once | 64, including ones authenticating or uploading | `max_pending_requests` |
| HTTP request bodies queued | 16 MiB | `max_queue_bytes` |
| Unread messages per thread end | 256 | `max_queue_size` on `ThreadTransport.pair()` |
| Concurrent login/logout requests | 16 | fixed |
| Password hashes running at once | 4 per validator | `max_concurrency` on the validator |
| SQLite session operations at once | 4 per store | `max_concurrency` on the store |
| Live sessions | 10,000 per store or namespace | `max_sessions` |

### Time

| What | Default | Set with |
|---|---|---|
| A call, or a whole batch, on the server | 30 s | `handler_timeout` on the app |
| App startup (transport + your hook) | 30 s | `startup_timeout` |
| Each shutdown phase, and draining in-flight requests | 10 s | `shutdown_timeout` on the app |
| A call on the client, including queueing | 30 s | `call_timeout` on `Client` |
| TCP: finishing a message once it starts / writing | 30 s / 30 s | `read_timeout`, `write_timeout` |
| TCP: no traffic before the connection closes | 300 s | `idle_timeout` (`None` to disable) |
| TCP / WebSocket: authentication | 10 s | `auth_timeout` |
| TCP client: connect + TLS + authentication | 10 s | `connect_timeout` |
| WebSocket client: each incoming message | none (calls are bounded by `call_timeout`) | `receive_timeout` |
| WebSocket: writes / closing | 10 s / 5 s | `write_timeout`, `close_timeout` |
| HTTP: a request waiting for its response | 30 s | `request_timeout` |
| stdio: each message / writes | 30 s / 10 s | `read_timeout`, `write_timeout` |
| stdio: child exit, then terminate, then kill | 5 s, 2 s, 2 s | `shutdown_timeout`, `kill_timeout` |
| Thread: send waiting for the other end to connect | 5 s | `connect_timeout` on `pair()` |
| TCP token / HMAC handshake | 5 s | `timeout` on the authenticator |
| Sessions | 24 h, absolute | `ttl` on the store |

## What happens at a limit

wire_rpc never buffers without bound. At a limit it either **waits** (applies
back-pressure) or **refuses** clearly:

- **A message is too large:** TCP, WebSocket and stdio close the connection,
  because the stream can't be resynchronized; HTTP answers `413`. A response
  that's too large becomes a `-32603` error for that call.
- **A queue is full:** TCP and WebSocket stop reading from that peer until there's
  room, which pushes back on the sender through the network. HTTP answers `503`.
  A thread end refuses the send.
- **Too many connections:** extra connections are closed (TCP) or get `503`
  (WebSocket).
- **A peer is busy:** on `MulticastApp`, a second overlapping request from the
  same peer gets `-32002`.
- **A deadline passes:** the operation is cancelled and the caller gets a timeout
  error. See [Errors](errors.md).

## Tuning

**Raise message limits** only as far as your largest legitimate message needs.
Every limit is also the most memory one peer can make you allocate. If you raise
`max_frame_size` on a multicast TCP server, raise `max_queue_bytes` to at least
the same value; the constructor enforces this. Raise the client's limit too, or
it will refuse large replies.

**Raise `handler_timeout`** for methods that are slow by nature, and raise the
client's `call_timeout` to match. Over stdio, also raise `read_timeout` (30 s),
which bounds the wait for each reply.

**Raise `max_concurrency`** on `MulticastApp` if handlers mostly wait on I/O (a
database, other services) and you have spare capacity. It doesn't help
CPU-bound handlers; those need more processes.

**Lower `max_connections`** to what your server can actually serve. It's also a
denial-of-service guard: each connection costs memory, and each slot holds while
a client authenticates.

**`max_batch_size`**: lower it if a single batch could hold the server too long,
or raise it if clients legitimately send bigger batches. A batch shares one
`handler_timeout` regardless of size.

## What the limits don't cover

These are limits on what wire_rpc itself holds. They aren't a cap on your
process's total memory: decoded objects, your handlers' own data, socket
buffers and in-progress messages also use memory. Size containers from
measurements under realistic load, and set memory limits at the process or
container level.

Limits also aren't **rate limits**. wire_rpc bounds how much happens *at once*,
not how often a client may try. Put per-client and per-IP rate limiting, especially
on login, at your reverse proxy or API gateway. See [Deployment](deployment.md).
