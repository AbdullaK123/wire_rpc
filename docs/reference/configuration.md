# Configuration reference

Every constructor parameter, with its default. Times are in seconds and sizes in
bytes. Parameters after `*` in a signature are keyword-only. Explanations of
what the limits protect are in [Limits](../limits.md).

## App and MulticastApp

```python
App(transport, codec=MsgSpecJsonCodec(), *, max_batch_size=100, handler_timeout=30.0,
    startup_timeout=30.0, shutdown_timeout=10.0, max_response_size=4194304)

MulticastApp(transport, codec=MsgSpecJsonCodec(), *, handler_timeout=30.0, startup_timeout=30.0,
             shutdown_timeout=10.0, max_batch_size=100, max_response_size=4194304,
             max_concurrency=16)
```

| Parameter | Default | Meaning |
|---|---|---|
| `transport` | required | A single-peer transport for `App`, a multi-peer one for `MulticastApp` |
| `codec` | `MsgSpecJsonCodec()` | Must match the client's codec |
| `handler_timeout` | 30.0 | Time allowed for each call, or for a whole batch |
| `startup_timeout` | 30.0 | Transport startup plus your startup hook |
| `shutdown_timeout` | 10.0 | Each shutdown phase, and draining in-flight requests on `stop()` |
| `max_batch_size` | 100 | Most calls in one batch |
| `max_response_size` | 4194304 (4 MiB) | Largest encoded reply; capped by the transport's message limit; must allow at least 2048 |
| `max_concurrency` | 16 | `MulticastApp` only: requests running at once across all peers |

Methods: `method(name)`, `include_router(router)`, `middleware(func)`,
`on_startup(func)`, `on_shutdown(func)`, `run(*, log_level="info")`, `serve()`,
`stop()`, `stats`. `MulticastApp` adds `broadcast(method, params=None)` and
`transport`.

## Client

```python
Client(transport, codec=MsgSpecJsonCodec(), *, call_timeout=30.0, close_timeout=5.0,
       on_notification=None)
```

| Parameter | Default | Meaning |
|---|---|---|
| `call_timeout` | 30.0 | `connect()`, and each call/notify/batch including time queued behind another |
| `close_timeout` | 5.0 | Closing the transport |
| `on_notification` | `None` | Called with each `Notification` received while waiting for a reply; sync or async |

Methods: `connect()`, `close()`, `call(method, response_type, params=None)`,
`notify(method, params=None)`, `batch(items)`, `receive_notification()`.

## Router

```python
Router(prefix, *, multicast=False)
```

`multicast=True` accepts handlers that take `client_id`, for use with
`MulticastApp`.

## TCP

```python
TcpServerTransport(host="0.0.0.0", port=9000, max_frame_size=1048576, read_timeout=30.0,
                   auth_timeout=10.0, write_timeout=30.0, idle_timeout=300.0,
                   ssl_handshake_timeout=10.0, ssl_shutdown_timeout=10.0, ssl_context=None,
                   keep_alive=TcpKeepaliveConfig(), auth=None, shutdown_timeout=30.0)

TcpMulticastServerTransport(host="0.0.0.0", port=9000, max_frame_size=1048576, read_timeout=30.0,
                            auth_timeout=10.0, write_timeout=30.0, idle_timeout=300.0,
                            max_connections=64, recv_queue_size=256, ssl_context=None,
                            ssl_handshake_timeout=10.0, ssl_shutdown_timeout=10.0,
                            keep_alive=TcpKeepaliveConfig(), auth=None, shutdown_timeout=30.0,
                            *, max_queue_bytes=16777216, per_peer_queue_size=16)

TcpClientTransport(host="localhost", port=9000, max_frame_size=1048576, read_timeout=30.0,
                   write_timeout=30.0, idle_timeout=300.0, ssl_handshake_timeout=10.0,
                   ssl_shutdown_timeout=10.0, ssl_context=None, keep_alive=TcpKeepaliveConfig(),
                   shutdown_timeout=30.0, *, connect_timeout=10.0, auth=None)
```

| Parameter | Default | Meaning |
|---|---|---|
| `host`, `port` | `"0.0.0.0"` / `"localhost"`, 9000 | Where to listen, or connect |
| `max_frame_size` | 1048576 (1 MiB) | Largest message; at most 4 GiB − 1 |
| `read_timeout` | 30.0 | Finishing a message once its first byte has arrived |
| `write_timeout` | 30.0 | Sending a message |
| `idle_timeout` | 300.0 | Close a connection with no traffic for this long; `None` disables |
| `auth_timeout` | 10.0 | Servers: the authentication handshake |
| `connect_timeout` | 10.0 | Client: connecting, TLS and authentication |
| `ssl_context` | `None` | Enables TLS |
| `ssl_handshake_timeout`, `ssl_shutdown_timeout` | 10.0 | TLS only |
| `keep_alive` | `TcpKeepaliveConfig(idle=60, interval=15, count=4)` | OS-level keepalive; `None` disables |
| `auth` | `None` | Servers: an `Authenticator`; client: a `ClientAuthenticator` |
| `shutdown_timeout` | 30.0 | Waiting for in-flight operations on close |
| `max_connections` | 64 | Multicast: connected or authenticating clients |
| `recv_queue_size` | 256 | Multicast: queued messages across all clients |
| `max_queue_bytes` | 16777216 (16 MiB) | Multicast: queued bytes; must be at least `max_frame_size` |
| `per_peer_queue_size` | 16 | Multicast: queued messages per client |

`TcpKeepaliveConfig(idle=60, interval=15, count=4)`: seconds of silence before
the first probe, seconds between probes, and unanswered probes before dropping.

## WebSocket

```python
WsServerTransport(host="0.0.0.0", port=8000, max_msg_size=1048576, recv_queue_size=256,
                  write_timeout=10.0, static_dir=None, auth=None, *, max_queue_bytes=16777216,
                  auth_timeout=10.0, close_timeout=5.0, allowed_origins=None, ssl_context=None)

MulticastWsServerTransport(host="0.0.0.0", port=8000, max_msg_size=1048576, recv_queue_size=256,
                           write_timeout=10.0, max_connections=64, static_dir=None, auth=None, *,
                           max_queue_bytes=16777216, per_peer_queue_size=16, auth_timeout=10.0,
                           close_timeout=5.0, allowed_origins=None, ssl_context=None)

WsClientTransport(url="ws://localhost:8000/ws", receive_timeout=None, close_timeout=5.0, *,
                  connect_timeout=10.0, write_timeout=10.0, max_msg_size=1048576, headers=None,
                  cookie_jar=None, ssl_context=None, allow_insecure_credentials=False)
```

| Parameter | Default | Meaning |
|---|---|---|
| `max_msg_size` | 1048576 (1 MiB) | Largest message in either direction |
| `recv_queue_size` | 256 | Servers: queued incoming messages |
| `max_queue_bytes` | 16777216 (16 MiB) | Servers: queued bytes; must be at least `max_msg_size` |
| `per_peer_queue_size` | 16 | Multicast: queued messages per client (the single-client server uses `recv_queue_size`) |
| `max_connections` | 64 | Multicast: clients at once |
| `write_timeout` | 10.0 | Sending a message |
| `auth_timeout` | 10.0 | Servers: authentication and the WebSocket handshake |
| `close_timeout` | 5.0 | Closing a connection |
| `static_dir` | `None` | Servers: serve `index.html` at `/` and files under `/static` |
| `auth` | `None` | Servers: an `Authenticator` |
| `allowed_origins` | `None` (the server's own origin only) | Servers: exact browser origins allowed; an empty set allows none |
| `ssl_context` | `None` | TLS (`wss://`) |
| `receive_timeout` | `None` (no limit) | Client: waiting for each incoming message; calls are bounded by `Client`'s `call_timeout` |
| `connect_timeout` | 10.0 | Client: connection and handshake |
| `headers`, `cookie_jar` | `None` | Client credentials |
| `allow_insecure_credentials` | `False` | Client: allow credentials over `ws://`, for local tests |

## HTTP

```python
HttpServerTransport(host, port, auth=None, *, max_pending_requests=64, request_timeout=30.0,
                    max_body_size=1048576, max_queue_bytes=16777216, shutdown_timeout=10.0,
                    ssl_context=None, allowed_origins=None)

HttpClientTransport(url, *, request_timeout=30.0, max_body_size=1048576, headers=None,
                    cookie_jar=None, ssl_context=None, allow_insecure_credentials=False)
```

| Parameter | Default | Meaning |
|---|---|---|
| `host`, `port` | required | Where to listen |
| `auth` | `None` | An `Authenticator` |
| `max_pending_requests` | 64 | Requests admitted at once, including authenticating and uploading |
| `request_timeout` | 30.0 | Server: a request waiting for its response. Client: the whole request |
| `max_body_size` | 1048576 (1 MiB) | Request and response bodies |
| `max_queue_bytes` | 16777216 (16 MiB) | Server: request bodies waiting to be handled |
| `shutdown_timeout` | 10.0 | Server: finishing open requests on close |
| `allowed_origins` | `None` (the server's own origin only) | Server: exact browser origins allowed; an empty set allows none |
| `url` | required | Client: the server's `.../rpc` URL |
| `headers`, `cookie_jar`, `ssl_context` | `None` | Client credentials and TLS |
| `allow_insecure_credentials` | `False` | Client: allow credentials over `http://`, for local tests |

## stdio

```python
StdIoTransport(*cmd, max_frame_size=16777216, read_timeout=30.0, write_timeout=10.0,
               shutdown_timeout=5.0, kill_timeout=2.0, trusted_principal=None)

StdIoServerTransport(*, max_frame_size=16777216, read_timeout=30.0, write_timeout=10.0,
                     trusted_principal=None)
```

| Parameter | Default | Meaning |
|---|---|---|
| `*cmd` | required | The program and arguments to launch |
| `max_frame_size` | 16777216 (16 MiB) | Largest message; at most 4 GiB − 1 |
| `read_timeout` | 30.0 | Waiting for each message |
| `write_timeout` | 10.0 | Sending a message |
| `shutdown_timeout` | 5.0 | Parent: the child's graceful exit after stdin closes |
| `kill_timeout` | 2.0 | Parent: each wait after terminate and after kill |
| `trusted_principal` | `None` | The principal handlers see |

## Threads

```python
ThreadTransport.pair(max_message_size=16777216, max_queue_size=256, connect_timeout=5.0)
```

| Parameter | Default | Meaning |
|---|---|---|
| `max_message_size` | 16777216 (16 MiB) | Largest message; empty messages are rejected too |
| `max_queue_size` | 256 | Unread messages per end; sending to a full end fails |
| `connect_timeout` | 5.0 | How long a send waits for the other end to connect |

## Authenticators

```python
BearerAuth(validator, *, require_tls=True)
ApiKeyAuth(validator, *, header="X-API-Key", require_tls=True)
CookieSessionAuth(credential_validator, session_store=None, cookie_name="session", secure=True,
                  max_age=86400, *, require_tls=None)
ClientCertificateAuth(fingerprints)

TcpTokenAuth(validator, *, require_tls=True, timeout=5)
TcpTokenClientAuth(token, *, require_tls=True, timeout=5)
HmacChallengeAuth(keys, *, require_tls=True, timeout=5)
HmacChallengeClientAuth(key_id, secret, *, require_tls=True, timeout=5)

credential_headers(*, bearer=None, api_key=None, api_key_header="X-API-Key")
```

| Parameter | Meaning |
|---|---|
| `require_tls` | Refuse requests or connections without TLS. For `CookieSessionAuth`, defaults to the value of `secure` |
| `header` | `ApiKeyAuth`: the header carrying the key |
| `session_store` | `CookieSessionAuth`: where sessions live; an `InMemorySessionStore` if omitted |
| `secure`, `max_age`, `cookie_name` | `CookieSessionAuth`: cookie attributes |
| `fingerprints` | `ClientCertificateAuth`: lowercase SHA-256 hex of a certificate's DER bytes → principal |
| `keys` | `HmacChallengeAuth`: key id → `(principal, secret)`; secrets at least 32 bytes |
| `timeout` | TCP authenticators: seconds for the handshake |

## Validators

```python
JwtTokenValidator(keys, *, algorithm, issuer, audience, leeway=0)
ApiKeyValidator(lookup)
SessionTokenValidator(store)
PasswordCredentialValidator(lookup, *, max_concurrency=4)
StaticPasswordCredentialValidator(accounts, *, max_concurrency=4)
CallbackCredentialValidator(callback)

ApiKeyRecord(principal, secret_digest, expires_at=None, enabled=True)
ApiKeyRecord.from_secret(principal, secret, *, expires_at=None)
PasswordAccount(principal, password_hash, enabled=True)
```

`leeway` is clock skew allowed for JWTs, 0 to 60 seconds. `max_concurrency` is
password hashes running at once per validator. See
[Credential validators](../authentication/credentials.md).

## Session stores

```python
InMemorySessionStore(ttl=86400, *, max_sessions=10000)
SQLiteSessionStore(path, ttl=86400, *, max_sessions=10000, timeout=5, max_concurrency=4)
RedisSessionStore(client, ttl=86400, *, namespace="wire-rpc", max_sessions=10000)
PostgresSessionStore(pool, ttl=86400, *, namespace="wire-rpc", max_sessions=10000)
```

| Parameter | Default | Meaning |
|---|---|---|
| `ttl` | 86400 (24 h) | Absolute session lifetime from creation |
| `max_sessions` | 10000 | Live sessions per store or namespace |
| `namespace` | `"wire-rpc"` | Redis/PostgreSQL: separates applications sharing a backend |
| `timeout` | 5 | SQLite: waiting for another process's lock |
| `max_concurrency` | 4 | SQLite: operations running at once per store |

## Middleware

```python
metrics_middleware(record)    # record: Callable[[CallRecord], None]
```

See [Observability](../observability.md#metrics).
