# TCP

TCP transports send each message as a 4-byte big-endian length followed by the
payload. They have the lowest overhead per call of the network transports, and
support TLS and three kinds of authentication.

```python
from wire_rpc import App, Client, MulticastApp
from wire_rpc.transports.tcp import (
    TcpClientTransport,
    TcpMulticastServerTransport,
    TcpServerTransport,
)
```

## Servers

### `TcpServerTransport`: one client

```python
app = App(TcpServerTransport(host="127.0.0.1", port=9000))
```

Serves exactly one connection. While that client is connected, further
connections are closed immediately. When it disconnects, the app shuts down.
Use it for a dedicated link between two processes.

### `TcpMulticastServerTransport`: many clients

```python
app = MulticastApp(TcpMulticastServerTransport(host="0.0.0.0", port=9000))
```

Each connection gets a random `client_id`. A reader task per connection queues
incoming messages; the queue is bounded overall (`recv_queue_size`, 256
messages), in bytes (`max_queue_bytes`, 16 MiB), and per client
(`per_peer_queue_size`, 16). A client that sends faster than it's served waits,
and can't starve the others.

At most `max_connections` (64) clients can be connected or authenticating at
once; extra connections are closed on accept.

`port=0` lets the operating system pick a free port, but the transport has no
public way to tell you which one it chose, so use a fixed port.

## Client

```python
client = Client(TcpClientTransport(host="127.0.0.1", port=9000))
```

`connect_timeout` (10 s) covers the TCP connection, the TLS handshake and
authentication together.

## Timeouts

| Setting | Default | Applies to |
|---|---|---|
| `read_timeout` | 30 s | Finishing a message once its first byte has arrived |
| `write_timeout` | 30 s | Sending a message |
| `idle_timeout` | 300 s | No traffic at all; the connection is closed. `None` disables it. |
| `auth_timeout` (servers) | 10 s | The authentication handshake |
| `connect_timeout` (client) | 10 s | Connecting, TLS and authentication |
| `shutdown_timeout` | 30 s | Waiting for in-flight operations when closing |

`read_timeout` only starts once a message begins, so a connection can sit quietly
between calls. `idle_timeout` is what closes a connection nobody uses.

**Listening for broadcasts:** a client that only waits for
`receive_notification()` sends nothing, so after `idle_timeout` (5 minutes) the
client side closes the connection. For a long-lived listener, pass
`idle_timeout=None` to the client transport, or make a cheap call now and then.

Any failure while reading a message closes that connection: once part of a
message has been read, there's no way to find the start of the next one.

## Keepalive

`keep_alive=TcpKeepaliveConfig(idle=60, interval=15, count=4)` (the default)
turns on the operating system's TCP keepalive: after 60 s of silence the OS sends
a probe every 15 s and drops the connection after 4 unanswered probes. This
detects peers that vanished without closing, like a crashed machine or a
dropped network path. Pass `keep_alive=None` to turn it off.

## TLS

```python
server = TcpMulticastServerTransport(host="0.0.0.0", port=9443, ssl_context=server_tls)
client = TcpClientTransport(host="rpc.internal", port=9443, ssl_context=client_tls)
```

`ssl_handshake_timeout` and `ssl_shutdown_timeout` (10 s each) apply only when
TLS is on. See [Transports overview](README.md#tls) for building contexts.

## Authentication

Pass `auth=` to the server and the matching client authenticator to the client:

```python
from wire_rpc.auth import TcpTokenAuth, TcpTokenClientAuth

server = TcpMulticastServerTransport(..., ssl_context=server_tls, auth=TcpTokenAuth(validator))
client = TcpClientTransport(..., ssl_context=client_tls, auth=TcpTokenClientAuth(token))
```

Authentication happens once, right after connecting, before any message is read.
Credentials are checked again before every request, so revoking a token cuts off
an open connection at its next call. See [TCP authentication](../authentication/tcp.md).

## Talking to it from another language

Send `length.to_bytes(4, "big") + payload` and read the same. Payloads are JSON
(or MessagePack, if the server uses that codec). If the server has
authentication, the client must complete the handshake described in
[TCP authentication](../authentication/tcp.md#wire-format) first.

## Counters

`TcpMulticastServerTransport.stats` returns queued `messages`, queued `bytes`,
`peers` with queued messages, and current `connections`.
