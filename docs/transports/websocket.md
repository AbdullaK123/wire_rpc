# WebSocket

WebSocket transports carry each message as one text frame on the `/ws`
endpoint. They work from browsers, pass through most HTTP infrastructure, and let
the server push notifications to clients.

```python
from wire_rpc import App, Client, MulticastApp
from wire_rpc.transports.websocket import (
    MulticastWsServerTransport,
    WsClientTransport,
    WsServerTransport,
)
```

## Servers

### `MulticastWsServerTransport`: many clients

```python
app = MulticastApp(MulticastWsServerTransport(host="0.0.0.0", port=8000))
```

Each connection gets a random `client_id`. Up to `max_connections` (64) clients
at once; more get `503` during the upgrade. Incoming messages queue with the same
bounds as TCP: `recv_queue_size` (256), `max_queue_bytes` (16 MiB) and
`per_peer_queue_size` (16).

### `WsServerTransport`: one client

```python
app = App(WsServerTransport(host="127.0.0.1", port=8000))
```

Accepts one WebSocket session. When it closes, the transport closes and the app
shuts down; later connection attempts get `503`.

### Routes

| Route | Serves |
|---|---|
| `GET /ws` | The WebSocket endpoint |
| `POST /login`, `POST /logout` | Only with an authenticator that has a login flow, such as `CookieSessionAuth` |
| `GET /`, `/static/...` | Only with `static_dir=`: `index.html` at `/` and the directory's files under `/static` |

`static_dir` is a convenience for serving a small single-page app from the same
origin as its WebSocket. For anything bigger, serve static files from a proper
web server or CDN.

## Client

```python
client = Client(WsClientTransport("ws://127.0.0.1:8000/ws"))
```

| Setting | Default | Applies to |
|---|---|---|
| `connect_timeout` | 10 s | Opening the connection and the WebSocket handshake |
| `receive_timeout` | none | Waiting for each incoming message; `Client`'s `call_timeout` bounds calls |
| `write_timeout` | 10 s | Sending a message |
| `close_timeout` | 5 s | Closing |
| `max_msg_size` | 1 MiB | Messages in either direction |

The transport sets no deadline on incoming messages by default. `Client`'s
`call_timeout` limits every call, and a client that only listens with
`receive_notification()` can wait as long as it needs. Set `receive_timeout` only
if you use the transport directly, without `Client`, and want a per-message limit.

The client refuses redirects, so credentials in headers or cookies can't be
forwarded to another host.

## Messages

The server sends text frames. It accepts text or binary frames from clients, and
measures size in bytes after UTF-8 encoding. A message over `max_msg_size` closes
the connection. Compression is disabled, which closes off compression-based
attacks and keeps memory use predictable.

The server pings every 30 seconds and closes connections that stop answering.

## Browsers

From a browser, use the standard `WebSocket` API and send JSON-RPC text:

```javascript
const ws = new WebSocket("wss://app.example.com/ws");
ws.onmessage = (event) => {
  const message = JSON.parse(event.data);
  if (message.id === undefined) handleNotification(message);   // a broadcast
  else resolvePending(message);
};
ws.onopen = () => ws.send(JSON.stringify({jsonrpc: "2.0", method: "ping", id: 1}));
```

Browsers can't set headers on a WebSocket, so authenticate browsers with a cookie
(`CookieSessionAuth`).

By default (`allowed_origins=None`), a browser request is accepted only from the
server's **own origin**: the same scheme, host and port the request arrived on.
That covers pages the server serves itself, or that are served from the same
address. Requests from other origins get `403`. Requests without an `Origin`
header (non-browser clients) are always allowed.

If your pages are served from somewhere else, list their origins exactly:

```python
MulticastWsServerTransport(
    host="0.0.0.0", port=8443,
    ssl_context=server_tls,
    auth=cookie_auth,
    allowed_origins={"https://app.example.com"},
)
```

With an explicit set, only those origins are allowed (not even the server's own,
unless listed), and an empty set allows no browsers at all. See
[HTTP and WebSocket authentication](../authentication/http-and-websocket.md) and
[Security model](../security.md#browsers).

## TLS

Pass `ssl_context=` to the server for `wss://`, and to the client to trust a
private CA. See [Transports overview](README.md#tls).

## Counters

Server transports' `stats` return queued `messages`, `bytes` and `peers`, and
current `connections`.
