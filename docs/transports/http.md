# HTTP

HTTP transports send each JSON-RPC message as the body of a `POST /rpc` request,
and the reply as the response body. Every call is an ordinary HTTP request, so
it works through proxies, load balancers and API gateways, and you can test it
with `curl`.

```python
from wire_rpc import App, Client
from wire_rpc.transports.http import HttpClientTransport, HttpServerTransport
```

## Server

```python
app = App(HttpServerTransport("0.0.0.0", 8080))
```

Use HTTP with `App`. There's no multicast HTTP server, because an HTTP server
can't push to clients.

### Requests and responses

| Request | Response |
|---|---|
| A call | `200` with the JSON-RPC response, `Content-Type: application/json` |
| A notification, or a batch of only notifications | `204 No Content` |
| Bad or missing credentials | `401` |
| A browser `Origin` that isn't allowed (by default, any origin but the server's own) | `403` |
| A body larger than `max_body_size` (1 MiB) | `413` |
| More than `max_pending_requests` (64) waiting, more than `max_queue_bytes` (16 MiB) of bodies queued, or authentication unavailable | `503` |
| No answer within `request_timeout` (30 s) | `504` |

JSON-RPC errors, such as an unknown method, still come back as `200` with an
`error` member. HTTP status codes only report problems with the HTTP exchange.

### Concurrency

The server accepts up to `max_pending_requests` requests at once, but `App` runs
them **one at a time** in arrival order. A slow method delays the requests behind
it, and if they wait longer than `request_timeout` they get `504`. To serve more
traffic, run several server processes behind a load balancer (see
[Deployment](../deployment.md#scaling)), keep handlers fast, and have clients
batch their calls.

## Client

```python
client = Client(HttpClientTransport("https://rpc.example.com/rpc", ssl_context=client_tls))
```

| Setting | Default | Meaning |
|---|---|---|
| `request_timeout` | 30 s | The whole request and response |
| `max_body_size` | 1 MiB | Request and response bodies |
| `headers` | none | Sent with every request, for example `Authorization` |
| `cookie_jar` | none | An aiohttp cookie jar, for example after a login |
| `ssl_context` | system defaults | Custom TLS trust or client certificate |
| `allow_insecure_credentials` | `False` | Allow `headers` or `cookie_jar` over plain `http://`; for local testing only |

The client:

- keeps one connection to the server and reuses it,
- never follows redirects, so credentials can't be sent to another host,
- rejects URLs with a username or password in them; use `headers` instead,
- refuses to send credentials over plain HTTP, or with a TLS context that
  doesn't verify the server, unless `allow_insecure_credentials=True`.

`receive_notification()` isn't available over HTTP.

## Calling it from anything

```sh
curl -s https://rpc.example.com/rpc \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"jsonrpc": "2.0", "method": "notes.get", "params": {"id": 7}, "id": 1}'
```

```javascript
const response = await fetch("/rpc", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  credentials: "same-origin",
  body: JSON.stringify({jsonrpc: "2.0", method: "notes.get", params: {id: 7}, id: 1}),
});
const reply = await response.json();
```

By default (`allowed_origins=None`), a browser request is accepted only from the
server's **own origin**: the same scheme, host and port the request arrived on.
That covers pages the server serves itself, or that are served from the same
address. Requests from other origins get `403`. Requests without an `Origin`
header (non-browser clients) are always allowed. If your pages live on a different origin, pass
`allowed_origins={"https://app.example.com"}`. The server doesn't answer CORS
preflight requests. Serve browser pages from the
same origin as the API, or configure CORS on a reverse proxy in front of it. See
[Security model](../security.md#browsers).

## Authentication

```python
HttpServerTransport("0.0.0.0", 8443, auth=BearerAuth(validator), ssl_context=server_tls)
```

Credentials are checked on every request, before the body is read. With an
authenticator that has a login flow (`CookieSessionAuth`), the server also serves
`POST /login` and `POST /logout`. See
[HTTP and WebSocket authentication](../authentication/http-and-websocket.md).

## Counters

`HttpServerTransport.stats` returns `pending` requests and `queued_bytes`.
