# HTTP and WebSocket Transport Hardening Roadmap

Wire RPC's HTTP and WebSocket transports are **browser-first transports**.

That means their job is not to behave like raw TCP. Their job is to provide a resilient Wire RPC transport while preserving complete, native browser compatibility.

The browser-facing happy path should remain:

```js
ws.send(JSON.stringify(request))

ws.onmessage = (event) => {
    const response = JSON.parse(event.data)
}
```

and:

```js
const response = await fetch("/rpc", {
    method: "POST",
    headers: {
        "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
})

const result = await response.json()
```

No binary decoder. No SDK. No custom browser runtime. No special transport library.

The core design rule is:

> **Let aiohttp own low-level HTTP/WebSocket networking. Wire RPC should only add the application-level hardening and policy that aiohttp cannot know about.**

---

# 1. WebSocket Transport Hardening

## 1.1 Explicit Message Size Limits

Add a configurable maximum WebSocket message size.

Server:

```python
web.WebSocketResponse(
    max_msg_size=max_message_size,
)
```

Client:

```python
session.ws_connect(
    url,
    max_msg_size=max_message_size,
)
```

Do not rely on aiohttp defaults accidentally.

Wire RPC should decide its own application-level limits.

---

## 1.2 Bound Every Receive Queue

Current receive queues must never be unbounded.

Bad:

```python
self._recv_queue = asyncio.Queue()
```

Better:

```python
self._recv_queue = asyncio.Queue(
    maxsize=recv_queue_size,
)
```

And use:

```python
await self._recv_queue.put(data)
```

instead of:

```python
self._recv_queue.put_nowait(data)
```

This gives real backpressure:

```text
Wire RPC receive queue fills
        ↓
WebSocket handler stops consuming
        ↓
aiohttp stops reading aggressively
        ↓
TCP buffers fill
        ↓
TCP flow control slows the sender
```

No uncontrolled memory growth.

---

## 1.3 WebSocket Heartbeat

Expose a configurable WebSocket heartbeat.

Example:

```python
web.WebSocketResponse(
    heartbeat=30.0,
    autoping=True,
    autoclose=True,
)
```

This is the WebSocket-level equivalent of dead-peer detection.

aiohttp handles:

- PING
- PONG
- automatic response handling
- dead-peer detection
- WebSocket close behavior

Wire RPC should not reimplement TCP keepalive logic here.

---

## 1.4 WebSocket Receive and Close Deadlines

Expose explicit receive and close timeout configuration.

For the client, use aiohttp's native WebSocket timeout facilities.

Conceptually:

```python
aiohttp.ClientWSTimeout(
    ws_receive=receive_timeout,
    ws_close=close_timeout,
)
```

Avoid arbitrary indefinite waits.

---

## 1.5 WebSocket Send Deadlines

Wire RPC should still impose an application-level write deadline.

Example:

```python
async with asyncio.timeout(write_timeout):
    await ws.send_str(payload)
```

A slow or wedged browser must not be able to hold an outbound RPC operation forever.

---

## 1.6 Preserve Browser-Native Text Frames

The browser-first WebSocket transport should deliberately send UTF-8 text frames.

Keep the transport adaptation:

```python
await ws.send_str(data.decode("utf-8"))
```

and inbound:

```python
data = msg.data.encode("utf-8")
```

This is intentional.

The transport's Python API may use `bytes`, while the physical browser wire format is UTF-8 WebSocket text.

Normalize invalid UTF-8 into a Wire RPC transport error rather than leaking a raw `UnicodeDecodeError`.

Example:

```python
def _decode_text_payload(data: bytes) -> str:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TransportError(
            "WebSocket transport requires UTF-8 encoded payloads"
        ) from exc
```

---

## 1.7 Explicit Frame-Type Policy

Wire RPC browser WebSockets should treat `TEXT` frames as application data.

aiohttp should own:

- PING
- PONG
- CLOSE
- CLOSING
- CLOSED

Binary application frames should be handled deliberately.

Recommended policy:

```text
TEXT
    → Wire RPC payload

PING / PONG / CLOSE
    → aiohttp protocol machinery

BINARY
    → reject or ignore explicitly
```

Do not silently support two incompatible application wire formats.

---

## 1.8 Unicast Connection Admission

`WsServerTransport` is a unicast transport.

It must explicitly enforce:

```text
maximum active clients = 1
```

A second browser connection must not silently overwrite:

```python
self._ws
```

Reject the new connection or close it immediately.

---

## 1.9 Multicast Connection Admission

`MulticastWsServerTransport` needs:

```python
max_connections: int
```

The implementation can be much simpler than raw TCP because aiohttp owns the actual socket mechanics.

Wire RPC only needs admission policy.

---

## 1.10 Separate Connection Identity From User Identity

Never use authenticated `user_id` as the WebSocket connection key.

Bad:

```python
client_id = user_id
self._clients[client_id] = ws
```

A user may open:

- multiple tabs
- multiple windows
- multiple devices
- multiple browser sessions

Instead:

```text
connection_id = unique WebSocket connection
principal     = authenticated identity
```

Example:

```python
connection_id = str(uuid.uuid4())
```

The connection registry should be keyed by `connection_id`.

Authentication identity should be stored separately.

---

## 1.11 Identity-Safe Cleanup

Never blindly do:

```python
del self._clients[client_id]
```

from a connection's cleanup path.

An old connection may disconnect after another connection has been registered under the same logical identity.

Cleanup should remove only the exact connection instance that owns the cleanup.

Conceptually:

```python
current = self._clients.get(connection_id)

if current is ws:
    self._clients.pop(connection_id, None)
```

---

## 1.12 Graceful Shutdown

Use the same simple shutdown philosophy as TCP, but delegate low-level work to aiohttp.

Wire RPC should own:

```text
_closing = True
↓
reject newly admitted Wire RPC work
↓
stop accepting new WebSocket upgrades
↓
drain already-admitted work
↓
close active WebSockets
↓
aiohttp completes handler cleanup
```

Do not rebuild TCP-level listener, framing, or socket shutdown mechanics.

Expose:

```python
shutdown_timeout: float = 30.0
```

After the grace period:

```text
cancel remaining Wire RPC tasks
close remaining WebSockets
```

---

## 1.13 Browser Origin Protection

Browsers send an `Origin` header during the WebSocket handshake.

Support an explicit allowed-origin policy.

Example concept:

```python
allowed_origins: set[str] | None
```

This matters especially for cookie-authenticated deployments.

Authentication answers:

> Who are you?

Origin validation answers:

> Which website initiated this browser connection?

This protects against cross-site WebSocket hijacking.

---

## 1.14 Concurrent Broadcast

Current multicast broadcast should not send to every browser sequentially forever.

Eventually use bounded concurrent fanout.

Conceptually:

```python
async with asyncio.TaskGroup() as tg:
    for client in clients:
        tg.create_task(send_to_client(client))
```

Each send must retain its own write timeout.

One slow browser should not delay every other connected client.

---

## 1.15 WSS / TLS

Expose `ssl.SSLContext`.

Do not implement TLS manually.

Use aiohttp's existing TLS integration.

Production browser deployments served over HTTPS generally need:

```text
https://
wss://
```

to avoid mixed-content problems.

---

# 2. HTTP Transport Hardening

## 2.1 Explicit Request Body Limit

Configure aiohttp's request body limit explicitly.

Example:

```python
web.Application(
    client_max_size=max_request_size,
)
```

Do not accidentally inherit aiohttp's default policy.

Wire RPC should choose its own RPC payload limit.

---

## 2.2 Replace the Global Response Queue

This is the most important HTTP architectural change.

Current shape:

```python
self._queue = asyncio.Queue()
self._response_queue = asyncio.Queue()
```

Every HTTP request pushes into one queue and waits on one shared response queue.

That only works safely while dispatch remains globally sequential.

Instead, every HTTP request must own its response channel.

Example model:

```python
@dataclass(slots=True)
class PendingHttpRequest:
    data: bytes
    response: asyncio.Future[bytes]
```

Handler:

```python
future = asyncio.get_running_loop().create_future()

await self._queue.put(
    PendingHttpRequest(
        data=data,
        response=future,
    )
)

response_data = await future
```

Then Wire RPC completes the exact request's future.

This gives:

```text
HTTP request A
    ↓
(data A, Future A)

HTTP request B
    ↓
(data B, Future B)

App completes A
    ↓
Future A.set_result(...)

App completes B
    ↓
Future B.set_result(...)
```

No response-order dependency.

This prepares HTTP for concurrent RPC dispatch.

---

## 2.3 Bound Pending HTTP Requests

aiohttp may accept many HTTP requests concurrently.

Wire RPC needs its own admission limit.

Example:

```python
max_pending_requests: int
```

Possible implementation:

```python
self._request_slots = asyncio.Semaphore(
    max_pending_requests
)
```

or a bounded pending queue.

The goal is simple:

```text
do not allow unlimited browser requests
to sit waiting for application processing
```

---

## 2.4 HTTP RPC Deadline

An HTTP request must not wait forever for its Wire RPC response.

Wrap the per-request future:

```python
async with asyncio.timeout(request_timeout):
    response_data = await future
```

On timeout:

- cancel or remove the pending future
- return an appropriate HTTP timeout response
- do not retain dead request state

---

## 2.5 Handle Browser Disconnects

A browser may close the tab or abort `fetch()` while the RPC is still pending.

Wire RPC should not retain the associated request forever.

When the HTTP request disconnects:

```text
cancel pending response future
remove pending request state
allow application work to notice cancellation when appropriate
```

---

## 2.6 HTTP Client Timeouts

Configure `aiohttp.ClientTimeout` explicitly.

Example:

```python
aiohttp.ClientSession(
    timeout=aiohttp.ClientTimeout(
        total=30.0,
        connect=10.0,
        sock_read=30.0,
    )
)
```

Do not rely on broad defaults.

---

## 2.7 HTTP Client Connection Pool Limits

Use aiohttp's native connection pool.

Expose a reasonable connector limit.

Example:

```python
aiohttp.TCPConnector(
    limit=max_connections,
)
```

Do not build a custom pool.

---

## 2.8 HTTP Graceful Shutdown

Wire RPC should use:

```text
_closing = True
↓
reject newly admitted RPC requests
↓
aiohttp stops accepting new HTTP work
↓
drain pending Wire RPC requests
↓
grace timeout
↓
cancel leftovers
↓
runner cleanup
```

Again, let aiohttp own actual HTTP/TCP shutdown mechanics.

---

## 2.9 Content-Type Semantics

HTTP is browser-first and JSON-first.

RPC responses should explicitly return:

```text
Content-Type: application/json; charset=utf-8
```

Browser RPC requests should have a clear accepted content-type policy.

Recommended:

```text
application/json
```

Reject unsupported content types deliberately.

---

## 2.10 CORS Policy

For same-origin browser apps:

```text
no special CORS configuration needed
```

For cross-origin clients, expose explicit configuration for:

- allowed origins
- allowed methods
- allowed headers
- credential support

Do not default to:

```text
Access-Control-Allow-Origin: *
```

when credentials may be involved.

CORS is browser policy and belongs at the HTTP transport boundary.

---

## 2.11 OPTIONS / Preflight Handling

Cross-origin browser POST requests may trigger CORS preflight.

The HTTP transport must correctly handle:

```text
OPTIONS /rpc
```

when CORS is enabled.

This must be compatible with native browser `fetch()` behavior.

---

## 2.12 HTTPS / TLS

Expose `ssl.SSLContext`.

Use aiohttp's native HTTPS support.

Do not implement TLS manually.

---

## 2.13 Fix Lifecycle Ownership

Keep the ownership rule:

```text
close()
    → network resources

shutdown()
    → owned dependency lifecycle
```

The HTTP transport must not shut down authentication dependencies from both `close()` and `shutdown()`.

Dependency shutdown should happen exactly once.

---

# 3. Browser Authentication Hardening

HTTP and WebSocket browser transports need browser-specific authentication policy.

## 3.1 Cookie Security

Cookie-backed authentication should support correct policy for:

- `Secure`
- `HttpOnly`
- `SameSite`
- cookie domain
- cookie path
- expiration

These belong to the authentication/browser boundary.

---

## 3.2 CORS + Credentials

Cross-origin cookie-authenticated HTTP requires coordinated:

```text
Access-Control-Allow-Credentials
specific allowed origin
browser fetch credentials mode
```

Never combine credentialed requests with a wildcard origin policy.

---

## 3.3 WebSocket Origin Validation

Cookie-authenticated WebSockets must validate browser origin independently of authentication.

A valid cookie alone must not imply that any website may initiate a WebSocket connection.

---

# 4. Codec Compatibility

Browser-first transports should use browser-native textual codecs.

Recommended:

```text
HTTP
    → JSON-compatible textual codecs

WebSocket
    → JSON-compatible textual codecs

TCP
    → JSON, MessagePack, arbitrary binary codecs

stdio
    → JSON, MessagePack, arbitrary binary codecs
```

Eventually codecs may expose capabilities such as:

```python
class Codec(Protocol):
    is_text: bool
    media_type: str
```

Then invalid combinations can fail during application construction.

Example:

```text
WsServerTransport + MsgPackCodec
    → configuration error

HttpServerTransport + MsgPackCodec
    → configuration error
```

Fail early instead of exploding at runtime during UTF-8 conversion.

---

# 5. App-Level Graceful Shutdown

Transport-level graceful shutdown is not enough once Wire RPC supports concurrent handler execution.

Eventually `App` must track active RPC handler tasks separately.

Conceptually:

```text
Transport
    stops accepting new transport work

App
    stops dispatching new RPC calls
    waits for already-running handlers

Transport
    flushes remaining responses
    closes connections
```

The transport should not know about application handlers.

Lifecycle ownership stays layered.

---

# 6. What aiohttp Already Owns

Do **not** reproduce the raw TCP transport machinery inside HTTP or WebSocket transports.

aiohttp already owns most of:

## HTTP

- HTTP parsing
- request framing
- `Content-Length`
- chunked transfer encoding
- TCP connection reuse
- HTTP keep-alive
- socket read/write flow control
- connection pooling
- TLS integration
- listener lifecycle
- request handler lifecycle

## WebSocket

- HTTP Upgrade
- WebSocket framing
- fragmentation handling
- masking
- PING/PONG
- close handshake
- automatic pong handling
- message-size enforcement
- compression support
- underlying TCP flow control
- TLS integration

Wire RPC should only add policy and guarantees that aiohttp cannot know about.

---

# 7. Recommended Implementation Order

## WebSocket

```text
1. explicit message-size limits
2. bounded receive queues
3. heartbeat
4. receive / close / send deadlines
5. unicast connection admission
6. multicast connection admission
7. separate connection identity from principal
8. identity-safe cleanup
9. graceful shutdown
10. origin policy
11. WSS support
12. concurrent multicast broadcast
13. codec capability validation
```

## HTTP

```text
1. explicit body-size limit
2. per-request Future response ownership
3. bounded pending requests
4. request deadline
5. browser disconnect cleanup
6. explicit client timeouts
7. client pool limits
8. graceful shutdown
9. JSON content-type policy
10. CORS
11. OPTIONS / preflight support
12. lifecycle cleanup
13. HTTPS support
14. codec capability validation
```

---

# 8. Final Architectural Split

The transport families should deliberately have different physical semantics.

```text
Binary/general transports
─────────────────────────
TCP
stdio
future Unix sockets
future shared memory

Browser-first transports
────────────────────────
WebSocket
HTTP
```

For TCP:

```text
physics = framed opaque bytes
```

For browser WebSocket:

```text
physics = UTF-8 WebSocket text messages
```

For HTTP:

```text
physics = browser-native HTTP JSON request/response bodies
```

The common Python interface can still expose `bytes`.

The transport is responsible for adapting those bytes to the physical environment it owns.

> **The application stays stable. The transport determines the physics.**
