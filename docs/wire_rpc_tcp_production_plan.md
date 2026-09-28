# Wire RPC TCP Production Hardening Plan

## Goal

Build a TCP transport that provides the kind of hardened connection substrate that HTTP servers are built on top of, but without HTTP parsing or HTTP semantics.

Wire RPC should sit directly on top of:

```text
TLS
↓
TCP
↓
length-prefixed framing
↓
opaque Wire RPC bytes
```

The transport should know about:

- connections
- framing
- bytes
- limits
- timeouts
- TLS
- backpressure
- graceful shutdown

It should **not** know about:

- MessagePack
- JSON
- Pydantic
- RPC methods
- handlers
- compression
- Wire RPC models

Codec middleware starts only after the TCP transport has safely produced a bounded byte frame.

---

# Phase 1 — Harden TCP Framing

Add `max_frame_size` to:

- `TcpServerTransport`
- `TcpMulticastServerTransport`
- `TcpClientTransport`

Validate the frame length **before** reading the payload.

```python
length = int.from_bytes(
    await reader.readexactly(4),
    "big",
)

if length > self._max_frame_size:
    raise FrameTooLarge(length)
```

Decide whether zero-length frames are legal. Prefer rejecting them unless Wire RPC gains a real need for them.

Introduce transport-level errors:

```python
class TransportError(Exception): ...
class FrameTooLarge(TransportError): ...
class ReadTimeout(TransportError): ...
class WriteTimeout(TransportError): ...
class ConnectionLimitReached(TransportError): ...
```

### Done when

A peer cannot make Wire RPC attempt to read an arbitrarily large frame.

---

# Phase 2 — Add Read and Authentication Deadlines

Add:

```python
read_timeout: float = 30.0
auth_timeout: float = 10.0
```

Bound frame-header reads:

```python
async with asyncio.timeout(self._read_timeout):
    header = await reader.readexactly(4)
```

Bound frame-body reads:

```python
async with asyncio.timeout(self._read_timeout):
    payload = await reader.readexactly(length)
```

Bound authentication:

```python
async with asyncio.timeout(self._auth_timeout):
    principal = await self._auth.verify(...)
```

If a deadline expires, close the connection.

### Done when

A client cannot occupy a socket forever by drip-feeding bytes or stalling during authentication.

---

# Phase 3 — Bound Connection Count

For multicast TCP, add:

```python
max_connections: int = 1024
```

Reject new connections when the cap is reached.

```python
if len(self._clients) >= self._max_connections:
    writer.close()
    await writer.wait_closed()
    return
```

Per-IP limits can come later.

### Done when

Opening sockets cannot cause unbounded connection-state growth.

---

# Phase 4 — Replace Unbounded Receive Queues

Replace:

```python
asyncio.Queue()
```

with:

```python
asyncio.Queue(maxsize=recv_queue_size)
```

For example:

```python
recv_queue_size: int = 1024
```

Use:

```python
await self._recv_queue.put(...)
```

instead of:

```python
self._recv_queue.put_nowait(...)
```

This gives natural TCP backpressure:

```text
application slows
↓
receive queue fills
↓
transport stops reading
↓
kernel receive buffer fills
↓
TCP receive window shrinks
↓
sender slows
```

### Done when

Traffic volume cannot turn directly into unbounded RAM consumption.

---

# Phase 5 — Make Writes Concurrency-Safe

Before introducing concurrent RPC dispatch, make frame writes atomic at the application level.

For unicast:

```python
self._write_lock = asyncio.Lock()
```

Then:

```python
async with self._write_lock:
    writer.write(header)
    writer.write(data)
    await writer.drain()
```

For multicast, use a write lock per connection.

### Done when

Concurrent tasks cannot interleave TCP frames.

---

# Phase 6 — Add Write Deadlines

Add:

```python
write_timeout: float = 30.0
```

Wrap `drain()`:

```python
async with asyncio.timeout(self._write_timeout):
    await writer.drain()
```

If the peer stops reading and the timeout expires, close the connection.

### Done when

A non-reading peer cannot stall server writers indefinitely.

---

# Phase 7 — Introduce a TCP Connection Object

Replace raw `(reader, writer)` tuples with explicit connection state.

```python
@dataclass(slots=True)
class TcpConnection:
    id: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    write_lock: asyncio.Lock
    principal: str | None = None
```

Possible future fields:

```python
connected_at: float
last_activity: float
peername: object
```

Multicast becomes:

```python
self._clients: dict[str, TcpConnection]
```

### Done when

Connection lifecycle and per-peer state have one obvious home.

---

# Phase 8 — Add Idle Timeout

Add:

```python
idle_timeout: float | None = 300.0
```

Keep it distinct from `read_timeout`.

```text
read_timeout
= maximum time one frame may take to arrive

idle_timeout
= maximum time a connection may remain completely inactive
```

Allow `idle_timeout=None` for intentionally permanent connections.

### Done when

Forgotten idle connections do not live forever unless explicitly configured to.

---

# Phase 9 — Add TLS Support

Expose TLS directly through the TCP transport.

```python
TcpServerTransport(
    ssl=tls_context,
)
```

Pass it into:

```python
asyncio.start_server(
    ...,
    ssl=self._ssl,
)
```

Client side:

```python
asyncio.open_connection(
    ...,
    ssl=self._ssl,
)
```

Do not invent custom socket-layer encryption.

TLS should own:

- confidentiality
- integrity
- secure key exchange
- certificate authentication

Wire RPC authentication remains separate.

```text
TLS
↓
TCP connection
↓
Wire RPC authenticator
↓
framed RPC messages
```

### Done when

TCP can be deployed safely across untrusted networks without custom cryptography.

---

# Phase 10 — Add TCP Keepalive

Expose:

```python
tcp_keepalive: bool = True
```

Initially enable `SO_KEEPALIVE`.

Keep the distinction:

```text
TCP keepalive
→ detect dead network paths / peers

Wire RPC idle timeout
→ application resource policy
```

---

# Phase 11 — Add Graceful Shutdown

Shutdown should become deliberate:

```text
1. stop accepting new connections
2. stop accepting new work
3. allow existing operations to finish
4. flush pending writes
5. close client sockets
6. release owned dependencies
```

Preserve the lifecycle distinction:

```text
startup / shutdown
= owned dependency lifecycle

connect / close
= transport physics
```

For multicast, stop the listener before draining clients.

### Done when

Shutdown is deterministic rather than abrupt.

---

# Freeze the TCP Substrate

At this point, resist adding Wire RPC semantics to TCP.

The transport contract should remain essentially:

```python
class Transport(Protocol):
    async def recv(self) -> bytes: ...
    async def send(self, data: bytes) -> None: ...
```

Multicast:

```python
async def recv(self) -> tuple[str, bytes]: ...
async def send(self, client_id: str, data: bytes) -> None: ...
async def broadcast(self, data: bytes) -> None: ...
```

The hardened TCP layer should know only about:

```text
connections
frames
bytes
limits
timeouts
TLS
backpressure
```

---

# Phase 12 — Build the Codec Middleware Abstraction

Keep the middleware contract tiny:

```python
class CodecMiddleware(Protocol):
    def encode(self, data: bytes) -> bytes: ...
    def decode(self, data: bytes) -> bytes: ...
```

Wrap a base codec:

```python
class MiddlewareCodec:
    def __init__(
        self,
        codec: Codec,
        middleware: Sequence[CodecMiddleware],
    ):
        ...
```

Encode direction:

```text
object
→ codec
→ middleware 1
→ middleware 2
→ middleware 3
→ bytes
```

Decode direction:

```text
bytes
→ middleware 3
→ middleware 2
→ middleware 1
→ codec
→ object
```

`convert()` should bypass byte middleware:

```python
def convert(self, obj, target):
    return self._codec.convert(obj, target)
```

### Done when

Existing codecs work unchanged through an empty middleware stack.

---

# Phase 13 — Add a Wire RPC Envelope Middleware

First real codec middleware:

```text
magic       4 bytes   WRPC
version     1 byte
flags       1 byte
reserved    2 bytes
payload     N bytes
```

Responsibilities:

- reject random garbage
- reject unsupported versions
- identify Wire RPC packets
- leave room for future evolution

Example:

```python
class WireEnvelopeMiddleware:
    MAGIC = b"WRPC"
    VERSION = 1
```

Inbound:

```python
if data[:4] != b"WRPC":
    raise CodecDecodeError(...)
```

### Done when

Arbitrary bytes cannot fall straight into MessagePack or JSON parsing.

---

# Phase 14 — Add Logical Payload Size Guard

TCP already has a physical `max_frame_size`.

Codec middleware should independently enforce the maximum logical encoded payload.

```python
EncodedSizeGuardMiddleware(
    max_size=16 * 1024 * 1024
)
```

This matters once transformations such as compression are involved.

---

# Phase 15 — Add Compression Middleware

Use Zstandard first.

```python
ZstdMiddleware(
    threshold=2048,
    max_decompressed_size=16 * 1024 * 1024,
)
```

Rules:

```text
payload < threshold
→ leave raw

otherwise
→ try compression

compressed >= original
→ leave raw
```

The compression envelope should contain:

```text
compressed flag
original size
payload
```

Inbound must validate `original_size <= max_decompressed_size` **before decompression**.

This is the decompression-bomb defense.

---

# Phase 16 — Add Optional Authenticated Encryption

Add AEAD only after the basic protocol machinery is stable.

Prefer one strong implementation such as:

```python
ChaCha20Poly1305Middleware
```

Responsibilities:

- nonce generation
- encryption
- authentication
- key identifiers for future rotation
- normalized codec errors

Correct ordering:

```text
MessagePack
→ compression
→ AEAD
→ Wire envelope
```

Do not compress encrypted data.

Do not add a separate HMAC when AEAD already authenticates the payload.

---

# Phase 17 — Define the Default TCP Codec Middleware Stack

TCP should come with a conservative, mandatory baseline.

Recommended default:

```text
WireEnvelopeMiddleware
LogicalSizeGuardMiddleware
```

Do **not** force:

- compression
- encryption
- signing

Those are policy-dependent.

User-provided middleware should sit on top of the default stack.

Example:

```python
App(
    transport=TcpServerTransport(),
    codec=MsgSpecMsgPackCodec(),
    codec_middleware=[
        ZstdMiddleware(),
        ChaCha20Poly1305Middleware(...),
    ],
)
```

Conceptually:

```text
MessagePack
↓
user middleware
↓
mandatory TCP / Wire RPC codec middleware
↓
TCP frame
```

The framework should own the exact ordering so it is never ambiguous.

---

# Phase 18 — Add Codec Lifecycle Propagation

Some codec middleware may eventually own resources:

- key providers
- HSMs
- compression dictionaries
- remote key services

Allow codec middleware to also satisfy `StartupComponent`.

Then:

```text
App
├── codec startup
│   └── middleware startup
└── transport startup
    └── auth startup
```

Preserve the rule:

> Lifecycle follows ownership.

---

# Phase 19 — Harden Error Semantics

Separate recoverable RPC decode errors from fatal wire-protocol violations.

Recoverable:

```text
valid WRPC packet
invalid params
bad MessagePack structure
schema mismatch
```

These can become:

```text
InvalidRequest
InvalidParams
```

Fatal protocol violations:

```text
frame too large
bad WRPC magic
unsupported protocol version
invalid AEAD tag
impossible compression metadata
repeated malformed frames
```

These should generally:

```text
CLOSE CONNECTION
```

A hostile peer should not be allowed to fuzz the wire parser indefinitely over one persistent socket.

---

# Phase 20 — Add Concurrent Dispatch

Only after the transport is concurrency-safe.

Prerequisites:

```text
writes serialized
queues bounded
connections explicitly tracked
shutdown graceful
```

Then evolve server dispatch from:

```text
recv
await handler
send
recv
```

into concurrent request execution.

At the same stage, make the client concurrency-safe with:

```text
background reader task
request IDs
pending Future map
write lock
```

---

# Recommended Commit Order

1. `harden tcp frame size limits`
2. `add tcp read and auth timeouts`
3. `bound tcp multicast connections and queues`
4. `serialize tcp writes and add write timeouts`
5. `introduce tcp connection state object`
6. `add tcp idle timeout and keepalive`
7. `add tls support to tcp transports`
8. `add graceful tcp shutdown`
9. `add codec middleware pipeline`
10. `add wire rpc envelope middleware`
11. `add codec payload size guard`
12. `add zstd codec middleware`
13. `add aead codec middleware`
14. `compile default tcp codec middleware stack`
15. `propagate lifecycle through codec middleware`
16. `classify fatal wire protocol violations`
17. `make server dispatch concurrent`
18. `make client multiplexed and concurrency-safe`

---

# Final Architecture

```text
                 WIRE RPC APPLICATION
                         │
                    typed objects
                         │
                         ▼
                  MessagePack / JSON
                         │
                  codec middleware
                  compression / AEAD
                  version envelope
                         │
                  bounded wire bytes
                         │
                         ▼
                HARDENED TCP ENGINE
                 frame size limits
                 bounded queues
                 connection limits
                 read/write deadlines
                 authentication deadline
                 backpressure
                 TLS
                 keepalive
                 graceful shutdown
                         │
                         ▼
                        TCP
```

The target is:

> The production-grade connection foundation beneath an HTTP server, except Wire RPC rides directly on top instead of HTTP.
