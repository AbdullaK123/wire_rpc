# Production contracts and deployment gate

This follow-up implements the plan in `production_readiness_plan.md`. It builds on the first hardening pass, rather than introducing compression, encryption middleware, retries, or a new wire format.

## Implemented behavior

### Call ownership and protocol

`Client` is single-flight: concurrent callers wait for the entire previous send/receive exchange. `call_timeout` defaults to 30 seconds and includes admission waiting. Cancellation while waiting does not affect the active call. Failure/cancellation after transmission closes and permanently invalidates that Client instance. Create a new Client/transport to reconnect. There are no automatic retries; a timeout does not prove that a mutation did not commit.

Responses must have `jsonrpc: "2.0"`, the exact string request ID, and exactly one of `result` or a well-formed `error`. Unexpected notifications received by this request/response Client are protocol errors, not silently consumed results. Applications consuming server broadcasts need a notification-aware client implementation; this release does not provide an idle background subscription reader.

The server supports individual typed requests and absent-ID notifications. Notifications have no RPC response; HTTP completes with 204. Explicit `id: null` remains a request with a null response ID. Batches are explicitly rejected. IDs are strings of at most 255 characters, signed 64-bit integers, or null; method names are nonempty and at most 255 characters. The existing typed-params extension remains: scalar/null params can be used if the handler's declared type accepts them. For strict JSON-RPC interoperability, use object/array params. Missing params are validated as None against the declared handler type.

`App` remains sequential. `MulticastApp` admits up to 16 request tasks by default and refuses overlapping requests from an already-busy peer with error -32002. This keeps one peer from occupying the entire execution pool waiting for its own ordering lock. It is explicit overload rejection, not a promise to queue every pipelined call. Applications still own synchronization between different peers mutating the same resource.

Handler exceptions, invalid parameters, output conversion failures, and response encoding/size failures are request-local. Exception text and validation inputs are not copied into public error data or library request-failure logs. A fallback envelope includes the original bounded request ID. Response budgets must allow at least 2 KiB for worst-case escaped error metadata.

### Identity and authorization

`current_request()` returns immutable `RequestContext(request_id, method, principal, connection_id)` while a handler or middleware runs. It raises outside request execution. Metadata comes from the transport, not from user-supplied params. Custom transports can implement `async get_principal(client_id=None)`; without it, principal is None.

Use the principal for application authorization:

```python
from wire_rpc import current_request

@app.middleware
async def require_login(request, ctx, next_handler):
    if current_request().principal is None:
        raise PermissionError("login required")
    return await next_handler(request, ctx)
```

Authentication alone does not authorize access to an arbitrary resource ID. Handlers/middleware must check resource ownership or permissions before side effects. Background tasks inherit Python context variables: treat captured request metadata as a snapshot, not as live authorization, and revalidate before delayed mutations.

HTTP and WebSocket authentication is revalidated before dispatch. A changed/expired/revoked WebSocket principal closes that socket and denies the handler. An idle revoked WebSocket is checked on its next request; this is not a periodic revocation push service. TCP authenticators bind a principal at connection establishment. Shared-secret/HMAC principal strings identify the authenticated remote endpoint; custom authenticators are needed for a stable user/service identity. TCP supports `auth=SharedSecretAuth(...)` and `auth=HmacChallengAuth(...)` on the client as well as the server, under the connect deadline.

Browser transports reject any supplied Origin not in `allowed_origins`. An empty allowlist rejects browser-origin requests; origin-less service clients remain allowed. This is an exact allowlist, not CORS wildcard handling. Cross-origin HTTP preflight support is not supplied; use a same-origin deployment or configure a trusted reverse proxy's explicit CORS policy. WebSocket compression is disabled.

Login/logout endpoints have a separate 16-request concurrent admission budget and deadlines. These are concurrency limits, not a complete rate limiter. Configure attempt-rate limits and abuse controls at the edge/credential provider.

`InMemorySessionStore` caps live sessions at 10,000 by default, prunes expired/unvisited sessions, and bounds expiration tombstones under login/logout churn. It is single-process and volatile. Multi-worker deployments need a shared SessionStore implementation; restarting a process invalidates its in-memory sessions.

### Resource limits

| Resource | Default / behavior |
|---|---|
| TCP frame / HTTP body / WebSocket message | 1 MiB |
| Stdio frame | 16 MiB; configurable |
| Multicast TCP/WS connections | 64, including handshakes |
| TCP/WS receive queue | 256 entries, 16 MiB queued bytes |
| Multicast queued messages per peer | 16 |
| HTTP pending requests | 64, including authentication/body reads |
| HTTP queued request bodies | 16 MiB; excess gets 503 |
| Multicast executing request tasks | 16 |
| App handler/startup deadline | 30 seconds each |
| App shutdown drain/hooks | 10 seconds per phase |
| App response bytes | 4 MiB, capped by transport capacity; at least 2 KiB |
| WebSocket auth/write/close | 10 / 10 / 5 seconds |
| TCP writer close | 5 seconds, abort on expiry/cancellation |
| Stdio read/write | 30 / 10 seconds |
| Stdio graceful exit / terminate / kill waits | 5 / 2 / 2 seconds |

These are wire/queue budgets, not a total process RSS guarantee. There can also be one in-progress frame per peer, underlying socket buffers, decoded object allocations, encoded response allocations, and application data. Encoding size checks occur after the codec produces bytes. Bound collections and output generation in application schemas/handlers and set process/container memory limits based on measurement.

Queue producers block at their byte/message/peer limits. Shutdown wakes blocked producers/consumers and frees queued payloads. Disconnect removes queued requests for that peer. HTTP overload rejects admission explicitly. Broadcast fanout is bounded by the connection cap and sends independently so a slow peer does not serialize every other peer's write.

`app.stats` exposes request, active, completion, exception-failure, timeout, and invalid-envelope counters. `transport.stats` exposes queue bytes/counts and connection/pending counts where relevant; stdio reports stderr bytes discarded. These are snapshots, not a metrics exporter. Integrate them with your monitor. `completed` counts successfully encoded responses, including declared RPC errors; `failed` counts exceptions/send failures. Notification execution is counted in requests/active but does not increment encoded completions.

### Lifecycle

Call `await app.stop()` to stop application admission and drain owned work. Startup failures are surfaced during context entry; successful authentication dependencies roll back when a later dependency fails. Shutdown hooks and auth dependencies have explicit ownership. Transport close is separate from dependency shutdown.

Asyncio cancellation is cooperative. Handler code that blocks the event loop or swallows cancellation cannot be forcibly stopped safely by this library. Use a supervised worker process for untrusted/CPU-bound work and a process supervisor for hard process-level stop guarantees. Shutdown limits are per phase, not one global wall-clock deadline.

Stdio continuously drains/discards stderr, drains stdout during process shutdown, closes stdin, waits briefly, then terminates and finally kills a stubborn child. Do not mix unrelated stderr readers with the owned drain task. Pipe supervision targets the direct child, not arbitrary descendant process trees. StdIoServerTransport uses platform stdin/stdout pipe APIs; native Windows support for this server-side mode must be validated for the chosen deployment (the cross-platform client subprocess tests do not establish it).

### Codec compatibility

| Codec | TCP / stdio | HTTP / browser WebSocket |
|---|---|---|
| MsgSpecJsonCodec | Supported | Supported |
| PydanticCodec | Supported | Supported |
| MsgSpecMsgPackCodec | Supported | Rejected during App/Client construction |

Custom text codecs must declare `is_text = True`. Built-ins also expose `media_type`. Codec-specific schema/coercion behavior remains that of the underlying codec; changing a codec is not a guarantee of identical coercion semantics. Use explicit constrained models and test migrations between codecs.

## Validation performed

- **182 tests passed** with warnings treated as errors on Linux / Python 3.12.14 using locked dependencies.
- Real TCP authenticated calls exercise both built-in authenticators with all three codecs, including failures followed by successful calls.
- Real HTTP/WS tests exercise principal propagation, rejected input, notification completion, session revocation, origin refusal, chunked response limits, and failed-upgrade cleanup.
- Event-controlled concurrency tests cover request ownership, cancellation, per-peer overload, byte backpressure, and broadcast isolation.
- A fixed-seed malformed corpus and every truncation of a valid request are checked against all codecs.
- Twenty real WebSocket open/send/close cycles showed no task or Linux file-descriptor growth. This is a short regression test, not a production-scale load/soak benchmark.
- Source and wheel builds pass. The installed wheel is imported outside the checkout, with its typing marker checked. The local artifact smoke check reused the installed locked dependency environment because an independent dependency download was unavailable. CI is configured to install the exported locked dependencies into a fresh wheel environment.
- GitHub Actions is configured for Python 3.12/3.13/3.14 on Linux/macOS/Windows. Configuring the matrix is not evidence those jobs passed; inspect the PR checks before merging.

## Deterministic execution traces

All observations in this table are **From code**, verified by the named regression surfaces.

| Step | Location | Condition/Event | State Change | Side Effect | Notes |
|---|---|---|---|---|---|
| 1 | Client.call | First caller starts exchange | Owns full-call lock | Other callers wait | Call ownership tests |
| 2 | Client.call | Cancellation after send | Client becomes permanently closed | Transport closes; no next call may consume a late reply | Cancellation tests |
| 3 | Client.call | Waiting caller cancelled instead | It never owned the lock | Active caller remains intact | Admission cancellation test |
| 4 | Shared process boundary | Handler/encoder raises | Failure counter increases | Sanitized correlated error is encoded | Failure-isolation tests |
| 5 | Shared process boundary | Next valid request arrives | New independent request context | Handler still runs | Actual TCP/HTTP/WS tests |
| 6 | Identity provider | Session revoked before dispatch | Principal check fails | Handler is not invoked; WS closes | Revocation tests |
| 7 | PayloadQueue.put | Byte or peer budget full | Producer waits without enqueuing | Queue stays within its configured wire-byte budget | Queue tests |
| 8 | Transport.close | Producer/consumer blocked | Queue closes and wakes both | Payload ownership released | Shutdown tests |
| 9 | Stdio.close | Child ignores graceful EOF | Terminate, then kill if needed | Parent no longer waits indefinitely on an uncooperative direct child | Injected-deadline test |

## Required deployment sign-off

Before treating a particular application as production-ready: pass the CI matrix for the intended platform, validate your TLS/proxy and Origin policy, configure authentication and application authorization, choose measured limits, wire monitoring, and run a workload-specific load/soak test. Backups, transaction isolation, idempotent mutations, dependency vulnerability response, and application data ownership are application/operations responsibilities.

Authoritative references used: [Python cancellation semantics](https://docs.python.org/3.12/library/asyncio-task.html), [Python subprocess pipe behavior](https://docs.python.org/3/library/asyncio-subprocess.html), [aiohttp client limits](https://docs.aiohttp.org/en/stable/client_reference.html), [msgspec missing fields](https://msgspec.dev/supported-types), [JSON-RPC notifications](https://www.jsonrpc.org/specification), [OWASP WebSocket security](https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html), and [uv GitHub Actions integration](https://docs.astral.sh/uv/guides/integration/github/).
