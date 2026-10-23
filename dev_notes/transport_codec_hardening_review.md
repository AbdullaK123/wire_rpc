# Transport and codec hardening: first adversarial pass

## Result and scope

Added the first test suite: pytest, pytest-asyncio, and pytest-timeout, with function-scoped async fixtures, attack-surface-specific files, event-controlled concurrency, real loopback integration checks, and production-consequence messages on every assertion. The timeout plugin is a deadlock guard, not the mechanism that orders a test.

Validation: `uv run --locked pytest -q -W error` — **91 passed** on Python 3.12.14, aiohttp 3.14.3, msgspec 0.21.1, and Pydantic 2.13.4. `git diff --check` also passes. Other Python versions and operating systems have not been exercised.

A clean original-commit snapshot was run against the initial 79 regression cases: **64 failed, 15 passed**. The only production-code adjustment in that snapshot was `Generator[None, None, None]`, needed to import TCP on Python 3.12. The remaining 12 cases cover lifecycle behavior, actual sockets, and explicit concurrent reads/writes on the hardened implementation. No failures were hidden with xfail or skip.

## Assumptions and compatibility changes

- HTTP's existing byte-only transport interface supports a sequential `recv()` → `send()` application loop. It carries no request handle for concurrent dispatch. A second `recv()` before the corresponding `send()` now raises `RuntimeError` instead of overwriting response ownership. An unsolicited `send()` also raises.
- Cancelling an HTTP caller after dispatch does not roll back application side effects. Its late response is discarded. Requests cancelled before dispatch are removed from the queue.
- HTTP defaults: 1,024 admitted requests (including authentication/body reads), a 30-second total request deadline, and a 4 MiB aiohttp body limit. These are keyword-only constructor options. The previous aiohttp-default body limit was 1 MiB.
- Multicast WebSocket `client_id` is now a full connection UUID, **not the authenticated principal**. Code relying on `client_id == user_id` must change. Authentication still gates admission; this patch does not add a principal propagation API.
- A unicast WebSocket transport instance accepts one successfully established session during its lifetime. Replacing a disconnected peer would allow queued requests and in-flight responses from the old peer to reach the new peer. Use multicast for multiple/replacement connections or create a new unicast transport with a new application lifecycle.
- TCP multicast IDs also use full UUIDs rather than eight-character prefixes.
- TCP/stdin/stdout frame limits must be positive and fit the four-byte unsigned length header. Stdio now defaults to 16 MiB and rejects empty frames in both directions. It accepts `max_frame_size` as a keyword option.
- Nonpositive receive-queue/message limits are configuration errors. They no longer silently select unbounded behavior.
- `convert(value, dict)` now validates the resulting builtins as a dictionary. Lists, scalars, and null no longer silently satisfy a dictionary target.

## Deterministic debug traces

All implementation observations below are **From code**. They describe the original implementation and the minimally required correction; no dependency behavior is inferred from undocumented APIs.

### HTTP response ownership — cross-caller disclosure

| Step | Location | Condition/Event | State Change | Side Effect | Notes |
|---|---|---|---|---|---|
| 1 | `HttpServerTransport._handle` / `recv` | A is dispatched | A waits on the shared response queue | Application handles A | From code |
| 2 | `_handle` | A is cancelled | Its queue waiter disappears | Application may still finish A | From code; cancellation forced explicitly |
| 3 | `send` | A's result arrives late | Private bytes enter shared queue | Bytes have no request owner | From code |
| 4 | `_handle` | B arrives | B consumes the queued result | B receives A's private response | From code; reproduced by unit test |
| 5 | Fixed `recv` / `send` | Same ordering | A retains a private Future; completed/cancelled Future is discarded by send | B can only receive B's response | From code; tested with actual HTTP clients too |

Minimal fix: per-request Futures and explicit ownership for the active sequential dispatch. Bound admission before the first authentication/body await, remove cancelled queued entries, and wake requests/receivers on shutdown. Dependency shutdown remains exclusively in `shutdown()`.

### TCP unicast admission — connection replacement

| Step | Location | Condition/Event | State Change | Side Effect | Notes |
|---|---|---|---|---|---|
| 1 | `_handle_client` | A begins authentication | No capacity reservation | Callback yields | From code; event barrier |
| 2 | `_handle_client` | B arrives | B also authenticates | Two contenders for one transport | From code |
| 3 | `_handle_client` | Callbacks complete | Later assignment replaces `_connection` | Subsequent send targets replacement peer | From code |
| 4 | Fixed callback | B arrives during auth or after registration | B rejected before authentication; writer closed | A retains response routing | From code |

Minimal fix: reserve the slot synchronously before authentication and reject admission while a connection already exists. Release an unsuccessful reservation in `finally`.

### TCP and stdio framing — channel desynchronization

| Step | Location | Condition/Event | State Change | Side Effect | Notes |
|---|---|---|---|---|---|
| 1 | TCP `_read_frame` | First header byte consumed | Reader cursor advances | Frame is incomplete | From code |
| 2 | Unicast `recv` | Cancellation while waiting for remaining header | Cancellation escapes timeout-only handlers | Writer remains reusable with an unknown next boundary | From code; event barrier |
| 3 | Unicast `recv` | Invalid size | Header consumed; error propagates | Writer also remains open | From code |
| 4 | Stdio `recv` | Arbitrary length header arrives | Original code requests that entire length | No payload bound before body read | From code; EOF distinguishes missing validation from a hang |
| 5 | Fixed readers | Any read failure/cancellation after lock acquisition | Close channel before propagating failure | Later reads cannot reuse it | From code |

Minimal fix: close failed channels, validate stdio lengths before body reads, serialize complete reads and writes, and reject reads on closed channels. A reset during TCP writer cleanup cannot mask the original frame exception. Outbound stdio validation runs before writing any header.

### WebSocket admission and identity — cross-connection corruption

| Step | Location | Condition/Event | State Change | Side Effect | Notes |
|---|---|---|---|---|---|
| 1 | `_handle_ws` | A passes capacity check | No slot reserved before auth/upgrade await | B can pass the same check | From code |
| 2 | Multicast `_handle_ws` | A and B authenticate as the same principal | Both register under the same key | B replaces A's routing entry | From code |
| 3 | Original handler `finally` | A disconnects | Unconditional delete removes shared key | B loses routing; its cleanup can raise KeyError | From code |
| 4 | Fixed admission/registration | Same conditions | Reserve capacity before await; use independent UUIDs | Each socket owns a distinct routing entry | From code |
| 5 | Fixed cleanup | A disconnects | Remove entry only if it still points to A | B remains registered | From code |
| 6 | Unicast replacement | First session already used | Reject a new session on that instance | Old replies cannot migrate to a different peer | From code |

Minimal fix: use the existing connection limiter for the complete handler lifetime, return HTTP 503 at capacity, separate physical connection identity from principal identity, and use identity-checked cleanup. Broadcast and close iterate snapshots because awaited sends/closes permit concurrent removal. Broadcast handles a send timeout as a failed peer and closes it.

### Codec conversion — target validation bypass

| Step | Location | Condition/Event | State Change | Side Effect | Notes |
|---|---|---|---|---|---|
| 1 | `convert` in all three codecs | Caller requests bare `dict` | Enters special builtins branch | Skips target conversion | From code |
| 2 | Builtins branch | Value is a list/scalar/null | Value is returned in its original shape | Caller receives a non-mapping despite its requested target | From code |
| 3 | Fixed branch | Same input | Apply `msgspec.convert(..., dict)` after conversion to builtins | Raises public `CodecConversionError` | From code |

Minimal fix: retain support for converting structured objects to builtins, then validate the requested mapping shape. Existing normalized decode/encode errors are also covered with malformed/trailing wire bytes and unsupported objects.

## Retrieved authoritative dependency evidence

- **From docs:** Python `StreamReader.readexactly(n)` raises `IncompleteReadError` if EOF arrives before all n bytes. [Python streams documentation](https://docs.python.org/3/library/asyncio-stream.html#asyncio.StreamReader.readexactly). The tests use this documented EOF behavior to distinguish header rejection from an attempted body read.
- **From docs:** aiohttp can cancel a handler when its client disconnects with `handler_cancellation` enabled. [aiohttp server advanced documentation](https://docs.aiohttp.org/en/stable/web_advanced.html#web-handler-cancellation). The patched HTTP runner enables this behavior; the integration test is valid regardless of whether disconnect observation precedes or follows the application's send.
- **From docs:** msgspec distinguishes conversion to plain builtins from conversion/validation into a target type. [msgspec usage documentation](https://msgspec.dev/usage). The fix composes both operations for the bare-dictionary special case.

## Remaining work observed in source

This is a first hardening pass, not a claim of complete production readiness:

- `HttpClientTransport.send` still overwrites a single `_pending` response; multiplexed client calls need an explicit concurrency contract and separate tests.
- WebSocket authentication has no explicit deadline or Origin allowlist. Client connection failure cleanup and full receive/shutdown wakeup semantics need separate coverage.
- Stdio still pipes stderr without draining it and waits for child exit without a termination deadline. This pass addresses framing, not subprocess supervision.
- Some TCP writer-close waits are unbounded; stalled shutdown and write-cancellation behavior need a dedicated lifecycle pass.
- `App` and `MulticastApp` include `str(exc)` in internal-error response data; sensitive exception content needs a separate error-disclosure patch and tests.
- No new codec compression, encryption, envelope protocol, or middleware was introduced. The planning documents describe future work, not existing features to test.
