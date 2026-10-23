# Changelog

All notable changes to wire_rpc are recorded here.

## Versioning policy

wire_rpc follows [Semantic Versioning](https://semver.org/). While the version is
`0.x`, a minor release (`0.1` → `0.2`) may contain breaking changes and a patch
release (`0.1.0` → `0.1.1`) never does. From `1.0` on, only a major release may
break compatibility.

A breaking change is any change that makes previously working code or deployments
fail or behave differently: a removed or renamed public name, a changed default, a
changed wire format, or a changed error type. Every breaking change is listed under
**Breaking** in its release, with what to do about it. Names starting with an
underscore are private and can change in any release.

## [Unreleased]

### Breaking
- **HTTP and WebSocket servers now accept browser requests from their own origin
  by default.** With `allowed_origins` unset, a request whose `Origin` header names
  the server's own scheme, host and port is allowed; every other origin still gets
  `403`. Previously every browser request was rejected until `allowed_origins` was
  set. To keep blocking all browsers, pass `allowed_origins=set()`. An explicit
  allowlist behaves as before. Rejected origins are now logged once as a warning.
- **`WsClientTransport` no longer limits how long it waits for each message by
  default** (`receive_timeout` was 10 seconds, and is now `None`). Calls are
  bounded by `Client`'s `call_timeout`, so methods slower than 10 seconds now work
  over WebSocket, and a listening client can wait indefinitely for broadcasts. Pass
  `receive_timeout=10` to restore the old limit.

### Added
- `App.serve()` and `MulticastApp.serve()`: a public way to run an app inside an
  event loop you already have. `run()` is unchanged.
- Batches refused as a whole (empty, too long, or too large to answer) are now
  counted in `app.stats["invalid"]` and logged as `<batch> -32600` in the access log.
- Full user documentation in `docs/`, split by topic: getting started, handlers,
  routers and middleware, apps and lifecycle, errors, the client, every transport,
  codecs, authentication, security, limits, observability, deployment, testing,
  protocol support and a configuration reference.

### Changed
- Internal design notes and hardening records moved from `docs/` to `dev_notes/`,
  along with development, testing and release instructions. `docs/guide.md` now
  points to the new pages.

## [0.2.0] - 2026-10-23

### Breaking
- **Removed the legacy single-secret HMAC authenticator** (`wire_rpc.auth.hmac_challenge.HmacChallengAuth`).
  Use `HmacChallengeAuth` with `HmacChallengeClientAuth` from `wire_rpc.auth`. Its
  wire protocol is different, so upgrade both ends together.
- **Batches are now executed instead of rejected.** A JSON array request used to
  get a single -32600 error; it now gets an array of responses. Clients that relied
  on the rejection will see a different reply shape.
- **Logging moved from loguru to Python's standard `logging` module, and the loguru
  dependency is gone.** `logger.enable("wire_rpc")` no longer does anything;
  configure the `wire_rpc` logger with `logging` instead. `App.run()` and
  `MulticastApp.run()` now print INFO logs to stderr by default, including one
  access line per request; pass `log_level=None` to turn that off.
- **`Client.call` raises `ConnectionError` for a dropped connection or an
  undecodable reply.** It previously let `asyncio.IncompleteReadError` (TCP) or
  `CodecDecodeError` escape. Code catching those around `call` should catch
  `ConnectionError`.

### Added
- `ThreadTransport` for JSON-RPC between two threads of one process, each with its
  own event loop. Either side can start first; sends wait up to `connect_timeout`.
- JSON-RPC 2.0 batch support on every transport and codec, with `max_batch_size`
  (default 100) on `App` and `MulticastApp`, one shared deadline per batch, and a
  per-call response budget.
- An end-to-end test suite (`tests/e2e/`) covering every transport/codec,
  transport/authenticator and session-backend combination.
- `Client.notify()` and `Client.batch()` (with `BatchCall` and `BatchNotification`);
  batch results come back in order, with failed calls returned as `WireRpcError`.
- `Client(on_notification=...)` receives server notifications that arrive during a
  call, and `Client.receive_notification()` waits for one on a listening client.
- An access log (`wire_rpc.access`): one line per request with the method,
  outcome and duration, never params or results.
- `wire_rpc.middleware.metrics_middleware`, which reports each executed call's
  method, duration, outcome, error code and principal to your exporter.
- A user guide (`docs/guide.md`), benchmarks (`docs/benchmarks.md`,
  `scripts/bench.py`), `SECURITY.md`, and ruff in CI.
- Docstrings across the library, and a quick start in the README.

### Changed
- `Client.call` passes server notifications (such as broadcasts) that arrive while
  a call is waiting to `on_notification`, or drops them, instead of failing the call.
- `HttpClientTransport.send` accepts `204 No Content`, so notifications can be sent
  over HTTP.
- Type checking uses pyrefly instead of mypy and pyright.
- Quieter logs: handler registration is DEBUG, a request for an unknown method is
  no longer a warning (the access log records it), and closing an idle TCP
  connection is INFO rather than ERROR.

### Fixed
- `SQLiteSessionStore.startup()` leaked a database connection (reported as a
  `ResourceWarning` on Python 3.13+).

## [0.1.0]

Initial version: `App`, `MulticastApp` and `Client`; TCP, HTTP, WebSocket and stdio
transports; JSON, MessagePack and Pydantic codecs; authenticators and session
stores. See `docs/production_contracts.md`.
