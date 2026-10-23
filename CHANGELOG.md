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
- **Removed the legacy single-secret HMAC authenticator** (`wire_rpc.auth.hmac_challenge.HmacChallengAuth`).
  Use `HmacChallengeAuth` with `HmacChallengeClientAuth` from `wire_rpc.auth`. Its
  wire protocol is different, so upgrade both ends together.
- **Batches are now executed instead of rejected.** A JSON array request used to
  get a single -32600 error; it now gets an array of responses. Clients that relied
  on the rejection will see a different reply shape.
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
- Docstrings across the library, and a quick start in the README.

### Changed
- `Client.call` skips server notifications (such as broadcasts) that arrive while a
  call is waiting, instead of failing the call.
- `HttpClientTransport.send` accepts `204 No Content`, so notifications can be sent
  over HTTP.
- Type checking uses pyrefly instead of mypy and pyright.

### Fixed
- `SQLiteSessionStore.startup()` leaked a database connection (reported as a
  `ResourceWarning` on Python 3.13+).

## [0.1.0]

Initial version: `App`, `MulticastApp` and `Client`; TCP, HTTP, WebSocket and stdio
transports; JSON, MessagePack and Pydantic codecs; authenticators and session
stores. See `docs/production_contracts.md`.
