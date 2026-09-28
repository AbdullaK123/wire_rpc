# Authentication suite

## Composition and transport support

Authentication returns a stable principal string or `None`. Operational failures
raise `AuthUnavailableError` and never fall back to anonymous access. Authorization
of individual RPC methods/resources remains application policy; inspect
`current_request().principal` in handlers or middleware.

| Component | Use |
| --- | --- |
| `CookieSessionAuth` | HTTP/browser WebSocket login, session cookies and logout |
| `BearerAuth` | HTTP and native WebSocket `Authorization: Bearer` tokens |
| `ApiKeyAuth` | HTTP and native WebSocket configurable API-key header |
| `TcpTokenAuth` / `TcpTokenClientAuth` | Versioned, bounded token handshake |
| `HmacChallengeAuth` / `HmacChallengeClientAuth` | Versioned identified HMAC proof over a fresh challenge |
| `ClientCertificateAuth` | TLS-verified client-certificate SHA-256 fingerprint mapping; HTTP, WebSocket, TCP |
| `trusted_principal` on stdio transports | Explicit local identity assigned by the process owner, never supplied by the child |

`Authenticator` requires only `verify`. `InteractiveAuthenticator` adds login and
logout; transports mount those routes only when supported. `ClientAuthenticator`
is the TCP client-side handshake contract. Existing user authenticators providing
all three old methods remain compatible.

The new token/HMAC authenticators require TLS by default on both ends. For a local,
isolated test only, both sides can explicitly set `require_tls=False`. The correctly
spelled `HmacChallengeAuth` is a **new wire protocol**, with a key identifier and
version/domain binding. It does not interoperate with legacy `HmacChallengAuth`.
Legacy classes remain in their original modules; their optional `principal=` now
establishes a stable identity instead of treating an ephemeral peer address as an
account. Legacy shared-secret/HMAC handshakes still rely on transport TLS supplied
by the caller; prefer the new defaults for new deployments.

Token TCP connections revalidate credentials before each request's dispatch. HMAC
connections recheck the configured key identity/digest, and certificate connections
recheck the mapping. HTTP and WebSocket transports also revalidate before dispatch.
Revocation cannot undo an operation that already passed its auth check; transactional
application authorization is required for that stronger guarantee.

## Validators

- `CallbackCredentialValidator(async_callback)` adapts an application's credential
  lookup. The callback must parameterize database queries and must not log secrets.
- `PasswordCredentialValidator(async_lookup)` takes `PasswordAccount(principal,
  password_hash, enabled)`. Passwords are verified with Argon2id off the event loop.
  The input is bounded; a missing account performs dummy verification. Use the same
  Argon2 parameters for stored hashes as the configured library defaults if comparable
  work is required. This is not a claim of constant-time database/network behavior.
- `StaticPasswordCredentialValidator(mapping)` copies configured username/account
  records. It takes password **hashes**, not plaintext configuration passwords.
- `ApiKeyValidator(async_lookup)` accepts `key_id.secret` and verifies an
  `ApiKeyRecord`. Store a SHA-256 digest of a high-entropy secret (generate at least
  32 random bytes); use `ApiKeyRecord.from_secret` during provisioning. Lookup is live
  for revocation/expiry. No positive identity cache is kept.
- `SessionTokenValidator(store)` resolves an opaque token and delegates lifecycle
  startup/shutdown to the store where supported.
- `JwtTokenValidator(keys, algorithm=..., issuer=..., audience=...)` verifies offline
  against trusted `kid -> key` configuration. It requires `exp`, `iat`, `sub`, `iss`,
  and `aud`, validates time/audience/issuer, restricts the algorithm, and rejects
  token-directed key URLs and unsupported critical headers. Refresh trusted keys by
  replacing/reconfiguring the validator under application control. This is not an
  OAuth authorization server, OIDC login flow, automatic JWKS fetcher, or JWT revocation
  service. JWTs can remain valid until expiration unless application policy adds a
  revocation check.

Password hashing admits at most four operations per validator by default; excess
work fails immediately instead of building an unbounded queue. Cancelling a request
while its native hash is running does **not** release that slot until the worker
actually finishes. This capacity guard does not replace distributed per-account/IP
login rate limiting, which applications must provide at their ingress layer.

## Session stores

All four stores implement `create`, `validate`, `destroy`, `get`, `rotate`, and
`revoke_all`. `get` returns a copied `SessionRecord(principal, payload)`. Payloads
must be JSON objects and the encoded record is limited to 16 KiB. IDs/principals are
bounded; malformed bearer IDs are rejected before storage access.

Tokens contain 32 cryptographically random bytes. Only SHA-256 token digests are
persisted. A record is usable only before its server-side expiry. Rotation consumes
one live token atomically, preserves metadata and **preserves absolute expiry**.
It does not extend session lifetime. Two competing rotations produce one winner;
rotation racing account revocation cannot leave a valid descendant. A collision
cannot overwrite another session or partially delete the original. Destroy is
idempotent. Revoke-all targets an exact principal and returns the number removed.

| Store | Ownership / concurrency |
| --- | --- |
| Memory | One process/event loop; atomic operations do not suspend. Bounded expiry bookkeeping. |
| SQLite | File-backed; `startup()` creates schema. Each operation owns a connection and `BEGIN IMMEDIATE` transaction in a worker thread. Multiple adapters/processes share database locking. `:memory:` is intentionally rejected. |
| Redis | Inject `redis.asyncio.Redis`; caller owns/ closes client. Lua executes atomic operations over two keys in one cluster hash slot. Dedicated namespace; all adapters must share TTL/capacity policy. |
| PostgreSQL | Inject an asyncpg pool; caller owns/closes it. `startup()` creates the fixed schema and indexes. Namespace advisory locks plus transactions serialize mutations and capacity checks across workers. |

Defaults: 24-hour absolute TTL, 10,000 sessions per store/namespace. SQLite uses a
five-second database lock timeout and at most four active worker operations per adapter.
Cancellation retains SQLite worker admission until the worker finishes, just as it does
for password hashing. Configure Redis socket timeouts and PostgreSQL
pool/command timeouts; transport/application deadlines provide an additional bound.

Redis expiration pruning and revoke-all perform work proportional to the bounded
namespace size. PostgreSQL operations serialize per namespace; SQLite serializes
writes to its file. These are correctness-first bounded stores, not an assertion of
unlimited throughput. Benchmark representative login/rotation workloads before
raising capacity. Use dedicated Redis persistence/eviction configuration appropriate
for canonical session state; cache loss invalidates sessions and missing expiry
metadata fails closed. Protect session payloads and backend access separately.

A cancelled SQLite caller does not stop its already-running worker transaction.
The transaction still commits or rolls back atomically, but the caller may not know
which. Never interpret cancellation as proof a mutation did not happen. The same
unknown-outcome principle applies to network loss around Redis/PostgreSQL commits.

## Browser policy and client configuration

Cookie auth defaults to Secure, HttpOnly, SameSite=Strict and refuses plaintext
requests. `secure=False` defaults to allowing plaintext for explicit local tests.
`require_tls=False, secure=True` is available only when a deployment intentionally
terminates trusted TLS upstream; forwarded headers are not trusted automatically.
Configure the server transport's exact `allowed_origins` for browser requests.
Origin rejection and SameSite policy are part of the browser CSRF boundary; applications
needing different cross-site flows must supply an appropriate CSRF policy.
Duplicate Authorization/API-key headers and duplicate named session cookies are
rejected. Logout backend failures return unavailable rather than falsely reporting
successful revocation. Login/body/auth capacities remain transport-bounded.

`HttpClientTransport` and `WsClientTransport` accept `headers`, `cookie_jar`, and
`ssl_context`. `credential_headers(bearer=...)` and `credential_headers(api_key=...)`
construct explicit credentials. Configured headers/cookies require HTTPS/WSS and
server verification by default. Local tests can set `allow_insecure_credentials=True`.
HTTP RPC requests and WebSocket upgrades do not follow redirects, avoiding credential
leakage through custom headers. URL userinfo is rejected. The application owns login,
refresh, and an injected cookie jar; the transport owns its internal aiohttp session.
Browser-native WebSockets use cookies; no long-lived token-in-query scheme is added.

mTLS requires a server SSL context with `verify_mode=ssl.CERT_REQUIRED`, configured
trust roots, and a client context containing its certificate/key. Map the SHA-256
fingerprint of the verified DER certificate to a principal. Subject strings or
forwarded certificate headers are never accepted as verification evidence.

## Installation and examples

Optional features do not add mandatory runtime dependencies:

```sh
uv add 'wire-rpc[password,jwt,redis,postgres]'
# Inside this repository, install every optional backend for development:
uv sync --locked --all-extras
```

Cookie login with an application account lookup:

```python
from wire_rpc.auth import CookieSessionAuth
from wire_rpc.auth.credentials import PasswordCredentialValidator
from wire_rpc.auth.sessions import SQLiteSessionStore
from wire_rpc.transports.http import HttpServerTransport

# async lookup(username) returns PasswordAccount or None.
auth = CookieSessionAuth(
    PasswordCredentialValidator(lookup),
    SQLiteSessionStore('sessions.sqlite'),
)
transport = HttpServerTransport(
    '127.0.0.1', 8443, auth=auth, ssl_context=server_tls,
    allowed_origins={'https://app.example.com'},
)
# App owns auth startup/shutdown. The TLS context and account lookup are supplied
# by the application; no credentials are hard-coded by the framework.
```

Shared opaque-token auth:

```python
from wire_rpc.auth import BearerAuth
from wire_rpc.auth.credentials import SessionTokenValidator
from wire_rpc.auth.sessions import RedisSessionStore

# redis_client is an application-owned async Redis client with timeouts configured.
store = RedisSessionStore(redis_client, namespace='my-app')
auth = BearerAuth(SessionTokenValidator(store))
# Issue tokens only after independent credential verification.
# await store.revoke_all(principal) invalidates existing sessions.
```

Do not share the same lifecycle-owning validator/auth instance between independently
started applications. Each app should use its own adapter while sharing caller-owned
backend pools. Applications own database migrations/permissions: the PostgreSQL
startup role must be allowed to create the fixed table/indexes (or an administrator
must provision them first). No backend silently opens or closes an injected pool.

## Adversarial verification

```sh
uv run --locked --all-extras mypy
uv run --locked --all-extras pyright
uv run --locked --all-extras pytest -q -W error
# For real shared-backend tests, configure isolated test services:
WIRE_TEST_REDIS_URL=redis://localhost:6379/0 \
WIRE_TEST_POSTGRES_DSN=postgresql://user:password@localhost/test_db \
uv run --locked --all-extras pytest -q tests/auth -W error
```

The CI shared-session job provisions real Redis 7 and PostgreSQL 17. Without those
environment variables, local shared-backend cases explicitly skip. The platform
matrix still exercises SQLite, memory, validators, TLS, real sockets, and lifecycle
failures. No timing threshold is used to prove password-work parity; cancellation
and mutation tests use controlled events or atomic backend outcomes.

From code, the security regression traces are:

| Step | Location | Condition/Event | State Change | Side Effect | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | Password validator | Caller cancels an active worker | Admission ownership transfers to completion callback | New work remains bounded | Callback observes exceptions and releases exactly once. |
| 2 | Session rotation | Two callers present the same token | One atomic consume/rename succeeds | One replacement exists | Original absolute expiry survives rotation. |
| 3 | TCP dispatch | Session revoked after handshake | Revalidation fails; connection closes | Handler is not authorized | Connection-time identity is insufficient for mutable credentials. |
| 4 | WebSocket client | Server sends a redirect | Trace hook closes response and aborts | No redirected credential-bearing request | Covers custom API-key headers, not only Authorization. |
| 5 | mTLS admission | Trusted but unmapped certificate | Application verification returns no principal | Request is rejected before RPC queue admission | TLS trust and application identity mapping are separate checks. |

Authoritative implementation references:
[Argon2-cffi](https://argon2-cffi.readthedocs.io/en/stable/howto.html),
[PyJWT](https://pyjwt.readthedocs.io/en/stable/api.html),
[JWT best practices](https://www.rfc-editor.org/rfc/rfc8725),
[bearer token usage](https://www.rfc-editor.org/rfc/rfc6750),
[PostgreSQL locks](https://www.postgresql.org/docs/current/explicit-locking.html),
[Redis scripts](https://redis.io/docs/latest/develop/programmability/eval-intro/).
