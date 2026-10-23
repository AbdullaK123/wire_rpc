# Security model

This page sets out what wire_rpc protects against, what it expects from you, and
how to configure it safely. To report a vulnerability, see
[SECURITY.md](../SECURITY.md).

## What wire_rpc protects

**Untrusted input is bounded and validated.** Every message has a size limit,
every queue a length and byte limit, and every wait a deadline (see
[Limits](limits.md)). Params are validated against your handler's types before
your code runs. Malformed bytes, truncated messages and invalid envelopes are
rejected without reaching a handler; the test suite feeds random and truncated
input through every codec.

**Errors don't leak.** Exception text, stack traces, validation details and the
caller's own input never appear in error responses or in wire_rpc's logs. The
access log records method names and outcomes, never params, results or
principals.

**Identity is trustworthy.** `current_request().principal` comes from the
transport's authenticator, never from the caller's params. Failing to
authenticate never yields an anonymous session. Credentials are checked before
every request, so revocation reaches open connections.

**Secrets are handled carefully.** Passwords are hashed with Argon2, API-key
secrets and session ids are stored only as SHA-256 hashes, and comparisons are
constant-time. Authenticators that transmit secrets require TLS by default, and
clients refuse to send credentials over plaintext, to an unverified server, or
through a redirect.

**One peer can't starve the others.** Multicast transports bound each peer's
queue and reject overlapping requests from the same peer; broadcasts don't wait
on slow peers.

## What's yours to handle

- **Authorization.** wire_rpc tells you who is calling. Whether they may read or
  change a particular resource is your application's decision; check it before
  any side effect. See [Authentication](authentication/README.md#authentication-is-not-authorization).
- **Rate limiting.** wire_rpc limits concurrency, not frequency. Limit request
  rates, and especially login attempts per account and per IP, at your edge.
- **TLS configuration.** Choose protocols and ciphers, manage certificates, and
  rotate keys. Or terminate TLS at a reverse proxy you trust.
- **Handler safety.** Use parameterized queries, bound the work a single call can
  trigger (pagination, size caps on lists in your params), and keep CPU-heavy
  work off the event loop.
- **Idempotency.** Timeouts mean "unknown outcome", not "didn't happen". Make
  writes that clients may retry idempotent.
- **Secrets at rest.** Store keys, HMAC secrets and database credentials in a
  secret manager, not in code.

## TLS

Use TLS for anything that crosses a network you don't fully control:

```python
import ssl

server_tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
server_tls.load_cert_chain("server.pem", "server.key")

client_tls = ssl.create_default_context(cafile="ca.pem")
```

`ssl.create_default_context` gives secure defaults: modern protocol versions,
certificate verification and hostname checking. Don't disable verification on
clients. If a client needs a private CA, pass `cafile=` rather than turning off
checks.

On Python 3.13 and later, the default context also rejects certificates missing
standard extensions (Authority and Subject Key Identifiers). Certificates from
public CAs have them; if you generate your own, include them.

## Browsers

Browser-facing HTTP and WebSocket servers need three things:

1. **An origin policy.** By default only the server's own origin (same scheme,
   host and port) is accepted, which is right when the server serves its own
   pages. Otherwise set `allowed_origins` to the exact origins of your pages, such
   as `{"https://app.example.com"}`. Requests from other origins get `403`.
   Requests without an `Origin` header (non-browser clients) are allowed.
2. **Cookie authentication** (`CookieSessionAuth`) with its defaults: `Secure`,
   `HttpOnly` and `SameSite=Strict` cookies, and TLS required.
3. **The same origin** for your pages and your API. wire_rpc doesn't answer CORS
   preflight requests. If pages and API must be on different origins, configure
   CORS on a reverse proxy, as narrowly as you can.

Together, origin checks and `SameSite=Strict` stop cross-site request forgery. If
your application needs cross-site flows, you need your own CSRF protection.

**Unauthenticated servers on a developer machine** should set `allowed_origins`
explicitly. The same-origin default compares the `Origin` header with the `Host`
the request arrived for, and a DNS-rebinding attack can make a malicious page
appear to share an origin with a server on `127.0.0.1`. Cookie authentication
isn't affected (the browser won't send your cookies to the attacker's host
name), but a server with no authentication at all is. An explicit allowlist, or
an empty one for servers browsers should never reach, closes that gap.

Browsers can't send custom headers on WebSockets, so there's no
token-in-the-URL scheme. Use cookies for browser WebSockets.

## Hardening checklist

- [ ] TLS on every network transport, or a trusted TLS-terminating proxy.
- [ ] An authenticator on every server that isn't on a private loopback link.
- [ ] Authorization checks in handlers or middleware for every resource access.
- [ ] `allowed_origins` set explicitly on browser-facing servers behind a proxy or
      serving pages from another origin, and on unauthenticated local servers.
- [ ] Rate limiting at the edge, especially on `/login`.
- [ ] Limits sized to your largest legitimate messages, not larger.
- [ ] A shared session store if you run more than one process.
- [ ] Short-lived JWTs, and a key rotation plan.
- [ ] Logs shipped somewhere you'll look, and metrics on error codes `-32001`
      (access denied) and `-32600`/`-32700` (probing). See [Observability](observability.md).
