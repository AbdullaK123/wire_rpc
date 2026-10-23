# Authentication

Authentication answers one question for every request: *who is calling?* The
answer is a **principal**, a short string such as `"alice"` or
`"billing-service"`, which your handlers read from `current_request().principal`.

```python
from wire_rpc import current_request

@app.method("notes.mine")
async def my_notes(ctx) -> list[Note]:
    return await ctx.db.notes_for(current_request().principal)
```

## How it fits together

```
client credentials ──▶ server transport ──▶ authenticator ──▶ validator ──▶ principal
                        (auth=...)          (how credentials     (are they
                                             are presented)       valid?)
```

- An **authenticator** reads credentials from a connection or request. It's
  specific to the transport: a bearer header for HTTP, a handshake for TCP.
- A **validator** decides whether credentials are good and whose they are: a JWT
  check, an API-key lookup, a password hash, a session store.
- You pass the authenticator to the **server transport** with `auth=`, and the
  matching credentials to the **client transport**.

```python
server = HttpServerTransport("0.0.0.0", 8443, ssl_context=server_tls,
                             auth=BearerAuth(JwtTokenValidator(...)))
client = HttpClientTransport("https://rpc.example.com:8443/rpc", ssl_context=client_tls,
                             headers=credential_headers(bearer=token))
```

## What to use

| Situation | Transport | Authenticator | Page |
|---|---|---|---|
| Browser app with a login form | HTTP or WebSocket | `CookieSessionAuth` | [HTTP and WebSocket](http-and-websocket.md#cookie-login) |
| API clients with tokens from your identity provider | HTTP or WebSocket | `BearerAuth` + `JwtTokenValidator` | [HTTP and WebSocket](http-and-websocket.md#bearer-tokens) |
| API clients with keys you issue | HTTP or WebSocket | `ApiKeyAuth` + `ApiKeyValidator` | [HTTP and WebSocket](http-and-websocket.md#api-keys) |
| Service to service, shared token | TCP | `TcpTokenAuth` / `TcpTokenClientAuth` | [TCP](tcp.md#token-handshake) |
| Service to service, secret never sent | TCP | `HmacChallengeAuth` / `HmacChallengeClientAuth` | [TCP](tcp.md#hmac-challenge) |
| Services with certificates (mutual TLS) | TCP, HTTP or WebSocket | `ClientCertificateAuth` | [TCP](tcp.md#client-certificates), [HTTP and WebSocket](http-and-websocket.md#client-certificates) |
| A child process you launched | stdio | `trusted_principal=` | [stdio](../transports/stdio.md#identity) |

## Guarantees

- **No credentials, no handler.** A request that fails authentication never
  reaches middleware or a handler.
- **Failure is never anonymous.** If an authenticator can't decide, for example
  because the database behind it is down, it raises `AuthUnavailableError` and
  the request is refused (`503` over HTTP). It never falls back to "no
  principal".
- **Checked before every request.** Credentials are checked again before each
  call, not just when the connection opens. Revoking a session, deleting an API
  key, or removing a certificate mapping cuts off an open connection at its next
  request. (A JWT stays valid until it expires; there's nothing to revoke.)
- **Identity comes from the transport, never from params.** A caller sending
  `{"principal": "admin"}` changes nothing.
- **TLS by default.** Authenticators that send secrets refuse to run without TLS
  unless you opt out with `require_tls=False`, and clients refuse to send
  credentials over plaintext or to an unverified server.
- **Bounded.** Authentication has deadlines and capacity limits, so a flood of
  slow logins can't tie up the server.

## Authentication is not authorization

The principal says who is calling. It doesn't say what they may do. Check
permissions yourself, in middleware or in the handler, before any side effect:

```python
@app.method("notes.delete")
async def delete(params: NoteId, ctx) -> None:
    note = await ctx.db.get_note(params.id)
    if note.owner != current_request().principal:
        raise PermissionError       # caller gets -32001 Access denied
    await ctx.db.delete_note(params.id)
```

Revocation also can't undo a request that already passed its check. If a
permission change must take effect mid-operation, check inside the transaction.

## Lifecycle

Authenticators, validators and session stores that need setup, such as
`SQLiteSessionStore` creating its table, are started by the app before your
startup hook and shut down after your shutdown hook. You don't call
`startup()` yourself.

Don't share one authenticator object between two independently started apps;
give each app its own. They can share the resources underneath, like a Redis
client or a PostgreSQL pool, which stay yours to open and close.

## Writing your own

An authenticator is any object with:

```python
async def verify(self, request) -> str | None: ...
```

`request` is an `aiohttp.web.Request` on HTTP and WebSocket, and a
`(reader, writer)` stream pair on TCP. Return the principal, return `None` to
reject, or raise `AuthUnavailableError` if you can't decide. On TCP, add
`async def revalidate(self, connection) -> str | None` to have credentials
rechecked before each request. Add `login(request)` and `logout(request)`
returning aiohttp responses to have HTTP and WebSocket servers mount
`POST /login` and `POST /logout`.

A TCP client authenticator has:

```python
async def authenticate(self, connection) -> None: ...   # connection is (reader, writer)
```

It must raise if the server rejects it.
