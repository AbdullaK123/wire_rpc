# HTTP and WebSocket authentication

HTTP and WebSocket servers share the same authenticators. HTTP checks credentials
on every request. WebSocket checks them on the upgrade request, and then again
before each call made over the connection.

```python
from wire_rpc.auth import ApiKeyAuth, BearerAuth, ClientCertificateAuth, CookieSessionAuth, credential_headers
```

## Bearer tokens

The client sends `Authorization: Bearer <token>`. A validator decides what the
token means.

**With JWTs from your identity provider:**

```python
from wire_rpc.auth import BearerAuth
from wire_rpc.auth.credentials import JwtTokenValidator

validator = JwtTokenValidator(
    {"key-2026-10": PUBLIC_KEY_PEM},
    algorithm="RS256",
    issuer="https://auth.example.com/",
    audience="notes-rpc",
)
server = HttpServerTransport("0.0.0.0", 8443, ssl_context=server_tls, auth=BearerAuth(validator))
```

The principal is the token's `sub` claim. See
[Credential validators](credentials.md#jwttokenvalidator) for what's checked.

**With opaque tokens from a session store:**

```python
from wire_rpc.auth.credentials import SessionTokenValidator
from wire_rpc.auth.sessions import RedisSessionStore

store = RedisSessionStore(redis_client, namespace="notes")
auth = BearerAuth(SessionTokenValidator(store))

token = await store.create("alice")      # after you've verified alice another way
await store.revoke_all("alice")          # cuts off every open connection at its next call
```

**Client:**

```python
client = HttpClientTransport(
    "https://rpc.example.com:8443/rpc",
    headers=credential_headers(bearer=token),
    ssl_context=client_tls,
)
```

`WsClientTransport` takes the same `headers=`.

Rules the server applies: exactly one `Authorization` header; the scheme is
`Bearer` (any case); the token is 1 to 16,384 printable ASCII characters with no
spaces. Anything else is rejected before the validator runs.

## API keys

The client sends the key in a header, `X-API-Key` by default:

```python
from wire_rpc.auth import ApiKeyAuth
from wire_rpc.auth.credentials import ApiKeyRecord, ApiKeyValidator

async def lookup(key_id: str) -> ApiKeyRecord | None:
    return await db.fetch_api_key(key_id)     # your storage

server = HttpServerTransport(..., auth=ApiKeyAuth(ApiKeyValidator(lookup)))
client = HttpClientTransport(url, headers=credential_headers(api_key=f"{key_id}.{secret}"), ...)
```

Keys have the form `key_id.secret`, and only a hash of the secret is stored. See
[Credential validators](credentials.md#apikeyvalidator) for issuing them. Use
`ApiKeyAuth(validator, header="X-Service-Key")` for a different header name, and
the same name in `credential_headers(api_key=..., api_key_header="X-Service-Key")`.

## Cookie login

For browsers. `CookieSessionAuth` adds `POST /login` and `POST /logout` to the
server, and authenticates RPC requests with the session cookie that login sets.

```python
from wire_rpc.auth import CookieSessionAuth
from wire_rpc.auth.credentials import PasswordAccount, PasswordCredentialValidator
from wire_rpc.auth.sessions import SQLiteSessionStore

async def lookup(username: str) -> PasswordAccount | None:
    row = await db.fetch_user(username)
    return None if row is None else PasswordAccount(row.id, row.password_hash, enabled=row.active)

auth = CookieSessionAuth(PasswordCredentialValidator(lookup), SQLiteSessionStore("sessions.sqlite"))

app = MulticastApp(MulticastWsServerTransport(
    "0.0.0.0", 8443,
    ssl_context=server_tls,
    auth=auth,
    allowed_origins={"https://app.example.com"},
))
```

### Logging in

```javascript
const response = await fetch("/login", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({username, password}),
});
```

The JSON body is passed to the credential validator. Responses:

| Status | Body | Meaning |
|---|---|---|
| `200` | `{"ok": true, "user_id": "..."}` | Logged in; the session cookie is set |
| `400` | `{"ok": false, "error": "Invalid request body"}` | Not a JSON object |
| `401` | `{"ok": false, "error": "Invalid credentials"}` | Wrong username or password |
| `403` | `{"ok": false, "error": "TLS required"}` | Plain HTTP while TLS is required |
| `503` | `{"ok": false, ...}` | The validator or session store is unavailable, or too many sessions |

`POST /logout` destroys the session and clears the cookie.

### The cookie

`HttpOnly`, `SameSite=Strict`, `Secure`, and valid for `max_age` seconds (24
hours by default). The session also expires on the server after the store's
`ttl`. A request carrying two cookies with the session cookie's name is
rejected, to stop cookie-injection tricks.

### Options

```python
CookieSessionAuth(
    credential_validator,
    session_store=None,     # an InMemorySessionStore if omitted: one process only
    cookie_name="session",
    secure=True,            # mark the cookie Secure
    max_age=86400,
    require_tls=None,       # defaults to `secure`
)
```

- `secure=False` is for local development over plain HTTP; it also turns off the
  TLS requirement.
- `secure=True, require_tls=False` is for a deployment where a trusted reverse
  proxy terminates TLS and forwards plain HTTP. Forwarded headers like
  `X-Forwarded-Proto` are not trusted automatically.

Login and logout requests have their own budget: at most 16 at once, each within
the transport's request or auth timeout. That's a concurrency limit, not rate
limiting; limit login attempts per account and per IP at your edge. See
[Security model](../security.md).

### From Python

```python
jar = aiohttp.CookieJar()
async with aiohttp.ClientSession(cookie_jar=jar) as session:
    await session.post("https://app.example.com/login", json={"username": "alice", "password": pw})

client = Client(WsClientTransport("wss://app.example.com/ws", cookie_jar=jar))
```

For an IP-address host such as `127.0.0.1`, use `aiohttp.CookieJar(unsafe=True)`.

## Client certificates

With mutual TLS, the client presents a certificate during the TLS handshake and
the server maps its fingerprint to a principal:

```python
import ssl
from wire_rpc.auth import ClientCertificateAuth

server_tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
server_tls.load_cert_chain("server.pem", "server.key")
server_tls.load_verify_locations("clients-ca.pem")
server_tls.verify_mode = ssl.CERT_REQUIRED          # required

auth = ClientCertificateAuth({
    "3f5c...e9a1": "billing-service",   # lowercase SHA-256 hex of the certificate's DER bytes
})
server = HttpServerTransport("0.0.0.0", 8443, ssl_context=server_tls, auth=auth)
```

```python
client_tls = ssl.create_default_context(cafile="server-ca.pem")
client_tls.load_cert_chain("billing.pem", "billing.key")
client = HttpClientTransport("https://rpc.internal:8443/rpc", ssl_context=client_tls)
```

Compute a fingerprint with:

```sh
openssl x509 -in billing.pem -outform DER | sha256sum
```

Both checks must pass: the certificate must chain to a CA the server trusts
*and* its fingerprint must be in the mapping. If the server's context isn't
`CERT_REQUIRED`, every request is rejected. Subject names and forwarded
certificate headers are never used as identity.

## Browsers and origins

By default (`allowed_origins=None`), a browser request is accepted only from the
server's **own origin**: the same scheme, host and port the request arrived on.
That covers pages the server serves itself, or that are served from the same
address. Requests from other origins get `403`. Requests without an `Origin`
header (non-browser clients) are always allowed.

If your pages are on a different origin, or you're behind a reverse proxy that
terminates TLS (so the server sees `http` while browsers use `https`), list the
origins exactly:

```python
HttpServerTransport(..., allowed_origins={"https://app.example.com"})
```

An explicit set replaces the same-origin rule; an empty set allows no browsers.
Rejections apply to `/login` and `/logout` too, and each rejected origin is
logged once as a warning. Together with `SameSite=Strict` cookies, this is the
defense against cross-site request forgery. There's no CORS support; see
[Security model](../security.md#browsers).

## Clients and credentials

`HttpClientTransport` and `WsClientTransport` protect the credentials you give
them:

- `headers` or `cookie_jar` require `https://`/`wss://` and a TLS context that
  verifies the server. Pass `allow_insecure_credentials=True` only for local tests.
- Redirects are never followed, so credentials can't be forwarded to another host.
- URLs with a username or password in them are rejected.
