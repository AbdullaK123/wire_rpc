# Credential validators

A validator decides whether credentials are good and who they belong to. It
returns a principal, returns `None` to reject, or raises `AuthUnavailableError`
when it can't decide (for example, the database is down). It never treats an
error as "no credentials".

```python
from wire_rpc.auth.credentials import (
    ApiKeyRecord,
    ApiKeyValidator,
    CallbackCredentialValidator,
    JwtTokenValidator,
    PasswordAccount,
    PasswordCredentialValidator,
    SessionTokenValidator,
    StaticPasswordCredentialValidator,
)
```

There are two kinds:

- **Token validators** check a single string: bearer tokens, API keys, TCP
  tokens. Use them with `BearerAuth`, `ApiKeyAuth` and `TcpTokenAuth`.
- **Credential validators** check a login body, a JSON object such as
  `{"username": ..., "password": ...}`. Use them with `CookieSessionAuth`.

Principals must be 1 to 255 printable characters without spaces. A validator
that returns anything else is treated as broken (`AuthUnavailableError`).

## `JwtTokenValidator`

Token validator. Needs the `jwt` extra.

```python
validator = JwtTokenValidator(
    {"key-2026-10": PUBLIC_KEY_PEM, "key-2026-04": OLD_PUBLIC_KEY_PEM},
    algorithm="RS256",
    issuer="https://auth.example.com/",
    audience="notes-rpc",
    leeway=5,          # seconds of clock skew allowed, 0 to 60
)
```

- The token's `kid` header picks the key from your mapping. Tokens without a known
  `kid` are rejected.
- Only the one configured algorithm is accepted. Supported: `RS256/384/512`,
  `ES256/384/512`, `EdDSA`, `HS256/384/512`. HMAC keys must be at least 32 bytes.
- `exp`, `iat`, `iss`, `aud` and `sub` are required. `iss` and `aud` must match,
  and `exp`, `iat` and `nbf` must be real numbers.
- The principal is `sub`.
- Keys are never fetched from URLs in the token (`jku`, `x5u`), and tokens with
  `crit` headers are rejected.

It verifies offline against keys you supply. It doesn't fetch JWKS, and it can't
revoke a token: a JWT stays valid until it expires, so keep lifetimes short. To
rotate keys, put both keys in the mapping, switch your issuer to the new one,
then remove the old. Build a new validator to change keys.

## `ApiKeyValidator`

Token validator for keys of the form `key_id.secret`. Only a SHA-256 hash of the
secret is stored.

**Issuing a key:**

```python
import secrets

key_id = secrets.token_hex(8)
secret = secrets.token_urlsafe(32)
record = ApiKeyRecord.from_secret("billing-service", secret, expires_at=None)
await db.save_api_key(key_id, record)           # store record; never store `secret`
print(f"Your key: {key_id}.{secret}")           # show it once
```

**Validating:**

```python
async def lookup(key_id: str) -> ApiKeyRecord | None:
    return await db.fetch_api_key(key_id)

validator = ApiKeyValidator(lookup)
```

`ApiKeyRecord(principal, secret_digest, expires_at=None, enabled=True)`:

- `expires_at` is a Unix timestamp; expired keys are rejected.
- `enabled=False` rejects the key.
- Secrets must be at least 32 characters.

`lookup` runs on every check, nothing is cached, so revoking or disabling a key
takes effect at the client's next request. Keep `lookup` fast.

## `SessionTokenValidator`

Token validator for opaque session tokens issued by a [session store](sessions.md):

```python
store = SQLiteSessionStore("sessions.sqlite")
validator = SessionTokenValidator(store)

token = await store.create("alice")     # hand this to the client after you've verified it
```

The validator starts and stops the store with the app.

## `PasswordCredentialValidator`

Credential validator for `{"username": ..., "password": ...}` login bodies, using
Argon2 password hashes. Needs the `password` extra.

```python
from argon2 import PasswordHasher

async def lookup(username: str) -> PasswordAccount | None:
    row = await db.fetch_user(username)
    if row is None:
        return None
    return PasswordAccount(principal=row.id, password_hash=row.password_hash, enabled=row.active)

validator = PasswordCredentialValidator(lookup, max_concurrency=4)

# When creating accounts:
password_hash = PasswordHasher().hash(new_password)
```

- Hashing runs in a worker thread, so it doesn't block the event loop.
- At most `max_concurrency` hashes run at once per validator. Beyond that, login
  fails immediately with `AuthUnavailableError` (`503`) instead of queuing. That
  protects your CPU, but it's not rate limiting; limit attempts per account and
  IP at your edge.
- An unknown username is checked against a dummy hash, so it takes about as long
  as a real one.
- Usernames are at most 255 characters and passwords at most 1,024.

## `StaticPasswordCredentialValidator`

A `PasswordCredentialValidator` over a fixed mapping, for small deployments and
tests. It takes password **hashes**, never plaintext:

```python
validator = StaticPasswordCredentialValidator({
    "alice": PasswordAccount("alice", "$argon2id$v=19$m=65536,t=3,p=4$..."),
})
```

## `CallbackCredentialValidator`

Wraps your own login check:

```python
async def check(credentials: dict) -> str | None:
    user = await ldap.authenticate(credentials.get("username"), credentials.get("password"))
    return user.uid if user else None

validator = CallbackCredentialValidator(check)
```

Any exception your callback raises becomes `AuthUnavailableError`. Use
parameterized queries, and don't log the credentials.

## Writing your own

A token validator is any object with:

```python
async def validate(self, token: str) -> str | None: ...
```

A credential validator:

```python
async def validate(self, credentials: dict) -> str | None: ...
```

Return the principal or `None`; raise `AuthUnavailableError` (from
`wire_rpc.auth`) if you can't decide. Exceptions of any other type are also
treated as "unavailable", never as "rejected" or "accepted".
