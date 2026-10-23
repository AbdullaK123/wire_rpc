# TCP authentication

TCP servers authenticate each connection right after it opens (and after the TLS
handshake), before reading any RPC message. The client runs the matching
handshake as part of `connect()`.

```python
from wire_rpc.auth import (
    ClientCertificateAuth,
    HmacChallengeAuth,
    HmacChallengeClientAuth,
    TcpTokenAuth,
    TcpTokenClientAuth,
)
```

Token and HMAC authentication require TLS on both ends by default. The server
refuses a plaintext connection before looking at its credentials, and the client
refuses to send them. Pass `require_tls=False` on both ends only for local tests.

## Token handshake

The client sends a token; the server checks it with any validator.

```python
from wire_rpc.auth.credentials import ApiKeyValidator

server = TcpMulticastServerTransport(
    host="0.0.0.0", port=9443, ssl_context=server_tls,
    auth=TcpTokenAuth(ApiKeyValidator(lookup)),
)
client = TcpClientTransport(
    host="rpc.internal", port=9443, ssl_context=client_tls,
    auth=TcpTokenClientAuth(f"{key_id}.{secret}"),
)
```

Any [validator](credentials.md) that checks tokens works: `ApiKeyValidator`,
`JwtTokenValidator`, `SessionTokenValidator`, or your own. The server keeps the
token and validates it again before every request on the connection, so revoking
it cuts off an open connection.

Options on both sides: `require_tls=True`, `timeout=5` (seconds for the
handshake).

## HMAC challenge

Both sides hold a shared secret, and the secret itself never crosses the wire.
The server sends a random challenge and the client proves it knows the secret by
signing it.

```python
import secrets

secret = secrets.token_bytes(32)          # share this with the client securely

server = TcpMulticastServerTransport(
    host="0.0.0.0", port=9443, ssl_context=server_tls,
    auth=HmacChallengeAuth({
        "billing-2026": ("billing-service", secret),     # key id -> (principal, secret)
    }),
)
client = TcpClientTransport(
    host="rpc.internal", port=9443, ssl_context=client_tls,
    auth=HmacChallengeClientAuth("billing-2026", secret),
)
```

- Secrets must be at least 32 bytes of random data.
- Each client has its own key id, so you can revoke one client without
  affecting the others. Before each request, the server checks the key is still
  configured with the same secret.
- Unknown key ids go through the same steps as known ones, so an attacker can't
  find valid ids by timing the server.
- To rotate a secret, add a new key id, move the client to it, then remove the old
  one.

## Client certificates

With mutual TLS, the TLS handshake proves the client's identity and
`ClientCertificateAuth` maps the certificate to a principal:

```python
server_tls.verify_mode = ssl.CERT_REQUIRED
server_tls.load_verify_locations("clients-ca.pem")

server = TcpMulticastServerTransport(
    host="0.0.0.0", port=9443, ssl_context=server_tls,
    auth=ClientCertificateAuth({"3f5c...e9a1": "billing-service"}),
)
client_tls.load_cert_chain("billing.pem", "billing.key")
client = TcpClientTransport(host="rpc.internal", port=9443, ssl_context=client_tls)
```

The client needs no `auth=`. See
[HTTP and WebSocket: client certificates](http-and-websocket.md#client-certificates)
for computing fingerprints; the rules are the same.

## Legacy: `SharedSecretAuth`

`wire_rpc.auth.shared_secret.SharedSecretAuth` sends the secret itself, gives
every client the same principal, and doesn't enforce TLS. It exists for
compatibility; use HMAC instead. If you must use it, use TLS.

## Failure behavior

- A rejected client's connection is closed, and the client's `connect()` raises:
  `PermissionError` when the server answers with something other than `OK`, or
  `asyncio.IncompleteReadError` / `ConnectionError` when it simply closes the
  connection, which is what the built-in authenticators do.
- Authentication must finish within the server's `auth_timeout` (10 s) and the
  authenticator's own `timeout` (5 s).
- Rejected or slow clients don't block others: on a multicast server each
  connection authenticates independently, and on `TcpServerTransport` a failed
  attempt frees the slot for the next client.

## Wire format

For clients written in other languages. Every handshake message uses the same
framing as RPC messages: a 4-byte big-endian length, then the bytes.

**Token** (`WRPC-TOKEN-1`):

1. Client sends `b"WRPC-TOKEN-1 " + token` (ASCII; the token is 1 to 16,000
   printable characters).
2. Server replies `b"OK"`, or closes the connection.

**HMAC** (`WRPC-HMAC-1`):

1. Client sends the key id (UTF-8, at most 255 bytes).
2. Server sends a 44-byte challenge: `b"WRPC-HMAC-1\x00"` followed by 32 random bytes.
3. Client sends `HMAC-SHA256(secret, challenge + b"\x00" + key_id)`, 32 bytes.
4. Server replies `b"OK"`, or closes the connection.

After `OK`, the connection carries normal RPC messages.
