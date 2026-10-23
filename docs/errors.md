# Errors

This page lists every error a caller can receive, what causes it, and the
exceptions the Python client raises.

## Error codes

Every JSON-RPC error has an integer `code`, a short `message`, and an optional
`data` member. wire_rpc never puts exception text, stack traces, or the caller's
input into `message` or `data`, so errors can't leak secrets or internals.
`data` is always `null` for errors wire_rpc generates.

| Code | Message | When |
|---|---|---|
| -32700 | Invalid encoded request | The bytes aren't valid JSON (or MessagePack). The `id` is `null`, because it couldn't be read. |
| -32600 | Invalid request | The message decoded but isn't a valid JSON-RPC 2.0 request: wrong `jsonrpc` version, missing or over-long `method`, an `id` that isn't a string, integer or null, an empty batch, a batch longer than `max_batch_size`, or a batch whose error replies alone wouldn't fit in `max_response_size`. Nothing in a refused batch runs. |
| -32601 | Method not found | No handler is registered under that name. |
| -32602 | Invalid params | The params don't match the handler's type hint, including missing params the hint doesn't allow. |
| -32603 | Internal error | The handler raised, its result didn't match its return hint, the result couldn't be encoded, or the encoded result was larger than `max_response_size`. |
| -32000 | Request deadline exceeded | The call, or the batch it was in, ran past `handler_timeout`. |
| -32001 | Access denied | The handler or a middleware raised `PermissionError`, or the caller's credentials were revoked or expired after the connection was authenticated. |
| -32002 | Peer already has an active request | On `MulticastApp`, the same connection sent a new request while one was still running. |

-32700 to -32603 are reserved by the JSON-RPC specification. -32000 to -32099 are
for servers to define; wire_rpc uses -32000 to -32002, and your middleware can use
the rest (see [Routers and middleware](routers-and-middleware.md#common-patterns)).

## Notifications never get errors

A notification (a request without an `id`) gets no reply at all, even if it
fails. That's the JSON-RPC contract. If the sender needs to know about failures,
send a call instead.

## Errors outside JSON-RPC

Some failures happen before a JSON-RPC message exists, so they're reported by the
transport instead:

| Transport | Situation | What the client sees |
|---|---|---|
| HTTP | Bad or missing credentials | `401` |
| HTTP | Browser `Origin` not allowed (by default, any origin other than the server's own) | `403` |
| HTTP | Body larger than `max_body_size` | `413` |
| HTTP | Too many requests waiting, or authentication unavailable | `503` |
| HTTP | Request not answered within `request_timeout` | `504` |
| WebSocket | Bad credentials, disallowed origin, no free connection slot, or authentication too slow | The upgrade fails with `401`, `403`, `503` or `504` |
| TCP | Authentication fails | The server closes the connection |
| TCP, WebSocket | A message larger than the limit | The connection is closed |

## Exceptions in the Python client

`Client` methods raise:

| Exception | Meaning | Client still usable? |
|---|---|---|
| `WireRpcError` | The server returned a JSON-RPC error. Has `.code`, `.message` and `.data`. | Yes |
| `ConnectionError` | The connection failed or dropped, the reply couldn't be decoded, or it didn't match the request. | No, it's closed |
| `TimeoutError` | The call didn't finish within `call_timeout`. | No, it's closed |
| `ValueError` | `batch([])`: nothing was sent. | Yes |
| `RuntimeError` | `receive_notification()` on a transport that can't receive pushes (HTTP). | Yes |

Errors raised by the transport while a call is in flight also close the client
and propagate as they are; for example `HttpClientTransport` raises `ValueError`
for a request body over its `max_body_size`, and the TCP and WebSocket clients
raise `TransportError` for an over-size message.

`Client.connect()` can also raise the transport's own errors: an `OSError` such as
`ConnectionRefusedError` when nothing is listening, `ssl.SSLError` for TLS
problems, `PermissionError` when a TCP client authenticator would send credentials without
TLS, or `asyncio.IncompleteReadError` when a TCP server rejects the handshake by
closing the connection.

```python
from wire_rpc.client import WireRpcError

try:
    note = await client.call("notes.get", Note, {"id": note_id})
except WireRpcError as error:
    if error.code == -32001:
        ...   # not allowed
    raise
except (ConnectionError, TimeoutError):
    ...       # the client is closed; see "Failures and reconnecting" in the client docs
```

Why a timeout closes the client: the server might still send the late reply. If
the client stayed open, that reply could be mistaken for the answer to the next
call. Closing is the only safe option. See [The client](client.md#failures-and-reconnecting).

## Errors raised when configuring

Constructors check their arguments and raise `ValueError` straight away for a
non-positive limit or timeout, a response budget under 2 KiB, a binary codec on a
text-only transport, credentials configured over plain HTTP, and similar
mistakes. Handler and middleware signatures that can't work raise `TypeError`
when you register them.
