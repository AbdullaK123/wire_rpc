# Protocol support

wire_rpc implements the [JSON-RPC 2.0 specification](https://www.jsonrpc.org/specification).
This page lists exactly how it reads and writes messages, including the places
where it is stricter or more permissive than the spec.

## Requests

```json
{"jsonrpc": "2.0", "method": "notes.get", "params": {"id": 7}, "id": 1}
```

| Member | Rule |
|---|---|
| `jsonrpc` | Must be exactly `"2.0"`. |
| `method` | A string of 1 to 255 characters. |
| `params` | Optional. Checked against the handler's type hint; see below. |
| `id` | Optional. A string of up to 255 characters, a signed 64-bit integer, or `null`. Floats and booleans are rejected. |

Anything else gets `-32600 Invalid request`. Unknown extra members are ignored.

### Params

The spec allows an object or an array. wire_rpc passes whatever arrives to the
handler's type hint, so it also accepts scalars and `null` when the hint does (a
handler declared with `params: int` accepts `21`). A missing `params` member is
treated as `null`. Clients that need to interoperate with other servers should
send objects.

### Notifications

A request **without** an `id` member is a notification. It runs, and nothing is
sent back, even on failure.

A request with `"id": null` is *not* a notification. The spec discourages it, but
it's a normal call whose response has `"id": null`.

## Responses

```json
{"result": 5, "id": 1, "jsonrpc": "2.0"}
{"error": {"message": "Method not found", "data": null, "code": -32601}, "id": 1, "jsonrpc": "2.0"}
```

A response has exactly one of `result` or `error`, and the request's `id`. If the
`id` couldn't be read (a parse error or an invalid request), it's `null`.
Members may appear in any order; don't depend on it. Error codes are listed in
[Errors](errors.md).

## Batches

A batch is an array of requests sent as one message. wire_rpc supports them on
every transport and codec.

```json
[
  {"jsonrpc": "2.0", "method": "add", "params": {"a": 1, "b": 2}, "id": 1},
  {"jsonrpc": "2.0", "method": "audit.log", "params": {"event": "x"}},
  {"jsonrpc": "2.0", "params": {"a": 3}, "id": 2},
  {"jsonrpc": "2.0", "method": "nope", "id": 3}
]
```

```json
[
  {"result": 3, "id": 1, "jsonrpc": "2.0"},
  {"error": {"message": "Invalid request", "data": null, "code": -32600}, "id": null, "jsonrpc": "2.0"},
  {"error": {"message": "Method not found", "data": null, "code": -32601}, "id": 3, "jsonrpc": "2.0"}
]
```

- **Each item is handled on its own.** An invalid item gets its own error, and
  the other items still run. Items that aren't objects, including nested arrays,
  are invalid items.
- **Items run in order,** one after another.
- **Notifications get no entry** in the reply.
- **A batch of only notifications gets no reply at all.** Over HTTP, that's a
  `204 No Content`.
- **An empty array gets a single error object**, not an array: `-32600`.
- **Too many items:** a batch longer than the app's `max_batch_size` (100 by
  default) is refused with a single `-32600` error object, and nothing runs.
- **One deadline:** the whole batch shares one `handler_timeout`. Items still
  running when it expires, and items that hadn't started, get `-32000`.
- **The reply always fits.** The batch reply must fit in `max_response_size`. A
  reply that would push it over the limit is replaced by a small `-32603` error
  for that item, so every call still gets an answer. Room for those fallback
  errors is reserved up front. If even the errors alone couldn't fit, for example
  100 calls with very long ids under a small budget, the batch is refused before
  anything runs.

Why the last rule matters: once a call has run, the caller must hear about it.
Silently dropping a reply for a call that changed data invites a retry that runs
it twice.

## What's not supported

- **Server-to-client requests.** Servers can send notifications to clients
  (`broadcast`), but not calls that expect a reply.
- **Custom error codes from handlers.** Handlers can't choose a code; middleware
  can return an error response with a code from -32099 to -32003. See
  [Routers and middleware](routers-and-middleware.md#common-patterns).
- **Positional (array) params mapped to arguments.** An array is passed to the
  handler as one value checked against its hint, such as `list[int]` or
  `tuple[int, str]`.

## Framing per transport

JSON-RPC doesn't define how messages are separated on the wire. wire_rpc uses:

| Transport | One message is |
|---|---|
| TCP, stdio | A 4-byte big-endian length, then that many bytes |
| WebSocket | One text frame (binary frames are accepted from clients) |
| HTTP | The body of one `POST /rpc` request, or of its response |
| Thread | One Python `bytes` object |

So a non-Python TCP client must send `len(payload).to_bytes(4, "big") + payload`.
