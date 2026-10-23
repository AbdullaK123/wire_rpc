# Handlers

A handler is an `async` function registered under a JSON-RPC method name. This
page covers what a handler can look like, how its params and results are typed,
and what it can learn about the call it's serving.

## Signatures

On `App`, a handler takes one of two shapes:

```python
@app.method("status")
async def status(ctx) -> str:            # no params
    ...

@app.method("add")
async def add(params: AddParams, ctx) -> int:   # with params
    ...
```

On `MulticastApp`, a handler may also ask for the caller's connection id by
naming a last parameter `client_id`:

```python
@app.method("whoami")
async def whoami(ctx, client_id) -> str: ...

@app.method("post")
async def post(params: Message, ctx, client_id) -> None: ...
```

Parameters must be positional; `*args`, `**kwargs` and keyword-only parameters
are rejected with `TypeError` when you register the handler, not when it's
first called.

Registering the same method name twice replaces the earlier handler.

## Typed params

The type hint on the first parameter decides how incoming params are validated
and converted. The codec does the conversion, so what you can use depends on the
codec ([Codecs](codecs.md)):

| Hint | Works with | Example call params |
|---|---|---|
| `msgspec.Struct` subclass | every codec | `{"a": 1, "b": 2}` |
| Pydantic `BaseModel` | `PydanticCodec` | `{"a": 1, "b": 2}` |
| `dict`, `list`, `dict[str, int]`, ... | every codec | `{"a": 1}` |
| Scalars: `int`, `str`, `int \| None`, ... | every codec | `21` |
| No hint | every codec | anything, passed through unchecked |

If params don't match the hint, the handler is never called and the caller gets
`-32602 Invalid params`. The error carries no detail about *why*, so it can't
leak your schema or the caller's input back to them.

**Missing params are validated as `None`.** A call with no `params` member is
checked against the hint exactly as if it sent `null`. So `add(params: AddParams, ...)`
called with no params gets `-32602`, while `params: int | None` receives `None`.

**Scalar params are an extension.** JSON-RPC 2.0 says params are an object or an
array. wire_rpc also accepts a scalar or `null` when your hint allows it. For
interoperability with other JSON-RPC clients, prefer object params.

**A handler without a params parameter ignores any params it's sent.** The call
succeeds; the params are dropped.

### Structs or Pydantic models

`msgspec.Struct` is the fastest option and works with every codec:

```python
import msgspec

class NewNote(msgspec.Struct):
    title: str
    body: str = ""
    tags: list[str] = []
```

Constraints use `msgspec.Meta`:

```python
from typing import Annotated
from msgspec import Meta

class Page(msgspec.Struct):
    limit: Annotated[int, Meta(ge=1, le=100)] = 20
```

With `PydanticCodec`, handlers can use Pydantic models and validators instead.
See [Codecs](codecs.md#pydanticcodec).

## Typed results

The return hint is used to convert the result before encoding. A handler that
returns a value that doesn't match its hint fails with `-32603 Internal error`
rather than sending something the caller didn't expect. Without a return hint,
the result is encoded as-is.

Return anything the codec can encode: Structs, dataclasses, dicts, lists,
strings, numbers, booleans and `None`. A result whose encoding is larger than the
app's `max_response_size` is replaced by a `-32603` error; see [Limits](limits.md).

## `ctx`: your application context

`ctx` is whatever your startup hook returned, the same object for every call.
Use it for long-lived resources like a database pool or an HTTP client:

```python
@app.on_startup
async def startup():
    return AppState(db=await create_pool(DSN))

@app.method("notes.get")
async def get_note(params: NoteId, ctx) -> Note:
    return await ctx.db.fetch_note(params.id)
```

Without a startup hook, `ctx` is `None`. See [Apps and lifecycle](apps-and-lifecycle.md).

## Request metadata: `current_request()`

Inside a handler or middleware, `current_request()` returns a `RequestContext`
with trusted information about the call:

| Field | Meaning |
|---|---|
| `request_id` | The JSON-RPC id, or `None` for a notification |
| `method` | The method name being called |
| `principal` | The identity the transport authenticated, or `None` |
| `connection_id` | The caller's `client_id` on a `MulticastApp`, else `None` |

```python
from wire_rpc import current_request

@app.method("notes.mine")
async def my_notes(ctx) -> list[Note]:
    owner = current_request().principal
    if owner is None:
        raise PermissionError
    return await ctx.db.notes_for(owner)
```

These values come from the transport, never from the caller's params, so a
caller can't claim to be someone else by sending `{"principal": "admin"}`.

`current_request()` works anywhere in the call's task, including functions your
handler calls. Outside a request it raises `RuntimeError`. Background tasks
started from a handler inherit a snapshot of it; don't treat that snapshot as
live authorization for work that happens later.

## Failing on purpose

Raise an exception; the caller gets a sanitized error. The exception's text is
never sent.

| Raise | Caller gets |
|---|---|
| `PermissionError` | `-32001 Access denied` |
| anything else | `-32603 Internal error` |

There's currently no way for a handler to choose its own error code and message.
To return application-level failures the caller should act on, make them part of
the result type:

```python
class TransferResult(msgspec.Struct):
    ok: bool
    reason: str | None = None
```

See [Errors](errors.md) for the full list of codes.

## Timeouts and cancellation

Each call must finish within the app's `handler_timeout` (30 seconds by
default), or it's cancelled and the caller gets `-32000`. For a
[batch](protocol.md#batches), the deadline covers the whole batch.

Cancellation in asyncio is cooperative: it only takes effect when your handler
`await`s. A handler that does blocking work (CPU-heavy loops, synchronous I/O)
can't be interrupted, and on `App` it also holds up every request behind it. Run
that work with `asyncio.to_thread(...)`, in a process pool, or in a separate
worker service.

## Notifications

A call without an `id` is a notification: the handler runs, and nothing is sent
back, not even an error. Write handlers the same way either way; wire_rpc decides
whether to reply. Use notifications for fire-and-forget work like audit events.
