# Routers and middleware

Routers group related methods under a shared prefix. Middleware runs code
around every handler call, for authorization checks, timing, or anything else
that applies to many methods.

## Routers

```python
from wire_rpc import Router

notes = Router("notes")

@notes.method("create")
async def create(params: NewNote, ctx) -> NoteId: ...

@notes.method("list")
async def list_notes(ctx) -> list[Note]: ...

app.include_router(notes)   # registers "notes.create" and "notes.list"
```

Routers nest. Prefixes join with dots:

```python
admin = Router("admin")
users = Router("users")

@users.method("ban")
async def ban(params: UserId, ctx) -> None: ...

admin.include_router(users)
app.include_router(admin)   # registers "admin.users.ban"
```

Everything flattens into one lookup table when you call `app.include_router`.
Dispatch is a single dictionary lookup however deeply you nest.

**Include routers after you've finished adding methods to them.** The app copies
the router's methods at `include_router` time; methods added to the router
afterwards aren't registered.

### Routers for `MulticastApp`

Pass `multicast=True` so the router accepts handlers that take `client_id`:

```python
chat = Router("chat", multicast=True)

@chat.method("post")
async def post(params: Message, ctx, client_id) -> None: ...
```

## Middleware

Middleware is an `async` function that receives the request, `ctx` and the next
step in the chain, and returns the response:

```python
@app.middleware
async def timing(request, ctx, call_next):
    started = time.perf_counter()
    response = await call_next(request, ctx)
    print(request.method, time.perf_counter() - started)
    return response
```

- `request.method`, `request.params` and `request.id` describe the call. By the
  time middleware runs, `params` has already been validated against the handler's
  type hint.
- `call_next(request, ctx)` runs the rest of the chain and the handler.
- Return the response you got, or a different one.

Middleware must take exactly three positional parameters and be `async`;
anything else is rejected with `TypeError` at registration.

### Order

App middleware runs first, in the order you registered it. Then each router's
middleware, outermost router first. Then the handler:

```
app middleware 1 → app middleware 2 → router "admin" → router "users" → handler
```

Router middleware applies only to that router's methods, including nested
routers' methods.

### What middleware doesn't see

Requests rejected before dispatch never reach middleware:

- bytes that aren't valid JSON (or MessagePack),
- envelopes that aren't valid JSON-RPC,
- calls to unknown methods,
- params that fail validation.

Those are counted in `app.stats` and appear in the access log; see
[Observability](observability.md).

### Common patterns

**Require authentication everywhere:**

```python
from wire_rpc import current_request

@app.middleware
async def require_login(request, ctx, call_next):
    if current_request().principal is None:
        raise PermissionError("login required")
    return await call_next(request, ctx)
```

Raising `PermissionError` gives the caller `-32001 Access denied`.

**Protect only part of the API** by putting the check on a router:

```python
admin = Router("admin")

@admin.middleware
async def admins_only(request, ctx, call_next):
    if current_request().principal not in ctx.admins:
        raise PermissionError
    return await call_next(request, ctx)
```

**Short-circuit with your own error response:**

```python
from wire_rpc.errors import ServerError
from wire_rpc.response import WireErrorResponse

@app.middleware
async def maintenance(request, ctx, call_next):
    if ctx.read_only and request.method.startswith("notes."):
        return WireErrorResponse(
            error=ServerError(message="Read-only maintenance", code=-32050),
            id=request.response_id,
        )
    return await call_next(request, ctx)
```

Server-defined codes must be between -32099 and -32000; wire_rpc itself uses
-32000 to -32002 (see [Errors](errors.md)).

**Metrics:** use the built-in `metrics_middleware`; see
[Observability](observability.md#metrics).

### Authentication is not authorization

Middleware like `require_login` only proves *who* is calling. It doesn't prove
they may touch the specific resource in the params. Check ownership or
permissions in the handler (or in middleware that inspects params) before any
side effect.
