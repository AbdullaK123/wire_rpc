# Session stores

A session store keeps server-side sessions: `CookieSessionAuth` uses one for
browser logins, and `SessionTokenValidator` uses one for opaque bearer or TCP
tokens.

```python
from wire_rpc.auth.sessions import (
    InMemorySessionStore,
    PostgresSessionStore,
    RedisSessionStore,
    SQLiteSessionStore,
)
```

## Choosing

| Store | Shared between | Survives restart | Needs |
|---|---|---|---|
| `InMemorySessionStore` | One process | No | Nothing |
| `SQLiteSessionStore` | Processes on one machine | Yes | A file path |
| `RedisSessionStore` | Any number of processes and machines | Depends on Redis persistence | The `redis` extra and a Redis server |
| `PostgresSessionStore` | Any number of processes and machines | Yes | The `postgres` extra and PostgreSQL |

If you run more than one server process, the store must be shared, or a user
logged in on one process won't be on another.

## Operations

```python
token = await store.create("alice", {"role": "editor"})   # new session, returns its id
principal = await store.validate(token)                   # "alice", or None if unknown/expired
record = await store.get(token)                           # SessionRecord(principal, payload), or None
new_token = await store.rotate(token)                     # new id, same session; old id stops working
await store.destroy(token)                                # end one session
count = await store.revoke_all("alice")                   # end all of alice's sessions
```

- **Session ids** are 32 random bytes (43 URL-safe characters). Only a SHA-256
  hash of each id is stored, so a leaked database doesn't hand out working
  sessions.
- **Payload** is an optional JSON object stored with the session, up to 16 KiB
  together with the principal.
- **Expiry** is absolute: `ttl` seconds after `create` (24 hours by default).
  Using a session doesn't extend it.
- **Rotation** gives the session a new id and keeps its original expiry, so
  rotating can't keep a stolen session alive forever. Rotate after a privilege
  change, such as a login or password change. If two callers rotate the same
  session at once, exactly one succeeds.
- **Capacity**: at most `max_sessions` live sessions (10,000) per store or
  namespace. Creating more raises `SessionCapacityError`, which a login reports
  as `503`. Expired sessions don't count.
- **Destroy** is safe to repeat.
- **Failures** raise `AuthUnavailableError`, and a damaged record is treated as an
  error, never as a valid session.

## `InMemorySessionStore`

```python
InMemorySessionStore(ttl=86400, max_sessions=10000)
```

Lives in one process and is lost on restart. Good for development, tests, and
single-process deployments where users can log in again after a restart.

## `SQLiteSessionStore`

```python
SQLiteSessionStore("/var/lib/notes/sessions.sqlite", ttl=86400, max_sessions=10000,
                   timeout=5, max_concurrency=4)
```

- Creates its table when the app starts.
- Several processes can share the same file; SQLite's locking keeps operations
  atomic.
- Each operation runs in a worker thread with its own transaction. At most
  `max_concurrency` run at once per store; beyond that, operations fail
  immediately with `AuthUnavailableError`.
- `timeout` is how long to wait for another process's lock.
- `:memory:` is rejected; use `InMemorySessionStore` instead.

## `RedisSessionStore`

```python
from redis.asyncio import Redis

redis = Redis.from_url("redis://cache.internal:6379/0", socket_timeout=5)
store = RedisSessionStore(redis, ttl=86400, namespace="notes", max_sessions=10000)
```

- You own the Redis client: create it, configure its timeouts, and close it.
- Every operation is one Lua script, so it's atomic.
- All of a namespace's keys share one hash slot, so it works on Redis Cluster.
- Use a separate `namespace` per application. Every process using a namespace
  must use the same `ttl` and `max_sessions`.
- Configure Redis persistence and eviction for data you can't afford to lose. If
  Redis evicts or loses keys, those sessions end; they never become valid by
  accident.

## `PostgresSessionStore`

```python
import asyncpg

pool = await asyncpg.create_pool(DSN, command_timeout=5)
store = PostgresSessionStore(pool, ttl=86400, namespace="notes", max_sessions=10000)
```

- You own the pool.
- Creates the `wire_rpc_sessions` table and its indexes when the app starts, so
  the database role needs permission to create them, or create them in advance.
- Operations in one namespace are serialized with an advisory lock, so the
  capacity limit holds across all your processes.

## Performance

These stores put correctness first. Redis cleanup and `revoke_all` take time
proportional to the number of sessions in the namespace; PostgreSQL serializes
operations per namespace; SQLite serializes writes to its file. All are fine for
typical login traffic. Benchmark your own login and rotation load before raising
`max_sessions` far above the default.

## Unknown outcomes

If a store operation is cancelled or the network drops mid-commit, it may or may
not have happened. A cancelled SQLite operation, for example, still finishes in
its worker thread. Never treat cancellation as proof that a session wasn't
created or revoked; check again.
