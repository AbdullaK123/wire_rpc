"""Transactional PostgreSQL sessions; injected pool remains caller-owned."""
import hashlib
import json
from typing import Any
from wire_rpc.auth._util import identity
from wire_rpc.auth.errors import SessionCapacityError
from wire_rpc.auth.sessions._base import SessionStoreBase

_SCHEMA = '''CREATE TABLE IF NOT EXISTS wire_rpc_sessions (
 namespace TEXT NOT NULL, digest TEXT NOT NULL, principal TEXT NOT NULL,
 value TEXT NOT NULL, expires DOUBLE PRECISION NOT NULL,
 PRIMARY KEY(namespace, digest))'''


class PostgresSessionStore(SessionStoreBase):
    def __init__(self, pool: Any, ttl: float = 86400, *, namespace: str = 'wire-rpc', max_sessions: int = 10000):
        super().__init__(ttl, max_sessions=max_sessions)
        self._namespace = identity(namespace)
        self._pool = pool
        self._lock_id = int.from_bytes(hashlib.sha256(('wire-rpc:' + namespace).encode()).digest()[:8], 'big', signed=True)

    async def startup(self) -> None:
        async with self._pool.acquire() as db:
            async with db.transaction():
                # Serialize schema setup across namespaces/processes as well.
                await db.execute('SELECT pg_advisory_xact_lock($1)', 812761182)
                await db.execute(_SCHEMA)
                await db.execute('CREATE INDEX IF NOT EXISTS wire_rpc_sessions_expiry ON wire_rpc_sessions(namespace, expires)')
                await db.execute('CREATE INDEX IF NOT EXISTS wire_rpc_sessions_principal ON wire_rpc_sessions(namespace, principal)')

    async def shutdown(self) -> None:
        pass

    async def _execute(self, operation: str, key: str = '', value: str = '', new_key: str = '') -> Any:
        async with self._pool.acquire() as db:
            async with db.transaction(isolation='read_committed'):
                await db.execute('SELECT pg_advisory_xact_lock($1)', self._lock_id)
                now = float(await db.fetchval('SELECT EXTRACT(EPOCH FROM clock_timestamp())'))
                ns = self._namespace
                await db.execute('DELETE FROM wire_rpc_sessions WHERE namespace=$1 AND expires <= $2', ns, now)
                if operation == 'create':
                    count = await db.fetchval('SELECT COUNT(*) FROM wire_rpc_sessions WHERE namespace=$1', ns)
                    if count >= self._max_sessions:
                        raise SessionCapacityError('Session capacity exhausted')
                    await db.execute('INSERT INTO wire_rpc_sessions VALUES ($1,$2,$3,$4,$5)', ns, key, json.loads(value)['principal'], value, now + self._ttl)
                elif operation == 'get':
                    return await db.fetchval('SELECT value FROM wire_rpc_sessions WHERE namespace=$1 AND digest=$2', ns, key)
                elif operation == 'destroy':
                    await db.execute('DELETE FROM wire_rpc_sessions WHERE namespace=$1 AND digest=$2', ns, key)
                elif operation == 'rotate':
                    return await db.fetchval('UPDATE wire_rpc_sessions SET digest=$3 WHERE namespace=$1 AND digest=$2 RETURNING TRUE', ns, key, new_key)
                elif operation == 'revoke':
                    rows = await db.fetch('DELETE FROM wire_rpc_sessions WHERE namespace=$1 AND principal=$2 RETURNING digest', ns, value)
                    return len(rows)
                else:
                    raise ValueError('Unknown session operation')
