"""Persistent sessions with cross-connection atomic writes and bounded lock waits."""
import asyncio
import json
import sqlite3
import time
from typing import Any
from wire_rpc.auth.errors import AuthUnavailableError, SessionCapacityError
from wire_rpc.auth.sessions._base import SessionStoreBase
from wire_rpc._validation import positive_timeout, positive_limit


class SQLiteSessionStore(SessionStoreBase):
    def __init__(self, path: str, ttl: float = 86400, *, max_sessions: int = 10000, timeout: float = 5, max_concurrency: int = 4):
        super().__init__(ttl, max_sessions=max_sessions)
        positive_timeout('timeout', timeout)
        positive_limit('max_concurrency', max_concurrency)
        self._limit, self._active = max_concurrency, 0
        if path == ':memory:' or not path:
            raise ValueError('SQLite sessions require a persistent file path')
        self._path, self._timeout = path, timeout
        self._ready = False

    async def startup(self) -> None:
        await asyncio.to_thread(self._initialize)
        self._ready = True

    def _initialize(self) -> None:
        with sqlite3.connect(self._path, timeout=self._timeout) as db:
            db.execute('CREATE TABLE IF NOT EXISTS wire_sessions (digest TEXT PRIMARY KEY, principal TEXT NOT NULL, value TEXT NOT NULL, expires REAL NOT NULL)')
            db.execute('CREATE INDEX IF NOT EXISTS wire_sessions_expiry ON wire_sessions(expires)')
            db.execute('CREATE INDEX IF NOT EXISTS wire_sessions_principal ON wire_sessions(principal)')

    async def shutdown(self) -> None:
        self._ready = False

    async def _execute(self, operation: str, key: str = '', value: str = '', new_key: str = '') -> Any:
        if not self._ready:
            raise AuthUnavailableError('Session store not started')
        # The thread owns its connection and transaction through commit/rollback,
        # even if the caller is cancelled while awaiting the outcome.
        if self._active >= self._limit:
            raise AuthUnavailableError('SQLite session capacity unavailable')
        self._active += 1
        task = asyncio.create_task(asyncio.to_thread(self._transaction, operation, key, value, new_key))
        deferred_release = False
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if not task.done():
                def finished(done: asyncio.Task[Any]) -> None:
                    self._active -= 1
                    if not done.cancelled():
                        done.exception()
                task.add_done_callback(finished)
                deferred_release = True
            raise
        finally:
            if not deferred_release:
                self._active -= 1

    def _transaction(self, operation: str, key: str, value: str, new_key: str) -> Any:
        db = sqlite3.connect(self._path, timeout=self._timeout)
        try:
            db.execute('BEGIN IMMEDIATE')
            now = time.time()
            db.execute('DELETE FROM wire_sessions WHERE expires <= ?', (now,))
            result: Any = None
            if operation == 'create':
                if db.execute('SELECT COUNT(*) FROM wire_sessions').fetchone()[0] >= self._max_sessions:
                    raise SessionCapacityError('Session capacity exhausted')
                db.execute('INSERT INTO wire_sessions VALUES (?, ?, ?, ?)', (key, json.loads(value)['principal'], value, now + self._ttl))
            elif operation == 'get':
                row = db.execute('SELECT value FROM wire_sessions WHERE digest = ?', (key,)).fetchone()
                result = row[0] if row else None
            elif operation == 'destroy':
                db.execute('DELETE FROM wire_sessions WHERE digest = ?', (key,))
            elif operation == 'rotate':
                result = db.execute('UPDATE wire_sessions SET digest = ? WHERE digest = ?', (new_key, key)).rowcount == 1
            elif operation == 'revoke':
                result = db.execute('DELETE FROM wire_sessions WHERE principal = ?', (value,)).rowcount
            else:
                raise ValueError('Unknown session operation')
            db.commit()
            return result
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()
