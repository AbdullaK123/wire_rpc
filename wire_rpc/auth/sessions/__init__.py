"""Server-side session storage for cookie login and session tokens.

Choose ``InMemorySessionStore`` for one process, ``SQLiteSessionStore`` for one
machine, and ``RedisSessionStore`` or ``PostgresSessionStore`` to share sessions
across processes. Stores keep only a SHA-256 digest of each session id.
"""

from wire_rpc.auth.errors import SessionCapacityError

from .memory import InMemorySessionStore
from .postgres import PostgresSessionStore
from .protocol import ManagedSessionStore, SessionRecord, SessionStore
from .redis import RedisSessionStore
from .sqlite import SQLiteSessionStore

__all__ = ['SessionStore', 'ManagedSessionStore', 'SessionRecord', 'InMemorySessionStore',
           'SQLiteSessionStore', 'RedisSessionStore', 'PostgresSessionStore', 'SessionCapacityError']
