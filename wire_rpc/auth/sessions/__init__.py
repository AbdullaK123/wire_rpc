"""Server-side session storage for cookie login and session tokens.

Choose ``InMemorySessionStore`` for one process, ``SQLiteSessionStore`` for one
machine, and ``RedisSessionStore`` or ``PostgresSessionStore`` to share sessions
across processes. Stores keep only a SHA-256 digest of each session id.
"""

from .protocol import SessionStore, ManagedSessionStore, SessionRecord
from .memory import InMemorySessionStore
from .sqlite import SQLiteSessionStore
from .redis import RedisSessionStore
from .postgres import PostgresSessionStore
from wire_rpc.auth.errors import SessionCapacityError

__all__ = ['SessionStore', 'ManagedSessionStore', 'SessionRecord', 'InMemorySessionStore',
           'SQLiteSessionStore', 'RedisSessionStore', 'PostgresSessionStore', 'SessionCapacityError']
