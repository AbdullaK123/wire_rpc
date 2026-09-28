"""Atomic bounded Redis session namespace. Caller owns the Redis client."""
from typing import Any
from wire_rpc.auth.errors import AuthUnavailableError, SessionCapacityError
from wire_rpc.auth.sessions._base import SessionStoreBase
from wire_rpc.auth._util import identity

# All keys use a single cluster hash tag. No network round trip splits a mutation.
_SCRIPT = '''
local op, key, value, newkey = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2])/1000000
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)
for _, k in ipairs(expired) do redis.call('HDEL', KEYS[1], k) end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if op == 'create' then
    if redis.call('HEXISTS', KEYS[1], key) == 1 then return -2 end
    if redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[6]) then return -1 end
    redis.call('HSET', KEYS[1], key, value)
    redis.call('ZADD', KEYS[2], now + tonumber(ARGV[5]), key)
    return 1
elseif op == 'get' then
    if not redis.call('ZSCORE', KEYS[2], key) then return false end
    return redis.call('HGET', KEYS[1], key)
elseif op == 'destroy' then
    redis.call('ZREM', KEYS[2], key)
    return redis.call('HDEL', KEYS[1], key)
elseif op == 'rotate' then
    local data = redis.call('HGET', KEYS[1], key)
    if not data then return 0 end
    if redis.call('HEXISTS', KEYS[1], newkey) == 1 then return -2 end
    local expiry = redis.call('ZSCORE', KEYS[2], key)
    if not expiry then return -2 end
    redis.call('HSET', KEYS[1], newkey, data)
    redis.call('ZADD', KEYS[2], expiry, newkey)
    redis.call('HDEL', KEYS[1], key)
    redis.call('ZREM', KEYS[2], key)
    return 1
elseif op == 'revoke' then
    local entries = redis.call('HGETALL', KEYS[1])
    local remove = {}
    -- Decode before mutating: Redis script errors do not roll back earlier writes.
    for i=1,#entries,2 do
        if cjson.decode(entries[i+1]).principal == value then table.insert(remove, entries[i]) end
    end
    for _, k in ipairs(remove) do
        redis.call('HDEL', KEYS[1], k)
        redis.call('ZREM', KEYS[2], k)
    end
    return #remove
end
return redis.error_reply('Unknown operation')
'''


class RedisSessionStore(SessionStoreBase):
    def __init__(self, client: Any, ttl: float = 86400, *, namespace: str = 'wire-rpc', max_sessions: int = 10000):
        super().__init__(ttl, max_sessions=max_sessions)
        identity(namespace)
        if '{' in namespace or '}' in namespace:
            raise ValueError('Namespace cannot contain cluster hash tags')
        self._client = client
        self._keys = (f'{{{namespace}}}:sessions', f'{{{namespace}}}:expiry')

    async def _execute(self, operation: str, key: str = '', value: str = '', new_key: str = '') -> Any:
        result = await self._client.eval(_SCRIPT, 2, *self._keys, operation, key, value, new_key, self._ttl, self._max_sessions)
        if result == -1:
            raise SessionCapacityError('Session capacity exhausted')
        if result == -2:
            raise AuthUnavailableError('Session collision or inconsistent record')
        return result
