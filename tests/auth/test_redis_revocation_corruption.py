import pytest
from wire_rpc.auth.errors import AuthUnavailableError


pytestmark = [pytest.mark.integration, pytest.mark.timeout(180)]


@pytest.mark.parametrize('stores', ['redis'], indirect=True)
async def test_corrupt_record_cannot_partially_commit_a_failed_account_revocation(stores):
    first, second = stores
    valid = await first.create('alice')
    corrupt = await first.create('alice')
    key = first._digest(corrupt)
    original = await first._client.hget(first._keys[0], key)
    await first._client.hset(first._keys[0], key, '{broken-json')
    with pytest.raises(AuthUnavailableError):
        await second.revoke_all('alice')
    assert await first.validate(valid) == 'alice', 'a Redis script error must not leave a partially revoked account while reporting the entire operation failed'
    await first._client.hset(first._keys[0], key, original)
    assert await second.revoke_all('alice') == 2, 'repairing corrupt metadata must permit a complete retry without previously lost session records'
    assert await first.validate(valid) is None, 'successful recovery must finally invalidate the credential on every adapter'
