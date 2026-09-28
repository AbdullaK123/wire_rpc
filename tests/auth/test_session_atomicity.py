import asyncio
import pytest
from wire_rpc.auth.errors import AuthUnavailableError, SessionCapacityError
from wire_rpc.auth.sessions import _base


async def test_concurrent_rotations_have_one_winner_and_old_token_never_authorizes(stores):
    first, second = stores
    old = await first.create('alice', {'roles': ['reader']})
    results = await asyncio.gather(first.rotate(old), second.rotate(old))
    winners = [token for token in results if token is not None]
    assert len(winners) == 1, 'a stolen rotating token must not fork into two independently valid sessions'
    assert await first.validate(old) is None, 'rotation must invalidate the consumed token before returning its replacement'
    record = await second.get(winners[0])
    assert record is not None and record.payload == {'roles': ['reader']}, 'atomic rotation must preserve the authenticated metadata without losing it'


async def test_revoke_all_racing_rotation_cannot_leave_a_valid_descendant(stores):
    first, second = stores
    old = await first.create('alice')
    replacement, _ = await asyncio.gather(first.rotate(old), second.revoke_all('alice'))
    assert await first.validate(old) is None, 'account revocation must invalidate the original token across workers'
    assert replacement is None or await second.validate(replacement) is None, 'rotation must not resurrect a revoked session under a new token'


async def test_concurrent_creates_cannot_overrun_canonical_capacity(stores):
    first, second = stores
    await first.create('existing')
    results = await asyncio.gather(first.create('alice'), second.create('bob'), return_exceptions=True)
    assert sum(isinstance(result, str) for result in results) == 1, 'two workers racing for the last slot must not bypass the configured session bound'
    assert sum(isinstance(result, SessionCapacityError) for result in results) == 1, 'capacity contention must surface as a controlled admission failure'


async def test_rotation_collision_rolls_back_without_destroying_either_owner(stores, monkeypatch):
    first, second = stores
    old = await first.create('alice')
    other = await second.create('bob')
    monkeypatch.setattr(_base.secrets, 'token_urlsafe', lambda size: other)
    with pytest.raises(AuthUnavailableError):
        await first.rotate(old)
    assert await first.validate(old) == 'alice', 'a failed rotation must not partially commit deletion of the only valid session'
    assert await second.validate(other) == 'bob', 'token collisions must never overwrite another account identity'


async def test_create_collision_cannot_overwrite_existing_identity(stores, monkeypatch):
    first, second = stores
    old = await first.create('alice')
    monkeypatch.setattr(_base.secrets, 'token_urlsafe', lambda size: old)
    with pytest.raises(AuthUnavailableError):
        await second.create('attacker')
    assert await first.validate(old) == 'alice', 'a token collision must fail closed instead of replacing the owner'


async def test_revocation_is_scoped_to_exact_principal_and_frees_capacity(stores):
    first, second = stores
    alice = await first.create('alice')
    bob = await first.create('alice-other')
    assert await second.revoke_all('alice') == 1, 'account revocation must target exact identities rather than prefixes or patterns'
    assert await first.validate(alice) is None and await first.validate(bob) == 'alice-other', 'revocation must propagate across adapters without logging out an unrelated account'
    new = await second.create('charlie')
    assert await first.validate(new) == 'charlie', 'revoked sessions must release admission capacity immediately'


async def test_destroyed_token_cannot_be_rotated_back_into_existence(stores):
    first, second = stores
    old = await first.create('alice')
    await second.destroy(old)
    assert await first.rotate(old) is None, 'rotation must conditionally consume an existing live record, never upsert from stale state'


async def test_mutating_input_or_returned_metadata_cannot_escalate_persisted_roles(stores):
    first, second = stores
    payload = {'roles': ['reader']}
    token = await first.create('alice', payload)
    payload['roles'].append('admin')
    record = await second.get(token)
    assert record is not None, 'the session must remain readable to test metadata ownership'
    record.payload['roles'].append('owner')
    stored = await first.get(token)
    assert stored is not None and stored.payload == {'roles': ['reader']}, 'session metadata must be copied at persistence and read boundaries to prevent shared-reference privilege changes'


@pytest.mark.parametrize('token', ['', 'x'*10000, "'; DROP TABLE wire_sessions;--", '\x00', 'é'*43])
async def test_malformed_tokens_cannot_trigger_backend_operations(stores, token, monkeypatch):
    first, _ = stores
    async def forbidden(*args):
        raise AssertionError('Malformed bearer tokens must be rejected before hitting shared storage')
    monkeypatch.setattr(first, '_execute', forbidden)
    assert await first.validate(token) is None, 'malformed session identifiers must fail locally instead of consuming backend capacity'
    assert await first.rotate(token) is None, 'malformed tokens must not initiate a transactional rotation'
    await first.destroy(token)
