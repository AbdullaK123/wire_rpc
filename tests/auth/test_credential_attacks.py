import asyncio
import threading
import time
from dataclasses import replace
from unittest.mock import Mock

import pytest
from wire_rpc.auth.credentials import (ApiKeyRecord, ApiKeyValidator, PasswordAccount,
    PasswordCredentialValidator, StaticPasswordCredentialValidator, CallbackCredentialValidator)
from wire_rpc.auth.errors import AuthUnavailableError


async def test_unknown_accounts_and_wrong_passwords_both_perform_one_hash_verification():
    from argon2 import PasswordHasher
    hasher = PasswordHasher(time_cost=1, memory_cost=8192, parallelism=1)
    validator = StaticPasswordCredentialValidator({'alice': PasswordAccount('alice', hasher.hash('correct'))})
    verify = Mock(side_effect=hasher.verify)
    validator._hasher = Mock(verify=verify)
    assert await validator.validate({'username': 'missing', 'password': 'wrong'}) is None, 'unknown accounts must never authenticate'
    missing_calls = verify.call_count
    verify.reset_mock()
    assert await validator.validate({'username': 'alice', 'password': 'wrong'}) is None, 'wrong passwords must never authenticate'
    assert missing_calls == verify.call_count == 1, 'missing accounts must not skip expensive password work and expose an obvious account-enumeration timing oracle'


async def test_disabled_account_cannot_authenticate_with_a_correct_password():
    from argon2 import PasswordHasher
    hashed = PasswordHasher(time_cost=1, memory_cost=8192, parallelism=1).hash('correct')
    validator = StaticPasswordCredentialValidator({'alice': PasswordAccount('alice', hashed, enabled=False)})
    assert await validator.validate({'username': 'alice', 'password': 'correct'}) is None, 'disabling an account must override possession of its still-valid password'


async def test_cancelled_hash_work_keeps_admission_reserved_until_native_worker_finishes(monkeypatch):
    validator = StaticPasswordCredentialValidator({}, max_concurrency=1)
    entered, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    workers = []
    async def controlled_thread(*args):
        workers.append(asyncio.current_task())
        entered.set()
        await release.wait()
        return False
    monkeypatch.setattr(asyncio, 'to_thread', controlled_thread)
    call = asyncio.create_task(validator.validate({'username': 'alice', 'password': 'x'}))
    await entered.wait()
    call.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await call
        with pytest.raises(AuthUnavailableError):
            await validator.validate({'username': 'bob', 'password': 'x'})
        assert validator._active == 1, 'cancelling requests must not bypass the bound on still-running native password hashes'
    finally:
        worker = workers[0]
        assert worker is not None, 'verification must run in an owned task so cancellation cannot orphan admission'
        worker.add_done_callback(lambda _: completed.set())
        release.set()
        await completed.wait()
    assert validator._active == 0, 'completed hash work must release its admission slot even after the requester disconnects'


async def test_cancelled_account_lookup_releases_capacity_before_hash_work_exists():
    entered = asyncio.Event()
    async def lookup(username):
        entered.set(); await asyncio.Event().wait()
    validator = PasswordCredentialValidator(lookup, max_concurrency=1)
    call = asyncio.create_task(validator.validate({'username': 'alice', 'password': 'x'}))
    await entered.wait(); call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert validator._active == 0, 'cancellation before starting a password worker must not permanently exhaust login capacity'


@pytest.mark.parametrize('credentials', [[], None, {}, {'username':'x','password':None}, {'username':'x','password':'x'*1025}])
async def test_malformed_password_inputs_do_not_reach_account_lookup(credentials):
    calls = []
    async def lookup(username):
        calls.append(username)
    validator = PasswordCredentialValidator(lookup)
    assert await validator.validate(credentials) is None and not calls, 'malformed or oversized login bodies must be rejected before database queries and hashing'


async def test_api_key_revocation_and_expiry_are_checked_on_every_validation():
    secret = 's' * 40
    records = {'service': ApiKeyRecord.from_secret('service-a', secret)}
    async def lookup(key):
        return records.get(key)
    validator = ApiKeyValidator(lookup)
    token = 'service.' + secret
    records['service'] = replace(records['service'], enabled=False)
    assert await validator.validate(token) is None, 'revoked API keys must not survive through a cached positive identity'
    records['service'] = replace(records['service'], enabled=True, expires_at=time.time()-1)
    assert await validator.validate(token) is None, 'expired API keys must cease authorizing without an application restart'
    assert await validator.validate('other.' + secret) is None, 'a secret must be bound to the requested key identifier'


async def test_callback_backend_failure_and_invalid_identity_do_not_become_success():
    async def fail(credentials):
        raise RuntimeError('VERY_SECRET')
    validator = CallbackCredentialValidator(fail)
    with pytest.raises(AuthUnavailableError) as error:
        await validator.validate({})
    assert 'VERY_SECRET' not in str(error.value), 'credential backend failures must not expose secrets in public exception messages'
    async def empty(credentials):
        return ''
    with pytest.raises(AuthUnavailableError):
        await CallbackCredentialValidator(empty).validate({})
