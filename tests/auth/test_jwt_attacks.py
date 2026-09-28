import time
import jwt
import pytest
from wire_rpc.auth.credentials import JwtTokenValidator

KEY = 'a' * 48
OTHER = 'b' * 48


def token(**changes):
    claims = {'sub':'alice', 'iss':'issuer', 'aud':'rpc', 'iat':time.time()-1, 'exp':time.time()+60}
    claims.update(changes)
    return jwt.encode(claims, KEY, algorithm='HS256', headers={'kid':'active'})


@pytest.mark.parametrize('changes', [
    {'sub':''}, {'sub':42}, {'iss':'attacker'}, {'aud':'other-service'},
    {'exp':0}, {'exp':True}, {'exp':'99999999999'}, {'exp':float('inf')},
    {'iat':time.time()+3600}, {'nbf':time.time()+3600}, {'nbf':False},
])
async def test_invalid_claims_cannot_cross_the_rpc_identity_boundary(changes):
    validator = JwtTokenValidator({'active':KEY}, algorithm='HS256', issuer='issuer', audience='rpc')
    assert await validator.validate(token(**changes)) is None, 'a signed token with invalid identity, audience, issuer or time claims must still fail authentication'


@pytest.mark.parametrize('missing', ['exp', 'iat', 'sub', 'aud', 'iss'])
async def test_missing_required_claims_are_not_accepted_by_library_defaults(missing):
    claims = {'sub':'alice','iss':'issuer','aud':'rpc','iat':time.time(),'exp':time.time()+60}
    del claims[missing]
    encoded = jwt.encode(claims, KEY, algorithm='HS256', headers={'kid':'active'})
    validator = JwtTokenValidator({'active':KEY}, algorithm='HS256', issuer='issuer', audience='rpc')
    assert await validator.validate(encoded) is None, 'required claims must be present rather than only validated when supplied'


@pytest.mark.parametrize('headers', [{'kid':'unknown'}, {'kid':['active']}, {'kid':'active','jku':'https://attacker/keys'}, {'kid':'active','crit':['unknown']}])
async def test_attacker_controlled_key_headers_cannot_select_untrusted_verification_material(headers):
    encoded = jwt.encode({'sub':'alice','iss':'issuer','aud':'rpc','iat':time.time(),'exp':time.time()+60}, KEY, algorithm='HS256', headers=headers) if isinstance(headers['kid'], str) else 'eyJraWQiOlsiYWN0aXZlIl0sImFsZyI6IkhTMjU2In0.e30.invalid'
    validator = JwtTokenValidator({'active':KEY}, algorithm='HS256', issuer='issuer', audience='rpc')
    assert await validator.validate(encoded) is None, 'token headers must not cause remote key retrieval or bypass the configured key allowlist'


async def test_unsigned_wrong_algorithm_and_wrong_signature_tokens_never_authorize():
    validator = JwtTokenValidator({'active':KEY}, algorithm='HS256', issuer='issuer', audience='rpc')
    claims = {'sub':'alice','iss':'issuer','aud':'rpc','iat':time.time(),'exp':time.time()+60}
    for algorithm, key in [('none',''), ('HS384',KEY), ('HS256',OTHER)]:
        encoded = jwt.encode(claims, key, algorithm=algorithm, headers={'kid':'active'})
        assert await validator.validate(encoded) is None, 'neither unsigned tokens nor attacker-selected algorithms or signatures may establish identity'
