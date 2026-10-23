# The test suite

The suite is organized by attack surface: each file asks how one part of the
framework can be broken. Test names describe the bug that exists if the test
fails (`test_one_invalid_item_fails_the_whole_batch`), and assertion messages
explain the production consequence.

## Layout

| Directory | Covers |
|---|---|
| `tests/app/` | Request processing: protocol validation, batches, failure isolation, limits, identity, logging, startup and shutdown. |
| `tests/client/` | `Client` ownership: interleaved callers, cancellation, late replies. |
| `tests/codecs/` | Codec conversion, malformed input (a fixed-seed corpus and every truncation of a valid request), transport compatibility. |
| `tests/transports/` | Each transport's framing, admission limits, queue budgets, lifecycle, concurrency and security, including `ThreadTransport`. |
| `tests/auth/` | Authenticators, validators and session stores under attack: JWT claims, credential leaks, session atomicity, capacity, expiry, revocation, cancellation, TLS. |
| `tests/e2e/` | Real clients against real servers, across every valid combination; see below. |

## The end-to-end suite

`tests/e2e/` runs every supported combination end to end:

| File | What it covers |
|---|---|
| `test_transport_codec.py` | Every transport × codec pairing (18): calls, errors, awkward data, large payloads, concurrent callers, notifications, mixed batches, oversized requests, broadcasts, peers reusing ids, peers vanishing mid-call. |
| `test_transport_auth.py` | Every transport × authenticator pairing (27): valid, wrong and missing credentials; rejected clients locking others out; principal spoofing through params. |
| `test_session_backends.py` | Every session store (4) behind cookie login and session tokens: logout, replay, revocation on an open connection, rotation, expiry. |
| `test_refused_combos.py` | Combinations that must fail: binary codecs on text transports, credentials without TLS, mismatched codecs, clients aimed at the wrong kind of server. |
| `test_client_api.py` | `notify`, `batch`, `on_notification`, `receive_notification`, and a scripted server that sends malformed batch replies. |
| `test_metrics.py` | `metrics_middleware` outcomes through a real transport. |

`harness.py` brings up any pairing with one call:

```python
async with serve('tcp-multi', 'msgpack', auth=..., ssl_context=..., setup=register) as server:
    async with Client(server.wire(auth=...), codec=server.codec) as client:
        ...
```

Every server registers the shared methods in `handlers.py` (`add`, `echo`,
`whoami`, `fail`, `note`, `notes`). `stdio_server.py` is the child process for
stdio tests. `make_tls()` builds a throwaway CA with server and client
certificates.

To cover a new transport, add a branch to `serve()` and `Server.wire()` in
`harness.py` and add its name to `KINDS`; the layer-1 tests then run against it
automatically.

## Docker-backed tests

Tests marked `integration` need Docker. The root `conftest.py` starts one
PostgreSQL 17 and one Redis 7 container per test session with Testcontainers
(`postgres_dsn` and `redis_url` fixtures), on random ports. Each test uses its
own namespace, so tests don't interfere. If Docker isn't available, these tests
fail rather than skip, so a missing backend can't hide a broken contract.

## Conventions

- **Deterministic concurrency.** Use events and explicit ordering, not sleeps, to
  set up races. Where a test has to measure wall-clock time, keep generous
  margins. Two tests do this and are the most likely to flake on a slow machine:
  `test_waiting_send_freezes_sender_event_loop` and
  `test_handler_timeout_applies_per_item_instead_of_per_batch`.
- **Timeouts are deadlock guards.** `pytest-timeout` fails any test after 10
  seconds (more where marked), so a hang shows up as a failure, not a stuck CI job.
- **Warnings are errors** (`-W error`). On Python 3.13+ that includes
  `ResourceWarning` for unclosed SQLite connections, which surfaces at garbage
  collection time in whichever test happens to be running. If failures move
  around between runs, look for an unclosed resource.
- **No mocks where real behavior is cheap.** Real sockets, real subprocesses and
  real containers. Doubles in `tests/helpers.py` (`UnusedTransport`,
  `UnusedAuthenticator`) fail loudly if a test touches something it shouldn't.
- **Check that new tests can fail.** After writing a test for a fix, revert the
  fix (or break the code on purpose) and confirm the test fails.
