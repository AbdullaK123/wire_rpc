# Production readiness execution plan

This work extends the first transport-hardening pass. Each stage adds adversarial tests before or with its implementation and updates this document with evidence.

## Contracts

- Client calls are single-flight per Client instance. A call deadline includes queue waiting. Cancellation/failure after transmission poisons and closes the channel; queued calls cannot reuse it. No automatic retries or exactly-once claim.
- HTTP remains sequential recv/send. Multicast dispatch is bounded and preserves per-connection ordering. Handlers must cooperate with asyncio cancellation; CPU-bound work belongs outside the event loop.
- Request identity is available through an immutable task-local context. Routing identity and authenticated principal are separate. Application code owns resource authorization and transaction/idempotency policies.
- JSON-compatible codecs work over all transports; MessagePack works over binary transports. Unsupported combinations fail at construction. The RPC profile supports individual requests and notifications; batches are explicitly unsupported.
- Resource limits cover payload bytes, queue entries, queued bytes, per-peer queued entries, handler concurrency, request duration, response bytes, and shutdown deadlines. Queue statistics and application counters are public snapshots for monitoring.
- Browser connections validate any presented Origin against an explicit allowlist, authenticate under a deadline, and revalidate authenticated sessions before dispatch. Origin-less service clients remain supported.
- HTTP/WebSocket TLS can terminate at a trusted reverse proxy or use an explicit SSLContext. Proxy limits and application authorization remain deployment responsibilities.

## Stages and acceptance gates

1. Client ownership: wrong-ID/malformed envelopes, interleaved callers, cancelled lock waiters, cancellation after send, and late responses cannot return another call's data.
2. Application isolation: missing/null params validated; exceptions and serialization failures return safe errors without stopping other requests; notifications receive no RPC response; scoped identity cannot bleed across calls; execution limits and deadlines apply.
3. Resource admission: byte-aware queues, per-peer limits and dead-peer eviction; HTTP body/queue/response bounds; bounded client reads; transport capability validation.
4. Lifecycle/security: failed connection cleanup, auth deadlines and browser Origin policy, session revalidation, bounded TCP/WS close, stdio stderr draining and terminate/kill escalation, application stop/drain, startup rollback.
5. Release gates: deterministic regressions, actual socket/subprocess tests, malformed-input matrix, repeat lifecycle tests, locked CI matrix and build checks, deployment documentation and operational statistics.

## Evidence and limitations

Implementation and test results will be recorded below. A passing local suite is not evidence of unexecuted operating-system/CI jobs or workload-specific capacity. No universal production certification is implied.

## Execution result

All five implementation stages are complete. See `production_contracts.md` for exact defaults, compatibility changes, execution traces, and deployment obligations. Local validation: 182 tests pass with warnings as errors; distribution build and installed-wheel smoke check pass. The wheel smoke check reused installed locked dependencies locally; fresh locked dependency installation is delegated to CI. The CI matrix must be checked independently before release.

Contract refinement from implementation: overlapping multicast calls from the same peer are explicitly rejected (-32002), rather than allowing one peer's ordering waiters to consume the execution pool. TCP/WS/HTTP default message capacity is now 1 MiB; defaults are enumerated in the contract document. Session checks happen before dispatch; idle connections are not periodically reauthenticated. Authentication admission is bounded; edge rate limiting remains deployment policy.
