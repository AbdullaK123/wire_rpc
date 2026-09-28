# wire_rpc

Typed asyncio RPC with JSON/Pydantic/MessagePack codecs and TCP, HTTP,
WebSocket, and stdio transports.

## Development and release checks

Requires Python 3.12 or newer and uv.

```sh
uv sync --locked
uv run --locked pytest -q -W error
uv build --no-sources
uv run --locked python scripts/check_wheel.py
```

Tests are organized by attack surface. Concurrency tests use events and
controlled ordering; socket tests use ephemeral ports. Test timeouts are
deadlock guards. The CI matrix covers Python 3.12–3.14 on Linux, macOS, and Windows.

Read [production contracts](docs/production_contracts.md) before deploying,
including codec compatibility, identity propagation, overload behavior,
cancellation semantics, resource budgets, and platform limitations.

The [execution plan](docs/production_readiness_plan.md) records the follow-up
hardening work. The [initial review](docs/transport_codec_hardening_review.md)
records the first transport/codec regression pass.
