# wire_rpc

Typed asyncio RPC with JSON/Pydantic/MessagePack codecs and TCP, HTTP,
WebSocket, and stdio transports.

## Development and release checks

Requires Python 3.12 or newer and uv.

```sh
uv sync --locked
uv run --locked mypy
uv run --locked pyright
uv run --locked pytest -q -W error
uv build --no-sources
uv run --locked python scripts/check_wheel.py
```

Mypy checks function bodies even when signatures are unannotated. Pyright runs
in standard mode with a Python 3.12 language baseline. Both check the package,
tests, and release scripts, and both run in CI. In VS Code, select the project's
`.venv` interpreter so Pylance resolves the same installed dependencies; it reads
the shared `[tool.pyright]` configuration. Pylance's editor extension itself is
not run by CI.

Tests are organized by attack surface. Concurrency tests use events and
controlled ordering; socket tests use ephemeral ports. Test timeouts are
deadlock guards. The CI matrix covers Python 3.12–3.14 on Linux, macOS, and Windows.

Read [production contracts](docs/production_contracts.md) before deploying,
including codec compatibility, identity propagation, overload behavior,
cancellation semantics, resource budgets, and platform limitations.

The [execution plan](docs/production_readiness_plan.md) records the follow-up
hardening work. The [initial review](docs/transport_codec_hardening_review.md)
records the first transport/codec regression pass.

The [authentication suite](docs/authentication.md) documents authenticators,
credential validators, session backends, client credentials, TLS policy, and
adversarial backend tests. Optional extras are `password`, `jwt`, `redis`, and
`postgres`; core imports do not require them.
