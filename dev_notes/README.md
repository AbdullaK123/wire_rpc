# Developer notes

Material for people working on wire_rpc itself. User documentation lives in
[`docs/`](../docs/README.md).

## Working on the code

- [Development setup and checks](development.md): installing, type checking,
  linting, running the tests, building.
- [The test suite](testing.md): how the tests are organized, the end-to-end
  harness, Docker-backed tests, and writing new tests.
- [Releasing](releasing.md): versioning, the changelog, and publishing to PyPI.

## Design and hardening records

Historical documents from the hardening passes. They record what was decided
and why, and the evidence gathered at the time. Some details have since
changed; the code, the tests and `docs/` are authoritative.

| Document | What it records |
|---|---|
| [transport_codec_hardening_review.md](transport_codec_hardening_review.md) | The first adversarial pass over transports and codecs. |
| [wire_rpc_tcp_production_plan.md](wire_rpc_tcp_production_plan.md) | The TCP transport hardening plan. |
| [http_ws_transport_hardening.md](http_ws_transport_hardening.md) | The HTTP and WebSocket transport hardening roadmap. |
| [production_readiness_plan.md](production_readiness_plan.md) | The staged production-readiness plan and its acceptance gates. |
| [production_contracts.md](production_contracts.md) | The production contracts as implemented, with validation evidence and execution traces. Its user-facing content is now spread across `docs/` (limits, protocol, client, deployment). |
| [authentication_suite.md](authentication_suite.md) | The authentication suite's design, including security regression traces. Its user-facing content is now in `docs/authentication/`. |
| [type_checking.md](type_checking.md) | The original mypy/pyright repair. The project has since moved to pyrefly. |
