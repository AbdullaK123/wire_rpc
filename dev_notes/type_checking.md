# Type-checking repair

## Scope and evidence

From code and local checker output: the baseline at main `9b244a2` had 10 mypy
errors and 69 Pyright errors across the package and tests. The final configuration
checks `wire_rpc`, `tests`, and `scripts` with mypy 2.3.1 (including unannotated
function bodies) and Pyright 1.1.414 in standard mode. No files were excluded and
no new diagnostic suppression settings were added.

From docs: Pyright supports shared `[tool.pyright]` settings. Pylance uses the
VS Code selected interpreter rather than Pyright's `venv` setting. See
[Microsoft's configuration reference](https://github.com/microsoft/pyright/blob/main/docs/configuration.md).
Select the project `.venv` in VS Code. These checks validate Pyright diagnostics;
they do not launch the Pylance editor extension or claim strict-mode coverage.

## Deterministic debug trace

Every row below is **From code**, including the installed, locked dependency source.

| Step | Location | Condition/Event | State Change | Side Effect | Notes |
| --- | --- | --- | --- | --- | --- |
| 1 | `request.py`, app dispatch | Request omits `id` | Raw ID is `UNSET`; internal response ID becomes `None` | Response construction satisfies its declared ID union | Notification detection still uses the original ID; notifications still emit no RPC reply. |
| 2 | `_execution.py` | Handler raises or times out | Error is selected from the declared `WireError` union | Safe error encoding | The local variable no longer incorrectly narrows to only `ServerError`. |
| 3 | HTTP receive/send | Notification completes | Owning future resolves with `None` | HTTP 204 and slot release | Futures explicitly carry `bytes | None`; ordinary replies still carry bytes. |
| 4 | HTTP connect | Authentication routes registered | Capture the non-null authenticator | Route closures retain a valid authentication dependency | No assertion or cast is needed for the optional member. |
| 5 | Stdio lifecycle | Pipes/tasks are absent before connect or after cleanup | Optional state is checked; live task sets contain tasks only | Drains safely return when no pipe exists | Task and pipe lifetimes are explicitly annotated. |
| 6 | Payload queue | Bytes or a peer/payload pair is admitted | Generic item type and peer counter retain their types | TCP receive returns its promised pair | Queue capacity and ordering behavior are unchanged. |
| 7 | TCP send | Write raises `TimeoutError` | Existing `OSError` branch closes the connection | Exception is re-raised | Removed later unreachable timeout handlers; cleanup stays in the reachable handler. |
| 8 | WebSocket client connect | Construct aiohttp `ClientWSTimeout` | Runtime `attrs` constructor receives `ws_close` | Existing close deadline is preserved | Installed aiohttp uses legacy `attr.ib(type=...)`; one localized callable cast bridges Pyright's generated-constructor inference gap. |
| 9 | Regression tests | Partial fakes or optional handles enter typed APIs | Complete fail-closed doubles and explicit guards | Unexpected fake operations fail immediately | Listener tests use the public runner address list. No test directory is excluded. |

## Verification

From executed commands on Python 3.12.14/Linux:

- `uv run --locked mypy`: no issues in 81 source files.
- `uv run --locked pyright`: zero errors and zero warnings.
- `uv run --locked pytest -q -W error`: 182 passed.
- Source distribution and wheel build succeeded.
- Installed-wheel import/codec smoke check passed outside the checkout using
  the existing locked dependencies (`--reuse-dependencies`, offline).

CI now runs both type checkers before the existing tests/build/wheel checks
in all nine Python 3.12–3.14 and Linux/macOS/Windows matrix jobs. Local results
above do not stand in for remote matrix results.
