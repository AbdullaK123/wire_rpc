# wire_rpc

## Development tests

Requires Python 3.12 or newer and uv.

```sh
uv sync --locked
uv run --locked pytest -q -W error
```

Tests are organized by attack surface under `tests/codecs/` and `tests/transports/`. Concurrency cases use events and controlled ordering; loopback tests use ephemeral ports. The test timeout is a deadlock guard.

See [the transport and codec hardening review](docs/transport_codec_hardening_review.md) for reproduced bugs, execution traces, compatibility changes, and remaining work.
