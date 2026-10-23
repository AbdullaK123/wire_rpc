# Observability

wire_rpc gives you three ways to see what's going on: logs, a metrics hook, and
counters.

## Logging

wire_rpc logs through Python's standard `logging` module, on two loggers:

| Logger | What it records |
|---|---|
| `wire_rpc` | Startup, connections, and problems worth a look: authentication outages, timeouts, rejected connections. |
| `wire_rpc.access` | One INFO line per request. |

### Defaults

- **`app.run()` shows logs** on stderr at INFO, like starting a server with
  Uvicorn:

  ```
  2026-10-23 14:02:11,512 INFO     wire_rpc: TCP multicast server listening on 0.0.0.0:9000
  2026-10-23 14:02:13,040 INFO     wire_rpc.access: notes.create ok 2.4ms peer=6f1c...
  2026-10-23 14:02:13,377 INFO     wire_rpc.access: notes.get -32001 0.6ms peer=6f1c...
  ```

  Choose the level with `app.run(log_level="warning")` (or `"debug"`, `"error"`,
  `"critical"`), or pass `log_level=None` to leave logging entirely to you.
- **If your application already configured logging**, for example with
  `logging.basicConfig(...)` or `logging.config.dictConfig(...)` before calling
  `run()`, `run()` leaves it alone and wire_rpc's records flow into your setup.
- **Importing wire_rpc configures nothing.** With `app.serve()` in your own event
  loop and no logging setup, only warnings and errors appear (through Python's
  fallback handler) until you configure logging.
- Logs never go to stdout, so a stdio server's messages stay intact.

### The access log

```
<method> <outcome> <duration>ms peer=<client_id or ->
```

- `outcome` is `ok`, `notification`, or the JSON-RPC error code the caller
  received (`-32601`, `-32001`, ...). `cancelled` means the request was cut off
  without a reply, for example during shutdown.
- Requests that couldn't be parsed log as `<unparseable> -32700`; invalid
  envelopes as `<invalid> -32600`; batches refused as a whole (empty, too long,
  or too large to answer) as `<batch> -32600`. Watching those is a cheap way to
  spot probing.
- Each call in a batch gets its own line.
- Params, results, exception text and principals are **never** logged.

To turn the access log off but keep everything else:

```python
logging.getLogger("wire_rpc.access").setLevel(logging.WARNING)
```

### Your own logs with request context

Add the request id and principal to your handlers' log lines with a filter:

```python
import logging
from wire_rpc import current_request

class RequestContextFilter(logging.Filter):
    def filter(self, record):
        try:
            request = current_request()
            record.request_id, record.principal = request.request_id, request.principal
        except RuntimeError:          # not inside a request
            record.request_id = record.principal = "-"
        return True

handler = logging.StreamHandler()
handler.addFilter(RequestContextFilter())
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(request_id)s %(principal)s] %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[handler])
```

Be careful what you log: principals can be personal data, and params can hold
secrets.

### Using loguru or structlog

Route standard logging into them in the usual way, for example loguru's
`InterceptHandler` recipe. wire_rpc has no logging dependency of its own.

## Metrics

`metrics_middleware` hands you a record for every executed call:

```python
from wire_rpc.middleware import CallRecord, metrics_middleware

def export(call: CallRecord) -> None:
    ...

app.middleware(metrics_middleware(export))     # register it first
```

`CallRecord` fields:

| Field | Meaning |
|---|---|
| `method` | The method name |
| `duration` | Seconds spent in middleware and the handler |
| `outcome` | `ok`, `error` (a middleware returned an error response), `exception` (the handler or a middleware raised), or `cancelled` (cut off, for example by `handler_timeout`) |
| `error_code` | The JSON-RPC code for an `error` outcome, else `None` |
| `principal` | The authenticated caller, if any |

Register it before other middleware so `duration` covers them. Your `export`
function runs inline on every call, so keep it fast and non-blocking. If it
raises, the error is logged and the call is unaffected.

Requests rejected before dispatch (unparseable, invalid, unknown method, invalid
params) never reach middleware. Count those with the access log or
`app.stats`.

### Prometheus

```python
from prometheus_client import Histogram

LATENCY = Histogram("rpc_duration_seconds", "RPC handler duration", ["method", "outcome"])

def export(call: CallRecord) -> None:
    LATENCY.labels(call.method, call.outcome).observe(call.duration)

app.middleware(metrics_middleware(export))
```

Method names come from your own registered handlers, so the label set is
bounded. Don't use principals as labels; there's one series per user.

### OpenTelemetry

```python
from opentelemetry import metrics

meter = metrics.get_meter("notes-rpc")
duration = meter.create_histogram("rpc.server.duration", unit="s")

def export(call: CallRecord) -> None:
    duration.record(call.duration, {"rpc.method": call.method, "outcome": call.outcome})
```

For tracing, write a small middleware that opens a span around `call_next`.

## Counters

`app.stats` is a snapshot dict:

| Key | Counts |
|---|---|
| `requests` | Messages received; a batch counts once |
| `active` | Calls running right now |
| `completed` | Calls that produced an encoded reply, including error replies such as Method not found |
| `failed` | Calls whose handler raised, timed out, or whose result couldn't be encoded |
| `timed_out` | Calls that hit `handler_timeout` (also counted in `failed`) |
| `invalid` | Messages that couldn't be parsed, invalid requests (including invalid items in a batch), and refused batches |

Server transports have their own `stats`:

| Transport | Keys |
|---|---|
| `TcpMulticastServerTransport`, WebSocket servers | `messages`, `bytes`, `peers` (queued), `connections` |
| `HttpServerTransport` | `pending`, `queued_bytes` |
| `StdIoTransport` | `stderr_bytes_discarded` |

Poll them on a timer and export them as gauges. Watch for queues that stay full
(clients sending faster than you serve), `active` stuck at `max_concurrency`, and
a rising `timed_out`.

## What to alert on

- A rising rate of `-32603` (handler bugs) or `-32000` (timeouts) in the access log or metrics.
- `-32001` spikes: credentials expiring or being revoked en masse, or someone probing.
- `-32700`/`-32600` spikes: misbehaving clients or scanning.
- Authentication unavailable warnings in the `wire_rpc` log: a database or
  session store behind your authenticator is down.
- Transport queues near their limits for long periods.
