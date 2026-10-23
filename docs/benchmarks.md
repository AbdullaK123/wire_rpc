# Benchmarks

Rough numbers for wire_rpc's own overhead. Reproduce them with:

```sh
uv run --locked python scripts/bench.py
```

## How they were measured

- Client and server run in **one Python process on loopback**, so there is no real
  network and both sides share one CPU core (the thread transport uses two
  threads). Real deployments add network latency and usually run the client and
  server on separate cores or machines.
- Each call is `add(a, b)`: a tiny typed request and an integer reply, JSON codec, no
  authentication, no TLS. Larger payloads, Pydantic models, TLS and authentication
  all add cost.
- "One client" is back-to-back calls on one `Client`, which makes one call at a
  time. "Batches of 100" sends JSON-RPC batches over one connection.
- 3,000 calls per measurement after a 200-call warm-up.

These numbers are for comparing transports and catching regressions. They are not
a capacity promise; measure your own workload before sizing a deployment.

## Results (2026-10-23)

Python 3.12.9, Linux x86_64 (WSL2), 12th Gen Intel Core i5-1235U, 12 logical CPUs.

| Transport | Calls/s, one client | p50 ms | p99 ms | Calls/s, batches of 100 |
|---|---:|---:|---:|---:|
| thread | 3,706 | 0.217 | 0.815 | 96,599 |
| tcp | 6,021 | 0.149 | 0.347 | 118,021 |
| tcp-multi | 5,684 | 0.158 | 0.365 | 105,926 |
| ws | 5,628 | 0.151 | 0.624 | 84,879 |
| ws-multi | 4,291 | 0.198 | 0.727 | 70,684 |
| http | 1,957 | 0.469 | 1.215 | 55,750 |
| stdio | 4,469 | 0.188 | 0.624 | — |

| Multicast transport | Calls/s, 8 clients at once |
|---|---:|
| tcp-multi | 5,810 |
| ws-multi | 7,111 |

## What the numbers say

- **Per-call overhead is the limit, not the wire.** One call costs roughly 0.15 to
  0.5 ms here, and a batch of 100 calls costs about as much as a handful of single
  calls. If a client has many small calls to make, batching them is the biggest
  available win: 15 to 30 times the throughput.
- **HTTP is the slowest per call** because each call is a full HTTP request and
  response. Prefer TCP or WebSocket for chatty clients.
- **The thread transport is slower than TCP for single calls.** Every message wakes
  the other thread's event loop, and that cross-thread wake-up costs more than a
  loopback socket write here. Use it for simplicity and isolation, not speed.
- **More clients did not add throughput in this setup,** because client and server
  share one core. On a multi-core deployment with separate client processes,
  `MulticastApp` runs up to `max_concurrency` requests at once.
- `App` handles one request at a time, so a slow handler delays every request behind
  it on that connection. Use `MulticastApp`, or move slow work out of the handler,
  when that matters.
