"""Rough latency and throughput numbers for each transport.

Usage: uv run python scripts/bench.py [--calls N]

Runs client and server in one process on loopback, so the numbers measure
wire_rpc's own overhead, not a network. Use them to compare transports and to
spot regressions; measure your own workload before sizing a deployment.
"""
import argparse
import asyncio
import pathlib
import platform
import statistics
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from tests.e2e.harness import serve  # noqa: E402
from wire_rpc import Client  # noqa: E402

KINDS = ['thread', 'tcp', 'tcp-multi', 'ws', 'ws-multi', 'http', 'stdio']


async def sequential(kind: str, calls: int) -> tuple[float, float, float]:
    """Return (calls per second, p50 ms, p99 ms) for back-to-back calls on one client."""
    async with serve(kind) as server:
        async with Client(server.wire(), call_timeout=30) as client:
            for _ in range(min(200, calls)):  # warm up
                await client.call('add', int, {'a': 1, 'b': 2})
            samples = []
            started = time.perf_counter()
            for i in range(calls):
                begin = time.perf_counter()
                await client.call('add', int, {'a': i, 'b': 1})
                samples.append((time.perf_counter() - begin) * 1000)
            elapsed = time.perf_counter() - started
    samples.sort()
    return calls / elapsed, statistics.median(samples), samples[int(len(samples) * 0.99) - 1]


async def batched(kind: str, calls: int, size: int = 100) -> float:
    """Return calls per second when sent as batches of ``size`` on one connection."""
    async with serve(kind) as server:
        wire = server.wire()
        await wire.connect()
        codec = server.codec
        message = codec.encode([{'jsonrpc': '2.0', 'method': 'add', 'params': {'a': i, 'b': 1}, 'id': i}
                                for i in range(size)])
        rounds = max(1, calls // size)
        try:
            started = time.perf_counter()
            for _ in range(rounds):
                await wire.send(message)
                await wire.recv()
            elapsed = time.perf_counter() - started
        finally:
            await wire.close()
    return rounds * size / elapsed


async def fan_in(kind: str, clients: int, calls: int) -> float:
    """Return aggregate calls per second with ``clients`` connections calling at once."""
    async with serve(kind) as server:
        connected = [Client(server.wire(), call_timeout=30) for _ in range(clients)]
        for client in connected:
            await client.connect()
        per_client = max(1, calls // clients)

        async def run(client):
            for i in range(per_client):
                await client.call('add', int, {'a': i, 'b': 1})
        try:
            started = time.perf_counter()
            await asyncio.gather(*(run(client) for client in connected))
            elapsed = time.perf_counter() - started
        finally:
            for client in connected:
                await client.close()
    return clients * per_client / elapsed


async def main(calls: int) -> None:
    print(f'Python {platform.python_version()} on {platform.system()} {platform.machine()}, '
          f'{calls} calls per measurement, JSON codec, loopback\n')
    print('| Transport | Calls/s, one client | p50 ms | p99 ms | Calls/s, batches of 100 |')
    print('|---|---:|---:|---:|---:|')
    for kind in KINDS:
        rate, p50, p99 = await sequential(kind, calls)
        batch = '' if kind == 'stdio' else f'{await batched(kind, calls):,.0f}'
        print(f'| {kind} | {rate:,.0f} | {p50:.3f} | {p99:.3f} | {batch} |')
    print('\n| Multicast transport | Calls/s, 8 clients at once |')
    print('|---|---:|')
    for kind in ('tcp-multi', 'ws-multi'):
        print(f'| {kind} | {await fan_in(kind, 8, calls):,.0f} |')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--calls', type=int, default=3000)
    asyncio.run(main(parser.parse_args().calls))
