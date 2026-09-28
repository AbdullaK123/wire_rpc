"""Fail-closed doubles for tests that exercise only part of a protocol."""
from typing import Any, Self

from aiohttp import web


class UnusedTransport:
    async def connect(self) -> None:
        raise AssertionError('This test must not open a real transport')

    async def close(self) -> None:
        raise AssertionError('This test must not close an unowned transport')

    async def recv(self) -> bytes:
        raise AssertionError('This test must not receive from an unused transport')

    async def send(self, data: bytes) -> None:
        raise AssertionError('This test must not send on an unused transport')

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


class UnusedAuthenticator:
    async def verify(self, request: Any) -> str | None:
        raise AssertionError('Unexpected authentication in this test')

    async def login(self, request: Any) -> Any:
        raise AssertionError('Unexpected login in this test')

    async def logout(self, request: Any) -> Any:
        raise AssertionError('Unexpected logout in this test')


def response_bytes(payload: bytes | None) -> bytes:
    assert payload is not None, 'a request with an ID must receive a reply or the caller will hang'
    return payload


def listening_port(runner: web.AppRunner | None) -> int:
    assert runner is not None, 'the listener must start before requests can test its security boundaries'
    addresses = runner.addresses
    assert addresses, 'a server without a bound address cannot exercise network failure paths'
    return addresses[0][1]
