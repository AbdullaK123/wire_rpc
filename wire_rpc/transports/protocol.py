"""The interfaces every transport implements.

A transport moves whole encoded messages; it never parses JSON-RPC. ``App`` and
``Client`` use a ``Transport`` (one peer), ``MulticastApp`` a ``MulticastTransport``
(many peers, each named by a ``client_id``).

Transports may also provide, and the apps read when present:
    max_message_size: Largest message in bytes; caps the app's response size.
    get_principal(client_id): The authenticated identity for ``RequestContext``.
    requires_text_codec: True if only text codecs can be used.
"""

from __future__ import annotations
from typing import Protocol, runtime_checkable
from urllib.parse import ParseResult
from typing_extensions import Self

@runtime_checkable
class StartupComponent(Protocol):
    """Something the app starts before serving and shuts down afterwards."""

    async def startup(self) -> None:
        """Acquire resources; runs before the app's startup hook."""
    async def shutdown(self) -> None:
        """Release resources; runs after the app's shutdown hook."""


class Transport(Protocol):
    """A connection to a single peer that carries whole messages."""

    async def connect(self): 
        """Open the connection. Called once, before any other method."""
    async def close(self): 
        """Close the connection and wake any pending ``recv``; safe to repeat."""
    async def recv(self) -> bytes: 
        """Wait for the next whole message.

        Raises:
            ConnectionError: The connection is closed.
        """
    async def send(self, data: bytes): 
        """Send one whole message.

        Raises:
            ConnectionError: The connection is closed.
        """
    async def __aenter__(self) -> Self: 
        """Connect and return this transport."""
    async def __aexit__(
        self, 
        exc_type: type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: object
    ) -> None: 
        """Close the transport."""


class MulticastTransport(Protocol):
    """A server-side connection to many peers, each identified by a ``client_id``."""

    async def connect(self):
        """Open the connection. Called once, before any other method."""
    async def close(self):
        """Close the connection and wake any pending ``recv``; safe to repeat."""
    async def recv(self) -> tuple[str, bytes]:
        """Wait for the next message from any peer, returned as ``(client_id, data)``.

        Raises:
            ConnectionError: The transport is closed.
        """
    async def send(self, client_id: str, data: bytes):
        """Send one message to the peer ``client_id``."""
    async def broadcast(self, data: bytes):
        """Send one message to every connected peer."""
    async def __aenter__(self) -> Self: 
        """Connect and return this transport."""
    async def __aexit__(
        self, 
        exc_type: type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: object
    ) -> None: 
        """Close the transport."""
