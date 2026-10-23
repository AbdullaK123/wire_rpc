"""Wire RPC client.

Connects to a Wire RPC server via any transport and makes typed RPC calls.
"""

import asyncio
import uuid
from typing import Any, Self, TypeVar, cast

from wire_rpc._validation import positive_timeout, validate_codec
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
from wire_rpc.codecs.protocol import Codec, CodecError
from wire_rpc.request import RawWireRequest
from wire_rpc.transports.protocol import Transport

T = TypeVar("T")


class WireRpcError(Exception):
    """Raised when the server returns a JSON-RPC error."""

    def __init__(self, code: int, message: str, data: Any = None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"[{code}] {message}")


class Client:
    """Makes JSON-RPC 2.0 calls over a transport, one at a time.

    Any transport or protocol failure during a call closes the client for good; create
    a new one to reconnect. Concurrent ``call``s queue and run in turn.

    Args:
        transport: The connection to the server.
        codec: Must match the server's codec; JSON via msgspec by default.
        call_timeout: Seconds for ``connect()``, and for each ``call`` including time
            spent waiting for an earlier call to finish.
        close_timeout: Seconds allowed to close the transport.

    Usage:
        async with Client(transport) as client:
            total = await client.call("add", int, {"a": 2, "b": 3})
    """

    def __init__(
        self,
        transport: Transport,
        codec: Codec = MsgSpecJsonCodec(),
        *,
        call_timeout: float = 30.0,
        close_timeout: float = 5.0,
    ):
        validate_codec(transport, codec)
        positive_timeout("call_timeout", call_timeout)
        positive_timeout("close_timeout", close_timeout)
        self._call_timeout = call_timeout
        self._close_timeout = close_timeout
        self._call_lock = asyncio.Lock()
        self._closed = False
        self._transport = transport
        self._codec = codec

    async def connect(self):
        """Open the transport.

        Raises:
            ConnectionError: The client was already closed.
        """
        if self._closed:
            raise ConnectionError("Client is closed; create a new client")
        try:
            async with asyncio.timeout(self._call_timeout):
                await self._transport.connect()
        except BaseException:
            await self._abort()
            raise

    async def close(self):
        """Close the transport. The client cannot be reused."""
        self._closed = True
        async with asyncio.timeout(self._close_timeout):
            await self._transport.close()

    async def _abort(self):
        """Close after a failure, keeping the original exception as the one that propagates."""
        self._closed = True
        try:
            await self.close()
        except Exception:
            pass  # Preserve the original failure, but never reuse this channel.

    async def __aenter__(self) -> Self:
        """Connect and return this client."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ):
        """Close the client."""
        await self.close()

    async def call(
        self,
        method: str,
        response_type: type[T],
        params: object | None = None,
    ) -> T:
        # The deadline includes admission waiting. A cancelled waiter does not
        # own the channel and must not close somebody else's active exchange.
        """Call ``method`` and return its result converted to ``response_type``.

        Notifications the server sends while the call is waiting, such as a
        ``MulticastApp.broadcast``, are skipped rather than taken as the reply.

        Raises:
            WireRpcError: The server returned a JSON-RPC error.
            ConnectionError: The client is closed, the connection dropped, or the reply
                could not be decoded or did not match the request. The client is
                closed afterwards.
            TimeoutError: The call did not finish within ``call_timeout``.
        """
        async with asyncio.timeout(self._call_timeout):
            async with self._call_lock:
                if self._closed:
                    raise ConnectionError("Client channel is closed")
                request_id = str(uuid.uuid4())
                request = RawWireRequest(method=method, id=request_id, params=params)
                data = self._codec.encode(request)
                try:
                    await self._transport.send(data)
                    while True:
                        envelope = self._codec.decode(await self._transport.recv(), dict)
                        if not self._is_notification(envelope):
                            break
                    self._validate_response(envelope, request_id)
                except (CodecError, EOFError) as exc:
                    # EOFError covers asyncio.IncompleteReadError: the peer closed mid-frame.
                    await self._abort()
                    raise ConnectionError("RPC channel broken or reply undecodable") from exc
                except BaseException:
                    await self._abort()
                    raise
                if "error" in envelope:
                    error = envelope["error"]
                    raise WireRpcError(error["code"], error["message"], error.get("data"))
                return cast(T, self._codec.convert(envelope["result"], response_type))

    @staticmethod
    def _is_notification(envelope: dict) -> bool:
        """Return True for a server-sent notification: a method and no id."""
        return "method" in envelope and "id" not in envelope

    @staticmethod
    def _validate_response(envelope: dict, request_id: str) -> None:
        """Reject a reply that is not a well-formed JSON-RPC 2.0 response to ``request_id``."""
        if (
            envelope.get("jsonrpc") != "2.0"
            or type(envelope.get("id")) is not str
            or envelope["id"] != request_id
            or (("result" in envelope) == ("error" in envelope))
        ):
            raise ConnectionError("Invalid RPC response envelope")
        if "error" in envelope:
            error = envelope["error"]
            if (not isinstance(error, dict) or type(error.get("code")) is not int
                    or not isinstance(error.get("message"), str)):
                raise ConnectionError("Invalid RPC error envelope")


__all__ = [
    "Client",
    "WireRpcError",
]
