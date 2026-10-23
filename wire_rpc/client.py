"""Wire RPC client.

Connects to a Wire RPC server via any transport and makes typed RPC calls.
"""

import asyncio
import inspect
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Self, TypeVar, cast

from wire_rpc._validation import positive_timeout, validate_codec
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
from wire_rpc.codecs.protocol import Codec, CodecError
from wire_rpc.logger import logger
from wire_rpc.request import RawWireRequest
from wire_rpc.transports.protocol import Transport

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Notification:
    """A notification the server sent, such as a ``MulticastApp.broadcast``."""

    method: str
    params: Any = None


@dataclass(frozen=True, slots=True)
class BatchCall:
    """One call inside ``Client.batch``; its result is converted to ``response_type``."""

    method: str
    response_type: Any = Any
    params: object | None = None


@dataclass(frozen=True, slots=True)
class BatchNotification:
    """One notification inside ``Client.batch``; it gets no result."""

    method: str
    params: object | None = None


type NotificationHandler = Callable[[Notification], Awaitable[None] | None]


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
    a new one to reconnect. Concurrent ``call``s queue and run in turn; to run calls in
    parallel, use several clients, and to send many small calls, use ``batch``.

    Args:
        transport: The connection to the server.
        codec: Must match the server's codec; JSON via msgspec by default.
        call_timeout: Seconds for ``connect()``, and for each ``call`` including time
            spent waiting for an earlier call to finish.
        close_timeout: Seconds allowed to close the transport.
        on_notification: Called with each ``Notification`` the server sends while
            this client is waiting for a reply. It may be sync or async; an exception
            it raises is logged and does not fail the call.

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
        on_notification: NotificationHandler | None = None,
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
        self._on_notification = on_notification

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
        """Call ``method`` and return its result converted to ``response_type``.

        Notifications the server sends while the call is waiting, such as a
        ``MulticastApp.broadcast``, go to ``on_notification`` (or are dropped if it
        is not set) rather than being taken as the reply.

        Raises:
            WireRpcError: The server returned a JSON-RPC error.
            ConnectionError: The client is closed, the connection dropped, or the reply
                could not be decoded or did not match the request. The client is
                closed afterwards.
            TimeoutError: The call did not finish within ``call_timeout``.
        """
        request_id = str(uuid.uuid4())
        data = self._codec.encode(RawWireRequest(method=method, id=request_id, params=params))
        envelope = await self._locked_exchange(data, lambda message: self._response(message, request_id))
        if "error" in envelope:
            error = envelope["error"]
            raise WireRpcError(error["code"], error["message"], error.get("data"))
        return cast(T, self._codec.convert(envelope["result"], response_type))

    async def notify(self, method: str, params: object | None = None) -> None:
        """Send a notification: the server runs ``method`` and sends nothing back.

        Raises:
            ConnectionError: The client is closed or the connection dropped. The
                client is closed afterwards.
            TimeoutError: Sending did not finish within ``call_timeout``.
        """
        await self._locked_exchange(self._codec.encode(RawWireRequest(method=method, params=params)), None)

    async def batch(self, items: Sequence[BatchCall | BatchNotification]) -> list[Any]:
        """Send several calls and notifications as one JSON-RPC batch.

        The server runs them in order under one shared deadline. Returns one entry
        per item, in the same order: the converted result for a ``BatchCall`` that
        succeeded, a ``WireRpcError`` (returned, not raised) for one that failed,
        and ``None`` for a ``BatchNotification``.

        Raises:
            ValueError: ``items`` is empty.
            WireRpcError: The server refused the whole batch, for example because
                it is longer than the server's ``max_batch_size``. Nothing ran.
            ConnectionError: The client is closed, the connection dropped, or the
                reply did not match the batch. The client is closed afterwards.
            TimeoutError: The batch did not finish within ``call_timeout``.
        """
        if not items:
            raise ValueError("A batch needs at least one item")
        ids: list[str | None] = []
        requests = []
        for item in items:
            request_id = str(uuid.uuid4()) if isinstance(item, BatchCall) else None
            ids.append(request_id)
            requests.append(RawWireRequest(method=item.method, id=request_id, params=item.params)
                            if request_id is not None else RawWireRequest(method=item.method, params=item.params))
        expected = {request_id for request_id in ids if request_id is not None}
        data = self._codec.encode(requests)
        if not expected:
            await self._locked_exchange(data, None)
            return [None] * len(items)
        reply = await self._locked_exchange(data, lambda message: self._batch_response(message, expected))
        if isinstance(reply, dict):
            error = reply["error"]
            raise WireRpcError(error["code"], error["message"], error.get("data"))
        by_id = {envelope["id"]: envelope for envelope in reply}
        results: list[Any] = []
        for item, request_id in zip(items, ids):
            if request_id is None:
                results.append(None)
                continue
            envelope = by_id[request_id]
            if "error" in envelope:
                error = envelope["error"]
                results.append(WireRpcError(error["code"], error["message"], error.get("data")))
            else:
                results.append(self._codec.convert(envelope["result"], cast(BatchCall, item).response_type))
        return results

    async def receive_notification(self) -> Notification:
        """Wait for the next notification the server sends, with no deadline.

        Meant for a client used only to listen for broadcasts: it holds this
        client's channel while waiting, so calls on the same client wait too.
        Cancelling it closes the client, because a partly read message cannot be
        resumed. Not available over HTTP, where the server cannot send unprompted.

        Raises:
            RuntimeError: The transport cannot receive unprompted messages.
            ConnectionError: The client is closed, the connection dropped, or the
                server sent something other than a notification.
        """
        if not getattr(self._transport, "supports_server_push", True):
            raise RuntimeError("This transport cannot receive server notifications")
        async with self._call_lock:
            if self._closed:
                raise ConnectionError("Client channel is closed")
            try:
                message = self._codec.decode(await self._transport.recv(), Any)
                if not (isinstance(message, dict) and self._is_notification(message)):
                    raise ConnectionError("Expected a server notification")
                return self._notification(message)
            except (CodecError, EOFError) as exc:
                await self._abort()
                raise ConnectionError("RPC channel broken or reply undecodable") from exc
            except BaseException:
                await self._abort()
                raise

    async def _locked_exchange(self, data: bytes, accept: Callable[[Any], Any] | None) -> Any:
        """Send ``data`` and, if ``accept`` is given, return its result for the first non-notification reply.

        Runs under the call lock and ``call_timeout``. Any failure closes the client;
        ``accept`` raises ``ConnectionError`` for a reply that does not belong here.
        """
        # The deadline includes admission waiting. A cancelled waiter does not
        # own the channel and must not close somebody else's active exchange.
        async with asyncio.timeout(self._call_timeout):
            async with self._call_lock:
                if self._closed:
                    raise ConnectionError("Client channel is closed")
                try:
                    await self._transport.send(data)
                    if accept is None:
                        return None
                    while True:
                        message = self._codec.decode(await self._transport.recv(), Any)
                        if isinstance(message, dict) and self._is_notification(message):
                            await self._deliver(self._notification(message))
                            continue
                        return accept(message)
                except (CodecError, EOFError) as exc:
                    # EOFError covers asyncio.IncompleteReadError: the peer closed mid-frame.
                    await self._abort()
                    raise ConnectionError("RPC channel broken or reply undecodable") from exc
                except BaseException:
                    await self._abort()
                    raise

    async def _deliver(self, notification: Notification) -> None:
        """Pass a server notification to ``on_notification``, if set, without letting it fail the call."""
        if self._on_notification is None:
            return
        try:
            outcome = self._on_notification(notification)
            if inspect.isawaitable(outcome):
                await outcome
        except Exception:
            logger.exception("on_notification handler failed")

    @staticmethod
    def _notification(message: dict) -> Notification:
        """Turn a notification envelope into a ``Notification``, rejecting malformed ones."""
        if message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str):
            raise ConnectionError("Invalid server notification")
        return Notification(message["method"], message.get("params"))

    @classmethod
    def _response(cls, message: Any, request_id: str) -> dict:
        """Return ``message`` if it is the response to ``request_id``."""
        if not isinstance(message, dict):
            raise ConnectionError("Invalid RPC response envelope")
        cls._validate_response(message, request_id)
        return message

    @classmethod
    def _batch_response(cls, message: Any, expected: set[str]) -> list[dict] | dict:
        """Return a batch reply holding exactly one response per id in ``expected``.

        A single error object with a null id is also accepted: the server refused the
        whole batch before running anything.
        """
        if isinstance(message, dict):
            if message.get("id") is not None or "error" not in message:
                raise ConnectionError("Invalid RPC batch response")
            cls._validate_error(message)
            return message
        if not isinstance(message, list):
            raise ConnectionError("Invalid RPC batch response")
        seen: set[str] = set()
        for envelope in message:
            if not isinstance(envelope, dict) or envelope.get("id") not in expected or envelope["id"] in seen:
                raise ConnectionError("Invalid RPC batch response")
            cls._validate_response(envelope, envelope["id"])
            seen.add(envelope["id"])
        if seen != expected:
            raise ConnectionError("Invalid RPC batch response")
        return message

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
            Client._validate_error(envelope)

    @staticmethod
    def _validate_error(envelope: dict) -> None:
        """Reject an error object without an int ``code`` and a string ``message``."""
        error = envelope["error"]
        if (not isinstance(error, dict) or type(error.get("code")) is not int
                or not isinstance(error.get("message"), str)):
            raise ConnectionError("Invalid RPC error envelope")


__all__ = [
    "BatchCall",
    "BatchNotification",
    "Client",
    "Notification",
    "WireRpcError",
]
