"""Per-request failure boundary shared by unicast and multicast apps."""
import asyncio
import time
from dataclasses import asdict, dataclass
from typing import Any, Awaitable, Callable, Protocol

from msgspec import UNSET

from wire_rpc.codecs import Codec, CodecError
from wire_rpc.context import RequestContext, _request_context
from wire_rpc.errors import (
    InternalError,
    InvalidRequestError,
    ParseError,
    ServerError,
    WireError,
)
from wire_rpc.logger import access_logger
from wire_rpc.request import RawWireRequest
from wire_rpc.response import WireErrorResponse, WireResponse

type Dispatch = Callable[[RawWireRequest], Awaitable[WireResponse]]


class PeerBusyError(Exception):
    """A peer already has a request executing."""


@dataclass
class ExecutionStats:
    """Counters describing request outcomes; safe to expose because they hold no request data."""
    requests: int = 0
    active: int = 0
    completed: int = 0
    failed: int = 0
    timed_out: int = 0
    invalid: int = 0

    def snapshot(self) -> dict[str, int]:
        """Return the counters as a plain dict."""
        return asdict(self)


class _ExecutionHost(Protocol):
    """What ``process`` needs from ``App`` and ``MulticastApp``."""

    _codec: Codec
    _stats: ExecutionStats
    _transport: Any
    _handler_timeout: float
    _max_response_size: int
    _max_batch_size: int


def _fallback(codec: Codec, item: object) -> bytes | None:
    """Encode the small error that replaces this item's reply if the batch runs out of room.

    Returns ``None`` only for a valid notification, which never gets a reply. An
    invalid item without an ``id`` still gets an error reply, so it reserves space.
    """
    if isinstance(item, dict) and 'id' not in item and item.get('jsonrpc') == '2.0':
        try:
            codec.convert(item, RawWireRequest)
            return None
        except CodecError:
            pass
    request_id = item.get('id') if isinstance(item, dict) else None
    if isinstance(request_id, bool) or not isinstance(request_id, (str, int)):
        request_id = None
    return codec.encode(WireErrorResponse(error=InternalError('Internal server error'), id=request_id))



async def _process_one(
    app: _ExecutionHost,
    envelope: object,
    dispatch: Dispatch,
    deadline: float,
    client_id: str | None,
) -> bytes | None:
    """Validate, dispatch and encode one already-decoded request.

    Returns:
        The encoded response, or ``None`` for a notification. Invalid requests
        and handler failures become sanitized JSON-RPC errors; only
        cancellation propagates.
    """
    try:
        if not isinstance(envelope, dict) or envelope.get('jsonrpc') != '2.0':
            raise ValueError('Invalid protocol version')
        request = app._codec.convert(envelope, RawWireRequest)
    except (CodecError, ValueError):
        app._stats.invalid += 1
        _access(None, '-32600', 0.0, client_id)
        return app._codec.encode(WireErrorResponse(error=InvalidRequestError('Invalid request')))

    notification = request.id is UNSET
    response_id = request.response_id
    app._stats.active += 1
    token = None
    started = time.perf_counter()
    outcome = 'cancelled'
    try:
        async with asyncio.timeout_at(deadline):
            principal_getter = getattr(app._transport, 'get_principal', None)
            principal = await principal_getter(client_id) if principal_getter else None
            token = _request_context.set(RequestContext(response_id, request.method, principal, client_id))
            response = await dispatch(request)
        if notification:
            outcome = 'notification'
            return None
        encoded = app._codec.encode(response)
        if len(encoded) > app._max_response_size:
            raise ValueError('Response exceeds configured size')
        app._stats.completed += 1
        outcome = str(response.error.code) if isinstance(response, WireErrorResponse) else 'ok'
        return encoded
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        app._stats.failed += 1
        error: WireError
        if isinstance(exc, TimeoutError):
            app._stats.timed_out += 1
            error = ServerError(message='Request deadline exceeded', code=-32000)
        elif isinstance(exc, PeerBusyError):
            error = ServerError(message="Peer already has an active request", code=-32002)
        elif isinstance(exc, PermissionError):
            error = ServerError(message='Access denied', code=-32001)
        else:
            error = InternalError(message='Internal server error')
        outcome = str(error.code)
        if notification:
            return None
        # Exception strings, request bodies and validation inputs never enter
        # public error data or library logs. Counters expose safe failure data.
        return app._codec.encode(WireErrorResponse(error=error, id=response_id))
    finally:
        app._stats.active -= 1
        if token is not None:
            _request_context.reset(token)
        _access(request.method, outcome, time.perf_counter() - started, client_id)


def _access(method: str | None, outcome: str, seconds: float, client_id: str | None) -> None:
    """Write one access-log line; params, results and exception text are never included."""
    access_logger.info('%s %s %.1fms peer=%s', method if method is not None else '<invalid>', outcome,
                       seconds * 1000, client_id if client_id is not None else '-')



async def process(
    app: _ExecutionHost,
    data: bytes,
    dispatch: Dispatch,
    client_id: str | None = None,
) -> bytes | None:
    """Decode, dispatch and encode one message without letting failures escape.

    A message is one request or a batch (a JSON-RPC array of requests). A batch
    runs its calls in order under one shared ``handler_timeout`` deadline, and
    its reply always fits ``max_response_size``: a reply that would not fit is
    replaced by a small -32603 error for that call. A batch longer than
    ``max_batch_size``, or too large to report its own errors, is refused
    before any call runs.

    Args:
        app: The owning ``App`` or ``MulticastApp``; supplies codec, limits and stats.
        data: The raw encoded request.
        dispatch: Routes a validated request to its handler.
        client_id: The multicast peer that sent the request, if any.

    Returns:
        The encoded response, an encoded array of responses for a batch, or
        ``None`` when nothing needs a reply (a notification, or a batch of only
        notifications). Handler errors become sanitized JSON-RPC errors; only
        cancellation propagates.
    """
    codec = app._codec
    stats = app._stats
    stats.requests += 1
    try:
        envelope = codec.decode(data, Any)
    except CodecError:
        stats.invalid += 1
        access_logger.info('<unparseable> -32700 peer=%s', client_id if client_id is not None else '-')
        return codec.encode(WireErrorResponse(error=ParseError('Invalid encoded request')))

    deadline = asyncio.get_running_loop().time() + app._handler_timeout 

    if isinstance(envelope, list):
        if len(envelope) == 0:
            return codec.encode(WireErrorResponse(error=InvalidRequestError('Invalid request')))
        if len(envelope) > app._max_batch_size:
            return codec.encode(WireErrorResponse(error=InvalidRequestError('Invalid request')))
        fallbacks = [_fallback(codec, item) for item in envelope]
        reserve = sum(len(f) for f in fallbacks if f is not None) + len(envelope) + 8
        if reserve > app._max_response_size:
            return codec.encode(WireErrorResponse(error=InvalidRequestError('Invalid request')))
        # Every unfinished item keeps its fallback's worth of space held back, so
        # the worst case (every reply replaced by its fallback) fits by construction.
        remaining = app._max_response_size - len(envelope) - 8
        reserved = reserve - len(envelope) - 8
        replies: list[bytes] = []
        for item, fallback in zip(envelope, fallbacks):
            reply = await _process_one(app, item, dispatch, client_id=client_id, deadline=deadline)
            if fallback is not None:
                reserved -= len(fallback)
            if reply is None:
                continue
            if fallback is not None and len(reply) > remaining - reserved:
                reply = fallback
            remaining -= len(reply)
            replies.append(reply)
        if not replies:
            return None
        return codec.encode([codec.decode(reply, Any) for reply in replies])
    return await _process_one(app, envelope, dispatch, client_id=client_id, deadline=deadline)
