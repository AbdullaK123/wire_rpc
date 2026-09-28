"""Per-request failure boundary shared by unicast and multicast apps."""
import asyncio
from dataclasses import asdict, dataclass
from typing import Callable, Awaitable, Any

from msgspec import UNSET

from wire_rpc.codecs import CodecError
from wire_rpc.context import RequestContext, _request_context
from wire_rpc.errors import InternalError, InvalidRequestError, ParseError, ServerError
from wire_rpc.request import RawWireRequest
from wire_rpc.response import WireErrorResponse


class PeerBusyError(Exception):
    """A peer already has a request executing."""


@dataclass
class ExecutionStats:
    requests: int = 0
    active: int = 0
    completed: int = 0
    failed: int = 0
    timed_out: int = 0
    invalid: int = 0

    def snapshot(self):
        return asdict(self)


async def process(app, data: bytes, dispatch: Callable[[RawWireRequest], Awaitable], client_id=None):
    codec = app._codec
    stats = app._stats
    stats.requests += 1
    try:
        envelope = codec.decode(data, Any)
    except CodecError:
        stats.invalid += 1
        return codec.encode(WireErrorResponse(error=ParseError('Invalid encoded request')))
    try:
        if not isinstance(envelope, dict) or envelope.get('jsonrpc') != '2.0':
            raise ValueError('Invalid protocol version')
        request = codec.convert(envelope, RawWireRequest)
    except (CodecError, ValueError):
        stats.invalid += 1
        return codec.encode(WireErrorResponse(error=InvalidRequestError('Invalid request')))

    notification = request.id is UNSET
    response_id = None if notification else request.id
    stats.active += 1
    token = None
    try:
        async with asyncio.timeout(app._handler_timeout):
            principal_getter = getattr(app._transport, 'get_principal', None)
            principal = await principal_getter(client_id) if principal_getter else None
            token = _request_context.set(RequestContext(response_id, request.method, principal, client_id))
            response = await dispatch(request)
        if notification:
            return None
        encoded = codec.encode(response)
        if len(encoded) > app._max_response_size:
            raise ValueError('Response exceeds configured size')
        stats.completed += 1
        return encoded
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        stats.failed += 1
        if isinstance(exc, TimeoutError):
            stats.timed_out += 1
            error = ServerError(message='Request deadline exceeded', code=-32000)
        elif isinstance(exc, PeerBusyError):
            error = ServerError(message="Peer already has an active request", code=-32002)
        elif isinstance(exc, PermissionError):
            error = ServerError(message='Access denied', code=-32001)
        else:
            error = InternalError(message='Internal server error')
        if notification:
            return None
        # Exception strings, request bodies and validation inputs never enter
        # public error data or library logs. Counters expose safe failure data.
        return codec.encode(WireErrorResponse(error=error, id=response_id))
    finally:
        stats.active -= 1
        if token is not None:
            _request_context.reset(token)
