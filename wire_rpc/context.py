"""Trusted, task-local metadata for the currently executing RPC request."""
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RequestContext:
    """Trusted metadata for the request being handled.

    Attributes:
        request_id: The JSON-RPC id, or ``None`` for a notification.
        method: The method name being called.
        principal: The identity the transport authenticated, or ``None``.
        connection_id: The multicast peer id, or ``None`` on unicast transports.
    """
    request_id: str | int | None
    method: str
    principal: str | None
    connection_id: str | None


_request_context: ContextVar[RequestContext] = ContextVar('wire_rpc_request')


def current_request() -> RequestContext:
    """Read authenticated metadata inside a handler/middleware; never client params."""
    try:
        return _request_context.get()
    except LookupError:
        raise RuntimeError('No RPC request is executing in this context') from None
