"""Trusted, task-local metadata for the currently executing RPC request."""
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RequestContext:
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
