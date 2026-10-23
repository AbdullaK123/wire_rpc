"""Per-call metrics: one record per executed handler, handed to your exporter."""
import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from wire_rpc.context import current_request
from wire_rpc.logger import logger
from wire_rpc.response import WireErrorResponse

type Outcome = Literal['ok', 'error', 'exception', 'cancelled']


@dataclass(frozen=True, slots=True)
class CallRecord:
    """What happened to one handler call.

    Attributes:
        method: The method name that was called.
        duration: Seconds spent in the middleware chain and handler.
        outcome: ``ok``; ``error`` when a JSON-RPC error response was returned;
            ``exception`` when the handler or a later middleware raised; or
            ``cancelled`` when the call was cut off, for example by
            ``handler_timeout``.
        error_code: The JSON-RPC error code for an ``error`` outcome, else ``None``.
        principal: The authenticated identity, if any.
    """

    method: str
    duration: float
    outcome: Outcome
    error_code: int | None
    principal: str | None


def metrics_middleware(record: Callable[[CallRecord], None]):
    """Build middleware that passes a ``CallRecord`` to ``record`` after every handler call.

    Register it first (``app.middleware(metrics_middleware(...))``) so the duration
    covers every other middleware. Requests rejected before dispatch (unparseable,
    unknown method, invalid params) never reach middleware and are not recorded;
    count those with ``app.stats``. ``record`` must be quick and must not block; an
    exception it raises is logged and does not affect the call.

    Usage:
        def export(call: CallRecord) -> None:
            latency.labels(call.method, call.outcome).observe(call.duration)

        app.middleware(metrics_middleware(export))
    """

    def emit(method: str, started: float, outcome: Outcome, error_code: int | None) -> None:
        try:
            record(CallRecord(method, time.perf_counter() - started, outcome, error_code,
                              current_request().principal))
        except Exception:
            logger.exception('metrics record callback failed')

    async def middleware(request: Any, ctx: Any, call_next: Any) -> Any:
        started = time.perf_counter()
        try:
            response = await call_next(request, ctx)
        except asyncio.CancelledError:
            emit(request.method, started, 'cancelled', None)
            raise
        except Exception:
            emit(request.method, started, 'exception', None)
            raise
        if isinstance(response, WireErrorResponse):
            emit(request.method, started, 'error', response.error.code)
        else:
            emit(request.method, started, 'ok', None)
        return response

    return middleware


__all__ = ['CallRecord', 'metrics_middleware']
