"""Middleware signature inspection shared by App, MulticastApp, and Router."""

from __future__ import annotations

import inspect
from typing import Any, Callable


def _call_target(func: Callable[..., Any]) -> Callable[..., Any]:
    if inspect.isfunction(func) or inspect.ismethod(func):
        return func

    call = getattr(func, "__call__", None)
    if call is None:
        raise TypeError(
            f"Middleware '{type(func).__name__}' must be callable"
        )
    return call


def inspect_middleware(func: Callable[..., Any]) -> None:
    """Validate the middleware call shape at registration time."""
    name = getattr(func, "__name__", type(func).__name__)
    target = _call_target(func)

    if not inspect.iscoroutinefunction(target):
        raise TypeError(f"Middleware '{name}' must be async")

    params = list(inspect.signature(target).parameters.values())
    supported_kinds = {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }
    if any(param.kind not in supported_kinds for param in params):
        raise TypeError(
            f"Middleware '{name}' may only use positional parameters"
        )

    if len(params) != 3:
        raise TypeError(
            f"Middleware '{name}' must have signature (request, ctx, next)"
        )


__all__ = ["inspect_middleware"]
