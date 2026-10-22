"""Handler signature inspection shared by App and MulticastApp."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, get_type_hints


@dataclass(frozen=True, slots=True)
class HandlerSpec:
    """What dispatch needs to know about a handler's signature, computed once at registration."""
    params_type: Any | None
    return_type: Any | None
    has_params: bool
    receives_client_id: bool = False


def _parameters(func: Callable[..., Any]) -> list[inspect.Parameter]:
    """Return the handler's parameters, rejecting keyword-only and variadic ones."""
    params = list(inspect.signature(func).parameters.values())
    supported_kinds = {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }
    if any(param.kind not in supported_kinds for param in params):
        raise TypeError(
            f"Handler '{func.__name__}' may only use positional parameters"
        )
    return params


def inspect_handler(func: Callable[..., Any], *, multicast: bool) -> HandlerSpec:
    """Validate a handler's signature and describe how to call it.

    Unicast handlers take ``(ctx)`` or ``(params, ctx)``. Multicast handlers may also
    take ``client_id`` as their last parameter. The type hint on ``params`` becomes
    the type incoming params are converted to.

    Raises:
        TypeError: The signature is not one of the supported shapes.
    """
    params = _parameters(func)
    hints = get_type_hints(func)
    count = len(params)

    if multicast:
        if count not in (1, 2, 3):
            raise TypeError(
                f"Multicast handler '{func.__name__}' must have one of these "
                "signatures: (ctx), (params, ctx), (ctx, client_id), "
                "or (params, ctx, client_id)"
            )

        if count == 1:
            has_params = False
            receives_client_id = False
        elif count == 2:
            receives_client_id = params[1].name == "client_id"
            has_params = not receives_client_id
        else:
            if params[2].name != "client_id":
                raise TypeError(
                    f"Three-argument multicast handler '{func.__name__}' must "
                    "use client_id as its third parameter"
                )
            has_params = True
            receives_client_id = True
    else:
        if count not in (1, 2):
            raise TypeError(
                f"Handler '{func.__name__}' must have signature "
                "(ctx) or (params, ctx)"
            )
        has_params = count == 2
        receives_client_id = False

    params_type = hints.get(params[0].name) if has_params else None

    return HandlerSpec(
        params_type=params_type,
        return_type=hints.get("return"),
        has_params=has_params,
        receives_client_id=receives_client_id,
    )


__all__ = ["HandlerSpec", "inspect_handler"]
