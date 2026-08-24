"""
Wire RPC Router.

Groups related handlers under a prefix with scoped middleware.
Routers nest — prefixes concatenate, middleware inherits down the tree.
Everything flattens to a dict at registration time. Dispatch is always
a single dict lookup regardless of nesting depth.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from wire_rpc._handler import HandlerSpec, inspect_handler
from wire_rpc._middleware import inspect_middleware
from wire_rpc.logger import logger
from wire_rpc.middleware import Middleware

type Handler = Callable[..., Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class HandlerEntry:
    """A fully resolved handler with its spec and middleware chain."""
    handler: Handler
    spec: HandlerSpec
    middleware: list[Middleware]


class Router:
    """
    A collection of related RPC methods under a shared prefix.

    Routers can nest via include_router(). Middleware registered
    on a router applies to all methods in that router and any
    child routers. Prefixes concatenate with dots.

    Usage:
        notes = Router("notes")

        @notes.method("create")
        async def create(params: CreateParams, ctx) -> NoteResult: ...

        @notes.method("list")
        async def list_notes(ctx) -> NoteListResult: ...

        app.include_router(notes)
        # registers "notes.create" and "notes.list"
    """

    def __init__(self, prefix: str, *, multicast: bool = False):
        self.prefix = prefix
        self._multicast = multicast
        self._handlers: dict[str, Handler] = {}
        self._specs: dict[str, HandlerSpec] = {}
        self._middleware: list[Middleware] = []
        self._children: list[Router] = []

    def method(self, name: str) -> Callable:
        """Register a handler on this router."""
        def decorator(func: Handler) -> Handler:
            spec = inspect_handler(func, multicast=self._multicast)
            self._handlers[name] = func
            self._specs[name] = spec
            logger.info(
                f"Router '{self.prefix}': registered method '{name}' "
                f"with parameter type '{spec.params_type}'"
            )
            return func
        return decorator

    def middleware(self, func: Middleware) -> Middleware:
        """Register middleware scoped to this router and its children."""
        inspect_middleware(func)
        self._middleware.append(func)
        logger.info(
            f"Router '{self.prefix}': registered middleware "
            f"'{getattr(func, '__name__', type(func).__name__)}'"
        )
        return func

    def include_router(self, router: Router) -> None:
        """Nest a child router under this router's prefix."""
        self._children.append(router)

    def _flatten(
        self,
        prefix: str = "",
        parent_middleware: list[Middleware] | None = None,
    ) -> dict[str, HandlerEntry]:
        """
        Recursively flatten the router tree into a flat handler registry.

        Prefixes concatenate: parent.child.method
        Middleware inherits: parent middleware runs before child middleware.
        """
        mw_chain = list(parent_middleware or []) + self._middleware
        full_prefix = f"{prefix}{self.prefix}."

        entries: dict[str, HandlerEntry] = {}

        # Local handlers
        for name, handler in self._handlers.items():
            full_name = f"{full_prefix}{name}"
            entries[full_name] = HandlerEntry(
                handler=handler,
                spec=self._specs[name],
                middleware=list(mw_chain),
            )

        # Child routers
        for child in self._children:
            entries.update(
                child._flatten(prefix=full_prefix, parent_middleware=mw_chain)
            )

        return entries


__all__ = ["Router", "HandlerEntry"]
