"""Wire RPC application core.

The App class ties everything together:
    - Handler registration via @app.method()
    - Middleware chain via @app.middleware
    - Lifecycle hooks via @app.on_startup / @app.on_shutdown
    - The listen loop: recv → decode → dispatch → encode → send
"""

import asyncio
from typing import Any, Awaitable, Callable, List, Optional

from wire_rpc._handler import HandlerSpec, inspect_handler
from wire_rpc._middleware import inspect_middleware
from wire_rpc.codecs import Codec, CodecConversionError, CodecDecodeError
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
from wire_rpc.errors import (
    InternalError,
    InvalidParamsError,
    InvalidRequestError,
    MethodNotFoundError,
)
from wire_rpc.logger import logger
from wire_rpc.middleware import Middleware
from wire_rpc.request import RawWireRequest
from wire_rpc.response import WireErrorResponse, WireResponse, WireSuccessResponse
from wire_rpc.router import Router
from wire_rpc.transports import Transport
from wire_rpc.transports.protocol import StartupComponent

type AppContext = Any
type Handler = Callable[..., Awaitable[Any]]
type StartupHook = Callable[[], Awaitable[Any]]
type ShutdownHook = Callable[[Any], Awaitable[None]]


class App:

    def __init__(
        self,
        transport: Transport,
        codec: Codec = MsgSpecJsonCodec(),
    ):
        self._transport = transport
        self._codec = codec
        self._ctx: Optional[Any] = None
        self._handlers: dict[str, Handler] = {}
        self._specs: dict[str, HandlerSpec] = {}
        self._router_middleware: dict[str, list[Middleware]] = {}
        self._middleware: List[Middleware] = []
        self._on_startup: Optional[StartupHook] = None
        self._on_shutdown: Optional[ShutdownHook] = None

    async def _internal_startup(self):
        if isinstance(self._transport, StartupComponent):
            await self._transport.startup()

    async def _internal_shutdown(self):
         if isinstance(self._transport, StartupComponent):
             await self._transport.shutdown()

    def method(self, name: str) -> Callable:
        def decorator(func: Handler) -> Handler:
            spec = inspect_handler(func, multicast=False)
            self._handlers[name] = func
            self._specs[name] = spec
            self._router_middleware[name] = []
            logger.info(
                f"Registered handler for method '{name}' "
                f"with parameter type '{spec.params_type}'"
            )
            return func

        return decorator

    def include_router(self, router: Router) -> None:
        entries = router._flatten()
        for name, entry in entries.items():
            self._handlers[name] = entry.handler
            self._specs[name] = entry.spec
            self._router_middleware[name] = entry.middleware
            logger.info(
                f"Registered handler for method '{name}' "
                f"with parameter type '{entry.spec.params_type}'"
            )

    def middleware(self, func: Middleware) -> Middleware:
        inspect_middleware(func)
        self._middleware.append(func)
        logger.info(
            f"Registered middleware '{getattr(func, '__name__', type(func).__name__)}'"
        )
        return func

    def on_startup(self, func: StartupHook) -> StartupHook:
        self._on_startup = func
        return func

    def on_shutdown(self, func: ShutdownHook) -> ShutdownHook:
        self._on_shutdown = func
        return func

    async def _dispatch(self, request: RawWireRequest) -> WireResponse:
        logger.debug(
            f"Received request (id={request.id}) for method '{request.method}'"
        )

        if request.method not in self._handlers:
            logger.warning(f"Method '{request.method}' not found.")
            return WireErrorResponse(
                error=MethodNotFoundError("Method not found"),
                id=request.id,
            )

        handler = self._handlers[request.method]
        spec = self._specs[request.method]
        router_mw = self._router_middleware.get(request.method, [])

        if request.params is not None and spec.params_type is not None and spec.has_params:
            try:
                request.params = self._codec.convert(request.params, spec.params_type)
            except CodecConversionError:
                logger.warning(
                    f"Invalid params for request (id={request.id}). "
                    f"Must be of type {spec.params_type}"
                )
                return WireErrorResponse(
                    error=InvalidParamsError("Invalid params"),
                    id=request.id,
                )

        async def call_handler(
            req: RawWireRequest,
            ctx: AppContext,
        ) -> WireResponse:
            if spec.has_params:
                result = await handler(req.params, ctx)
            else:
                result = await handler(ctx)

            if spec.return_type is not None:
                result = self._codec.convert(result, spec.return_type)

            return WireSuccessResponse(result=result, id=req.id)

        # Build chain: app middleware → router middleware → handler
        chain = call_handler

        for mw in reversed(router_mw):
            next_fn = chain
            chain = lambda req, ctx, n=next_fn, m=mw: m(req, ctx, n)  # type: ignore

        for mw in reversed(self._middleware):
            next_fn = chain
            chain = lambda req, ctx, n=next_fn, m=mw: m(req, ctx, n)  # type: ignore

        return await chain(request, self._ctx)

    async def _listen(self):
        async with self._transport as t:
            while True:
                try:
                    data = await t.recv()
                except (asyncio.IncompleteReadError, ConnectionError):
                    logger.info("Server has shutdown. Goodbye...")
                    break

                try:
                    request = self._codec.decode(data, RawWireRequest)
                except CodecDecodeError:
                    logger.warning("Failed to decode request bytes.")
                    err = WireErrorResponse(
                        error=InvalidRequestError(
                            "Invalid request. Request must follow json rpc 2.0 spec"
                        )
                    )
                    await t.send(self._codec.encode(err))
                    continue

                try:
                    response = await self._dispatch(request)
                except Exception as exc:
                    logger.opt(exception=True).error(
                        f"Request (id={request.id}) failed with exception:\n {exc}"
                    )
                    err = InternalError(
                        message="Something went wrong. Please check logs.",
                        data={"error": str(exc)},
                    )
                    await t.send(
                        self._codec.encode(
                            WireErrorResponse(error=err, id=request.id)
                        )
                    )
                    continue

                await t.send(self._codec.encode(response))
                logger.info(f"Responded to request (id={request.id})")

    def run(self):
        asyncio.run(self._run())

    async def _run(self):

        await self._internal_startup()

        try:
            if self._on_startup:
                logger.info("Starting wire_rpc server...")
                self._ctx = await self._on_startup()

            try:
                await self._listen()
            finally:
                if self._on_shutdown:
                    logger.info("Shutting down server...")
                    await self._on_shutdown(self._ctx)
        finally:

            await self._internal_shutdown()
