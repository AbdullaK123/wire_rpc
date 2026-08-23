"""
Wire RPC multicast application core.

Like App, but for multi-client scenarios. Each handler
receives a client_id identifying who sent the request.
The transport is exposed for broadcasting to all clients.
"""

import asyncio
import inspect
from typing import Callable, Any, Awaitable, List, Optional, get_type_hints, get_args
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
from wire_rpc.errors import InternalError, InvalidParamsError, InvalidRequestError, MethodNotFoundError
from wire_rpc.middleware import Middleware
from wire_rpc.request import RawWireRequest
from wire_rpc.response import WireErrorResponse, WireResponse, WireSuccessResponse
from wire_rpc.transports.protocol import MulticastTransport
from wire_rpc.logger import logger
from wire_rpc.codecs.protocol import Codec
import msgspec

type AppContext = Any
type Handler = Callable[..., Awaitable[Any]]
type StartupHook = Callable[[], Awaitable[Any]]
type ShutdownHook = Callable[[Any], Awaitable[None]]


class MulticastApp:

    def __init__(
        self,
        transport: MulticastTransport,
        codec: Codec = MsgSpecJsonCodec()
    ):
        self._transport = transport
        self._codec = codec
        self._ctx: Optional[Any] = None
        self._handlers: dict[str, Handler] = {}
        self._param_types: dict[str, Any] = {}
        self._return_types: dict[str, Any] = {}
        self._param_counts: dict[str, int] = {}
        self._middleware: List[Middleware] = []
        self._on_startup: Optional[StartupHook] = None
        self._on_shutdown: Optional[ShutdownHook] = None

    @property
    def transport(self) -> MulticastTransport:
        return self._transport

    def method(self, name: str) -> Callable:
        def decorator(func: Handler) -> Handler:
            hints = get_type_hints(func)
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())
            # Count params excluding ctx and client_id
            # Handlers: (ctx) or (params, ctx) or (params, ctx, client_id) or (ctx, client_id)
            self._param_counts[name] = len(sig.parameters)
            if len(params) >= 1:
                first_hint = hints.get(params[0])
                # If first param type is str, it's client_id for a no-params handler
                # Otherwise check if it's a Struct (params type)
                if first_hint is not None and first_hint is not str and first_hint is not type(None):
                    self._param_types[name] = first_hint
                else:
                    self._param_types[name] = None
            else:
                self._param_types[name] = None
            self._return_types[name] = hints.get("return", None)
            self._handlers[name] = func
            logger.info(f"Registered handler for method '{name}' with parameter type '{self._param_types[name]}'")
            return func
        return decorator

    def middleware(self, func: Middleware) -> Middleware:
        self._middleware.append(func)
        logger.info(f"Registered middleware '{getattr(func, '__name__', type(func).__name__)}'")
        return func

    def on_startup(self, func: StartupHook) -> StartupHook:
        self._on_startup = func
        return func

    def on_shutdown(self, func: ShutdownHook) -> ShutdownHook:
        self._on_shutdown = func
        return func

    async def broadcast(self, method: str, data: Any) -> None:
        """Broadcast a JSON-RPC notification to all connected clients."""
        notification = RawWireRequest(method=method, params=msgspec.to_builtins(data))
        encoded = self._codec.encode(notification)
        await self._transport.broadcast(encoded)

    async def _dispatch(self, request: RawWireRequest, client_id: str) -> WireResponse:

        logger.debug(f"Received request (id={request.id}) from client {client_id} for method '{request.method}'")

        if request.method not in self._handlers.keys():
            logger.warning(f"Method '{request.method}' not found.")
            return WireErrorResponse(
                error=MethodNotFoundError("Method not found"),
                id=request.id
            )

        handler = self._handlers[request.method]
        params_type = self._param_types[request.method]
        return_type = self._return_types[request.method]
        param_count = self._param_counts[request.method]

        if request.params is not None and params_type is not None:
            try:
                request.params = msgspec.convert(request.params, params_type)
            except msgspec.ValidationError:
                logger.warning(f"Invalid params for request (id={request.id}). Must be of type {str(params_type)}")
                return WireErrorResponse(
                    error=InvalidParamsError("Invalid params"),
                    id=request.id
                )

        async def call_handler(req: RawWireRequest, ctx: AppContext) -> WireResponse:
            # Determine handler signature:
            # 1 param:  (ctx)
            # 2 params: (params, ctx) or (ctx, client_id)
            # 3 params: (params, ctx, client_id)
            if param_count == 1:
                result = await handler(ctx)
            elif param_count == 2:
                if params_type is not None:
                    result = await handler(req.params, ctx)
                else:
                    result = await handler(ctx, client_id)
            else:
                result = await handler(req.params, ctx, client_id)

            if return_type is not None:
                result = msgspec.convert(result, return_type)
            return WireSuccessResponse(result=result, id=req.id)

        chain = call_handler

        for mw in reversed(self._middleware):
            next_fn = chain
            chain = lambda req, ctx, n=next_fn, m=mw: m(req, ctx, n)  # type: ignore

        return await chain(request, self._ctx)

    async def _listen(self):
        async with self._transport as t:
            while True:

                try:
                    client_id, data = await t.recv()
                except (asyncio.IncompleteReadError, ConnectionError):
                    logger.info("Server has shutdown. Goodbye...")
                    break

                try:
                    request = self._codec.decode(data, RawWireRequest)
                except msgspec.DecodeError:
                    logger.warning(f"Failed to decode request bytes from client {client_id}.")
                    err = WireErrorResponse(
                        error=InvalidRequestError(
                            "Invalid request. Request must follow json rpc 2.0 spec"
                        )
                    )
                    encoded = self._codec.encode(err)
                    try:
                        await t.send(client_id, encoded)
                    except ConnectionError:
                        pass
                    continue

                try:
                    response = await self._dispatch(request, client_id)
                except Exception as e:
                    logger.opt(exception=True).error(f"Request (id={request.id}) from client {client_id} failed")
                    err = InternalError(
                        message="Something went wrong. Please check logs.",
                        data={"error": str(e)}
                    )
                    err_response = WireErrorResponse(error=err, id=request.id)
                    encoded = self._codec.encode(err_response)
                    try:
                        await t.send(client_id, encoded)
                    except ConnectionError:
                        pass
                    continue

                encoded = self._codec.encode(response)
                try:
                    await t.send(client_id, encoded)
                except ConnectionError:
                    logger.warning(f"Client {client_id} disconnected before response could be sent")
                logger.debug(f"Responded to request (id={request.id}) for client {client_id}")

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        if self._on_startup:
            logger.info("Starting wire_rpc multicast server...")
            self._ctx = await self._on_startup()
        try:
            await self._listen()
        finally:
            if self._on_shutdown:
                logger.info("Shutting down multicast server...")
                await self._on_shutdown(self._ctx)