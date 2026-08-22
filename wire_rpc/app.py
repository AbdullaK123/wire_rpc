import asyncio
from typing import Callable, Any, Awaitable, List, Optional
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
from wire_rpc.errors import InternalError, InvalidParamsError, InvalidRequestError, MethodNotFoundError
from wire_rpc.middleware import Middleware
from wire_rpc.request import WireRequest
from wire_rpc.response import WireErrorResponse, WireResponse, WireSuccessResponse
from wire_rpc.transports import Transport
from wire_rpc.logger import logger
from wire_rpc.codecs import Codec
import msgspec
from typing import get_type_hints, get_args
import inspect

type AppContext = Any
type Handler = Callable[[WireRequest, AppContext], Awaitable[WireResponse]]
type StartupHook = Callable[[], Awaitable[Any]] 
type ShutdownHook = Callable[[Any], Awaitable[None]] 


class App:

    def __init__(
        self,
        transport: Transport,
        codec: Codec = MsgSpecJsonCodec()
    ):
        self._transport = transport
        self._codec = codec
        self._ctx: Optional[Any] = None
        self._handlers : dict[str, Handler] = {}
        self._param_types: dict[str, Any] = {}
        self._return_types: dict[str, Any] = {}
        self._middleware: List[Middleware] = []
        self._on_startup: Optional[StartupHook] = None
        self._on_shutdown: Optional[ShutdownHook] = None

    def method(self, name: str) -> Callable:
        def decorator(func: Handler) -> Handler:
            hints = get_type_hints(func)
            sig = inspect.signature(func)
            first_param = list(sig.parameters.keys())[0]
            req_hint = hints[first_param]
            args = get_args(req_hint)
            self._param_types[name] = args[0] if args else None
            self._return_types[name] = hints.get("return", None)
            self._handlers[name] = func
            logger.info(f"Registered handler for method '{name}' with parameter type '{self._param_types[name]}'")
            return func
        return decorator

    def middleware(self, func: Middleware) -> Callable:
        self._middleware.append(func)
        logger.info(f"Registered middleware '{getattr(func, '__name__', type(func).__name__)}'")
        return func

    def on_startup(self, func: StartupHook) -> StartupHook:
        self._on_startup = func
        return func

    def on_shutdown(self, func: ShutdownHook) -> ShutdownHook:
        self._on_shutdown = func
        return func

    async def _dispatch(self, request: WireRequest) -> WireResponse:

        logger.debug(f"Received request (id={request.id}) for method '{request.method}'")

        if request.method not in self._handlers.keys():
            logger.warning(f"Method '{request.method}' not found.")
            return WireErrorResponse(
                error=MethodNotFoundError("Method not found"),
                id=request.id
            )   

        handler = self._handlers[request.method]
        params_type = self._param_types[request.method]
        return_type = self._return_types[request.method]

        if request.params is not None and params_type is not None:
            try:
                request.params = msgspec.convert(request.params, params_type)
            except msgspec.ValidationError:
                logger.warning(f"Invalid params for request (id={request.id}). Must be of type {str(params_type)}")
                return WireErrorResponse(
                    error=InvalidParamsError("Invalid params"),
                    id=request.id
                )

        async def call_handler(req: WireRequest, ctx: AppContext) -> WireResponse:
            result = await handler(req, ctx)
            if return_type is not None:
                result = msgspec.convert(result, return_type)
            return WireSuccessResponse(result=result, id=req.id)

        chain = call_handler

        for mw in reversed(self._middleware):
            next_fn = chain
            chain = lambda req, ctx, n=next_fn, m=mw: m(req, ctx, n) # type: ignore

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
                    request = self._codec.decode(data, WireRequest)
                except msgspec.DecodeError:
                    logger.warning(f"Failed to decode request bytes.")
                    err = WireErrorResponse(
                        error=InvalidRequestError("Invalid request. Request must follow json rpc 2.0 spec")
                    )
                    encoded = self._codec.encode(err)
                    await t.send(encoded)
                    continue

                try: 
                    response = await self._dispatch(request)
                except Exception as e:
                    logger.opt(exception=True).error(f"Request (id={request.id}) failed with exception:\n {str(e)}")
                    err = InternalError(
                        message="Something went wrong. Please check logs.",
                        data={
                            "error": str(e)
                        }
                    )
                    err_response = WireErrorResponse(error=err, id=request.id)
                    encoded = self._codec.encode(err_response)
                    await t.send(encoded)
                    continue

                encoded = self._codec.encode(response)
                await t.send(encoded)
                logger.info(f"Responded to request (id={request.id})")

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        if self._on_startup:
            logger.info("Starting wire_rpc server...")
            self._ctx = await self._on_startup()
        try:
            await self._listen()
        finally:
            if self._on_shutdown:
                logger.info("Shutting down server...")
                await self._on_shutdown(self._ctx)



        
