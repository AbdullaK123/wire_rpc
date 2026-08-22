import asyncio
from typing import Callable, Any, Awaitable, List, Optional
from wire_rpc.codecs.msgspec import MsgSpecJsonCodec
from wire_rpc.errors import InternalError, InvalidRequestError, MethodNotFoundError
from wire_rpc.middleware import Middleware
from wire_rpc.request import WireRequest
from wire_rpc.response import WireErrorResponse, WireResponse, WireSuccessResponse
from wire_rpc.transports import Transport
from wire_rpc.codecs import Codec
import msgspec

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
        self._middleware: List[Middleware] = []
        self._on_startup: Optional[StartupHook] = None
        self._on_shutdown: Optional[ShutdownHook] = None

    def method(self, name: str) -> Callable:
        def decorator(func: Handler) -> Handler:
            self._handlers[name] = func
            return func
        return decorator

    def middleware(self, func: Middleware) -> Callable:
        self._middleware.append(func)
        return func

    def on_startup(self, func: StartupHook) -> StartupHook:
        self._on_startup = func
        return func

    def on_shutdown(self, func: ShutdownHook) -> ShutdownHook:
        self._on_shutdown = func
        return func

    async def _dispatch(self, request: WireRequest) -> WireResponse:

        if request.method not in self._handlers.keys():
            return WireErrorResponse(
                error=MethodNotFoundError("Method not found"),
                id=request.id
            )   

        handler = self._handlers[request.method]

        async def call_handler(req: WireRequest, ctx: AppContext) -> WireResponse:
            result = await handler(req, ctx)
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
                    break

                try:
                    request = self._codec.decode(data, WireRequest)
                except msgspec.DecodeError:
                    err = WireErrorResponse(
                        error=InvalidRequestError("Invalid request. Request must follow json rpc 2.0 spec")
                    )
                    encoded = self._codec.encode(err)
                    await t.send(encoded)
                    continue

                try: 
                    response = await self._dispatch(request)
                except Exception as e:
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

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        if self._on_startup:
            self._ctx = await self._on_startup()
        try:
            await self._listen()
        finally:
            if self._on_shutdown:
                await self._on_shutdown(self._ctx)



        
