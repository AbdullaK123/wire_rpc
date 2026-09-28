"""Wire RPC multicast application core.

Like App, but for multi-client scenarios. Each handler may receive a
client_id identifying who sent the request, and the app can broadcast
notifications to all connected clients.
"""

import asyncio
from wire_rpc._execution import ExecutionStats, PeerBusyError, process
from wire_rpc._validation import positive_timeout, positive_limit, validate_codec
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
from wire_rpc.transports.protocol import MulticastTransport, StartupComponent

type AppContext = Any
type Handler = Callable[..., Awaitable[Any]]
type StartupHook = Callable[[], Awaitable[Any]]
type ShutdownHook = Callable[[Any], Awaitable[None]]


class MulticastApp:

    def __init__(
        self,
        transport: MulticastTransport,
        codec: Codec = MsgSpecJsonCodec(),
        *,
        handler_timeout: float = 30.0,
        startup_timeout: float = 30.0,
        shutdown_timeout: float = 10.0,
        max_response_size: int = 4 * 1024 * 1024,
        max_concurrency: int = 16,
    ):
        validate_codec(transport, codec)
        positive_timeout('handler_timeout', handler_timeout)
        positive_timeout('startup_timeout', startup_timeout)
        self._startup_timeout = startup_timeout
        positive_timeout('shutdown_timeout', shutdown_timeout)
        positive_limit('max_response_size', max_response_size)
        if max_response_size < 256:
            raise ValueError('Response budget must allow at least 256 bytes for errors')
        self._handler_timeout = handler_timeout
        self._shutdown_timeout = shutdown_timeout
        self._max_response_size = min(max_response_size, getattr(transport, "max_message_size", max_response_size))
        if self._max_response_size < 2048:
            raise ValueError("Response budget must allow 2048 bytes for bounded error envelopes")
        self._stats = ExecutionStats()
        self._stopping = asyncio.Event()
        self._running = False
        positive_limit('max_concurrency', max_concurrency)
        self._max_concurrency = max_concurrency
        self._tasks: set[asyncio.Task] = set()
        self._peer_locks: dict[str, tuple[asyncio.Lock, int]] = {}
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

    @property
    def transport(self) -> MulticastTransport:
        return self._transport

    def method(self, name: str) -> Callable:
        def decorator(func: Handler) -> Handler:
            spec = inspect_handler(func, multicast=True)
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

    async def broadcast(self, method: str, data: Any = None) -> None:
        """Broadcast a JSON-RPC notification to all connected clients."""
        notification = RawWireRequest(method=method, params=data)
        await self._transport.broadcast(self._codec.encode(notification))

    async def _dispatch(
        self,
        request: RawWireRequest,
        client_id: str,
    ) -> WireResponse:


        if request.method not in self._handlers:
            logger.warning("RPC method not found")
            return WireErrorResponse(
                error=MethodNotFoundError("Method not found"),
                id=request.response_id,
            )

        handler = self._handlers[request.method]
        spec = self._specs[request.method]
        router_mw = self._router_middleware.get(request.method, [])

        if spec.params_type is not None and spec.has_params:
            try:
                request.params = self._codec.convert(request.params, spec.params_type)
            except CodecConversionError:
                return WireErrorResponse(
                    error=InvalidParamsError("Invalid params"),
                    id=request.response_id,
                )

        async def call_handler(
            req: RawWireRequest,
            ctx: AppContext,
        ) -> WireResponse:
            if spec.has_params and spec.receives_client_id:
                result = await handler(req.params, ctx, client_id)
            elif spec.has_params:
                result = await handler(req.params, ctx)
            elif spec.receives_client_id:
                result = await handler(ctx, client_id)
            else:
                result = await handler(ctx)

            if spec.return_type is not None:
                result = self._codec.convert(result, spec.return_type)

            return WireSuccessResponse(result=result, id=req.response_id)

        # Build chain: app middleware → router middleware → handler
        chain = call_handler

        for mw in reversed(router_mw):
            next_fn = chain
            chain = lambda req, ctx, n=next_fn, m=mw: m(req, ctx, n)  # type: ignore

        for mw in reversed(self._middleware):
            next_fn = chain
            chain = lambda req, ctx, n=next_fn, m=mw: m(req, ctx, n)  # type: ignore

        return await chain(request, self._ctx)

    @property
    def stats(self) -> dict:
        return self._stats.snapshot()

    async def stop(self):
        """Stop admission and let the listen loop drain owned work."""
        self._stopping.set()

    async def _next(self, transport):
        return await self._await_or_stop(transport.recv())

    async def _await_or_stop(self, operation):
        receive = asyncio.create_task(operation)
        stopped = asyncio.create_task(self._stopping.wait())
        try:
            done, _ = await asyncio.wait({receive, stopped}, return_when=asyncio.FIRST_COMPLETED)
            if stopped in done:
                raise ConnectionError('Application stopping')
            return receive.result()
        finally:
            for task in (receive, stopped):
                if not task.done():
                    task.cancel()
            await asyncio.gather(receive, stopped, return_exceptions=True)

    async def _serve_request(self, client_id, data, lock, slots, busy=False):
        try:
            if busy:
                async def reject(req):
                    raise PeerBusyError
                response = await process(self, data, reject, client_id)
            else:
                async with lock:
                    response = await process(self, data, lambda req: self._dispatch(req, client_id), client_id)
            if response is not None:
                try:
                    await self._transport.send(client_id, response)
                except Exception:
                    self._stats.failed += 1
        finally:
            _, count = self._peer_locks[client_id]
            if count == 1:
                del self._peer_locks[client_id]
            else:
                self._peer_locks[client_id] = (lock, count - 1)
            slots.release()

    async def _listen(self):
        slots = asyncio.Semaphore(self._max_concurrency)
        async with self._transport as t:
            try:
                while not self._stopping.is_set():
                    try:
                        await self._await_or_stop(slots.acquire())
                    except ConnectionError:
                        break
                    try:
                        client_id, data = await self._next(t)
                    except (asyncio.IncompleteReadError, ConnectionError):
                        slots.release()
                        break
                    lock, count = self._peer_locks.get(client_id, (asyncio.Lock(), 0))
                    self._peer_locks[client_id] = (lock, count + 1)
                    task = asyncio.create_task(self._serve_request(client_id, data, lock, slots, busy=count > 0))
                    self._tasks.add(task)
                    # Keep completed tasks until reaped so failures are observed.
                    for finished in tuple(self._tasks):
                        if finished.done():
                            self._tasks.remove(finished)
                            finished.result()
            finally:
                if self._tasks:
                    _, pending = await asyncio.wait(self._tasks, timeout=self._shutdown_timeout)
                    for task in pending:
                        task.cancel()
                    await asyncio.gather(*self._tasks, return_exceptions=True)
                    self._tasks.clear()

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
        if self._running or self._stopping.is_set():
            raise RuntimeError('Application is already running or stopped')
        self._running = True
        dependencies_started = False
        application_started = False
        try:
            async with asyncio.timeout(self._startup_timeout):
                await self._internal_startup()
                dependencies_started = True
                if self._on_startup:
                    self._ctx = await self._on_startup()
            application_started = True
            await self._listen()
        finally:
            self._stopping.set()
            try:
                if application_started and self._on_shutdown:
                    async with asyncio.timeout(self._shutdown_timeout):
                        await self._on_shutdown(self._ctx)
            finally:
                try:
                    if dependencies_started:
                        async with asyncio.timeout(self._shutdown_timeout):
                            await self._internal_shutdown()
                finally:
                    self._running = False
