import asyncio
from typing import Self
import sys

from wire_rpc.transports.errors import InvalidFrameSizeError
from wire_rpc._validation import positive_timeout, positive_limit


async def _read_frame(reader, writer, max_frame_size):
    if writer.is_closing():
        raise ConnectionError("Stdio channel closed")
    try:
        length = int.from_bytes(await reader.readexactly(4), "big")
        if length == 0 or length > max_frame_size:
            raise InvalidFrameSizeError(max_frame_size)
        return await reader.readexactly(length)
    except BaseException:
        writer.close()
        raise


async def _write_frame(writer, data, max_frame_size):
    if len(data) == 0 or len(data) > max_frame_size:
        raise InvalidFrameSizeError(max_frame_size)
    if writer.is_closing():
        raise ConnectionError("Stdio channel closed")
    try:
        writer.write(len(data).to_bytes(4, "big") + data)
        await writer.drain()
    except BaseException:
        writer.close()
        raise

class StdIoTransport:

    def __init__(self, *cmd: str, max_frame_size: int = 16 * 1024 * 1024,
                 read_timeout=30.0, write_timeout=10.0, shutdown_timeout=5.0, kill_timeout=2.0):
        positive_limit("max_frame_size",max_frame_size)
        if not 0 < max_frame_size <= 0xFFFFFFFF:
            raise ValueError("Frame size must fit a positive 4-byte length")
        self._max_frame_size = max_frame_size
        self._read_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        for name,value in [('read_timeout',read_timeout),('write_timeout',write_timeout),
                           ('shutdown_timeout',shutdown_timeout),('kill_timeout',kill_timeout)]:
            positive_timeout(name,value)
        self._read_timeout, self._write_timeout = read_timeout, write_timeout
        self._shutdown_timeout, self._kill_timeout = shutdown_timeout, kill_timeout
        self._stderr_task = None
        self._stderr_bytes = 0
        self._close_lock = asyncio.Lock()
        self._operations = set()
        self._closing = False
        self._cmd = cmd
        self.proc: asyncio.subprocess.Process | None = None
        self.stdin: asyncio.StreamWriter | None = None
        self.stdout: asyncio.StreamReader | None = None
        self.stderr: asyncio.StreamReader | None = None

    async def connect(self):
        if self.proc is not None or self._closing:
            raise RuntimeError('Transport already connected or closed')
        self.proc = await asyncio.create_subprocess_exec(
            *self._cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        self.stdin = self.proc.stdin
        self.stdout = self.proc.stdout
        self.stderr = self.proc.stderr
        self._stderr_task = asyncio.create_task(self._drain_stderr())

    @property
    def max_message_size(self):
        return self._max_frame_size

    @property
    def stats(self):
        return {'stderr_bytes_discarded':self._stderr_bytes}

    async def _drain_stderr(self):
        while chunk := await self.stderr.read(65536):
            self._stderr_bytes += len(chunk)

    @staticmethod
    async def _discard(reader):
        while await reader.read(65536):
            pass

    async def close(self):
        async with self._close_lock:
            self._closing = True
            proc = self.proc
            if proc is None:
                return
            current = asyncio.current_task()
            tasks = [task for task in self._operations if task is not current]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.stdin is not None:
                self.stdin.close()
            drain = asyncio.create_task(self._discard(self.stdout))
            try:
                try:
                    async with asyncio.timeout(self._shutdown_timeout):
                        await proc.wait()
                except TimeoutError:
                    if proc.returncode is None:
                        proc.terminate()
                    try:
                        async with asyncio.timeout(self._kill_timeout):
                            await proc.wait()
                    except TimeoutError:
                        if proc.returncode is None:
                            proc.kill()
                        async with asyncio.timeout(self._kill_timeout):
                            await proc.wait()
            finally:
                # Cancellation of close must not leave a running child behind.
                if proc.returncode is None:
                    proc.kill()
                    async with asyncio.timeout(self._kill_timeout):
                        await proc.wait()
                for task in (drain, self._stderr_task):
                    if task is not None:
                        task.cancel()
                await asyncio.gather(drain, *([self._stderr_task] if self._stderr_task else []), return_exceptions=True)
                self.stdin = self.stdout = self.stderr = self.proc = None
                self._stderr_task = None

    async def recv(self) -> bytes:

        if self.stdout is None or self.stdin is None:
            raise RuntimeError("Can not receive bytes without calling .connect() first.")
        
        if self._closing:
            raise ConnectionError('Transport closed')
        task = asyncio.current_task()
        self._operations.add(task)
        try:
            async with self._read_lock:
                async with asyncio.timeout(self._read_timeout):
                    return await _read_frame(self.stdout, self.stdin, self._max_frame_size)
        finally:
            self._operations.discard(task)

    async def send(self, data: bytes) -> None:

        if self.stdin is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        if self._closing:
            raise ConnectionError('Transport closed')
        task = asyncio.current_task()
        self._operations.add(task)
        try:
            async with self._write_lock:
                async with asyncio.timeout(self._write_timeout):
                    await _write_frame(self.stdin, data, self._max_frame_size)
        finally:
            self._operations.discard(task)

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: object   
    ) -> None:
        await self.close()

class StdIoServerTransport:

    def __init__(self, *, max_frame_size: int = 16 * 1024 * 1024, read_timeout=30.0, write_timeout=10.0):
        positive_limit("max_frame_size",max_frame_size)
        if not 0 < max_frame_size <= 0xFFFFFFFF:
            raise ValueError("Frame size must fit a positive 4-byte length")
        positive_timeout('read_timeout',read_timeout)
        positive_timeout('write_timeout',write_timeout)
        self._read_timeout, self._write_timeout = read_timeout, write_timeout
        self._read_transport = None
        self._max_frame_size = max_frame_size
        self._read_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self.writer: asyncio.StreamWriter | None = None
        self.reader: asyncio.StreamReader | None = None

    async def connect(self):

        loop = asyncio.get_event_loop()

        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        self._read_transport, _ = await loop.connect_read_pipe(lambda: protocol, sys.stdin.buffer)
        self.reader = reader

        write_protocol = asyncio.StreamReaderProtocol(asyncio.StreamReader())
        try:
            write_transport, _ = await loop.connect_write_pipe(lambda: write_protocol, sys.stdout.buffer)
            self.writer = asyncio.StreamWriter(write_transport, write_protocol, None, loop)
        except BaseException:
            self._read_transport.close()
            self._read_transport = None
            raise
        

    @property
    def max_message_size(self):
        return self._max_frame_size

    async def close(self):
        if self.writer is not None:
            self.writer.close()
            try:
                async with asyncio.timeout(self._write_timeout):
                    await self.writer.wait_closed()
            except (TimeoutError, ConnectionError):
                pass
        if self._read_transport is not None:
            self._read_transport.close()
            self._read_transport = None
        self.writer = self.reader = None

    async def recv(self) -> bytes:

        if self.reader is None or self.writer is None:
            raise RuntimeError("Can not receive bytes without calling .connect() first.")
        
        async with self._read_lock:
            async with asyncio.timeout(self._read_timeout):
                return await _read_frame(self.reader, self.writer, self._max_frame_size)

    async def send(self, data: bytes) -> None:

        if self.writer is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        async with self._write_lock:
            async with asyncio.timeout(self._write_timeout):
                await _write_frame(self.writer, data, self._max_frame_size)

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: object   
    ) -> None:
        await self.close()


__all__ = [
    "StdIoTransport",
    "StdIoServerTransport"
]