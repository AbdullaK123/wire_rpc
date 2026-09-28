import asyncio
from typing import Self
import sys

from wire_rpc.transports.errors import InvalidFrameSizeError


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

    def __init__(self, *cmd: str, max_frame_size: int = 16 * 1024 * 1024):
        if not 0 < max_frame_size <= 0xFFFFFFFF:
            raise ValueError("Frame size must fit a positive 4-byte length")
        self._max_frame_size = max_frame_size
        self._read_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._cmd = cmd
        self.proc: asyncio.subprocess.Process | None = None
        self.stdin: asyncio.StreamWriter | None = None
        self.stdout: asyncio.StreamReader | None = None
        self.stderr: asyncio.StreamReader | None = None

    async def connect(self):

        self.proc = await asyncio.create_subprocess_exec(
            *self._cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        self.stdin = self.proc.stdin
        self.stdout = self.proc.stdout
        self.stderr = self.proc.stderr

    async def close(self):

        if self.stdin is None or self.proc is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        self.stdin.close()
        await self.stdin.wait_closed()
        await self.proc.wait()

        self.stdin = None
        self.stdout = None
        self.stderr = None
        self.proc = None

    async def recv(self) -> bytes:

        if self.stdout is None or self.stdin is None:
            raise RuntimeError("Can not receive bytes without calling .connect() first.")
        
        async with self._read_lock:
            return await _read_frame(self.stdout, self.stdin, self._max_frame_size)

    async def send(self, data: bytes) -> None:

        if self.stdin is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        async with self._write_lock:
            await _write_frame(self.stdin, data, self._max_frame_size)

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

    def __init__(self, *, max_frame_size: int = 16 * 1024 * 1024):
        if not 0 < max_frame_size <= 0xFFFFFFFF:
            raise ValueError("Frame size must fit a positive 4-byte length")
        self._max_frame_size = max_frame_size
        self._read_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self.writer: asyncio.StreamWriter | None = None
        self.reader: asyncio.StreamReader | None = None

    async def connect(self):

        loop = asyncio.get_event_loop()

        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin.buffer)
        self.reader = reader

        write_transport, _ = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin, sys.stdout.buffer
        )

        self.writer = asyncio.StreamWriter(write_transport, protocol, reader, loop)
        

    async def close(self):

        if self.writer is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        self.writer.close()
        await self.writer.wait_closed()

        self.writer = None
        self.reader = None

    async def recv(self) -> bytes:

        if self.reader is None or self.writer is None:
            raise RuntimeError("Can not receive bytes without calling .connect() first.")
        
        async with self._read_lock:
            return await _read_frame(self.reader, self.writer, self._max_frame_size)

    async def send(self, data: bytes) -> None:

        if self.writer is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        async with self._write_lock:
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