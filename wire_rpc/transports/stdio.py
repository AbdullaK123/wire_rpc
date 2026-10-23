"""Transports over a child process's stdin/stdout.

Each message is a frame: a 4-byte big-endian length followed by that many bytes.
``StdIoTransport`` launches and talks to a child; ``StdIoServerTransport`` is the
child's side, reading its own stdin and writing its own stdout.
"""

import asyncio
import contextlib
import os
import queue
import sys
import threading
from typing import Any, Self

from wire_rpc._validation import positive_limit, positive_timeout
from wire_rpc.transports.errors import InvalidFrameSizeError


async def _read_frame(reader, writer, max_frame_size):
    """Read one length-prefixed frame, closing ``writer`` on any failure.

    Raises:
        InvalidFrameSizeError: The length is zero or above ``max_frame_size``.
        asyncio.IncompleteReadError: The stream ended mid-frame.
    """
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
    """Write one length-prefixed frame, closing ``writer`` on any failure.

    Raises:
        InvalidFrameSizeError: ``data`` is empty or above ``max_frame_size``.
    """
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
    """Launches a child process and exchanges frames over its stdin/stdout.

    The child's stderr is read and discarded so a chatty child cannot block on a
    full pipe. Closing stops the child, escalating from closing stdin to terminate
    to kill.

    Args:
        *cmd: The program and its arguments.
        max_frame_size: Largest frame in bytes, at most 4 GiB - 1.
        read_timeout: Seconds ``recv`` waits for a frame.
        write_timeout: Seconds ``send`` waits for the pipe to drain.
        shutdown_timeout: Seconds the child gets to exit after stdin closes.
        kill_timeout: Seconds to wait after terminate, and again after kill.
        trusted_principal: Identity reported for every request, since a child you
            launched yourself needs no further authentication.
    """

    def __init__(self, *cmd: str, max_frame_size: int = 16 * 1024 * 1024,
                 read_timeout=30.0, write_timeout=10.0, shutdown_timeout=5.0, kill_timeout=2.0, trusted_principal: str | None = None):
        from wire_rpc.auth._util import identity
        self._trusted_principal = None if trusted_principal is None else identity(trusted_principal)
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
        self._stderr_task: asyncio.Task[None] | None = None
        self._stderr_bytes = 0
        self._close_lock = asyncio.Lock()
        self._operations: set[asyncio.Task[Any]] = set()
        self._closing = False
        self._cmd = cmd
        self.proc: asyncio.subprocess.Process | None = None
        self.stdin: asyncio.StreamWriter | None = None
        self.stdout: asyncio.StreamReader | None = None
        self.stderr: asyncio.StreamReader | None = None

    async def connect(self):
        """Launch the child process.

        Raises:
            RuntimeError: Already connected or closed.
        """
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

    async def get_principal(self, client_id=None) -> str | None:
        """Return the configured trusted principal, if any."""
        return self._trusted_principal

    @property
    def max_message_size(self):
        """Largest frame in bytes."""
        return self._max_frame_size

    @property
    def stats(self):
        """Diagnostics; currently how many stderr bytes have been discarded."""
        return {'stderr_bytes_discarded':self._stderr_bytes}

    async def _drain_stderr(self) -> None:
        """Read and count the child's stderr until it closes."""
        stderr = self.stderr
        if stderr is None:
            return
        while chunk := await stderr.read(65536):
            self._stderr_bytes += len(chunk)

    @staticmethod
    async def _discard(reader: asyncio.StreamReader | None):
        """Read and drop everything left on ``reader``."""
        if reader is None:
            return
        while await reader.read(65536):
            pass

    async def close(self):
        """Cancel pending reads and writes, then stop the child and release its pipes."""
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
                for drain_task in (drain, self._stderr_task):
                    if drain_task is not None:
                        drain_task.cancel()
                await asyncio.gather(drain, *([self._stderr_task] if self._stderr_task else []), return_exceptions=True)
                self.stdin = self.stdout = self.stderr = self.proc = None
                self._stderr_task = None

    async def recv(self) -> bytes:

        """Read the next frame from the child.

        Raises:
            RuntimeError: ``connect()`` was not called.
            ConnectionError: The transport is closing.
            TimeoutError: No frame within ``read_timeout``.
        """
        if self.stdout is None or self.stdin is None:
            raise RuntimeError("Can not receive bytes without calling .connect() first.")
        
        if self._closing:
            raise ConnectionError('Transport closed')
        task = asyncio.current_task()
        if task is not None:
            self._operations.add(task)
        try:
            async with self._read_lock:
                async with asyncio.timeout(self._read_timeout):
                    return await _read_frame(self.stdout, self.stdin, self._max_frame_size)
        finally:
            if task is not None:
                self._operations.discard(task)

    async def send(self, data: bytes) -> None:

        """Write one frame to the child.

        Raises:
            RuntimeError: ``connect()`` was not called.
            ConnectionError: The transport is closing.
            TimeoutError: The pipe did not drain within ``write_timeout``.
        """
        if self.stdin is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        if self._closing:
            raise ConnectionError('Transport closed')
        task = asyncio.current_task()
        if task is not None:
            self._operations.add(task)
        try:
            async with self._write_lock:
                async with asyncio.timeout(self._write_timeout):
                    await _write_frame(self.stdin, data, self._max_frame_size)
        finally:
            if task is not None:
                self._operations.discard(task)

    async def __aenter__(self) -> Self:
        """Connect and return this transport."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: object   
    ) -> None:
        """Close the transport."""
        await self.close()

class _StdinPump:
    """Feeds a file descriptor into a StreamReader from a daemon thread.

    The reader treats this as its transport, so it pauses the thread when its
    buffer is full and resumes it when a read needs more data. Uses ``os.read``
    rather than a buffered file so no lock is held if the process exits mid-read.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, reader: asyncio.StreamReader, fd: int):
        self._loop, self._reader, self._fd = loop, reader, fd
        self._resume = threading.Event()
        self._resume.set()
        self._closed = False
        reader.set_transport(self)  # pyrefly: ignore[bad-argument-type]
        threading.Thread(target=self._run, name='wire-rpc-stdin', daemon=True).start()

    def pause_reading(self) -> None:
        self._resume.clear()

    def resume_reading(self) -> None:
        self._resume.set()

    def close(self) -> None:
        self._closed = True
        self._resume.set()

    def _run(self) -> None:
        while True:
            self._resume.wait()
            if self._closed:
                return
            try:
                chunk = os.read(self._fd, 65536)
            except OSError as exc:
                self._post(self._reader.set_exception, ConnectionError(f'stdin failed: {exc}'))
                return
            if not chunk:
                self._post(self._reader.feed_eof)
                return
            self._post(self._reader.feed_data, chunk)

    def _post(self, callback, *args) -> None:
        if self._closed:
            return
        try:
            self._loop.call_soon_threadsafe(callback, *args)
        except RuntimeError:  # the loop is closed
            self._closed = True


class _StdoutWriter:
    """The part of the StreamWriter API ``_write_frame`` uses, writing from a daemon thread.

    ``drain`` waits until the bytes written so far have reached the pipe, which is
    the back-pressure a pipe transport would give. ``close`` flushes queued writes,
    then closes ``file``.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, file):
        self._loop, self._file = loop, file
        self._fd = file.fileno()
        self._queue: queue.SimpleQueue[tuple[bytes, asyncio.Future[OSError | None]] | None] = queue.SimpleQueue()
        self._closing = False
        self._last: asyncio.Future[OSError | None] | None = None
        self._finished: asyncio.Future[None] = loop.create_future()
        threading.Thread(target=self._run, name='wire-rpc-stdout', daemon=True).start()

    def write(self, data: bytes) -> None:
        if self._closing:
            raise ConnectionError('Stdio channel closed')
        written: asyncio.Future[OSError | None] = self._loop.create_future()
        self._last = written
        self._queue.put((data, written))

    async def drain(self) -> None:
        if self._last is not None:
            # Shielded: a timed-out drain leaves the write running, and the caller closes the channel.
            error = await asyncio.shield(self._last)
            if error is not None:
                self._closing = True
                raise error

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        if not self._closing:
            self._closing = True
            self._queue.put(None)

    async def wait_closed(self) -> None:
        await asyncio.shield(self._finished)

    def _run(self) -> None:
        error: OSError | None = None
        while (item := self._queue.get()) is not None:
            data, written = item
            if error is None:
                try:
                    view = memoryview(data)
                    while view:
                        view = view[os.write(self._fd, view):]
                except OSError as exc:
                    error = exc
            self._settle(written, error)
        self._settle(self._finished, None)

    def _settle(self, future: asyncio.Future, value) -> None:
        def settle():
            if future is self._finished:
                with contextlib.suppress(OSError):
                    self._file.close()
            if not future.done():
                future.set_result(value)
        try:
            self._loop.call_soon_threadsafe(settle)
        except RuntimeError:  # the loop is closed
            pass


class StdIoServerTransport:
    """The child-process side of a stdio connection, using this process's stdin/stdout.

    Nothing else in the process may write to stdout, or the frames will be corrupted.
    stdin and stdout are serviced by two daemon threads, since not every event loop
    can watch them directly (Windows' cannot).

    Args:
        max_frame_size: Largest frame in bytes, at most 4 GiB - 1.
        read_timeout: Seconds ``recv`` waits for a frame.
        write_timeout: Seconds ``send`` and ``close`` wait for stdout.
        trusted_principal: Identity reported for every request.
    """

    def __init__(self, *, max_frame_size: int = 16 * 1024 * 1024, read_timeout=30.0, write_timeout=10.0, trusted_principal: str | None = None):
        from wire_rpc.auth._util import identity
        self._trusted_principal = None if trusted_principal is None else identity(trusted_principal)
        positive_limit("max_frame_size",max_frame_size)
        if not 0 < max_frame_size <= 0xFFFFFFFF:
            raise ValueError("Frame size must fit a positive 4-byte length")
        positive_timeout('read_timeout',read_timeout)
        positive_timeout('write_timeout',write_timeout)
        self._read_timeout, self._write_timeout = read_timeout, write_timeout
        self._stdin_pump: _StdinPump | None = None
        self._max_frame_size = max_frame_size
        self._read_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self.writer: _StdoutWriter | None = None
        self.reader: asyncio.StreamReader | None = None

    async def connect(self):

        """Attach to this process's stdin and stdout."""
        loop = asyncio.get_running_loop()
        stdin_fd = sys.stdin.fileno()
        self.writer = _StdoutWriter(loop, sys.stdout.buffer)
        self.reader = asyncio.StreamReader()
        self._stdin_pump = _StdinPump(loop, self.reader, stdin_fd)


    async def get_principal(self, client_id=None) -> str | None:
        """Return the configured trusted principal, if any."""
        return self._trusted_principal

    @property
    def max_message_size(self):
        """Largest frame in bytes."""
        return self._max_frame_size

    async def close(self):
        """Flush and close stdout, then stop reading stdin."""
        if self.writer is not None:
            self.writer.close()
            try:
                async with asyncio.timeout(self._write_timeout):
                    await self.writer.wait_closed()
            except (TimeoutError, ConnectionError):
                pass
        if self._stdin_pump is not None:
            self._stdin_pump.close()
            self._stdin_pump = None
        self.writer = self.reader = None

    async def recv(self) -> bytes:

        """Read the next frame from stdin.

        Raises:
            RuntimeError: ``connect()`` was not called.
            TimeoutError: No frame within ``read_timeout``.
        """
        if self.reader is None or self.writer is None:
            raise RuntimeError("Can not receive bytes without calling .connect() first.")
        
        async with self._read_lock:
            async with asyncio.timeout(self._read_timeout):
                return await _read_frame(self.reader, self.writer, self._max_frame_size)

    async def send(self, data: bytes) -> None:

        """Write one frame to stdout.

        Raises:
            RuntimeError: ``connect()`` was not called.
            TimeoutError: stdout did not drain within ``write_timeout``.
        """
        if self.writer is None:
            raise RuntimeError("Can not send bytes without calling .connect() first.")

        async with self._write_lock:
            async with asyncio.timeout(self._write_timeout):
                await _write_frame(self.writer, data, self._max_frame_size)

    async def __aenter__(self) -> Self:
        """Connect and return this transport."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None, 
        exc_val: BaseException | None, 
        exc_tb: object   
    ) -> None:
        """Close the transport."""
        await self.close()


__all__ = [
    "StdIoTransport",
    "StdIoServerTransport"
]