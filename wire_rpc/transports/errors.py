

"""Errors raised by transports."""

class TransportError(Exception):
    """Base class for transport failures."""

class InvalidFrameSizeError(TransportError):
    """A message was empty or larger than the transport's maximum size."""

    def __init__(self, max_frame_size: int):
        super().__init__(
            f"Invalid frame size. Must be less than max frame size of {max_frame_size} bytes"
        )

class IdleTimeoutError(TransportError):
    """A connection was closed after being idle too long."""
