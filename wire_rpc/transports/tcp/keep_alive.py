"""OS-level TCP keepalive, which detects peers that vanished without closing."""

import asyncio
import socket
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TcpKeepaliveConfig:
    """How the OS probes an idle connection.

    Args:
        idle: Seconds of silence before the first probe.
        interval: Seconds between probes.
        count: Unanswered probes before the connection is dropped.
    """
    idle: int = 60
    interval: int = 15
    count: int = 4

    def __post_init__(self) -> None:
        """Reject non-positive settings."""
        if self.idle <= 0:
            raise ValueError("Keepalive idle must be greater than zero")
        if self.interval <= 0:
            raise ValueError("Keepalive interval must be greater than zero")
        if self.count <= 0:
            raise ValueError("Keepalive count must be greater than zero")


def configure_keepalive(
    writer: asyncio.StreamWriter,
    config: TcpKeepaliveConfig,
) -> None:
    """Enable keepalive on ``writer``'s socket, applying each option the platform supports.

    Closes ``writer`` and re-raises if configuration fails.
    """
    try:
        sock = writer.get_extra_info("socket")

        if sock is None:
            raise RuntimeError("TCP transport does not expose its socket")

        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_KEEPALIVE,
            1,
        )

        tcp_keepidle = getattr(socket, "TCP_KEEPIDLE", None)
        tcp_keepalive = getattr(socket, "TCP_KEEPALIVE", None)
        tcp_keepintvl = getattr(socket, "TCP_KEEPINTVL", None)
        tcp_keepcnt = getattr(socket, "TCP_KEEPCNT", None)

        if tcp_keepidle is not None:
            sock.setsockopt(
                socket.IPPROTO_TCP,
                tcp_keepidle,
                config.idle,
            )
        elif tcp_keepalive is not None:
            sock.setsockopt(
                socket.IPPROTO_TCP,
                tcp_keepalive,
                config.idle,
            )

        if tcp_keepintvl is not None:
            sock.setsockopt(
                socket.IPPROTO_TCP,
                tcp_keepintvl,
                config.interval,
            )

        if tcp_keepcnt is not None:
            sock.setsockopt(
                socket.IPPROTO_TCP,
                tcp_keepcnt,
                config.count,
            )
    except BaseException:
        writer.close()
        raise


__all__ = ["TcpKeepaliveConfig", "configure_keepalive"]
