"""Library logging through Python's standard ``logging`` module.

Two loggers:
    ``wire_rpc``: startup, connections, and problems worth a look.
    ``wire_rpc.access``: one INFO line per request with the method, the outcome
    (``ok``, ``notification`` or the JSON-RPC error code) and the duration.
    Params, results and exception text are never logged.

Importing wire_rpc configures nothing. Warnings and errors still reach stderr
through Python's fallback handler, and applications control everything else with
normal ``logging`` configuration. ``App.run()`` and ``MulticastApp.run()`` call
``configure_logging`` so a script started from a terminal shows its logs, the way
a server runner such as Uvicorn does.
"""
import logging
import sys

logger = logging.getLogger('wire_rpc')
access_logger = logging.getLogger('wire_rpc.access')

_LEVELS = ('debug', 'info', 'warning', 'error', 'critical')


def configure_logging(level: str = 'info') -> None:
    """Print wire_rpc logs to stderr at ``level``, unless logging is already set up.

    Does nothing if the application has configured the root logger or the
    ``wire_rpc`` logger, so it never overrides or duplicates your own setup. Logs
    go to stderr, never stdout, so a stdio server's frames stay intact.

    Raises:
        ValueError: ``level`` is not debug, info, warning, error or critical.
    """
    if level not in _LEVELS:
        raise ValueError(f'log_level must be one of {", ".join(_LEVELS)}')
    if logging.getLogger().handlers or logger.handlers:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s %(name)s: %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(level.upper())


__all__ = ['access_logger', 'configure_logging', 'logger']
