"""Library logging through loguru, disabled by default.

Call ``logger.enable("wire_rpc")`` in an application to see the library's logs.
"""

from loguru import logger

logger.disable("wire_rpc") 