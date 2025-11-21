from __future__ import annotations

import logging

from dask.sizeof import sizeof
from dask.utils import format_bytes, typename

logger = logging.getLogger(__name__)


def safe_sizeof(obj: object, default_size: float = 1e6) -> int:
    """Safe variant of sizeof that captures and logs exceptions

    This returns a default size of 1e6 if the sizeof function fails
    """
    try:
        return sizeof(obj)
    except Exception:
        error_message = (
            f"Sizeof calculation for object of type '{typename(obj)}' failed. "
            f"Defaulting to {format_bytes(int(default_size))}",
        )
        logger.warning(error_message)
        logger.debug(error_message, exc_info=True)
        return int(default_size)
