import logging

from dask.sizeof import sizeof

logger = logging.getLogger(__name__)


def safe_sizeof(obj, default_size=1e6):
    """Safe variant of sizeof that captures and logs exceptions

    This returns a default size of 1e6 if the sizeof function fails
    """
    try:
        return sizeof(obj)
    except Exception:
        logger.warning("Sizeof calculation failed.  Defaulting to 1MB", exc_info=True)
        return int(default_size)
