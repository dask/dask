from __future__ import print_function, division, absolute_import

from .core import connect, listen, Comm, CommClosedError


def is_zmq_enabled():
    """
    Whether the experimental ZMQ transport is enabled by a special
    config option.
    """
    from ..config import config
    return bool(config.get('experimental-zmq'))


def _register_transports():
    from . import tcp

    if is_zmq_enabled():
        try:
            import zmq as _zmq
        except ImportError:
            pass
        else:
            # This registers the ZMQ event loop, event if ZMQ is unused
            from . import zmq


_register_transports()
