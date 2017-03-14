from __future__ import print_function, division, absolute_import

from .addressing import (parse_address, unparse_address,
                         normalize_address, parse_host_port,
                         unparse_host_port, resolve_address,
                         get_address_host_port, get_address_host,
                         get_local_address_for,
                         )
from .core import connect, listen, Comm, CommClosedError


def is_zmq_enabled():
    """
    Whether the experimental ZMQ transport is enabled by a special
    config option.
    """
    from ..config import config
    return bool(config.get('experimental-zmq'))


def _register_transports():
    from . import inproc
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
