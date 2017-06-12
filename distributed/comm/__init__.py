from __future__ import print_function, division, absolute_import

from .addressing import (parse_address, unparse_address,
                         normalize_address, parse_host_port,
                         unparse_host_port, resolve_address,
                         get_address_host_port, get_address_host,
                         get_local_address_for,
                         )
from .core import connect, listen, Comm, CommClosedError


def _register_transports():
    from . import inproc
    from . import tcp


_register_transports()
