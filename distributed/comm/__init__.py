from .addressing import (
    parse_address,
    unparse_address,
    normalize_address,
    parse_host_port,
    unparse_host_port,
    resolve_address,
    get_address_host_port,
    get_address_host,
    get_local_address_for,
)
from .core import connect, listen, Comm, CommClosedError
from .utils import get_tcp_server_address


def _register_transports():
    from . import inproc
    from . import tcp

    try:
        from . import ucx
    except ImportError:
        pass


_register_transports()
