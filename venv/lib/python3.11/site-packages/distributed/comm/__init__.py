from __future__ import annotations

from distributed.comm.addressing import (
    get_address_host,
    get_address_host_port,
    get_local_address_for,
    normalize_address,
    parse_address,
    parse_host_port,
    resolve_address,
    unparse_address,
    unparse_host_port,
)
from distributed.comm.core import Comm, CommClosedError, connect, listen
from distributed.comm.registry import backends
from distributed.comm.utils import get_tcp_server_address, get_tcp_server_addresses


def _register_transports():
    from distributed.comm import inproc, tcp, ws

    backends["tcp"] = tcp.TCPBackend()
    backends["tls"] = tcp.TLSBackend()

    try:
        # If `distributed-ucxx` is installed, it takes over the protocol="ucx" support
        import distributed_ucxx
    except ImportError:
        try:
            # Else protocol="ucx" will raise a deprecation warning and exception
            from distributed.comm import ucx
        except ImportError:
            pass


_register_transports()
