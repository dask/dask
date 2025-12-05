from __future__ import annotations

import itertools

import dask

from distributed.comm import registry
from distributed.utils import get_ip_interface


def parse_address(addr: str, strict: bool = False) -> tuple[str, str]:
    """
    Split address into its scheme and scheme-dependent location string.

    >>> parse_address('tcp://127.0.0.1')
    ('tcp', '127.0.0.1')

    If strict is set to true the address must have a scheme.
    """
    if not isinstance(addr, str):
        raise TypeError("expected str, got %r" % addr.__class__.__name__)
    scheme, sep, loc = addr.rpartition("://")
    if strict and not sep:
        msg = (
            "Invalid url scheme. "
            "Must include protocol like tcp://localhost:8000. "
            "Got %s" % addr
        )
        raise ValueError(msg)
    if not sep:
        scheme = dask.config.get("distributed.comm.default-scheme")
    return scheme, loc


def unparse_address(scheme: str, loc: str) -> str:
    """
    Undo parse_address().

    >>> unparse_address('tcp', '127.0.0.1')
    'tcp://127.0.0.1'
    """
    return f"{scheme}://{loc}"


def normalize_address(addr: str) -> str:
    """
    Canonicalize address, adding a default scheme if necessary.

    >>> normalize_address('tls://[::1]')
    'tls://[::1]'
    >>> normalize_address('[::1]')
    'tcp://[::1]'
    """
    return unparse_address(*parse_address(addr))


def parse_host_port(
    address: str | tuple[str, int], default_port: str | int | None = None
) -> tuple[str, int]:
    """
    Parse an endpoint address given in the form "host:port".
    """
    if isinstance(address, tuple):
        return address

    def _fail():
        raise ValueError(
            f"invalid address {address!r}; maybe: ipv6 needs brackets like [::1]"
        )

    def _default():
        if default_port is None:
            raise ValueError(f"missing port number in address {address!r}")
        return default_port

    if "://" in address:
        _, address = address.split("://")
    if address.startswith("["):
        # IPv6 notation: '[addr]:port' or '[addr]'.
        # The address may contain multiple colons.
        host, sep, tail = address[1:].partition("]")
        if not sep:
            _fail()
        if not tail:
            port = _default()
        else:
            if not tail.startswith(":"):
                _fail()
            port = tail[1:]
    else:
        # Generic notation: 'addr:port' or 'addr'.
        host, sep, port = address.rpartition(":")
        if not sep:
            host = port
            port = _default()
        elif ":" in host:
            _fail()

    return host, int(port)


def unparse_host_port(host: str, port: int | None = None) -> str:
    """
    Undo parse_host_port().
    """
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if port is not None:
        return f"{host}:{port}"
    else:
        return host


def get_address_host_port(addr: str, strict: bool = False) -> tuple[str, int]:
    """
    Get a (host, port) tuple out of the given address.
    For definition of strict check parse_address
    ValueError is raised if the address scheme doesn't allow extracting
    the requested information.

    >>> get_address_host_port('tcp://1.2.3.4:80')
    ('1.2.3.4', 80)
    >>> get_address_host_port('tcp://[::1]:80')
    ('::1', 80)
    """
    scheme, loc = parse_address(addr, strict=strict)
    backend = registry.get_backend(scheme)
    try:
        return backend.get_address_host_port(loc)
    except NotImplementedError:
        raise ValueError(
            f"don't know how to extract host and port for address {addr!r}"
        )


def get_address_host(addr: str) -> str:
    """
    Return a hostname / IP address identifying the machine this address
    is located on.

    In contrast to get_address_host_port(), this function should always
    succeed for well-formed addresses.

    >>> get_address_host('tcp://1.2.3.4:80')
    '1.2.3.4'
    """
    scheme, loc = parse_address(addr)
    backend = registry.get_backend(scheme)
    return backend.get_address_host(loc)


def get_local_address_for(addr: str) -> str:
    """
    Get a local listening address suitable for reaching *addr*.

    For instance, trying to reach an external TCP address will return
    a local TCP address that's routable to that external address.

    >>> get_local_address_for('tcp://8.8.8.8:1234')
    'tcp://192.168.1.68'
    >>> get_local_address_for('tcp://127.0.0.1:1234')
    'tcp://127.0.0.1'
    """
    scheme, loc = parse_address(addr)
    backend = registry.get_backend(scheme)
    return unparse_address(scheme, backend.get_local_address_for(loc))


def resolve_address(addr: str) -> str:
    """
    Apply scheme-specific address resolution to *addr*, replacing
    all symbolic references with concrete location specifiers.

    In practice, this can mean hostnames are resolved to IP addresses.

    >>> resolve_address('tcp://localhost:8786')
    'tcp://127.0.0.1:8786'
    """
    scheme, loc = parse_address(addr)
    backend = registry.get_backend(scheme)
    return unparse_address(scheme, backend.resolve_address(loc))


def uri_from_host_port(
    host_arg: str | None, port_arg: str | None, default_port: int
) -> str:
    """
    Process the *host* and *port* CLI options.
    Return a URI.
    """
    # Much of distributed depends on a well-known IP being assigned to
    # each entity (Worker, Scheduler, etc.), so avoid "universal" addresses
    # like '' which would listen on all registered IPs and interfaces.
    scheme, loc = parse_address(host_arg or "")

    host, port = parse_host_port(
        loc, port_arg if port_arg is not None else default_port
    )

    # Note `port = 0` means "choose a random port"
    if port != 0 and port_arg and port != int(port_arg):
        raise ValueError(
            "port number given twice in options: "
            f"host {host_arg} and port {port_arg}"
        )

    loc = unparse_host_port(host, port)
    addr = unparse_address(scheme, loc)

    return addr


def addresses_from_user_args(  # type: ignore[no-untyped-def]
    host=None,
    port=None,
    interface=None,
    protocol=None,
    peer=None,
    security=None,
    default_port=0,
) -> list:
    """Get a list of addresses if the inputs are lists

    This is like ``address_from_user_args`` except that it also accepts lists
    for some of the arguments.  If these arguments are lists then it will map
    over them accordingly.

    Examples
    --------
    >>> addresses_from_user_args(host="127.0.0.1", protocol=["inproc", "tcp"])
    ["inproc://127.0.0.1:", "tcp://127.0.0.1:"]
    """

    def listify(obj):
        if isinstance(obj, (tuple, list)):
            return obj
        else:
            return itertools.repeat(obj)

    if any(isinstance(x, (tuple, list)) for x in (host, port, interface, protocol)):
        return [
            address_from_user_args(
                host=h,
                port=p,
                interface=i,
                protocol=pr,
                peer=peer,
                security=security,
                default_port=default_port,
            )
            for h, p, i, pr in zip(*map(listify, (host, port, interface, protocol)))
        ]
    else:
        return [
            address_from_user_args(
                host, port, interface, protocol, peer, security, default_port
            )
        ]


def address_from_user_args(  # type: ignore[no-untyped-def]
    host=None,
    port=None,
    interface=None,
    protocol=None,
    peer=None,
    security=None,
    default_port=0,
) -> str:
    """Get an address to listen on from common user provided arguments"""

    if security and security.require_encryption and not protocol:
        protocol = "tls"

    if protocol and protocol.endswith("://"):
        protocol, _, _ = protocol.rpartition("://")

    if protocol == "inplace":
        if host or port or interface:
            raise ValueError(
                "Can not specify inproc protocol and host or port or interface"
            )
        else:
            return "inproc://"

    if interface:
        if host:
            raise ValueError("Can not specify both interface and host", interface, host)
        else:
            host = get_ip_interface(interface)

    if protocol and host and "://" not in host:
        host = protocol + "://" + host

    if host or port:
        addr = uri_from_host_port(host, port, default_port)
    else:
        addr = ""

    if protocol:
        addr = protocol + "://" + addr.split("://")[-1]

    return addr
