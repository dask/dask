from __future__ import print_function, division, absolute_import

import six

from ..config import config
from ..utils import ensure_ip


DEFAULT_SCHEME = config.get('default-scheme', 'tcp')


def parse_address(addr):
    """
    Split address into its scheme and scheme-dependent location string.
    """
    if not isinstance(addr, six.string_types):
        raise TypeError("expected str, got %r" % addr.__class__.__name__)
    scheme, sep, loc = addr.rpartition('://')
    if not sep:
        scheme = DEFAULT_SCHEME
    return scheme, loc


def unparse_address(scheme, loc):
    """
    Undo parse_address().
    """
    return '%s://%s' % (scheme, loc)


def normalize_address(addr):
    """
    Canonicalize address, adding a default scheme if necessary.
    """
    return unparse_address(*parse_address(addr))


def parse_host_port(address, default_port=None):
    """
    Parse an endpoint address given in the form "host:port".
    """
    if isinstance(address, tuple):
        return address
    if address.startswith('tcp:'):
        address = address[4:]

    def _fail():
        raise ValueError("invalid address %r" % (address,))

    def _default():
        if default_port is None:
            raise ValueError("missing port number in address %r" % (address,))
        return default_port

    if address.startswith('['):
        host, sep, tail = address[1:].partition(']')
        if not sep:
            _fail()
        if not tail:
            port = _default()
        else:
            if not tail.startswith(':'):
                _fail()
            port = tail[1:]
    else:
        host, sep, port = address.partition(':')
        if not sep:
            port = _default()
        elif ':' in host:
            _fail()

    return host, int(port)


def unparse_host_port(host, port=None):
    """
    Undo parse_host_port().
    """
    if ':' in host and not host.startswith('['):
        host = '[%s]' % host
    if port:
        return '%s:%s' % (host, port)
    else:
        return host


def get_address_host_port(addr):
    """
    Get a (host, port) tuple out of the given address.
    """
    scheme, loc = parse_address(addr)
    if scheme not in ('tcp', 'zmq'):
        raise ValueError("don't know how to extract host and port "
                         "for address %r" % (addr,))
    return parse_host_port(loc)


def resolve_address(addr):
    """
    Apply scheme-specific address resolution to *addr*, ensuring
    all symbolic references are replaced with concrete location
    specifiers.

    In practice, this means hostnames are resolved to IP addresses.
    """
    # XXX circular import; reorganize APIs into a distributed.comms.addressing module?
    #from ..utils import ensure_ip
    scheme, loc = parse_address(addr)
    if scheme not in ('tcp', 'zmq'):
        return addr

    host, port = parse_host_port(loc)
    loc = unparse_host_port(ensure_ip(host), port)
    addr = unparse_address(scheme, loc)
    return addr
