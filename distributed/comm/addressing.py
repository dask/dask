from __future__ import print_function, division, absolute_import

import six

from ..config import config
from . import registry


DEFAULT_SCHEME = config.get('default-scheme', 'tcp')


def parse_address(addr):
    """
    Split address into its scheme and scheme-dependent location string.

    >>> parse_address('tcp://127.0.0.1')
    ('tcp', '127.0.0.1')
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

    >>> unparse_address('tcp', '127.0.0.1')
    'tcp://127.0.0.1'
    """
    return '%s://%s' % (scheme, loc)


def normalize_address(addr):
    """
    Canonicalize address, adding a default scheme if necessary.

    >>> normalize_address('tls://[::1]')
    'tls://[::1]'
    >>> normalize_address('[::1]')
    'tcp://[::1]'
    """
    return unparse_address(*parse_address(addr))


def parse_host_port(address, default_port=None):
    """
    Parse an endpoint address given in the form "host:port".
    """
    if isinstance(address, tuple):
        return address

    def _fail():
        raise ValueError("invalid address %r" % (address,))

    def _default():
        if default_port is None:
            raise ValueError("missing port number in address %r" % (address,))
        return default_port

    if address.startswith('['):
        # IPv6 notation: '[addr]:port' or '[addr]'.
        # The address may contain multiple colons.
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
        # Generic notation: 'addr:port' or 'addr'.
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

    ValueError is raised if the address scheme doesn't allow extracting
    the requested information.

    >>> get_address_host_port('tcp://1.2.3.4:80')
    ('1.2.3.4', 80)
    """
    scheme, loc = parse_address(addr)
    backend = registry.get_backend(scheme)
    try:
        return backend.get_address_host_port(loc)
    except NotImplementedError:
        raise ValueError("don't know how to extract host and port "
                         "for address %r" % (addr,))


def get_address_host(addr):
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


def get_local_address_for(addr):
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


def resolve_address(addr):
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
