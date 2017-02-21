from __future__ import print_function, division, absolute_import

from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import timedelta
import logging

from six import string_types, with_metaclass

from tornado import gen
from tornado.ioloop import IOLoop

from ..config import config
from ..metrics import time


logger = logging.getLogger(__name__)

# Connector instances

connectors = {
    #'tcp': ...,
    #'zmq': ...,
    }


# Listener classes

listeners = {
    #'tcp': ...,
    # 'zmq': ...,
    }


DEFAULT_SCHEME = config.get('default-scheme', 'tcp')


class CommClosedError(IOError):
    pass


class Comm(with_metaclass(ABCMeta)):
    """
    A message-oriented communication object, representing an established
    communication channel.  There should be only one reader and one
    writer at a time: to manage current communications, even with a
    single peer, you must create distinct ``Comm`` objects.

    Messages are arbitrary Python objects.  Concrete implementations
    of this class can implement different serialization mechanisms
    depending on the underlying transport's characteristics.
    """

    # XXX add set_close_callback()?

    @abstractmethod
    def read(self, deserialize=None):
        """
        Read and return a message (a Python object).  If *deserialize*
        is not None, it overrides this communication's default setting.

        This method is a coroutine.
        """

    @abstractmethod
    def write(self, msg):
        """
        Write a message (a Python object).

        This method is a coroutine.
        """

    @abstractmethod
    def close(self):
        """
        Close the communication cleanly.  This will attempt to flush
        outgoing buffers before actually closing the underlying transport.

        This method is a coroutine.
        """

    @abstractmethod
    def abort(self):
        """
        Close the communication immediately and abruptly.
        Useful in destructors or generators' ``finally`` blocks.
        """

    @abstractmethod
    def closed(self):
        """
        Return whether the stream is closed.
        """

    @abstractproperty
    def peer_address(self):
        """
        The peer's address.  For logging and debugging purposes only.
        """


class Listener(with_metaclass(ABCMeta)):

    @abstractmethod
    def start(self):
        """
        Start listening for incoming connections.
        """

    @abstractmethod
    def stop(self):
        """
        Stop listening.  This does not shutdown already established
        communications, but prevents accepting new ones.
        """
        tcp_server, self.tcp_server = self.tcp_server, None
        if tcp_server is not None:
            tcp_server.stop()

    @abstractproperty
    def listen_address(self):
        """
        The listening address as a URI string.
        """

    @abstractproperty
    def contact_address(self):
        """
        An address this listener can be contacted on.  This can be
        different from `listen_address` if the latter is some wildcard
        address such as 'tcp://0.0.0.0:123'.
        """


def parse_address(addr):
    """
    Split address into its scheme and scheme-dependent location string.
    """
    if not isinstance(addr, string_types):
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


def normalize_address(addr):
    """
    Canonicalize address, adding a default scheme if necessary.
    """
    return unparse_address(*parse_address(addr))


def resolve_address(addr):
    """
    Apply scheme-specific address resolution to *addr*, ensuring
    all symbolic references are replaced with concrete location
    specifiers.

    In practice, this means hostnames are resolved to IP addresses.
    """
    # XXX circular import; reorganize APIs into a distributed.comms.addressing module?
    from ..utils import ensure_ip
    scheme, loc = parse_address(addr)
    if scheme not in ('tcp', 'zmq'):
        raise ValueError("don't know how to extract host and port "
                         "for address %r" % (addr,))
    host, port = parse_host_port(loc)
    loc = unparse_host_port(ensure_ip(host), port)
    addr = unparse_address(scheme, loc)
    return addr


@gen.coroutine
def connect(addr, timeout=3, deserialize=True):
    """
    Connect to the given address (a URI such as ``tcp://127.0.0.1:1234``)
    and yield a ``Comm`` object.  If the connection attempt fails, it is
    retried until the *timeout* is expired.
    """
    scheme, loc = parse_address(addr)
    connector = connectors.get(scheme)
    if connector is None:
        raise ValueError("unknown scheme %r in address %r" % (scheme, addr))

    start = time()
    deadline = start + timeout
    error = None

    def _raise(error):
        error = error or "connect() didn't finish in time"
        msg = ("Timed out trying to connect to %r after %s s: %s"
               % (addr, timeout, error))
        raise IOError(msg)

    while True:
        try:
            future = connector.connect(loc, deserialize=deserialize)
            comm = yield gen.with_timeout(timedelta(seconds=deadline - time()),
                                          future,
                                          quiet_exceptions=EnvironmentError)
        except EnvironmentError as e:
            error = str(e)
            if time() < deadline:
                yield gen.sleep(0.01)
                logger.debug("sleeping on connect")
            else:
                _raise(error)
        except gen.TimeoutError:
            _raise(error)
        else:
            break

    raise gen.Return(comm)


def listen(addr, handle_comm, deserialize=True):
    """
    Create a listener object with the given parameters.  When its ``start()``
    method is called, the listener will listen on the given address
    (a URI such as ``tcp://0.0.0.0``) and call *handle_comm* with a
    ``Comm`` object for each incoming connection.

    *handle_comm* can be a regular function or a coroutine.
    """
    scheme, loc = parse_address(addr)
    listener_class = listeners.get(scheme)
    if listener_class is None:
        raise ValueError("unknown scheme %r in address %r" % (scheme, addr))

    return listener_class(loc, handle_comm, deserialize)
