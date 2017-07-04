from __future__ import print_function, division, absolute_import

from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import timedelta
import logging

from six import with_metaclass

from tornado import gen

from ..config import config
from ..metrics import time
from . import registry
from .addressing import parse_address


logger = logging.getLogger(__name__)


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
    def read(self):
        """
        Read and return a message (a Python object).

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
    def local_address(self):
        """
        The local address.  For logging and debugging purposes only.
        """

    @abstractproperty
    def peer_address(self):
        """
        The peer's address.  For logging and debugging purposes only.
        """

    @property
    def extra_info(self):
        """
        Return backend-specific information about the communication,
        as a dict.  Typically, this is information which is initialized
        when the communication is established and doesn't vary afterwards.
        """
        return {}

    def __repr__(self):
        clsname = self.__class__.__name__
        if self.closed():
            return "<closed %s>" % (clsname,)
        else:
            return ("<%s local=%s remote=%s>"
                    % (clsname, self.local_address, self.peer_address))


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

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc):
        self.stop()


class Connector(with_metaclass(ABCMeta)):

    @abstractmethod
    def connect(self, address, deserialize=True):
        """
        Connect to the given address and return a Comm object.
        This function is a coroutine.   It may raise EnvironmentError
        if the other endpoint is unreachable or unavailable.  It
        may raise ValueError if the address is malformed.
        """


@gen.coroutine
def connect(addr, timeout=None, deserialize=True, connection_args=None):
    """
    Connect to the given address (a URI such as ``tcp://127.0.0.1:1234``)
    and yield a ``Comm`` object.  If the connection attempt fails, it is
    retried until the *timeout* is expired.
    """
    if timeout is None:
        timeout = float(config.get('connect-timeout', 3))  # default 3 s.

    scheme, loc = parse_address(addr)
    backend = registry.get_backend(scheme)
    connector = backend.get_connector()

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
            future = connector.connect(loc, deserialize=deserialize,
                                       **(connection_args or {}))
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


def listen(addr, handle_comm, deserialize=True, connection_args=None):
    """
    Create a listener object with the given parameters.  When its ``start()``
    method is called, the listener will listen on the given address
    (a URI such as ``tcp://0.0.0.0``) and call *handle_comm* with a
    ``Comm`` object for each incoming connection.

    *handle_comm* can be a regular function or a coroutine.
    """
    scheme, loc = parse_address(addr)
    backend = registry.get_backend(scheme)

    return backend.get_listener(loc, handle_comm, deserialize,
                                **(connection_args or {}))
