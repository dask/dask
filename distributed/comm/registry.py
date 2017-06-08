from __future__ import print_function, division, absolute_import

from abc import ABCMeta, abstractmethod

from six import with_metaclass


class Backend(with_metaclass(ABCMeta)):
    """
    A communication backend, selected by a given URI scheme (e.g. 'tcp').
    """

    # I/O

    @abstractmethod
    def get_connector(self):
        """
        Get a connector object usable for connecting to addresses.
        """

    @abstractmethod
    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        """
        Get a listener object for the scheme-less address *loc*.
        """

    # Address handling

    @abstractmethod
    def get_address_host(self, loc):
        """
        Get a host name (normally an IP address) identifying the host the
        address is located on.
        *loc* is a scheme-less address.
        """

    @abstractmethod
    def resolve_address(self, loc):
        """
        Resolve the address into a canonical form.
        *loc* is a scheme-less address.

        Simple implementations may return *loc* unchanged.
        """

    def get_address_host_port(self, loc):
        """
        Get the (host, port) tuple of the scheme-less address *loc*.
        This should only be implemented by IP-based transports.
        """
        raise NotImplementedError

    @abstractmethod
    def get_local_address_for(self, loc):
        """
        Get the local listening address suitable for reaching *loc*.
        """


# The {scheme: Backend} mapping
backends = {}


def get_backend(scheme):
    """
    Get the Backend instance for the given *scheme*.
    """
    backend = backends.get(scheme)
    if backend is None:
        raise ValueError("unknown address scheme %r (known schemes: %s)"
                         % (scheme, sorted(backends)))
    return backend
