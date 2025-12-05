from __future__ import annotations

import importlib.metadata
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Protocol


class _EntryPoints(Protocol):
    def __call__(self, **kwargs: str) -> Iterable[importlib.metadata.EntryPoint]: ...


_entry_points: _EntryPoints = importlib.metadata.entry_points  # type: ignore[assignment]


class Backend(ABC):
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
backends: dict[str, Backend] = {}


def get_backend(scheme: str) -> Backend:
    """
    Get the Backend instance for the given *scheme*.
    It looks for matching scheme in dask's internal cache, and falls-back to
    package metadata for the group name ``distributed.comm.backends``
    """

    backend = backends.get(scheme)
    if backend is not None:
        return backend

    for backend_class_ep in _entry_points(
        name=scheme, group="distributed.comm.backends"
    ):
        backend = backend_class_ep.load()()
        backends[scheme] = backend
        return backend

    raise ValueError(
        f"unknown address scheme {scheme!r} (known schemes: {sorted(backends)})"
    )
