from __future__ import annotations

import textwrap
import warnings
from collections.abc import Awaitable, Callable
from typing import Any

from distributed.comm.core import BaseListener, Comm, Connector
from distributed.comm.registry import Backend, backends


def _raise_deprecated():
    message = textwrap.dedent(
        """\
        The 'ucx' protocol was removed from Distributed because UCX-Py has been deprecated.
        To continue using protocol='ucx', please install 'distributed-ucxx' (conda-forge)
        or 'distributed-ucxx-cu[12,13]' (PyPI, selecting 12 for CUDA version 12.*, and 13
        for CUDA version 13.*).
        """
    )
    warnings.warn(message, FutureWarning)
    raise FutureWarning(message)


class UCX(Comm):
    def __init__(self):
        _raise_deprecated()


class UCXConnector(Connector):
    prefix = "ucx://"
    comm_class = UCX
    encrypted = False


class UCXListener(BaseListener):
    prefix = UCXConnector.prefix
    comm_class = UCXConnector.comm_class
    encrypted = UCXConnector.encrypted

    def __init__(
        self,
        address: str,
        comm_handler: Callable[[UCX], Awaitable[None]] | None = None,
        deserialize: bool = False,
        allow_offload: bool = True,
        **connection_args: Any,
    ):
        _raise_deprecated()

    @property
    def port(self):
        return self.ucp_server.port

    async def start(self):
        _raise_deprecated()

    def stop(self):
        _raise_deprecated()

    @property
    def listen_address(self):
        _raise_deprecated()

    @property
    def contact_address(self):
        _raise_deprecated()


class UCXBackend(Backend):
    def get_connector(self):
        return UCXConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return UCXListener(loc, handle_comm, deserialize, **connection_args)

    def get_address_host(self, loc):
        _raise_deprecated()

    def resolve_address(self, loc):
        _raise_deprecated()

    def get_local_address_for(self, loc):
        _raise_deprecated()


backends["ucx"] = UCXBackend()
