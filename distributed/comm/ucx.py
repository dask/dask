"""
:ref:`UCX`_ based communications for distributed.

See :ref:`communications` for more.

.. _UCX: https://github.com/openucx/ucx
"""
import ucp

import logging
import concurrent

import dask
import numpy as np

from .addressing import parse_host_port, unparse_host_port
from .core import Comm, Connector, Listener, CommClosedError
from .registry import Backend, backends
from .utils import ensure_concrete_host, to_frames, from_frames
from ..utils import ensure_ip, get_ip, get_ipv6, nbytes, log_errors

import dask
import numpy as np


logger = logging.getLogger(__name__)


# In order to avoid double init when forking/spawning new processes (multiprocess),
# we make sure only to import and initialize UCX once at first use.
ucp = None
cuda_array = None


def init_once():
    global ucp, cuda_array
    if ucp is not None:
        return

    import ucp as _ucp

    ucp = _ucp
    ucp.init(options=dask.config.get("ucx"), env_takes_precedence=True)

    # Find the function, `cuda_array()`, to use when allocating new CUDA arrays
    try:
        import rmm

        if hasattr(rmm, "DeviceBuffer"):
            cuda_array = lambda n: rmm.DeviceBuffer(size=n)
        else:  # pre-0.11.0
            cuda_array = lambda n: rmm.device_array(n, dtype=np.uint8)
    except ImportError:
        try:
            import numba.cuda

            cuda_array = lambda n: numba.cuda.device_array((n,), dtype=np.uint8)
        except ImportError:

            def cuda_array(n):
                raise RuntimeError(
                    "In order to send/recv CUDA arrays, Numba or RMM is required"
                )


class UCX(Comm):
    """Comm object using UCP.

    Parameters
    ----------
    ep : ucp.Endpoint
        The UCP endpoint.
    address : str
        The address, prefixed with `ucx://` to use.
    deserialize : bool, default True
        Whether to deserialize data in :meth:`distributed.protocol.loads`

    Notes
    -----
    The read-write cycle uses the following pattern:

    Each msg is serialized into a number of "data" frames. We prepend these
    real frames with two additional frames

        1. is_gpus: Boolean indicator for whether the frame should be
           received into GPU memory. Packed in '?' format. Unpack with
           ``<n_frames>?`` format.
        2. frame_size : Unsigned int describing the size of frame (in bytes)
           to receive. Packed in 'Q' format, so a length-0 frame is equivalent
           to an unsized frame. Unpacked with ``<n_frames>Q``.

    The expected read cycle is

    1. Read the frame describing number of frames
    2. Read the frame describing whether each data frame is gpu-bound
    3. Read the frame describing whether each data frame is sized
    4. Read all the data frames.
    """

    def __init__(self, ep, local_addr: str, peer_addr: str, deserialize=True):
        Comm.__init__(self)
        self._ep = ep
        if local_addr:
            assert local_addr.startswith("ucx")
        assert peer_addr.startswith("ucx")
        self._local_addr = local_addr
        self._peer_addr = peer_addr
        self.deserialize = deserialize
        self.comm_flag = None
        logger.debug("UCX.__init__ %s", self)

    @property
    def local_address(self) -> str:
        return self._local_addr

    @property
    def peer_address(self) -> str:
        return self._peer_addr

    async def write(
        self,
        msg: dict,
        serializers=("cuda", "dask", "pickle", "error"),
        on_error: str = "message",
    ):
        with log_errors():
            if self.closed():
                raise CommClosedError("Endpoint is closed -- unable to send message")
            try:
                if serializers is None:
                    serializers = ("cuda", "dask", "pickle", "error")
                # msg can also be a list of dicts when sending batched messages
                frames = await to_frames(
                    msg, serializers=serializers, on_error=on_error
                )

                # Send meta data
                await self.ep.send(np.array([len(frames)], dtype=np.uint64))
                await self.ep.send(
                    np.array(
                        [hasattr(f, "__cuda_array_interface__") for f in frames],
                        dtype=np.bool,
                    )
                )
                await self.ep.send(
                    np.array([nbytes(f) for f in frames], dtype=np.uint64)
                )
                # Send frames
                for frame in frames:
                    if nbytes(frame) > 0:
                        await self.ep.send(frame)
                return sum(map(nbytes, frames))
            except (ucp.exceptions.UCXBaseException):
                self.abort()
                raise CommClosedError("While writing, the connection was closed")

    async def read(self, deserializers=("cuda", "dask", "pickle", "error")):
        with log_errors():
            if self.closed():
                raise CommClosedError("Endpoint is closed -- unable to read message")

            if deserializers is None:
                deserializers = ("cuda", "dask", "pickle", "error")

            try:
                # Recv meta data
                nframes = np.empty(1, dtype=np.uint64)
                await self.ep.recv(nframes)
                is_cudas = np.empty(nframes[0], dtype=np.bool)
                await self.ep.recv(is_cudas)
                sizes = np.empty(nframes[0], dtype=np.uint64)
                await self.ep.recv(sizes)
            except (
                ucp.exceptions.UCXBaseException,
                concurrent.futures._base.CancelledError,
            ):
                self.abort()
                raise CommClosedError("While reading, the connection was closed")
            else:
                # Recv frames
                frames = []
                for is_cuda, size in zip(is_cudas.tolist(), sizes.tolist()):
                    if size > 0:
                        if is_cuda:
                            frame = cuda_array(size)
                        else:
                            frame = np.empty(size, dtype=np.uint8)
                        await self.ep.recv(frame)
                        frames.append(frame)
                    else:
                        if is_cuda:
                            frames.append(cuda_array(size))
                        else:
                            frames.append(b"")
                msg = await from_frames(
                    frames, deserialize=self.deserialize, deserializers=deserializers
                )
                return msg

    async def close(self):
        if self._ep is not None:
            await self._ep.close()
            self._ep = None

    def abort(self):
        if self._ep is not None:
            self._ep.abort()
            self._ep = None

    @property
    def ep(self):
        if self._ep is not None:
            return self._ep
        else:
            raise CommClosedError("UCX Endpoint is closed")

    def closed(self):
        return self._ep is None


class UCXConnector(Connector):
    prefix = "ucx://"
    comm_class = UCX
    encrypted = False

    async def connect(self, address: str, deserialize=True, **connection_args) -> UCX:
        logger.debug("UCXConnector.connect: %s", address)
        ip, port = parse_host_port(address)
        init_once()
        ep = await ucp.create_endpoint(ip, port)
        return self.comm_class(
            ep,
            local_addr=None,
            peer_addr=self.prefix + address,
            deserialize=deserialize,
        )


class UCXListener(Listener):
    prefix = UCXConnector.prefix
    comm_class = UCXConnector.comm_class
    encrypted = UCXConnector.encrypted

    def __init__(
        self, address: str, comm_handler: None, deserialize=False, **connection_args
    ):
        if not address.startswith("ucx"):
            address = "ucx://" + address
        self.ip, self._input_port = parse_host_port(address, default_port=0)
        self.comm_handler = comm_handler
        self.deserialize = deserialize
        self._ep = None  # type: ucp.Endpoint
        self.ucp_server = None
        self.connection_args = connection_args

    @property
    def port(self):
        return self.ucp_server.port

    @property
    def address(self):
        return "ucx://" + self.ip + ":" + str(self.port)

    async def start(self):
        async def serve_forever(client_ep):
            ucx = UCX(
                client_ep,
                local_addr=self.address,
                peer_addr=self.address,
                deserialize=self.deserialize,
            )
            if self.comm_handler:
                await self.comm_handler(ucx)

        init_once()
        self.ucp_server = ucp.create_listener(serve_forever, port=self._input_port)

    def stop(self):
        self.ucp_server = None

    def get_host_port(self):
        # TODO: TCP raises if this hasn't started yet.
        return self.ip, self.port

    @property
    def listen_address(self):
        return self.prefix + unparse_host_port(*self.get_host_port())

    @property
    def contact_address(self):
        host, port = self.get_host_port()
        host = ensure_concrete_host(host)  # TODO: ensure_concrete_host
        return self.prefix + unparse_host_port(host, port)

    @property
    def bound_address(self):
        # TODO: Does this become part of the base API? Kinda hazy, since
        # we exclude in for inproc.
        return self.get_host_port()


class UCXBackend(Backend):
    # I / O

    def get_connector(self):
        return UCXConnector()

    def get_listener(self, loc, handle_comm, deserialize, **connection_args):
        return UCXListener(loc, handle_comm, deserialize, **connection_args)

    # Address handling
    # This duplicates BaseTCPBackend

    def get_address_host(self, loc):
        return parse_host_port(loc)[0]

    def get_address_host_port(self, loc):
        return parse_host_port(loc)

    def resolve_address(self, loc):
        host, port = parse_host_port(loc)
        return unparse_host_port(ensure_ip(host), port)

    def get_local_address_for(self, loc):
        host, port = parse_host_port(loc)
        host = ensure_ip(host)
        if ":" in host:
            local_host = get_ipv6(host)
        else:
            local_host = get_ip(host)
        return unparse_host_port(local_host, None)


backends["ucx"] = UCXBackend()
