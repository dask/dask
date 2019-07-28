"""
:ref:`UCX`_ based communications for distributed.

See :ref:`communications` for more.

.. _UCX: https://github.com/openucx/ucx
"""
import asyncio
import logging
import struct

from .addressing import parse_host_port, unparse_host_port
from .core import Comm, Connector, Listener, CommClosedError
from .registry import Backend, backends
from .utils import ensure_concrete_host, to_frames, from_frames
from ..utils import ensure_ip, get_ip, get_ipv6, nbytes

import ucp

import os

os.environ.setdefault("UCX_RNDV_SCHEME", "put_zcopy")
os.environ.setdefault("UCX_MEMTYPE_CACHE", "n")
os.environ.setdefault("UCX_TLS", "rc,cuda_copy")

logger = logging.getLogger(__name__)
MAX_MSG_LOG = 23


# ----------------------------------------------------------------------------
# Comm Interface
# ----------------------------------------------------------------------------


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

    def __init__(
        self, ep: ucp.Endpoint, local_addr: str, peer_addr: str, deserialize=True
    ):
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
        if serializers is None:
            serializers = ("cuda", "dask", "pickle", "error")
        # msg can also be a list of dicts when sending batched messages
        frames = await to_frames(msg, serializers=serializers, on_error=on_error)
        is_gpus = b"".join(
            [
                struct.pack("?", hasattr(frame, "__cuda_array_interface__"))
                for frame in frames
            ]
        )
        sizes = b"".join([struct.pack("Q", nbytes(frame)) for frame in frames])

        nframes = struct.pack("Q", len(frames))

        meta = b"".join([nframes, is_gpus, sizes])

        await self.ep.send_obj(meta)

        for frame in frames:
            await self.ep.send_obj(frame)
        return sum(map(nbytes, frames))

    async def read(self, deserializers=("cuda", "dask", "pickle", "error")):
        if deserializers is None:
            deserializers = ("cuda", "dask", "pickle", "error")
        resp = await self.ep.recv_future()
        obj = ucp.get_obj_from_msg(resp)
        (nframes,) = struct.unpack(
            "Q", obj[:8]
        )  # first eight bytes for number of frames

        gpu_frame_msg = obj[
            8 : 8 + nframes
        ]  # next nframes bytes for if they're GPU frames
        is_gpus = struct.unpack("{}?".format(nframes), gpu_frame_msg)

        sized_frame_msg = obj[8 + nframes :]  # then the rest for frame sizes
        sizes = struct.unpack("{}Q".format(nframes), sized_frame_msg)

        frames = []

        for i, (is_gpu, size) in enumerate(zip(is_gpus, sizes)):
            if size > 0:
                resp = await self.ep.recv_obj(size, cuda=is_gpu)
            else:
                resp = await self.ep.recv_future()
            frame = ucp.get_obj_from_msg(resp)
            frames.append(frame)

        msg = await from_frames(
            frames, deserialize=self.deserialize, deserializers=deserializers
        )

        return msg

    def abort(self):
        if self._ep:
            ucp.destroy_ep(self._ep)
            logger.debug("Destroyed UCX endpoint")
            self._ep = None

    @property
    def ep(self):
        if self._ep:
            return self._ep
        else:
            raise CommClosedError("UCX Endpoint is closed")

    async def close(self):
        # TODO: Handle in-flight messages?
        # sleep is currently used to help flush buffer
        self.abort()

    def closed(self):
        return self._ep is None


class UCXConnector(Connector):
    prefix = "ucx://"
    comm_class = UCX
    encrypted = False

    async def connect(self, address: str, deserialize=True, **connection_args) -> UCX:
        logger.debug("UCXConnector.connect: %s", address)
        ucp.init()
        ip, port = parse_host_port(address)
        ep = await ucp.get_endpoint(ip.encode(), port)
        return self.comm_class(
            ep,
            local_addr=None,
            peer_addr=self.prefix + address,
            deserialize=deserialize,
        )


class UCXListener(Listener):
    # MAX_LISTENERS 256 in ucx-py
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
        self.listener_instance = None  # type: ucp.ListenerFuture
        self.ucp_server = None
        self._task = None

        self.connection_args = connection_args
        self._task = None

    @property
    def port(self):
        return self.ucp_server.port

    @property
    def address(self):
        return "ucx://" + self.ip + ":" + str(self.port)

    def start(self):
        async def serve_forever(client_ep, listener_instance):
            ucx = UCX(
                client_ep,
                local_addr=self.address,
                peer_addr=self.address,  # TODO: https://github.com/Akshay-Venkatesh/ucx-py/issues/111
                deserialize=self.deserialize,
            )
            self.listener_instance = listener_instance
            if self.comm_handler:
                await self.comm_handler(ucx)

        ucp.init()
        self.ucp_server = ucp.start_listener(
            serve_forever, listener_port=self._input_port, is_coroutine=True
        )

        try:
            loop = asyncio.get_running_loop()
        except (RuntimeError, AttributeError):
            loop = asyncio.get_event_loop()

        t = loop.create_task(self.ucp_server.coroutine)
        self._task = t

    def stop(self):
        # What all should this do?
        if self._task:
            self._task.cancel()

        if self._ep:
            ucp.destroy_ep(self._ep)
        # if self.listener_instance:
        #   ucp.stop_listener(self.listener_instance)

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
