from __future__ import print_function, division, absolute_import

import logging
import socket

from .. import protocol
from ..utils import get_ip, get_ipv6


logger = logging.getLogger(__name__)


def to_frames(msg):
    """
    Serialize a message into a list of Distributed protocol frames.
    """
    try:
        return list(protocol.dumps(msg))
    except Exception as e:
        logger.info("Unserializable Message: %s", msg)
        logger.exception(e)
        raise


def from_frames(frames, deserialize=True):
    """
    Unserialize a list of Distributed protocol frames.
    """
    try:
        return protocol.loads(frames, deserialize=deserialize)
    except EOFError:
        size = sum(map(len, frames))
        if size > 1000:
            datastr = "[too large to display]"
        else:
            datastr = frames
        # Aid diagnosing
        logger.error("truncated data stream (%d bytes): %s", size,
                     datastr)
        raise


def get_tcp_server_address(tcp_server):
    """
    Get the bound address of a started Tornado TCPServer.
    """
    sockets = list(tcp_server._sockets.values())
    if not sockets:
        raise RuntimeError("TCP Server %r not started yet?" % (tcp_server,))

    def _look_for_family(fam):
        for sock in sockets:
            if sock.family == fam:
                return sock
        return None

    # If listening on both IPv4 and IPv6, prefer IPv4 as defective IPv6
    # is common (e.g. Travis-CI).
    sock = _look_for_family(socket.AF_INET)
    if sock is None:
        sock = _look_for_family(socket.AF_INET6)
    if sock is None:
        raise RuntimeError("No Internet socket found on TCPServer??")

    return sock.getsockname()


def ensure_concrete_host(host):
    """
    Ensure the given host string (or IP) denotes a concrete host, not a
    wildcard listening address.
    """
    if host in ('0.0.0.0', ''):
        return get_ip()
    elif host == '::':
        return get_ipv6()
    else:
        return host
