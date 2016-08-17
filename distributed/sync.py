""" Synchronous versions of functions found in core.py

These are not relevant for normal operation, but are sometimes useful for
demonstration.
"""
from __future__ import print_function, division, absolute_import

import socket
import struct

from . import protocol


def connect_sync(ip, port):
    """ Connect with a blocking socket

    This is not typically used.  See connect() instead
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    return s


def read_bytes_sync(sock, nbytes):
    """ Read number of bytes from a blocking socket

    This is not typically used, see IOStream.read_bytes instead
    """
    frames = []
    while nbytes:
        frame = sock.recv(nbytes)
        frames.append(frame)
        nbytes -= len(frame)

    if len(frames) == 1:
        return frames[0]
    else:
        return b''.join(frames)


def read_sync(sock):
    """ Read message from blocking socket

    This is not typically used.  See read() instead
    """
    n_frames = read_bytes_sync(sock, 8)
    n_frames = struct.unpack('Q', n_frames)[0]

    lengths = read_bytes_sync(sock, 8 * n_frames)
    lengths = struct.unpack('Q' * n_frames, lengths)

    frames = []
    for length in lengths:
        if length:
            frame = read_bytes_sync(sock, length)
        else:
            frame = b''
        frames.append(frame)

    msg = protocol.loads(frames)
    return msg


def write_sync(sock, msg):
    """ Write a message synchronously to a socket

    This is not typically used.  See write() instead
    """
    try:
        frames = protocol.dumps(msg)
    except Exception as e:
        logger.exception(e)
        raise

    lengths = ([struct.pack('Q', len(frames))] +
               [struct.pack('Q', len(frame)) for frame in frames])
    sock.sendall(b''.join(lengths))

    for frame in frames:
        sock.sendall(frame)
