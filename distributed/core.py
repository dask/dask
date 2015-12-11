from __future__ import print_function, division, absolute_import

import logging
import signal
import socket
import struct
from time import sleep, time
import uuid

from toolz import assoc
import tornado
import pickle
import cloudpickle
from tornado import ioloop, gen
from tornado.gen import Return
from tornado.tcpserver import TCPServer
from tornado.tcpclient import TCPClient
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream, StreamClosedError


def dumps(x):
    return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)


loads = cloudpickle.loads


logger = logging.getLogger(__name__)

try:
    import psutil
    MAX_BUFFER_SIZE = psutil.virtual_memory().total / 2
except ImportError:
    MAX_BUFFER_SIZE = 2e9  # 2GB


def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)


class Server(TCPServer):
    """ Distributed TCP Server

    Superclass for both Worker and Center objects.
    Inherits from ``tornado.tcpserver.TCPServer``, adding a protocol for RPC.

    **Handlers**

    Servers define operations with a ``handlers`` dict mapping operation names
    to functions.  The first argument of a handler function must be a stream for
    the connection to the client.  Other arguments will receive inputs from the
    keys of the incoming message which will always be a dictionary.

    >>> def pingpong(stream):
    ...     return b'pong'

    >>> def add(stream, x, y):
    ...     return x + y

    >>> handlers = {'ping': pingpong, 'add': add}
    >>> server = Server(handlers)
    >>> server.listen(8000)

    **Message Format**

    The server expects messages to be dictionaries with a special key, `'op'`
    that corresponds to the name of the operation, and other key-value pairs as
    required by the function.

    So in the example above the following would be good messages.

    *  ``{'op': 'ping'}``
    *  ``{'op': 'add': 'x': 10, 'y': 20}``
    """
    def __init__(self, handlers, max_buffer_size=MAX_BUFFER_SIZE, **kwargs):
        self.handlers = assoc(handlers, 'identity', self.identity)
        self.id = uuid.uuid1()
        super(Server, self).__init__(max_buffer_size=max_buffer_size, **kwargs)

    def identity(self, stream):
        return {'type': type(self).__name__, 'id': self.id}

    @gen.coroutine
    def handle_stream(self, stream, address):
        """ Dispatch new connections to coroutine-handlers

        Handlers is a dictionary mapping operation names to functions or
        coroutines.

            {'get_data': get_data,
             'ping': pingpong}

        Coroutines should expect a single IOStream object.
        """
        ip, port = address
        logger.info("Connection from %s:%d to %s", ip, port,
                    type(self).__name__)
        try:
            while True:
                try:
                    msg = yield read(stream)
                    logger.debug("Message from %s:%d: %s", ip, port, msg)
                except StreamClosedError:
                    logger.info("Lost connection: %s", str(address))
                    break
                if not isinstance(msg, dict):
                    raise TypeError("Bad message type.  Expected dict, got\n  "
                                    + str(msg))
                op = msg.pop('op')
                close = msg.pop('close', False)
                reply = msg.pop('reply', True)
                if op == 'close':
                    if reply:
                        yield write(stream, b'OK')
                    break
                try:
                    handler = self.handlers[op]
                except KeyError:
                    result = b'No handler found: ' + op.encode()
                    logger.warn(result)
                else:
                    logger.debug("Calling into handler %s", handler.__name__)
                    try:
                        result = yield gen.maybe_future(handler(stream, **msg))
                    except Exception as e:
                        logger.exception(e)
                        raise
                if reply:
                    try:
                        yield write(stream, result)
                    except StreamClosedError:
                        logger.info("Lost connection: %s" % str(address))
                        break
                if close:
                    break
        finally:
            try:
                stream.close()
            except Exception as e:
                logger.warn("Failed while closing writer",  exc_info=True)
        logger.info("Close connection from %s:%d to %s", address[0], address[1],
                    type(self).__name__)


def connect_sync(host, port, timeout=1):
    start = time()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    while True:
        try:
            s.connect((host, port))
            break
        except socket.error:
            if time() - start > timeout:
                raise
            else:
                sleep(0.1)

    return s


def write_sync(sock, msg):
    msg = dumps(msg)
    sock.send(msg)
    sock.send(sentinel)


def read_sync(s):
    bytes = []
    while b''.join(bytes[-len(sentinel):]) != sentinel:
        bytes.append(s.recv(1))
    msg = b''.join(bytes[:-len(sentinel)])
    return loads(msg)


sentinel = b'7f57da0f9202f6b4df78e251058be6f0'

@gen.coroutine
def read(stream):
    """ Read a message from a stream """
    msg = yield stream.read_until(sentinel)
    try:
        msg = msg[:-len(sentinel)]
        msg = loads(msg)
    except Exception as e:
        logger.exception(e)
    raise Return(msg)


@gen.coroutine
def write(stream, msg):
    """ Write a message to a stream """
    try:
        msg = dumps(msg)
        yield stream.write(msg + sentinel)
    except Exception as e:
        logger.exception(e)


def pingpong(stream):
    return b'pong'


@gen.coroutine
def connect(ip, port, timeout=1):
    client = TCPClient()
    start = time()
    while True:
        try:
            stream = yield client.connect(ip, port,
                    max_buffer_size=MAX_BUFFER_SIZE)
            raise Return(stream)
        except StreamClosedError:
            if time() - start < timeout:
                yield gen.sleep(0.01)
                logger.debug("sleeping on connect")
            else:
                raise


@gen.coroutine
def send_recv(stream=None, ip=None, port=None, reply=True, **kwargs):
    """ Send and recv with a stream

    Keyword arguments turn into the message

    response = yield send_recv(stream, op='ping', reply=True)
    """
    if stream is None:
        stream = yield connect(ip, port)

    msg = kwargs
    msg['reply'] = reply

    yield write(stream, msg)

    if reply:
        response = yield read(stream)
    else:
        response = None
    if kwargs.get('close'):
        stream.close()
    raise Return(response)


def send_recv_sync(stream=None, ip=None, port=None, reply=True, **kwargs):
    return IOLoop.current().run_sync(
            lambda: send_recv(stream=stream, ip=ip, port=port, reply=reply,
                              **kwargs))


class rpc(object):
    """ Conveniently interact with a remote server

    Normally we construct messages as dictionaries and send them with read/write

    >>> stream = yield connect(ip, port)  # doctest: +SKIP
    >>> msg = {'op': 'add', 'x': 10, 'y': 20}  # doctest: +SKIP
    >>> yield write(stream, msg)  # doctest: +SKIP
    >>> response = yield read(stream)  # doctest: +SKIP

    To reduce verbosity we use an ``rpc`` object.

    >>> remote = rpc(ip=ip, port=port)  # doctest: +SKIP
    >>> response = yield remote.add(x=10, y=20)  # doctest: +SKIP

    One rpc object can be reused for several interactions.
    Additionally, this object creates and destroys many streams as necessary
    and so is safe to use in multiple overlapping communications.

    When done, close streams explicitly.

    >>> remote.close_streams()  # doctest: +SKIP
    """
    def __init__(self, stream=None, ip=None, port=None):
        self.streams = dict()
        if stream:
            self.streams[stream] = True
        self.ip = ip
        self.port = port

    @gen.coroutine
    def live_stream(self):
        """ Get an open stream

        Some streams to the ip/port target may be in current use by other
        coroutines.  We track this with the `streams` dict

            :: {stream: True/False if open and ready for use}

        This function produces an open stream, either by taking one that we've
        already made or making a new one if they are all taken.  This also
        removes streams that have been closed.

        When the caller is done with the stream they should set

            self.streams[stream] = True

        As is done in __getattr__ below.
        """
        to_clear = set()
        open = False
        for stream, open in self.streams.items():
            if stream.closed():
                to_clear.add(stream)
            if open:
                break
        if not open or stream.closed():
            stream = yield connect(self.ip, self.port)
            self.streams[stream] = True
        for s in to_clear:
            del self.streams[s]
        self.streams[stream] = False     # mark as taken
        # assert not stream.closed()
        raise Return(stream)

    def close_streams(self):
        for stream in self.streams:
            stream.close()

    def __getattr__(self, key):
        @gen.coroutine
        def _(**kwargs):
            stream = yield self.live_stream()
            result = yield send_recv(stream=stream, op=key, **kwargs)
            self.streams[stream] = True  # mark as open
            raise Return(result)
        return _


def coerce_to_rpc(o):
    if isinstance(o, tuple):
        return rpc(ip=o[0], port=o[1])
    if isinstance(o, str):
        ip, port = o.split(':')
        return rpc(ip=ip, port=int(port))
    elif isinstance(o, IOStream):
        return rpc(stream=o)
    elif isinstance(o, rpc):
        return o
    else:
        raise TypeError()
