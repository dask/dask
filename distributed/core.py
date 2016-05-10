from __future__ import print_function, division, absolute_import

from datetime import timedelta
import logging
import six
import struct
from time import time
import traceback
import uuid

from toolz import assoc, first

try:
    import cPickle as pickle
except ImportError:
    import pickle
import cloudpickle
from tornado import gen
from tornado.gen import Return
from tornado.tcpserver import TCPServer
from tornado.tcpclient import TCPClient
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream, StreamClosedError

from .compatibility import PY3, unicode
from .utils import get_traceback, truncate_exception, ignoring
from . import protocol

pickle_types = [str, bytes]
with ignoring(ImportError):
    import numpy as np
    pickle_types.append(np.ndarray)
with ignoring(ImportError):
    import pandas as pd
    pickle_types.append(pd.core.generic.NDFrame)
pickle_types = tuple(pickle_types)


def dumps(x):
    """ Manage between cloudpickle and pickle

    1.  Try pickle
    2.  If it is short then check if it contains __main__
    3.  If it is long, then first check type, then check __main__
    """
    try:
        result = pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        if len(result) < 1000:
            if b'__main__' in result:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                return result
        else:
            if isinstance(x, pickle_types) or b'__main__' not in result:
                return result
            else:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
    except:
        try:
            return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            logger.info("Failed to serialize %s", x, exc_info=True)
            raise


def loads(x):
    try:
        return pickle.loads(x)
    except Exception:
        logger.info("Failed to deserialize %s", x, exc_info=True)
        raise


logger = logging.getLogger(__name__)


def get_total_physical_memory():
    try:
        import psutil
        return psutil.virtual_memory().total / 2
    except ImportError:
        return 2e9


MAX_BUFFER_SIZE = get_total_physical_memory()


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
    >>> server = Server(handlers)  # doctest: +SKIP
    >>> server.listen(8000)  # doctest: +SKIP

    **Message Format**

    The server expects messages to be dictionaries with a special key, `'op'`
    that corresponds to the name of the operation, and other key-value pairs as
    required by the function.

    So in the example above the following would be good messages.

    *  ``{'op': 'ping'}``
    *  ``{'op': 'add': 'x': 10, 'y': 20}``
    """
    default_port = 0

    def __init__(self, handlers, max_buffer_size=MAX_BUFFER_SIZE, **kwargs):
        self.handlers = assoc(handlers, 'identity', self.identity)
        self.id = str(uuid.uuid1())
        self._port = None
        self._rpcs = dict()
        super(Server, self).__init__(max_buffer_size=max_buffer_size, **kwargs)

    @property
    def port(self):
        if not self._port:
            try:
                self._port = first(self._sockets.values()).getsockname()[1]
            except StopIteration:
                raise OSError("Server has no port.  Please call .listen first")
        return self._port

    def identity(self, stream):
        return {'type': type(self).__name__, 'id': self.id}

    def listen(self, port=None):
        if port is None:
            port = self.default_port
        while True:
            try:
                super(Server, self).listen(port)
                break
            except OSError:
                if port:
                    raise
                else:
                    logger.info('Randomly assigned port taken for %s. Retrying',
                                type(self).__name__)

    @gen.coroutine
    def handle_stream(self, stream, address):
        """ Dispatch new connections to coroutine-handlers

        Handlers is a dictionary mapping operation names to functions or
        coroutines.

            {'get_data': get_data,
             'ping': pingpong}

        Coroutines should expect a single IOStream object.
        """
        stream.set_nodelay(True)
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
                except Exception as e:
                    yield write(stream, error_message(e, status='uncaught-error'))
                    continue
                if not isinstance(msg, dict):
                    raise TypeError("Bad message type.  Expected dict, got\n  "
                                    + str(msg))
                op = msg.pop('op')
                close = msg.pop('close', False)
                reply = msg.pop('reply', True)
                if op == 'close':
                    if reply:
                        yield write(stream, 'OK')
                    break
                try:
                    handler = self.handlers[op]
                except KeyError:
                    result = 'No handler found: ' + op.encode()
                    logger.warn(result)
                else:
                    logger.debug("Calling into handler %s", handler.__name__)
                    try:
                        result = yield gen.maybe_future(handler(stream, **msg))
                    except Exception as e:
                        logger.exception(e)
                        result = error_message(e, status='uncaught-error')
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

    def rpc(self, arg=None, ip=None, port=None, addr=None):
        """ Cached rpc objects """
        key = arg, ip, port, addr
        if key not in self._rpcs:
            self._rpcs[key] = rpc(arg=arg, ip=ip, port=port, addr=addr)
        return self._rpcs[key]


@gen.coroutine
def read(stream):
    """ Read a message from a stream """
    if isinstance(stream, BatchedStream):
        msg = yield stream.recv()
        raise Return(msg)
    else:
        n_frames = yield stream.read_bytes(8)
        n_frames = struct.unpack('Q', n_frames)[0]

        lengths = yield stream.read_bytes(8 * n_frames)
        lengths = struct.unpack('Q' * n_frames, lengths)

        frames = []
        for length in lengths:
            if length:
                frame = yield stream.read_bytes(length)
            else:
                frame = b''
            frames.append(frame)

        msg = protocol.loads(frames)
        raise Return(msg)


@gen.coroutine
def write(stream, msg):
    """ Write a message to a stream """
    if isinstance(stream, BatchedStream):
        stream.send(msg)
    else:
        try:
            frames = protocol.dumps(msg)
        except Exception as e:
            logger.exception(e)
            raise

        lengths = ([struct.pack('Q', len(frames))] +
                   [struct.pack('Q', len(frame)) for frame in frames])
        yield stream.write(b''.join(lengths))

        for frame in frames:
            yield stream.write(frame)


def pingpong(stream):
    return b'pong'


@gen.coroutine
def connect(ip, port, timeout=3):
    client = TCPClient()
    start = time()
    while True:
        future = client.connect(ip, port, max_buffer_size=MAX_BUFFER_SIZE)
        try:
            stream = yield gen.with_timeout(timedelta(seconds=timeout), future)
            stream.set_nodelay(True)
            raise Return(stream)
        except StreamClosedError:
            if time() - start < timeout:
                yield gen.sleep(0.01)
                logger.debug("sleeping on connect")
            else:
                raise
        except gen.TimeoutError:
            raise IOError("Timed out while connecting to %s:%d" % (ip, port))


@gen.coroutine
def send_recv(stream=None, arg=None, ip=None, port=None, addr=None, reply=True, **kwargs):
    """ Send and recv with a stream

    Keyword arguments turn into the message

    response = yield send_recv(stream, op='ping', reply=True)
    """
    if arg:
        if isinstance(arg, (unicode, bytes)):
            addr = arg
        if isinstance(arg, tuple):
            ip, port = arg
    if addr:
        assert not ip and not port
        if PY3 and isinstance(addr, bytes):
            addr = addr.decode()
        ip, port = addr.split(':')
        port = int(port)
    if PY3 and isinstance(ip, bytes):
        ip = ip.decode()
    if stream is None:
        stream = yield connect(ip, port)

    msg = kwargs
    msg['reply'] = reply

    yield write(stream, msg)

    if reply:
        response = yield read(stream)
        if isinstance(response, dict) and response.get('status') == 'uncaught-error':
            six.reraise(*clean_exception(**response))
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
    def __init__(self, arg=None, stream=None, ip=None, port=None, addr=None, timeout=3):
        if arg:
            if isinstance(arg, (unicode, bytes)):
                addr = arg
            if isinstance(arg, tuple):
                ip, port = arg
        if addr:
            if PY3 and isinstance(addr, bytes):
                addr = addr.decode()
            assert not ip and not port
            ip, port = addr.split(':')
            port = int(port)
        if PY3 and isinstance(ip, bytes):
            ip = ip.decode()
        self.streams = dict()
        if stream:
            self.streams[stream] = True
        self.ip = ip
        self.port = port
        self.timeout = timeout
        assert self.ip
        assert self.port

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
            stream = yield connect(self.ip, self.port, timeout=self.timeout)
            self.streams[stream] = True
        for s in to_clear:
            del self.streams[s]
        self.streams[stream] = False     # mark as taken
        # assert not stream.closed()
        raise Return(stream)

    def close_streams(self):
        for stream in self.streams:
            with ignoring(OSError, IOError):
                stream.close()

    def __getattr__(self, key):
        @gen.coroutine
        def send_recv_from_rpc(**kwargs):
            stream = yield self.live_stream()
            result = yield send_recv(stream=stream, op=key, **kwargs)
            self.streams[stream] = True  # mark as open
            raise Return(result)
        return send_recv_from_rpc

    def __del__(self):
        self.close_streams()


def coerce_to_address(o, out=str):
    if PY3 and isinstance(o, bytes):
        o = o.decode()
    if isinstance(o, (unicode, str)):
        ip, port = o.split(':')
        port = int(port)
        o = (ip, port)
    if isinstance(o, list):
        o = tuple(o)
    if isinstance(o, tuple) and isinstance(o[0], bytes):
        o = (o[0].decode(), o[1])

    if out == str:
        o = '%s:%d' % o

    return o


def coerce_to_rpc(o, **kwargs):
    if isinstance(o, (bytes, str, tuple, list)):
        ip, port = coerce_to_address(o, out=tuple)
        return rpc(ip=ip, port=int(port), **kwargs)
    elif isinstance(o, IOStream):
        return rpc(stream=o, **kwargs)
    elif isinstance(o, rpc):
        return o
    else:
        raise TypeError()


def error_message(e, status='error'):
    """ Produce message to send back given an exception has occurred

    This does the following:

    1.  Gets the traceback
    2.  Trunctes the exception and the traceback
    3.  Serializes the exception and traceback or
    4.  If they can't be serialized send string versions
    5.  Format a message and return

    See Also
    --------
    clean_exception: deserialize and unpack message into exception/traceback
    six.reraise: raise exception/traceback
    """
    tb = get_traceback()
    e2 = truncate_exception(e, 1000)
    try:
        e3 = dumps(e2)
        loads(e3)
    except Exception:
        e3 = Exception(str(e2))
        e3 = dumps(e3)
    try:
        tb2 = dumps(tb)
    except Exception:
        tb2 = ''.join(traceback.format_tb(tb))
        tb2 = dumps(tb2)

    if len(tb2) > 10000:
        tb2 = None

    return {'status': status, 'exception': e3, 'traceback': tb2}


def clean_exception(exception, traceback, **kwargs):
    """ Reraise exception and traceback. Deserialize if necessary

    See Also
    --------
    error_message: create and serialize errors into message
    """
    if isinstance(exception, bytes):
        exception = loads(exception)
    if isinstance(traceback, bytes):
        traceback = loads(traceback)
    if isinstance(traceback, str):
        traceback = None
    return type(exception), exception, traceback


from .batched import BatchedStream
