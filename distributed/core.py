from __future__ import print_function, division, absolute_import

from collections import defaultdict
from datetime import timedelta
from functools import partial
import logging
import six
import socket
import struct
import sys
import traceback
import uuid

from toolz import assoc, first

try:
    import cPickle as pickle
except ImportError:
    import pickle
import cloudpickle
from tornado import gen
from tornado.locks import Event
from tornado.tcpserver import TCPServer
from tornado.tcpclient import TCPClient
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import IOStream, StreamClosedError

from .compatibility import PY3, unicode, WINDOWS
from .config import config
from .metrics import time
from .system_monitor import SystemMonitor
from .utils import get_traceback, truncate_exception, ignoring
from . import protocol


class RPCClosed(IOError):
    pass


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

    Superclass for both Worker and Scheduler objects.
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

    def __init__(self, handlers, max_buffer_size=MAX_BUFFER_SIZE,
            connection_limit=512, deserialize=True, **kwargs):
        self.handlers = assoc(handlers, 'identity', self.identity)
        self.id = str(uuid.uuid1())
        self._port = None
        self._listen_streams = dict()
        self.rpc = ConnectionPool(limit=connection_limit,
                                  deserialize=deserialize)
        self.deserialize = deserialize
        self.monitor = SystemMonitor()
        self.counters = None
        self.digests = None
        if hasattr(self, 'loop'):
            with ignoring(ImportError):
                from .counter import Digest
                self.digests = defaultdict(partial(Digest, loop=self.loop))

            from .counter import Counter
            self.counters = defaultdict(partial(Counter, loop=self.loop))

            pc = PeriodicCallback(self.monitor.update, 500, io_loop=self.loop)
            self.loop.add_callback(pc.start)
            if self.digests is not None:
                self._last_tick = time()
                self._tick_pc = PeriodicCallback(self._measure_tick, 20, io_loop=self.loop)
                self.loop.add_callback(self._tick_pc.start)


        self.__stopped = False

        super(Server, self).__init__(max_buffer_size=max_buffer_size, **kwargs)

    def stop(self):
        if not self.__stopped:
            self.__stopped = True
            super(Server, self).stop()

    def _measure_tick(self):
        now = time()
        self.digests['tick-duration'].add(now - self._last_tick)
        self._last_tick = now

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
            except (socket.error, OSError):
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
        set_tcp_timeout(stream)
        op = None

        ip, port = address
        logger.debug("Connection from %s:%d to %s", ip, port,
                    type(self).__name__)
        self._listen_streams[stream] = op
        try:
            while True:
                try:
                    msg = yield read(stream, deserialize=self.deserialize)
                    logger.debug("Message from %s:%d: %s", ip, port, msg)
                except EnvironmentError as e:
                    logger.debug("Lost connection to %s while reading message: %s."
                                " Last operation: %s",
                                str(address), e, op)
                    break
                except Exception as e:
                    logger.exception(e)
                    yield write(stream, error_message(e, status='uncaught-error'))
                    continue
                if not isinstance(msg, dict):
                    raise TypeError("Bad message type.  Expected dict, got\n  "
                                    + str(msg))
                op = msg.pop('op')
                if self.counters is not None:
                    self.counters['op'].add(op)
                self._listen_streams[stream] = op
                close_desired = msg.pop('close', False)
                reply = msg.pop('reply', True)
                if op == 'close':
                    if reply:
                        yield write(stream, 'OK')
                    break
                try:
                    handler = self.handlers[op]
                except KeyError:
                    result = "No handler found: %s" % op
                    logger.warn(result, exc_info=True)
                else:
                    logger.debug("Calling into handler %s", handler.__name__)
                    try:
                        result = yield gen.maybe_future(handler(stream, **msg))
                    except StreamClosedError as e:
                        logger.warn("Lost connection to %s: %s", str(address), e)
                        break
                    except Exception as e:
                        logger.exception(e)
                        result = error_message(e, status='uncaught-error')
                if reply and result != 'dont-reply':
                    try:
                        yield write(stream, result)
                    except EnvironmentError as e:
                        logger.warn("Lost connection to %s while sending result: %s",
                                    str(address), e)
                        break
                if close_desired or stream.closed():
                    break
        finally:
            logger.debug("Closing connection from %s:%d to %s", ip, port,
                         type(self).__name__)
            del self._listen_streams[stream]
            try:
                yield close(stream)
            except Exception as e:
                logger.error("Failed while closing connection to %s: %s",
                             address, e)
            finally:
                stream.close()


def set_tcp_timeout(stream):
    """
    Set kernel-level TCP timeout on the stream.
    """
    if stream.closed():
        return

    timeout = int(config.get('tcp-timeout', 30))

    sock = stream.socket

    # Default (unsettable) value on Windows
    # https://msdn.microsoft.com/en-us/library/windows/desktop/dd877220(v=vs.85).aspx
    nprobes = 10
    assert timeout >= nprobes + 1, "Timeout too low"

    idle = max(2, timeout // 4)
    interval = max(1, (timeout - idle) // nprobes)
    idle = timeout - interval * nprobes
    assert idle > 0

    try:
        if sys.platform.startswith("win"):
            logger.debug("Setting TCP keepalive: idle=%d, interval=%d",
                         idle, interval)
            sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, idle * 1000, interval * 1000))
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            try:
                TCP_KEEPIDLE = socket.TCP_KEEPIDLE
                TCP_KEEPINTVL = socket.TCP_KEEPINTVL
                TCP_KEEPCNT = socket.TCP_KEEPCNT
            except AttributeError:
                if sys.platform == "darwin":
                    TCP_KEEPIDLE = 0x10  # (named "TCP_KEEPALIVE" in C)
                    TCP_KEEPINTVL = 0x101
                    TCP_KEEPCNT = 0x102
                else:
                    TCP_KEEPIDLE = None

            if TCP_KEEPIDLE is not None:
                logger.debug("Setting TCP keepalive: nprobes=%d, idle=%d, interval=%d",
                             nprobes, idle, interval)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPCNT, nprobes)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPIDLE, idle)
                sock.setsockopt(socket.SOL_TCP, TCP_KEEPINTVL, interval)

        if sys.platform.startswith("linux"):
            logger.debug("Setting TCP user timeout: %d ms",
                         timeout * 1000)
            TCP_USER_TIMEOUT = 18  # since Linux 2.6.37
            sock.setsockopt(socket.SOL_TCP, TCP_USER_TIMEOUT, timeout * 1000)
    except EnvironmentError as e:
        logger.warn("Could not set timeout on TCP stream: %s", e)


@gen.coroutine
def read(stream, deserialize=True):
    """ Read a message from a stream """
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

    msg = protocol.loads(frames, deserialize=deserialize)
    raise gen.Return(msg)


@gen.coroutine
def write(stream, msg):
    """ Write a message to a stream """
    try:
        frames = protocol.dumps(msg)
    except Exception as e:
        logger.info("Unserializable Message: %s", msg)
        logger.exception(e)
        raise

    lengths = ([struct.pack('Q', len(frames))] +
               [struct.pack('Q', len(frame)) for frame in frames])
    stream.write(b''.join(lengths))

    for frame in frames:
        # Can't wait for the write() Future as it may be lost
        # ("If write is called again before that Future has resolved,
        #   the previous future will be orphaned and will never resolve")
        stream.write(frame)

    yield gen.moment
    raise gen.Return(sum(map(len, frames)))


@gen.coroutine
def close(stream):
    """Close a stream after flushing it.
    No concurrent write() should be issued during execution of this
    coroutine.
    """
    if not stream.closed():
        try:
            # Flush the stream's write buffer by waiting for a last write.
            if stream.writing():
                yield stream.write(b'')
        except EnvironmentError:
            pass
        finally:
            stream.close()


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
            set_tcp_timeout(stream)
            raise gen.Return(stream)
        except EnvironmentError:
            if time() - start < timeout:
                yield gen.sleep(0.01)
                logger.debug("sleeping on connect")
            else:
                raise
        except gen.TimeoutError:
            raise IOError("Timed out while connecting to %s:%d" % (ip, port))


@gen.coroutine
def send_recv(stream=None, arg=None, ip=None, port=None, addr=None, reply=True,
        deserialize=True, **kwargs):
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
        ip, port = addr.rsplit(':', 1)
        port = int(port)
    if PY3 and isinstance(ip, bytes):
        ip = ip.decode()
    if stream is None:
        stream = yield connect(ip, port)

    msg = kwargs
    msg['reply'] = reply

    yield write(stream, msg)

    if reply:
        response = yield read(stream, deserialize=deserialize)
        if isinstance(response, dict) and response.get('status') == 'uncaught-error':
            six.reraise(*clean_exception(**response))
    else:
        response = None
    if kwargs.get('close'):
        close(stream)
    raise gen.Return(response)


def ip_port_from_args(arg=None, addr=None, ip=None, port=None):
    if arg:
        if isinstance(arg, (unicode, bytes)):
            addr = arg
        if isinstance(arg, tuple):
            ip, port = arg
    if addr:
        if PY3 and isinstance(addr, bytes):
            addr = addr.decode()
        assert not ip and not port
        ip, port = addr.rsplit(':', 1)
        port = int(port)
    if PY3 and isinstance(ip, bytes):
        ip = ip.decode()

    return ip, port


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
    active = 0

    def __init__(self, arg=None, stream=None, ip=None, port=None, addr=None,
            deserialize=True, timeout=3):
        ip, port = ip_port_from_args(arg=arg, addr=addr, ip=ip, port=port)
        self.streams = dict()
        self.ip = ip
        self.port = port
        self.timeout = timeout
        self.status = 'running'
        self.deserialize = deserialize
        rpc.active += 1
        assert self.ip
        assert self.port

    @property
    def address(self):
        return '%s:%d' % (self.ip, self.port)

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
        if self.status == 'closed':
            raise RPCClosed("RPC Closed")
        to_clear = set()
        open = False
        for stream, open in self.streams.items():
            if stream.closed():
                to_clear.add(stream)
            if open:
                break
        if not open or stream.closed():
            stream = yield connect(self.ip, self.port, timeout=self.timeout)
        for s in to_clear:
            del self.streams[s]
        self.streams[stream] = False     # mark as taken
        raise gen.Return(stream)

    def close_streams(self):
        for stream in self.streams:
            if stream and not stream.closed():
                try:
                    close(stream)
                except EnvironmentError:
                    pass
        self.streams.clear()

    def __getattr__(self, key):
        @gen.coroutine
        def send_recv_from_rpc(**kwargs):
            try:
                stream = yield self.live_stream()
                result = yield send_recv(stream=stream, op=key,
                        deserialize=self.deserialize, **kwargs)
            except (RPCClosed, StreamClosedError) as e:
                raise e.__class__("%s: while trying to call remote method %r"
                                  % (e, key,))

            self.streams[stream] = True  # mark as open
            raise gen.Return(result)
        return send_recv_from_rpc

    def close_rpc(self):
        if self.status != 'closed':
            rpc.active -= 1
        self.status = 'closed'
        self.close_streams()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close_rpc()

    def __del__(self):
        if self.status != 'closed':
            rpc.active -= 1
            n_open = sum(not stream.closed() for stream in self.streams)
            if n_open:
                logger.warn("rpc object %s deleted with %d open streams",
                            self, n_open)


class RPCCall(object):
    """ The result of ConnectionPool()('host:port')

    See Also:
        ConnectionPool
    """
    def __init__(self, ip, port, pool):
        self.ip = ip
        self.port = port
        self.pool = pool

    def __getattr__(self, key):
        @gen.coroutine
        def send_recv_from_rpc(**kwargs):
            stream = yield self.pool.connect(self.ip, self.port)
            try:
                result = yield send_recv(stream=stream, op=key,
                        deserialize=self.pool.deserialize, **kwargs)
            finally:
                if not stream.closed():
                    self.pool.available[self.ip, self.port].add(stream)
                    self.pool.occupied[self.ip, self.port].remove(stream)
                    self.pool.active -= 1

            raise gen.Return(result)
        return send_recv_from_rpc

    def close_rpc(self):
        pass


class ConnectionPool(object):
    """ A maximum sized pool of Tornado IOStreams

    This provides a connect method that mirrors the normal distributed.connect
    method, but provides connection sharing and tracks connection limits.

    This object provides an ``rpc`` like interface::

        >>> rpc = ConnectionPool(limit=512)
        >>> scheduler = rpc('127.0.0.1:8786')
        >>> workers = [rpc(ip=ip, port=port) for ip, port in ...]

        >>> info = yield scheduler.identity()

    It creates enough streams to satisfy concurrent connections to any
    particular address::

        >>> a, b = yield [scheduler.who_has(), scheduler.has_what()]

    It reuses existing streams so that we don't have to continuously reconnect.

    It also maintains a stream limit to avoid "too many open file handle"
    issues.  Whenever this maximum is reached we clear out all idling streams.
    If that doesn't do the trick then we wait until one of the occupied streams
    closes.

    Parameters
    ----------
    limit: int
        The number of open streams to maintain at once
    deserialize: bool
        Whether or not to deserialize data by default or pass it through
    """
    def __init__(self, limit=512, deserialize=True):
        self.open = 0
        self.active = 0
        self.limit = limit
        self.available = defaultdict(set)
        self.occupied = defaultdict(set)
        self.deserialize = deserialize
        self.event = Event()

    def __str__(self):
        return "<ConnectionPool: open=%d, active=%d>" % (self.open,
                self.active)

    __repr__ = __str__

    def __call__(self, arg=None, ip=None, port=None, addr=None):
        """ Cached rpc objects """
        ip, port = ip_port_from_args(arg=arg, addr=addr, ip=ip, port=port)
        return RPCCall(ip, port, self)

    @gen.coroutine
    def connect(self, ip, port, timeout=3):
        if self.available.get((ip, port)):
            stream = self.available[ip, port].pop()
            self.active += 1
            self.occupied[ip, port].add(stream)
            raise gen.Return(stream)

        while self.open >= self.limit:
            self.event.clear()
            self.collect()
            yield self.event.wait()
            yield gen.sleep(0.01)

        self.open += 1
        stream = yield connect(ip=ip, port=port, timeout=timeout)
        stream.set_close_callback(lambda: self.on_close(ip, port, stream))
        self.active += 1
        self.occupied[ip, port].add(stream)

        if self.open >= self.limit:
            self.event.clear()

        raise gen.Return(stream)

    def on_close(self, ip, port, stream):
        self.open -= 1

        if stream in self.available[ip, port]:
            self.available[ip, port].remove(stream)
        if stream in self.occupied[ip, port]:
            self.occupied[ip, port].remove(stream)
            self.active -= 1

        if self.open <= self.limit:
            self.event.set()

    def collect(self):
        logger.info("Collecting unused streams.  open: %d, active: %d",
                    self.open, self.active)
        for streams in list(self.available.values()):
            for stream in streams:
                close(stream)

    def close(self):
        for streams in list(self.available.values()):
            for stream in streams:
                close(stream)
        for streams in list(self.occupied.values()):
            for stream in streams:
                close(stream)


def coerce_to_address(o, out=str):
    if PY3 and isinstance(o, bytes):
        o = o.decode()
    if isinstance(o, (unicode, str)):
        ip, port = o.rsplit(':', 1)
        port = int(port)
        o = (ip, port)
    if isinstance(o, list):
        o = tuple(o)
    if isinstance(o, tuple) and isinstance(o[0], bytes):
        o = (o[0].decode(), o[1])

    if out == str:
        o = '%s:%s' % o

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
    2.  Truncates the exception and the traceback
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
        e3 = protocol.pickle.dumps(e2)
        protocol.pickle.loads(e3)
    except Exception:
        e3 = Exception(str(e2))
        e3 = protocol.pickle.dumps(e3)
    try:
        tb2 = protocol.pickle.dumps(tb)
    except Exception:
        tb2 = ''.join(traceback.format_tb(tb))
        tb2 = protocol.pickle.dumps(tb2)

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
        exception = protocol.pickle.loads(exception)
    if isinstance(traceback, bytes):
        traceback = protocol.pickle.loads(traceback)
    if isinstance(traceback, str):
        traceback = None
    return type(exception), exception, traceback
