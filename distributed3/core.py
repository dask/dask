from __future__ import print_function, division, absolute_import

import signal
import socket
import struct
from time import sleep, time

import tornado
from dill import loads, dumps
from tornado import ioloop, gen
from tornado.gen import Return
from tornado.tcpserver import TCPServer
from tornado.tcpclient import TCPClient
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError


log = print


def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)


class Server(TCPServer):
    def __init__(self, handlers):
        self.handlers = handlers
        super(Server, self).__init__()

    @gen.coroutine
    def handle_stream(self, stream, address):
        """ Dispatch new connections to coroutine-handlers

        Handlers is a dictionary mapping operation names to functions or
        coroutines.

            {'get_data': get_data,
             'ping': pingpong}

        Coroutines should expect a single IOStream object.
        """
        log("Connection from %s:%d" % address)
        try:
            while True:
                try:
                    msg = yield read(stream)
                except StreamClosedError:
                    log("Lost connection: %s" % str(address))
                    break
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
                    log(result)
                else:
                    result = yield gen.maybe_future(handler(stream, **msg))
                if reply:
                    try:
                        yield write(stream, result)
                    except StreamClosedError:
                        log("Lost connection: %s" % str(address))
                        break
                if close:
                    break
        finally:
            try:
                stream.close()
            except Exception as e:
                log("Failed while closing writer")
                log(str(e))


def connect_sync(host, port, timeout=1):
    start = time()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
    if not isinstance(msg, bytes):
        msg = dumps(msg)
    sock.send(msg + sentinel)


def read_sync(s):
    bytes = []
    while b''.join(bytes[-len(sentinel):]) != sentinel:
        bytes.append(s.recv(1))
    msg = b''.join(bytes[:-len(sentinel)])
    try:
        return loads(msg)
    except:
        return msg


sentinel = b'7f57da0f9202f6b4df78e251058be6f0'

@gen.coroutine
def read(stream):
    msg = yield stream.read_until(sentinel)
    msg = msg[:-len(sentinel)]
    try:
        msg = loads(msg)
    except:
        pass
    raise Return(msg)


@gen.coroutine
def write(stream, msg):
    if not isinstance(msg, bytes):
        msg = dumps(msg)
    yield stream.write(msg + sentinel)


def pingpong(stream):
    return b'pong'


@gen.coroutine
def connect(ip, port, timeout=1):
    client = TCPClient()
    try:
        stream = yield client.connect(ip, port)
        raise Return(stream)
    except StreamClosedError:
        if time() - start < timeout:
            yield gen.sleep(0.01)
            print("sleeping on connect")
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
    """ Use send_recv to cause rpc computations on client_connected calls

    By convention the `client_connected` coroutine looks for operations by name
    in the `op` key of a message.

    >>> msg = {'op': 'func', 'key1': 100, 'key2': 1000}
    >>> result = yield send_recv(stream, **msg)  # doctest: +SKIP

    This class uses this convention to provide a Python interface for calling
    remote functions

    >>> remote = rpc(stream=stream)  # doctest: +SKIP
    >>> result = yield remote.func(key1=100, key2=1000)  # doctest: +SKIP
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
        assert not stream.closed()
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


if __name__ == '__main__':
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    server = Server({'ping': pingpong})
    server.listen(8889)
    IOLoop.current().start()
    IOLoop.current().close()
