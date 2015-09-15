from __future__ import print_function, division

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
                    yield write(stream, result)
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


def write_sync(s, msg):
    if not isinstance(msg, bytes):
        msg = dumps(msg)
    s.send(struct.pack('L', len(msg)))
    s.send(msg)


def read_sync(s):
    b = b''
    while len(b) < 8:
        b += s.recv(8 - len(b))
    nbytes = struct.unpack('L', b)[0]
    msg = b''
    while len(msg) < nbytes:
        msg += s.recv(nbytes - len(msg))
    try:
        return loads(msg)
    except:
        return msg


@gen.coroutine
def read(stream):
    b = yield stream.read_bytes(8)
    nbytes = struct.unpack('L', b)[0]
    msg = yield stream.read_bytes(nbytes)
    try:
        msg = loads(msg)
    except:
        pass
    raise Return(msg)


@gen.coroutine
def write(stream, msg):
    if not isinstance(msg, bytes):
        msg = dumps(msg)
    yield stream.write(struct.pack('L', len(msg)))
    yield stream.write(msg)


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
        else:
            raise

@gen.coroutine
def send_recv(ip_or_stream, port=None, reply=True, loop=None, **kwargs):
    """ Send and recv with a stream

    Keyword arguments turn into the message

    response = yield send_recv(stream, op='ping', reply=True)
    """
    if isinstance(ip_or_stream, tuple):
        ip_or_stream, port = ip_or_stream
    if port is not None:
        given_ip_port = True
        stream = yield connect(ip_or_stream, port)
    else:
        given_ip_port = False
        stream = ip_or_stream
    msg = kwargs
    msg['reply'] = reply
    if 'close' not in msg:
        msg['close'] = given_ip_port
    yield write(stream, msg)
    if reply:
        response = yield read(stream)
    else:
        response = None
    if kwargs['close']:
        stream.close()
    raise Return(response)


class rpc(object):
    """ Use send_recv to cause rpc computations on client_connected calls

    By convention the `client_connected` coroutine looks for operations by name
    in the `op` key of a message.

    >>> msg = {'op': 'func', 'key1': 100, 'key2': 1000}
    >>> result = yield send_recv(stream, **msg)  # doctest: +SKIP

    This class uses this convention to provide a Python interface for calling
    remote functions

    >>> remote = rpc(stream)  # doctest: +SKIP
    >>> result = yield remote.func(key1=100, key2=1000)  # doctest: +SKIP
    """
    def __init__(self, *args):
        self.args = args

    def __getattr__(self, key):
        def _(**kwargs):
            return send_recv(*self.args, op=key, **kwargs)
        return _



if __name__ == '__main__':
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    server = Server({'ping': pingpong})
    server.listen(8889)
    IOLoop.current().start()
    IOLoop.current().close()
