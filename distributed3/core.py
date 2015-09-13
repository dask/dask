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
from tornado.ioloop import IOLoop


log = print


def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)


class EchoServer(TCPServer):
    def __init__(self, handlers):
        self.handlers = handlers
        super(EchoServer, self).__init__()

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
                msg = yield read(stream)
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


if __name__ == '__main__':
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    server = EchoServer({'ping': pingpong})
    server.listen(8889)
    IOLoop.current().start()
    IOLoop.current().close()
