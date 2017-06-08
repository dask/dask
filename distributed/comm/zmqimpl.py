from __future__ import print_function, division, absolute_import

from collections import deque, namedtuple
from itertools import chain

from tornado.concurrent import Future

import zmq
from zmq import POLLOUT, POLLIN
from zmq.eventloop.ioloop import IOLoop


# mixins for tornado/asyncio compatibility

_FutureRecvEvent = namedtuple('_FutureRecvEvent',
                              ('future', 'flags', 'copy', 'track'))

_FutureSendEvent = namedtuple('_FutureSendEvent',
                              ('future', 'msg', 'flags', 'copy', 'track'))


class _AsyncSocket(object):
    """
    An alternative to pyzmq's builtin "async" socket for use with Tornado.

    Also fixes https://github.com/zeromq/pyzmq/issues/962
    """

    _Future = Future
    _READ = IOLoop.READ
    _WRITE = IOLoop.WRITE

    def __init__(self, context, socket_type, io_loop=None):
        self.io_loop = io_loop or self._default_loop()
        self._recv_futures = deque()
        self._send_futures = deque()
        self._state = 0
        self._sock = zmq.Socket(context, socket_type)
        self._init_io_state()

    def _default_loop(self):
        return IOLoop.current()

    def close(self, linger=None):
        if not self.closed:
            for event in chain(self._recv_futures, self._send_futures):
                if not event.future.done():
                    event.future.cancel()
            self._clear_io_state()
            self._sock.close(linger=linger)

    close.__doc__ = zmq.Socket.close.__doc__

    def bind(self, addr):
        return self._sock.bind(addr)

    def connect(self, addr):
        return self._sock.connect(addr)

    def disconnect(self, addr):
        return self._sock.disconnect(addr)

    @property
    def closed(self):
        return self._sock.closed

    @property
    def _events(self):
        return self._sock.get(zmq.EVENTS)

    def get(self, option):
        return self._sock.get(option)

    getsockopt = get

    def set(self, option, value):
        self._sock.set(option, value)

    setsockopt = set

    def recv_multipart(self, flags=0, copy=True, track=False):
        """Receive a complete multipart zmq message.

        Returns a Future whose result will be a multipart message.
        """
        f = self._Future()
        self._recv_futures.append(
            _FutureRecvEvent(f, flags, copy, track)
        )

        #if hasattr(zmq, 'RCVTIMEO'):
            ##timeout_ms = self._shadow_sock.rcvtimeo
            #timeout_ms = self._sock.rcvtimeo
            #if timeout_ms >= 0:
                #self._add_timeout(f, timeout_ms * 1e-3)

        if self._events & POLLIN:
            # recv immediately, if we can
            self._handle_recv()
        if self._recv_futures:
            self._add_io_state(self._READ)
        return f

    def send_multipart(self, msg, flags=0, copy=True, track=False):
        """Send a complete multipart zmq message.

        Returns a Future that resolves when sending is complete.
        """
        f = self._Future()
        self._send_futures.append(
            _FutureSendEvent(f, msg, flags, copy, track)
        )

        #if hasattr(zmq, 'SNDTIMEO'):
            #timeout_ms = self._sock.sndtimeo
            #if timeout_ms >= 0:
                #self._add_timeout(f, timeout_ms * 1e-3)

        if self._events & POLLOUT:
            # send immediately if we can
            self._handle_send()
        if self._send_futures:
            self._add_io_state(self._WRITE)
        return f

    def _handle_recv(self):
        """Handle recv events"""
        if not self._events & POLLIN:
            # event triggered, but state may have been changed between trigger and callback
            return
        f = None
        q = self._recv_futures
        while q:
            f, flags, copy, track = q.popleft()
            # skip any cancelled futures
            if f.done():
                f = None
            else:
                break

        if not q:
            # No further futures remaining
            self._drop_io_state(self._READ)
        if f is None:
            return

        flags |= zmq.DONTWAIT
        try:
            parts = [self._sock.recv(flags, copy=copy, track=track)]
            # have first part already, only loop while more to receive
            while self._sock.get(zmq.RCVMORE):
                part = self._sock.recv(flags, copy=copy, track=track)
                parts.append(part)
        except Exception as e:
            f.set_exception(e)
        else:
            f.set_result(parts)

    def _handle_send(self):
        #print("<_handle_send A>")
        if not self._events & POLLOUT:
            # event triggered, but state may have been changed between trigger and callback
            return
        #print("<_handle_send B>")
        f = None
        while self._send_futures:
            f, msg, flags, copy, track = self._send_futures.popleft()
            # skip any cancelled futures
            if f.done():
                f = None
            else:
                break

        if not self._send_futures:
            # No further futures remaining
            self._drop_io_state(self._WRITE)
        if f is None:
            return
        #print("<_handle_send C>", flags)

        flags |= zmq.DONTWAIT
        try:
            for frame in msg[:-1]:
                self._sock.send(frame, zmq.SNDMORE | flags, copy=copy, track=track)
            # Send the last part without the extra SNDMORE flag.
            result = self._sock.send(msg[-1], flags, copy=copy, track=track)
        except Exception as e:
            f.set_exception(e)
            #print("<_handle_send ERR>", e)
        else:
            f.set_result(result)
            #print("<_handle_send D>", result)

    # event masking from ZMQStream
    def _handle_events(self, fd, events):
        """Dispatch IO events to _handle_recv, etc."""
        #print("** handle_events: %s on %s" % (events, fd))
        if events & self._READ:
            self._handle_recv()
        if events & self._WRITE:
            self._handle_send()

    def _add_io_state(self, state):
        """Add io_state to poller."""
        if not self._state & state:
            self._state = self._state | state
            self._update_handler(self._state)

    def _drop_io_state(self, state):
        """Stop poller from watching an io_state."""
        if self._state & state:
            self._state = self._state & (~state)
            self._update_handler(self._state)

    def _update_handler(self, state):
        """Update IOLoop handler with state."""
        self._state = state
        self.io_loop.update_handler(self._sock, state)

    def _init_io_state(self):
        """initialize the ioloop event handler"""
        #print("-- add_handler:", self._state, self._sock)
        self.io_loop.add_handler(self._sock, self._handle_events, self._state)

    def _clear_io_state(self):
        """unregister the ioloop event handler

        called once during close
        """
        #print("-- remove_handler:", self._sock)
        self.io_loop.remove_handler(self._sock)


class Context(zmq.Context):

    @property
    def _socket_class(self):
        return _AsyncSocket
