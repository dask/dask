from __future__ import print_function, division, absolute_import

from collections import Iterable
import os
import socket
import sys
import tempfile

from tornado import gen


def funcname(func):
    """Get the name of a function."""
    while hasattr(func, 'func'):
        func = func.func
    try:
        return func.__name__
    except:
        return str(func)


def get_ip():
    return [(s.connect(('8.8.8.8', 80)), s.getsockname()[0], s.close())
        for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]


from contextlib import contextmanager

@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass


@gen.coroutine
def ignore_exceptions(coroutines, *exceptions):
    """ Process list of coroutines, ignoring certain exceptions

    >>> coroutines = [cor(...) for ...]  # doctest: +SKIP
    >>> x = yield ignore_exceptions(coroutines, TypeError)  # doctest: +SKIP
    """
    wait_iterator = gen.WaitIterator(*coroutines)
    results = []
    while not wait_iterator.done():
        with ignoring(*exceptions):
            result = yield wait_iterator.next()
            results.append(result)
    raise gen.Return(results)


@gen.coroutine
def All(*args):
    """ Wait on many tasks at the same time

    Err once any of the tasks err.

    See https://github.com/tornadoweb/tornado/issues/1546
    """
    if len(args) == 1 and isinstance(args[0], Iterable):
        args = args[0]
    tasks = gen.WaitIterator(*args)
    results = [None for _ in args]
    while not tasks.done():
        result = yield tasks.next()
        results[tasks.current_index] = result
    raise gen.Return(results)


def sync(loop, func, *args, **kwargs):
    """ Run coroutine in loop running in separate thread """
    from threading import Event
    e = Event()
    result = [None]

    @gen.coroutine
    def f():
        result[0] = yield gen.maybe_future(func(*args, **kwargs))
        e.set()

    a = loop.add_callback(f)
    e.wait()
    return result[0]


@contextmanager
def tmp_text(filename, text):
    fn = os.path.join(tempfile.gettempdir(), filename)
    with open(fn, 'w') as f:
        f.write(text)

    try:
        yield fn
    finally:
        if os.path.exists(fn):
            os.remove(fn)


def clear_queue(q):
    while not q.empty():
        q.get_nowait()



import logging
logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

# http://stackoverflow.com/questions/21234772/python-tornado-disable-logging-to-stderr
stream = logging.StreamHandler(sys.stderr)
stream.setLevel(logging.CRITICAL)
logging.getLogger('tornado').addHandler(stream)
logging.getLogger('tornado').propagate = False
