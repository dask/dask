from collections import Iterator
import pytest
from threading import Thread
import threading
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.locks import Event
from time import time, sleep

import dask
from distributed.utils import (All, sync, is_kernel, ensure_ip, str_graph,
        truncate_exception, get_traceback, queue_to_iterator,
        iterator_to_queue, _maybe_complex)
from distributed.utils_test import loop, inc, throws, div
from distributed.compatibility import Queue, isqueue


def test_All(loop):
    @gen.coroutine
    def throws():
        1 / 0

    @gen.coroutine
    def slow():
        yield gen.sleep(10)

    @gen.coroutine
    def inc(x):
        raise gen.Return(x + 1)

    @gen.coroutine
    def f():

        results = yield All(*[inc(i) for i in range(10)])
        assert results == list(range(1, 11))

        start = time()
        for tasks in [[throws(), slow()], [slow(), throws()]]:
            try:
                yield All(tasks)
                assert False
            except ZeroDivisionError:
                pass
            end = time()
            assert end - start < 10

    loop.run_sync(f)


def test_sync(loop):
    e = Event()
    e2 = threading.Event()

    @gen.coroutine
    def wait_until_event():
        e2.set()
        yield e.wait()

    thread = Thread(target=loop.run_sync, args=(wait_until_event,))
    thread.daemon = True
    thread.start()

    e2.wait()
    result = sync(loop, inc, 1)
    assert result == 2

    loop.add_callback(e.set)
    thread.join()


def test_sync_error(loop):
    e = Event()

    @gen.coroutine
    def wait_until_event():
        yield e.wait()

    thread = Thread(target=loop.run_sync, args=(wait_until_event,))
    thread.daemon = True
    thread.start()
    while not loop._running:
        sleep(0.01)

    with pytest.raises(Exception):
        result = sync(loop, throws, 1)

    loop.add_callback(e.set)
    thread.join()


def test_sync_inactive_loop(loop):
    @gen.coroutine
    def f(x):
        raise gen.Return(x + 1)

    y = sync(loop, f, 1)
    assert y == 2


def test_is_kernel():
    pytest.importorskip('IPython')
    assert is_kernel() is False


def test_ensure_ip():
    assert ensure_ip('localhost') == '127.0.0.1'
    assert ensure_ip('123.123.123.123') == '123.123.123.123'


def test_truncate_exception():
    e = ValueError('a'*1000)
    assert len(str(e)) >= 1000
    f = truncate_exception(e, 100)
    assert type(f) == type(e)
    assert len(str(f)) < 200
    assert 'aaaa' in str(f)

    e = ValueError('a')
    assert truncate_exception(e) is e


def test_get_traceback():
    def a(x):
        return div(x, 0)
    def b(x):
        return a(x)
    def c(x):
        return b(x)

    try:
        c(x)
    except Exception as e:
        tb = get_traceback()
        assert type(tb).__name__ == 'traceback'


def test_queue_to_iterator():
    q = Queue()
    q.put(1)
    q.put(2)

    seq = queue_to_iterator(q)
    assert isinstance(seq, Iterator)
    assert next(seq) == 1
    assert next(seq) == 2


def test_iterator_to_queue():
    seq = iter([1, 2, 3])

    q = iterator_to_queue(seq)
    assert isqueue(q)
    assert q.get() == 1


def test_str_graph():
    dsk = {b'x': 1}
    assert str_graph(dsk) == dsk

    dsk = {('x', 1): (inc, 1)}
    assert str_graph(dsk) == {str(('x', 1)): (inc, 1)}

    dsk = {('x', 1): (inc, 1), ('x', 2): (inc, ('x', 1))}
    assert str_graph(dsk) == {str(('x', 1)): (inc, 1),
                              str(('x', 2)): (inc, str(('x', 1)))}

    dsks = [{'x': 1},
            {('x', 1): (inc, 1), ('x', 2): (inc, ('x', 1))},
            {('x', 1): (sum, [1, 2, 3]),
             ('x', 2): (sum, [('x', 1), ('x', 1)])}]
    for dsk in dsks:
        sdsk = str_graph(dsk)
        keys = list(dsk)
        skeys = [str(k) for k in keys]
        assert all(isinstance(k, (str, bytes)) for k in sdsk)
        assert dask.get(dsk, keys) == dask.get(sdsk, skeys)


def test_maybe_complex():
    assert not _maybe_complex(1)
    assert not _maybe_complex('x')
    assert _maybe_complex((inc, 1))
    assert _maybe_complex([(inc, 1)])
    assert _maybe_complex([(inc, 1)])
    assert _maybe_complex({'x': (inc, 1)})
