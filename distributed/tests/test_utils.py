from __future__ import print_function, division, absolute_import

from collections import Iterator
from functools import partial
import io
import socket
import sys
from time import sleep
from threading import Thread
import threading
import traceback

import numpy as np
import pytest
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.locks import Event

import dask
from distributed.compatibility import Queue, Empty, isqueue, PY2
from distributed.metrics import time
from distributed.utils import (All, sync, is_kernel, ensure_ip, str_graph,
                               truncate_exception, get_traceback, queue_to_iterator,
                               iterator_to_queue, _maybe_complex, read_block, seek_delimiter,
                               funcname, ensure_bytes, open_port, get_ip_interface, nbytes,
                               set_thread_state, thread_state, LoopRunner,
                               parse_bytes)
from distributed.utils_test import loop, loop_in_thread  # flake8: noqa
from distributed.utils_test import div, has_ipv6, inc, throws, gen_test


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


def test_sync(loop_in_thread):
    loop = loop_in_thread
    result = sync(loop, inc, 1)
    assert result == 2


def test_sync_error(loop_in_thread):
    loop = loop_in_thread
    try:
        result = sync(loop, throws, 1)
    except Exception as exc:
        f = exc
        assert 'hello' in str(exc)
        tb = get_traceback()
        L = traceback.format_tb(tb)
        assert any('throws' in line for line in L)

    def function1(x):
        return function2(x)

    def function2(x):
        return throws(x)

    try:
        result = sync(loop, function1, 1)
    except Exception as exc:
        assert 'hello' in str(exc)
        tb = get_traceback()
        L = traceback.format_tb(tb)
        assert any('function1' in line for line in L)
        assert any('function2' in line for line in L)


def test_sync_timeout(loop_in_thread):
    loop = loop_in_thread
    with pytest.raises(gen.TimeoutError):
        sync(loop_in_thread, gen.sleep, 0.5, callback_timeout=0.05)


def test_sync_closed_loop():
    loop = IOLoop.current()
    loop.close()
    IOLoop.clear_current()
    IOLoop.clear_instance()

    with pytest.raises(RuntimeError) as exc_info:
        sync(loop, inc, 1)
    exc_info.match("IOLoop is clos(ed|ing)")


def test_is_kernel():
    pytest.importorskip('IPython')
    assert is_kernel() is False


#@pytest.mark.leaking('fds')
#def test_zzz_leaks(l=[]):
    #import os, subprocess
    #l.append(b"x" * (17 * 1024**2))
    #os.open(__file__, os.O_RDONLY)
    #subprocess.Popen('sleep 100', shell=True, stdin=subprocess.DEVNULL)


def test_ensure_ip():
    assert ensure_ip('localhost') in ('127.0.0.1', '::1')
    assert ensure_ip('123.123.123.123') == '123.123.123.123'
    assert ensure_ip('8.8.8.8') == '8.8.8.8'
    if has_ipv6():
        assert ensure_ip('2001:4860:4860::8888') == '2001:4860:4860::8888'
        assert ensure_ip('::1') == '::1'


def test_get_ip_interface():
    if sys.platform == 'darwin':
        assert get_ip_interface('lo0') == '127.0.0.1'
    elif sys.platform.startswith('linux'):
        assert get_ip_interface('lo') == '127.0.0.1'
    else:
        pytest.skip("test needs to be enhanced for platform %r" % (sys.platform,))
    with pytest.raises(KeyError):
        get_ip_interface('__non-existent-interface')


def test_truncate_exception():
    e = ValueError('a' * 1000)
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
        c(1)
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
    dsk = {'x': 1}
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
        assert all(isinstance(k, str) for k in sdsk)
        assert dask.get(dsk, keys) == dask.get(sdsk, skeys)


def test_maybe_complex():
    assert not _maybe_complex(1)
    assert not _maybe_complex('x')
    assert _maybe_complex((inc, 1))
    assert _maybe_complex([(inc, 1)])
    assert _maybe_complex([(inc, 1)])
    assert _maybe_complex({'x': (inc, 1)})


def test_read_block():
    delimiter = b'\n'
    data = delimiter.join([b'123', b'456', b'789'])
    f = io.BytesIO(data)

    assert read_block(f, 1, 2) == b'23'
    assert read_block(f, 0, 1, delimiter=b'\n') == b'123\n'
    assert read_block(f, 0, 2, delimiter=b'\n') == b'123\n'
    assert read_block(f, 0, 3, delimiter=b'\n') == b'123\n'
    assert read_block(f, 0, 5, delimiter=b'\n') == b'123\n456\n'
    assert read_block(f, 0, 8, delimiter=b'\n') == b'123\n456\n789'
    assert read_block(f, 0, 100, delimiter=b'\n') == b'123\n456\n789'
    assert read_block(f, 1, 1, delimiter=b'\n') == b''
    assert read_block(f, 1, 5, delimiter=b'\n') == b'456\n'
    assert read_block(f, 1, 8, delimiter=b'\n') == b'456\n789'

    for ols in [[(0, 3), (3, 3), (6, 3), (9, 2)],
                [(0, 4), (4, 4), (8, 4)]]:
        out = [read_block(f, o, l, b'\n') for o, l in ols]
        assert b"".join(filter(None, out)) == data


def test_seek_delimiter_endline():
    f = io.BytesIO(b'123\n456\n789')

    # if at zero, stay at zero
    seek_delimiter(f, b'\n', 5)
    assert f.tell() == 0

    # choose the first block
    for bs in [1, 5, 100]:
        f.seek(1)
        seek_delimiter(f, b'\n', blocksize=bs)
        assert f.tell() == 4

    # handle long delimiters well, even with short blocksizes
    f = io.BytesIO(b'123abc456abc789')
    for bs in [1, 2, 3, 4, 5, 6, 10]:
        f.seek(1)
        seek_delimiter(f, b'abc', blocksize=bs)
        assert f.tell() == 6

    # End at the end
    f = io.BytesIO(b'123\n456')
    f.seek(5)
    seek_delimiter(f, b'\n', 5)
    assert f.tell() == 7


def test_funcname():
    def f():
        pass

    assert funcname(f) == 'f'
    assert funcname(partial(f)) == 'f'
    assert funcname(partial(partial(f))) == 'f'


def test_ensure_bytes():
    data = [b'1', '1', memoryview(b'1'), bytearray(b'1')]
    if PY2:
        data.append(buffer(b'1'))  # flake8: noqa
    for d in data:
        result = ensure_bytes(d)
        assert isinstance(result, bytes)
        assert result == b'1'


def test_nbytes():
    def check(obj, expected):
        assert nbytes(obj) == expected
        assert nbytes(memoryview(obj)) == expected

    check(b'123', 3)
    check(bytearray(b'4567'), 4)

    multi_dim = np.ones(shape=(10, 10))
    scalar = np.array(1)

    check(multi_dim, multi_dim.nbytes)
    check(scalar, scalar.nbytes)


def test_open_port():
    port = open_port()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    s.close()


def test_set_thread_state():
    with set_thread_state(x=1):
        assert thread_state.x == 1

    assert not hasattr(thread_state, 'x')


def assert_running(loop):
    """
    Raise if the given IOLoop is not running.
    """
    q = Queue()
    loop.add_callback(q.put, 42)
    assert q.get(timeout=1) == 42

def assert_not_running(loop):
    """
    Raise if the given IOLoop is running.
    """
    q = Queue()
    try:
        loop.add_callback(q.put, 42)
    except RuntimeError:
        # On AsyncIOLoop, can't add_callback() after the loop is closed
        pass
    else:
        with pytest.raises(Empty):
            q.get(timeout=0.02)


def test_loop_runner(loop_in_thread):
    # Implicit loop
    loop = IOLoop()
    loop.make_current()
    runner = LoopRunner()
    assert runner.loop not in (loop, loop_in_thread)
    assert not runner.is_started()
    assert_not_running(runner.loop)
    runner.start()
    assert runner.is_started()
    assert_running(runner.loop)
    runner.stop()
    assert not runner.is_started()
    assert_not_running(runner.loop)

    # Explicit loop
    loop = IOLoop()
    runner = LoopRunner(loop=loop)
    assert runner.loop is loop
    assert not runner.is_started()
    assert_not_running(loop)
    runner.start()
    assert runner.is_started()
    assert_running(loop)
    runner.stop()
    assert not runner.is_started()
    assert_not_running(loop)

    # Explicit loop, already started
    runner = LoopRunner(loop=loop_in_thread)
    assert not runner.is_started()
    assert_running(loop_in_thread)
    runner.start()
    assert runner.is_started()
    assert_running(loop_in_thread)
    runner.stop()
    assert not runner.is_started()
    assert_running(loop_in_thread)

    # Implicit loop, asynchronous=True
    loop = IOLoop()
    loop.make_current()
    runner = LoopRunner(asynchronous=True)
    assert runner.loop is loop
    assert not runner.is_started()
    assert_not_running(runner.loop)
    runner.start()
    assert runner.is_started()
    assert_not_running(runner.loop)
    runner.stop()
    assert not runner.is_started()
    assert_not_running(runner.loop)

    # Explicit loop, asynchronous=True
    loop = IOLoop()
    runner = LoopRunner(loop=loop, asynchronous=True)
    assert runner.loop is loop
    assert not runner.is_started()
    assert_not_running(runner.loop)
    runner.start()
    assert runner.is_started()
    assert_not_running(runner.loop)
    runner.stop()
    assert not runner.is_started()
    assert_not_running(runner.loop)


def test_two_loop_runners(loop_in_thread):
    # Loop runners tied to the same loop should cooperate

    # ABCCBA
    loop = IOLoop()
    a = LoopRunner(loop=loop)
    b = LoopRunner(loop=loop)
    assert_not_running(loop)
    a.start()
    assert_running(loop)
    c = LoopRunner(loop=loop)
    b.start()
    assert_running(loop)
    c.start()
    assert_running(loop)
    c.stop()
    assert_running(loop)
    b.stop()
    assert_running(loop)
    a.stop()
    assert_not_running(loop)

    # ABCABC
    loop = IOLoop()
    a = LoopRunner(loop=loop)
    b = LoopRunner(loop=loop)
    assert_not_running(loop)
    a.start()
    assert_running(loop)
    b.start()
    assert_running(loop)
    c = LoopRunner(loop=loop)
    c.start()
    assert_running(loop)
    a.stop()
    assert_running(loop)
    b.stop()
    assert_running(loop)
    c.stop()
    assert_not_running(loop)

    # Explicit loop, already started
    a = LoopRunner(loop=loop_in_thread)
    b = LoopRunner(loop=loop_in_thread)
    assert_running(loop_in_thread)
    a.start()
    assert_running(loop_in_thread)
    b.start()
    assert_running(loop_in_thread)
    a.stop()
    assert_running(loop_in_thread)
    b.stop()
    assert_running(loop_in_thread)


@gen_test()
def test_loop_runner_gen():
    runner = LoopRunner(asynchronous=True)
    assert runner.loop is IOLoop.current()
    assert not runner.is_started()
    yield gen.sleep(0.01)
    runner.start()
    assert runner.is_started()
    yield gen.sleep(0.01)
    runner.stop()
    assert not runner.is_started()
    yield gen.sleep(0.01)


def test_parse_bytes():
    assert parse_bytes('100') == 100
    assert parse_bytes('100 MB') == 100000000
    assert parse_bytes('100M') == 100000000
    assert parse_bytes('5kB') == 5000
    assert parse_bytes('5.4 kB') == 5400
    assert parse_bytes('1kiB') == 1024
    assert parse_bytes('1Mi') == 2**20
    assert parse_bytes('1e6') == 1000000
    assert parse_bytes('1e6 kB') == 1000000000
    assert parse_bytes('MB') == 1000000
