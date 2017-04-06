from __future__ import print_function, division, absolute_import

from collections import Iterator
from functools import partial
import io
import logging
import socket
from time import sleep
from threading import Thread
import threading
import traceback

import pytest
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.locks import Event

import dask
from distributed.compatibility import Queue, isqueue, PY2
from distributed.metrics import time
from distributed.utils import (All, sync, is_kernel, ensure_ip, str_graph,
        truncate_exception, get_traceback, queue_to_iterator,
        iterator_to_queue, _maybe_complex, read_block, seek_delimiter,
        funcname, ensure_bytes, open_port)
from distributed.utils_test import (loop, inc, throws, div, captured_handler,
                                    captured_logger, has_ipv6)


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

    loop.add_callback(e.set)
    thread.join()


def test_sync_inactive_loop(loop):
    @gen.coroutine
    def f(x):
        raise gen.Return(x + 1)

    y = sync(loop, f, 1)
    assert y == 2


def test_sync_timeout(loop):
    e = Event()

    @gen.coroutine
    def wait_until_event():
        yield e.wait()

    thread = Thread(target=loop.run_sync, args=(wait_until_event,))
    thread.daemon = True
    thread.start()
    while not loop._running:
        sleep(0.01)

    with pytest.raises(gen.TimeoutError):
        sync(loop, gen.sleep, 0.5, callback_timeout=0.05)

    loop.add_callback(e.set)
    thread.join()


def test_is_kernel():
    pytest.importorskip('IPython')
    assert is_kernel() is False


def test_ensure_ip():
    assert ensure_ip('localhost') in ('127.0.0.1', '::1')
    assert ensure_ip('123.123.123.123') == '123.123.123.123'
    assert ensure_ip('8.8.8.8') == '8.8.8.8'
    if has_ipv6():
        assert ensure_ip('2001:4860:4860::8888') == '2001:4860:4860::8888'
        assert ensure_ip('::1') == '::1'


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
        data.append(buffer(b'1'))
    for d in data:
        result = ensure_bytes(d)
        assert isinstance(result, bytes)
        assert result == b'1'


def dump_logger_list():
    root = logging.getLogger()
    loggers = root.manager.loggerDict
    print()
    print("== Loggers (name, level, effective level, propagate) ==")

    def logger_info(name, logger):
        return (name, logging.getLevelName(logger.level),
                logging.getLevelName(logger.getEffectiveLevel()),
                logger.propagate)

    infos = []
    infos.append(logger_info('<root>', root))

    for name, logger in sorted(loggers.items()):
        if not isinstance(logger, logging.Logger):
            # Skip 'PlaceHolder' objects
            continue
        assert logger.name == name
        infos.append(logger_info(name, logger))

    for info in infos:
        print("%-40s %-8s %-8s %-5s" % info)

    print()


def test_logging():
    """
    Test default logging configuration.
    """
    d = logging.getLogger('distributed')
    assert len(d.handlers) == 1
    assert isinstance(d.handlers[0], logging.StreamHandler)

    # Work around Bokeh messing with the root logger level
    # https://github.com/bokeh/bokeh/issues/5793
    root = logging.getLogger('')
    old_root_level = root.level
    root.setLevel('WARN')

    try:
        dfb = logging.getLogger('distributed.foo.bar')
        f = logging.getLogger('foo')
        fb = logging.getLogger('foo.bar')

        with captured_handler(d.handlers[0]) as distributed_log:
            with captured_logger(root) as foreign_log:
                h = logging.StreamHandler(foreign_log)
                fmt = '[%(levelname)s in %(name)s] - %(message)s'
                h.setFormatter(logging.Formatter(fmt))
                fb.addHandler(h)
                fb.propagate = False

                # For debugging
                dump_logger_list()

                d.debug("1: debug")
                d.info("2: info")
                dfb.info("3: info")
                fb.info("4: info")
                fb.error("5: error")
                f.info("6: info")
                f.error("7: error")

        distributed_log = distributed_log.getvalue().splitlines()
        foreign_log = foreign_log.getvalue().splitlines()

        # distributed log is configured at INFO level by default
        assert distributed_log == [
            "distributed - INFO - 2: info",
            "distributed.foo.bar - INFO - 3: info",
            ]

        # foreign logs should be unaffected by distributed's logging
        # configuration.  They get the default ERROR level from logging.
        assert foreign_log == [
            "[ERROR in foo.bar] - 5: error",
            "7: error",
            ]

    finally:
        root.setLevel(old_root_level)


def test_open_port():
    port = open_port()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', port))
    s.close()
