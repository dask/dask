from __future__ import print_function, division, absolute_import

from numbers import Integral
from operator import add
import os
import shutil
import sys
import traceback
import logging
import re

import pytest
from toolz import pluck
from tornado import gen
from tornado.ioloop import TimeoutError

from distributed.batched import BatchedStream
from distributed.core import rpc, dumps, loads, connect, read, write
from distributed.client import _wait
from distributed.scheduler import Scheduler
from distributed.sizeof import sizeof
from distributed.worker import Worker, error_message, logger
from distributed.utils import ignoring
from distributed.utils_test import (loop, inc, gen_cluster,
        slow, slowinc, throws, current_loop, gen_test)



def test_worker_ncores():
    from distributed.worker import _ncores
    w = Worker('127.0.0.1', 8019)
    try:
        assert w.executor._max_workers == _ncores
    finally:
        shutil.rmtree(w.local_dir)


def test_identity():
    w = Worker('127.0.0.1', 8019)
    ident = w.identity(None)
    assert ident['type'] == 'Worker'
    assert ident['scheduler'] == ('127.0.0.1', 8019)
    assert isinstance(ident['ncores'], int)
    assert isinstance(ident['memory_limit'], int)


def test_health():
    w = Worker('127.0.0.1', 8019)
    d = w.host_health()
    assert isinstance(d, dict)
    d = w.host_health()
    try:
        import psutil
    except ImportError:
        pass
    else:
        assert 'disk-read' in d
        assert 'disk-write' in d
        assert 'network-recv' in d
        assert 'network-send' in d


@gen_cluster()
def test_worker_bad_args(c, a, b):
    aa = rpc(ip=a.ip, port=a.port)
    bb = rpc(ip=b.ip, port=b.port)

    class NoReprObj(object):
        """ This object cannot be properly represented as a string. """
        def __str__(self):
            raise ValueError("I have no str representation.")
        def __repr__(self):
            raise ValueError("I have no repr representation.")

    response = yield aa.compute(key='x',
                                function=dumps(NoReprObj),
                                args=dumps(()),
                                who_has={})
    assert not a.active
    assert response['status'] == 'OK'
    assert a.data['x']
    assert isinstance(response['compute_start'], float)
    assert isinstance(response['compute_stop'], float)
    assert isinstance(response['thread'], Integral)

    def bad_func(*args, **kwargs):
        1 / 0

    class MockLoggingHandler(logging.Handler):
        """Mock logging handler to check for expected logs."""

        def __init__(self, *args, **kwargs):
            self.reset()
            logging.Handler.__init__(self, *args, **kwargs)

        def emit(self, record):
            self.messages[record.levelname.lower()].append(record.getMessage())

        def reset(self):
            self.messages = {
                'debug': [],
                'info': [],
                'warning': [],
                'error': [],
                'critical': [],
            }

    hdlr = MockLoggingHandler()
    old_level = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(hdlr)
    response = yield bb.compute(key='y',
                                function=dumps(bad_func),
                                args=dumps(['x']),
                                kwargs=dumps({'k': 'x'}),
                                who_has={'x': [a.address]})
    assert not b.active
    assert response['status'] == 'error'
    # Make sure job died because of bad func and not because of bad
    # argument.
    assert isinstance(loads(response['exception']), ZeroDivisionError)
    if sys.version_info[0] >= 3:
        assert any('1 / 0' in line
                  for line in pluck(3, traceback.extract_tb(
                      loads(response['traceback'])))
                  if line)
    assert hdlr.messages['warning'][0] == " Compute Failed\n" \
        "Function: bad_func\n" \
        "args:     (< could not convert arg to str >)\n" \
        "kwargs:   {'k': < could not convert arg to str >}\n"
    assert re.match(r"^Send compute response to scheduler: y, " \
        "\{.*'args': \(< could not convert arg to str >\), .*" \
        "'kwargs': \{'k': < could not convert arg to str >\}.*\}",
        hdlr.messages['debug'][0]) or \
        re.match("^Send compute response to scheduler: y, " \
        "\{.*'kwargs': \{'k': < could not convert arg to str >\}, .*" \
        "'args': \(< could not convert arg to str >\).*\}",
        hdlr.messages['debug'][0])
    logger.setLevel(old_level)

    # Now we check that both workers are still alive.

    assert not a.active
    response = yield aa.compute(key='z',
                                function=dumps(add),
                                args=dumps([1, 2]),
                                who_has={},
                                close=True)
    assert not a.active
    assert response['status'] == 'OK'
    assert a.data['z'] == 3
    assert isinstance(response['compute_start'], float)
    assert isinstance(response['compute_stop'], float)
    assert isinstance(response['thread'], Integral)

    assert not b.active
    response = yield bb.compute(key='w',
                                function=dumps(add),
                                args=dumps([1, 2]),
                                who_has={},
                                close=True)
    assert not b.active
    assert response['status'] == 'OK'
    assert b.data['w'] == 3
    assert isinstance(response['compute_start'], float)
    assert isinstance(response['compute_stop'], float)
    assert isinstance(response['thread'], Integral)

    aa.close_rpc()
    bb.close_rpc()


@gen_cluster()
def test_worker(c, a, b):
    aa = rpc(ip=a.ip, port=a.port)
    bb = rpc(ip=b.ip, port=b.port)

    result = yield aa.identity()
    assert not a.active
    response = yield aa.compute(key='x',
                                function=dumps(add),
                                args=dumps([1, 2]),
                                who_has={},
                                close=True)
    assert not a.active
    assert response['status'] == 'OK'
    assert a.data['x'] == 3
    assert isinstance(response['compute_start'], float)
    assert isinstance(response['compute_stop'], float)
    assert isinstance(response['thread'], Integral)

    response = yield bb.compute(key='y',
                                function=dumps(add),
                                args=dumps(['x', 10]),
                                who_has={'x': [a.address]})
    assert response['status'] == 'OK'
    assert b.data['y'] == 13
    assert response['nbytes'] == sizeof(b.data['y'])
    assert isinstance(response['transfer_start'], float)
    assert isinstance(response['transfer_stop'], float)

    def bad_func():
        1 / 0

    response = yield bb.compute(key='z',
                                function=dumps(bad_func),
                                args=dumps(()),
                                close=True)
    assert not b.active
    assert response['status'] == 'error'
    assert isinstance(loads(response['exception']), ZeroDivisionError)
    if sys.version_info[0] >= 3:
        assert any('1 / 0' in line
                  for line in pluck(3, traceback.extract_tb(
                      loads(response['traceback'])))
                  if line)

    aa.close_rpc()
    yield a._close()

    assert a.address not in c.ncores and b.address in c.ncores

    assert list(c.ncores.keys()) == [b.address]

    assert isinstance(b.address, str)
    assert b.ip in b.address
    assert str(b.port) in b.address

    bb.close_rpc()


def test_compute_who_has(current_loop):
    @gen.coroutine
    def f():
        s = Scheduler()
        s.listen(0)
        x = Worker(s.ip, s.port, ip='127.0.0.1')
        y = Worker(s.ip, s.port, ip='127.0.0.1')
        z = Worker(s.ip, s.port, ip='127.0.0.1')
        x.data['a'] = 1
        y.data['a'] = 2
        yield [x._start(), y._start(), z._start()]

        zz = rpc(ip=z.ip, port=z.port)
        yield zz.compute(function=dumps(inc),
                         args=dumps(('a',)),
                         who_has={'a': [x.address]},
                         key='b')
        assert z.data['b'] == 2

        if 'a' in z.data:
            del z.data['a']
        yield zz.compute(function=dumps(inc),
                         args=dumps(('a',)),
                         who_has={'a': [y.address]},
                         key='c')
        assert z.data['c'] == 3

        yield [x._close(), y._close(), z._close()]
        zz.close_rpc()

    current_loop.run_sync(f, timeout=5)


@gen_cluster()
def dont_test_workers_update_center(s, a, b):
    aa = rpc(ip=a.ip, port=a.port)

    response = yield aa.update_data(data={'x': dumps(1), 'y': dumps(2)})
    assert response['status'] == 'OK'
    assert response['nbytes'] == {'x': sizeof(1), 'y': sizeof(2)}

    assert a.data == {'x': 1, 'y': 2}
    assert s.who_has == {'x': {a.address},
                         'y': {a.address}}
    assert s.has_what[a.address] == {'x', 'y'}

    yield aa.delete_data(keys=['x'], close=True)
    assert not s.who_has['x']
    assert all('x' not in s for s in c.has_what.values())

    aa.close_rpc()


@slow
@gen_cluster()
def dont_test_delete_data_with_missing_worker(c, a, b):
    bad = '127.0.0.1:9001'  # this worker doesn't exist
    c.who_has['z'].add(bad)
    c.who_has['z'].add(a.address)
    c.has_what[bad].add('z')
    c.has_what[a.address].add('z')
    a.data['z'] = 5

    cc = rpc(ip=c.ip, port=c.port)

    yield cc.delete_data(keys=['z'])  # TODO: this hangs for a while
    assert 'z' not in a.data
    assert not c.who_has['z']
    assert not c.has_what[bad]
    assert not c.has_what[a.address]

    cc.close_rpc()


@gen_cluster()
def test_upload_file(s, a, b):
    assert not os.path.exists(os.path.join(a.local_dir, 'foobar.py'))
    assert not os.path.exists(os.path.join(b.local_dir, 'foobar.py'))
    assert a.local_dir != b.local_dir

    aa = rpc(ip=a.ip, port=a.port)
    bb = rpc(ip=b.ip, port=b.port)
    yield [aa.upload_file(filename='foobar.py', data=b'x = 123'),
           bb.upload_file(filename='foobar.py', data='x = 123')]

    assert os.path.exists(os.path.join(a.local_dir, 'foobar.py'))
    assert os.path.exists(os.path.join(b.local_dir, 'foobar.py'))

    def g():
        import foobar
        return foobar.x

    yield aa.compute(function=dumps(g),
                     key='x')
    result = yield aa.get_data(keys=['x'])
    assert result == {'x': dumps(123)}

    yield a._close()
    yield b._close()
    aa.close_rpc()
    bb.close_rpc()
    assert not os.path.exists(os.path.join(a.local_dir, 'foobar.py'))


@gen_cluster()
def test_upload_egg(s, a, b):
    eggname = 'mytestegg-1.0.0-py3.4.egg'
    local_file = __file__.replace('test_worker.py', eggname)
    assert not os.path.exists(os.path.join(a.local_dir, eggname))
    assert not os.path.exists(os.path.join(b.local_dir, eggname))
    assert a.local_dir != b.local_dir

    aa = rpc(ip=a.ip, port=a.port)
    bb = rpc(ip=b.ip, port=b.port)
    with open(local_file, 'rb') as f:
        payload = f.read()
    yield [aa.upload_file(filename=eggname, data=payload),
           bb.upload_file(filename=eggname, data=payload)]

    assert os.path.exists(os.path.join(a.local_dir, eggname))
    assert os.path.exists(os.path.join(b.local_dir, eggname))

    def g(x):
        import testegg
        return testegg.inc(x)

    yield aa.compute(function=dumps(g), key='x', args=dumps((10,)))
    result = yield aa.get_data(keys=['x'])
    assert result == {'x': dumps(10 + 1)}

    yield a._close()
    yield b._close()
    aa.close_rpc()
    bb.close_rpc()
    assert not os.path.exists(os.path.join(a.local_dir, eggname))


@gen_cluster()
def test_broadcast(s, a, b):
    with rpc(ip=s.ip, port=s.port) as cc:
        results = yield cc.broadcast(msg={'op': 'ping'})
        assert results == {a.address: b'pong', b.address: b'pong'}


@gen_test()
def test_worker_with_port_zero():
    s = Scheduler()
    s.listen(8007)
    w = Worker(s.ip, s.port, ip='127.0.0.1')
    yield w._start()
    assert isinstance(w.port, int)
    assert w.port > 1024


@slow
def test_worker_waits_for_center_to_come_up(current_loop):
    @gen.coroutine
    def f():
        w = Worker('127.0.0.1', 8007, ip='127.0.0.1')
        yield w._start()

    try:
        current_loop.run_sync(f, timeout=4)
    except TimeoutError:
        pass


@gen_cluster()
def test_worker_task(s, a, b):
    with rpc(ip=a.ip, port=a.port) as aa:
        yield aa.compute(task=dumps((inc, 1)), key='x', report=False)
        assert a.data['x'] == 2


@gen_cluster()
def test_worker_task_data(s, a, b):
    with rpc(ip=a.ip, port=a.port) as aa:
        yield aa.compute(task=dumps(2), key='x', report=False)

    assert a.data['x'] == 2


@gen_cluster()
def test_worker_task_bytes(s, a, b):
    with rpc(ip=a.ip, port=a.port) as aa:
        yield aa.compute(task=dumps((inc, 1)), key='x', report=False)
        assert a.data['x'] == 2

        yield aa.compute(function=dumps(inc), args=dumps((10,)), key='y',
                report=False)
        assert a.data['y'] == 11


def test_error_message():
    class MyException(Exception):
        def __init__(self, a, b):
            self.args = (a + b,)
        def __str__(self):
            return "MyException(%s)" % self.args

    msg = error_message(MyException('Hello', 'World!'))
    assert 'Hello' in str(msg['exception'])


@gen_cluster()
def test_gather(s, a, b):
    b.data['x'] = 1
    b.data['y'] = 2
    with rpc(ip=a.ip, port=a.port) as aa:
        resp = yield aa.gather(who_has={'x': [b.address], 'y': [b.address]})
        assert resp['status'] == 'OK'

        assert a.data['x'] == b.data['x']
        assert a.data['y'] == b.data['y']


@gen_cluster()
def test_compute_stream(s, a, b):
    stream = yield connect(a.ip, a.port)
    yield write(stream, {'op': 'compute-stream'})
    msgs = [{'op': 'compute-task', 'function': dumps(inc), 'args': dumps((i,)), 'key': 'x-%d' % i}
            for i in range(10)]

    bstream = BatchedStream(stream, 0)
    for msg in msgs[:5]:
        yield write(stream, msg)

    for i in range(5):
        msg = yield read(bstream)
        assert msg['status'] == 'OK'
        assert msg['key'][0] == 'x'

    for msg in msgs[5:]:
        yield write(stream, msg)

    for i in range(5):
        msg = yield read(bstream)
        assert msg['status'] == 'OK'
        assert msg['key'][0] == 'x'

    yield write(stream, {'op': 'close'})


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)])
def test_active_holds_tasks(e, s, w):
    future = e.submit(slowinc, 1, delay=0.2)
    yield gen.sleep(0.1)
    assert future.key in w.active
    yield future._result()
    assert future.key not in w.active

    future = e.submit(throws, 1)
    with ignoring(Exception):
        yield _wait([future])
    assert not w.active


def test_io_loop(loop):
    s = Scheduler(loop=loop)
    s.listen(0)
    assert s.io_loop is loop
    w = Worker(s.ip, s.port, loop=loop)
    assert w.io_loop is loop


@gen_cluster(client=True, ncores=[])
def test_spill_to_disk(e, s):
    np = pytest.importorskip('numpy')
    w = Worker(s.ip, s.port, loop=s.loop, memory_limit=1000)
    yield w._start()

    x = e.submit(np.random.randint, 0, 255, size=500, dtype='u1', key='x')
    yield _wait(x)
    y = e.submit(np.random.randint, 0, 255, size=500, dtype='u1', key='y')
    yield _wait(y)

    assert set(w.data) == {x.key, y.key}
    assert set(w.data.fast) == {x.key, y.key}

    z = e.submit(np.random.randint, 0, 255, size=500, dtype='u1', key='z')
    yield _wait(z)
    assert set(w.data) == {x.key, y.key, z.key}
    assert set(w.data.fast) == {y.key, z.key}
    assert set(w.data.slow) == {x.key}

    yield x._result()
    assert set(w.data.fast) == {x.key, z.key}
    assert set(w.data.slow) == {y.key}
    yield w._close()


@gen_cluster(client=True)
def test_access_key(c, s, a, b):
    def f(i):
        from distributed.worker import thread_state
        return thread_state.key

    futures = [c.submit(f, i, key='x-%d' % i) for i in range(20)]
    results = yield c._gather(futures)
    assert list(results) == ['x-%d' % i for i in range(20)]


@gen_cluster(client=True)
def test_run_dask_worker(c, s, a, b):
    def f(dask_worker=None):
        return dask_worker.id

    response = yield c._run(f)
    assert response == {a.address: a.id, b.address: b.id}
