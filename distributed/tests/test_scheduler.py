from __future__ import print_function, division, absolute_import

import cloudpickle
from collections import defaultdict, deque
from copy import deepcopy
from datetime import timedelta
import json
from operator import add, mul
import sys
from time import sleep

import dask
from dask import delayed
from dask.core import get_deps
from toolz import merge, concat, valmap, first, frequencies
from tornado.queues import Queue
from tornado.gen import TimeoutError
from tornado import gen

import pytest

from distributed import Nanny, Worker, Client
from distributed.core import connect, rpc, CommClosedError
from distributed.scheduler import validate_state, Scheduler, BANDWIDTH
from distributed.client import _wait, _first_completed
from distributed.metrics import time
from distributed.protocol.pickle import dumps
from distributed.worker import dumps_function, dumps_task
from distributed.utils_test import (inc, ignoring, dec, gen_cluster, gen_test,
        loop, readone, slowinc, slowadd, cluster, div)
from distributed.utils import All, tmpfile
from distributed.utils_test import slow
from dask.compatibility import apply


alice = 'alice:1234'
bob = 'bob:1234'

occupancy = defaultdict(lambda: 0)


@gen_cluster()
def test_administration(s, a, b):
    assert isinstance(s.address, str)
    assert s.address in str(s)
    assert str(sum(s.ncores.values())) in repr(s)
    assert str(len(s.ncores)) in repr(s)


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)])
def test_respect_data_in_memory(c, s, a):
    x = delayed(inc)(1)
    y = delayed(inc)(x)
    f = c.persist(y)
    yield _wait([f])

    assert s.released == {x.key}
    assert s.who_has == {y.key: {a.address}}

    z = delayed(add)(x, y)
    f2 = c.persist(z)
    while not f2.key in s.who_has:
        assert y.key in s.who_has
        yield gen.sleep(0.0001)


@gen_cluster(client=True)
def test_recompute_released_results(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)

    yy = c.persist(y)
    yield _wait(yy)

    while x.key in s.who_has or x.key in a.data or x.key in b.data:  # let x go away
        yield gen.sleep(0.01)

    z = delayed(dec)(x)
    zz = c.compute(z)
    result = yield zz
    assert result == 1


@gen_cluster(client=True)
def test_decide_worker_with_many_independent_leaves(c, s, a, b):
    xs = yield [c._scatter(list(range(0, 100, 2)), workers=a.address),
                c._scatter(list(range(1, 100, 2)), workers=b.address)]
    xs = list(concat(zip(*xs)))
    ys = [delayed(inc)(x) for x in xs]

    y2s = c.persist(ys)
    yield _wait(y2s)

    nhits = (sum(y.key in a.data for y in y2s[::2]) +
             sum(y.key in b.data for y in y2s[1::2]))

    assert nhits > 80


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_decide_worker_with_restrictions(client, s, a, b, c):
    x = client.submit(inc, 1, workers=[a.address, b.address])
    yield _wait(x)
    assert x.key in a.data or x.key in b.data


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_move_data_over_break_restrictions(client, s, a, b, c):
    [x] = yield client._scatter([1], workers=b.address)
    y = client.submit(inc, x, workers=[a.address, b.address])
    yield _wait(y)
    assert y.key in a.data or y.key in b.data


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_balance_with_restrictions(client, s, a, b, c):
    [x], [y] = yield [client._scatter([[1, 2, 3]], workers=a.address),
                      client._scatter([1], workers=c.address)]
    z = client.submit(inc, 1, workers=[a.address, c.address])
    yield _wait(z)

    assert s.who_has[z.key] == {c.address}


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_no_valid_workers(client, s, a, b, c):
    x = client.submit(inc, 1, workers='127.0.0.5:9999')
    while not s.tasks:
        yield gen.sleep(0.01)

    assert x.key in s.unrunnable

    with pytest.raises(gen.TimeoutError):
        yield gen.with_timeout(timedelta(milliseconds=50), x)


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_no_valid_workers_loose_restrictions(client, s, a, b, c):
    x = client.submit(inc, 1, workers='127.0.0.5:9999',
                      allow_other_workers=True)

    result = yield x
    assert result == 2


@gen_cluster(client=True, ncores=[])
def test_no_workers(client, s):
    x = client.submit(inc, 1)
    while not s.tasks:
        yield gen.sleep(0.01)

    assert x.key in s.unrunnable

    with pytest.raises(gen.TimeoutError):
        yield gen.with_timeout(timedelta(milliseconds=50), x)


@gen_cluster(ncores=[])
def test_retire_workers_empty(s):
    yield s.retire_workers(workers=[])


@pytest.mark.skip
def test_validate_state():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies = {'x': set(), 'y': {'x'}}
    waiting = {'y': {'x'}}
    ready = deque(['x'])
    dependents = {'x': {'y'}, 'y': set()}
    waiting_data = {'x': {'y'}}
    who_has = dict()
    stacks = {alice: [], bob: []}
    processing = {alice: dict(), bob: dict()}
    finished_results = set()
    released = set()
    in_play = {'x', 'y'}
    who_wants = {'y': {'client'}}
    wants_what = {'client': {'y'}}
    erred = {}

    validate_state(**locals())

    who_has['x'] = {alice}
    with pytest.raises(Exception):
        validate_state(**locals())

    ready.remove('x')
    with pytest.raises(Exception):
        validate_state(**locals())

    waiting['y'].remove('x')
    with pytest.raises(Exception):
        validate_state(**locals())

    del waiting['y']
    ready.appendleft('y')
    validate_state(**locals())

    stacks[alice].append('y')
    with pytest.raises(Exception):
        validate_state(**locals())

    ready.remove('y')
    validate_state(**locals())

    stacks[alice].pop()
    with pytest.raises(Exception):
        validate_state(**locals())

    processing[alice]['y'] = 1
    validate_state(**locals())

    del processing[alice]['y']
    with pytest.raises(Exception):
        validate_state(**locals())

    who_has['y'] = {alice}
    with pytest.raises(Exception):
        validate_state(**locals())

    finished_results.add('y')
    with pytest.raises(Exception):
        validate_state(**locals())

    waiting_data.pop('x')
    who_has.pop('x')
    released.add('x')
    validate_state(**locals())


@gen_cluster()
def test_server(s, a, b):
    comm = yield connect(s.address)
    yield comm.write({'op': 'register-client', 'client': 'ident'})
    yield comm.write({'op': 'update-graph',
                     'tasks': {'x': dumps_task((inc, 1)),
                               'y': dumps_task((inc, 'x'))},
                     'dependencies': {'x': [], 'y': ['x']},
                     'keys': ['y'],
                     'client': 'ident'})

    while True:
        msg = yield readone(comm)
        if msg['op'] == 'key-in-memory' and msg['key'] == 'y':
            break

    yield comm.write({'op': 'close-stream'})
    msg = yield readone(comm)
    assert msg == {'op': 'stream-closed'}
    with pytest.raises(CommClosedError):
        yield readone(comm)
    yield comm.close()


@gen_cluster()
def test_remove_client(s, a, b):
    s.update_graph(tasks={'x': dumps_task((inc, 1)),
                          'y': dumps_task((inc, 'x'))},
                   dependencies={'x': [], 'y': ['x']},
                   keys=['y'],
                   client='ident')

    assert s.tasks
    assert s.dependencies

    s.remove_client(client='ident')

    assert not s.tasks
    assert not s.dependencies


@gen_cluster()
def test_server_listens_to_other_ops(s, a, b):
    with rpc(s.address) as r:
        ident = yield r.identity()
        assert ident['type'] == 'Scheduler'


@gen_cluster()
def test_remove_worker_from_scheduler(s, a, b):
    dsk = {('x-%d' % i): (inc, i) for i in range(20)}
    s.update_graph(tasks=valmap(dumps_task, dsk), keys=list(dsk),
                   dependencies={k: set() for k in dsk})

    assert a.address in s.worker_comms
    s.remove_worker(address=a.address)
    assert a.address not in s.ncores
    assert len(s.processing[b.address]) == len(dsk)  # b owns everything
    s.validate_state()


@gen_cluster()
def test_add_worker(s, a, b):
    w = Worker(s.ip, s.port, ncores=3)
    w.data['x-5'] = 6
    w.data['y'] = 1
    yield w._start(0)

    dsk = {('x-%d' % i): (inc, i) for i in range(10)}
    s.update_graph(tasks=valmap(dumps_task, dsk), keys=list(dsk), client='client',
                   dependencies={k: set() for k in dsk})

    s.add_worker(address=w.address, keys=list(w.data),
                 ncores=w.ncores, services=s.services)

    s.validate_state()

    assert w.ip in s.host_info
    assert s.host_info[w.ip]['addresses'] == {a.address, b.address, w.address}
    yield w._close()


@gen_cluster()
def test_feed(s, a, b):
    def func(scheduler):
        return dumps(scheduler.processing)

    comm = yield connect(s.address)
    yield comm.write({'op': 'feed',
                      'function': dumps(func),
                      'interval': 0.01})

    for i in range(5):
        response = yield comm.read()
        expected = s.processing
        assert cloudpickle.loads(response) == expected

    yield comm.close()


@gen_cluster()
def test_feed_setup_teardown(s, a, b):
    def setup(scheduler):
        return 1

    def func(scheduler, state):
        assert state == 1
        return 'OK'

    def teardown(scheduler, state):
        scheduler.flag = 'done'

    comm = yield connect(s.address)
    yield comm.write({'op': 'feed',
                      'function': dumps(func),
                      'setup': dumps(setup),
                      'teardown': dumps(teardown),
                      'interval': 0.01})

    for i in range(5):
        response = yield comm.read()
        assert response == 'OK'

    yield comm.close()
    start = time()
    while not hasattr(s, 'flag'):
        yield gen.sleep(0.01)
        assert time() - start < 5


@gen_cluster()
def test_feed_large_bytestring(s, a, b):
    np = pytest.importorskip('numpy')

    x = np.ones(10000000)

    def func(scheduler):
        y = x
        return True

    comm = yield connect(s.address)
    yield comm.write({'op': 'feed',
                      'function': dumps(func),
                      'interval': 0.01})

    for i in range(5):
        response = yield comm.read()
        assert response == True

    yield comm.close()


@gen_test(timeout=None)
def test_scheduler_as_center():
    s = Scheduler(validate=True)
    done = s.start(0)
    a = Worker(s.address, ncores=1)
    a.data.update({'x': 1, 'y': 2})
    b = Worker(s.address, ncores=2)
    b.data.update({'y': 2, 'z': 3})
    c = Worker(s.address, ncores=3)
    yield [w._start(0) for w in [a, b, c]]

    assert s.ncores == {w.address: w.ncores for w in [a, b, c]}
    assert not s.who_has

    s.update_graph(tasks={'a': dumps_task((inc, 1))},
                   keys=['a'],
                   dependencies={'a': []})
    start = time()
    while not 'a' in s.who_has:
        assert time() - start < 5
        yield gen.sleep(0.01)
    assert 'a' in a.data or 'a' in b.data or 'a' in c.data

    yield [w._close() for w in [a, b, c]]

    assert s.ncores == {}
    assert s.who_has == {}

    yield s.close()


@gen_cluster(client=True)
def test_delete_data(c, s, a, b):
    d = yield c._scatter({'x': 1, 'y': 2, 'z': 3})

    assert set(s.who_has) == {'x', 'y', 'z'}
    assert set(a.data) | set(b.data) == {'x', 'y', 'z'}
    assert merge(a.data, b.data) == {'x': 1, 'y': 2, 'z': 3}

    del d['x']
    del d['y']

    start = time()
    while set(a.data) | set(b.data) != {'z'}:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)])
def test_delete(c, s, a):
    x = c.submit(inc, 1)
    yield x
    assert x.key in a.data

    yield c._cancel(x)

    start = time()
    while x.key in a.data:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster()
def test_filtered_communication(s, a, b):
    c = yield connect(s.address)
    f = yield connect(s.address)
    yield c.write({'op': 'register-client', 'client': 'c'})
    yield f.write({'op': 'register-client', 'client': 'f'})
    yield c.read()
    yield f.read()

    assert set(s.comms) == {'c', 'f'}

    yield c.write({'op': 'update-graph',
                   'tasks': {'x': dumps_task((inc, 1)),
                             'y': dumps_task((inc, 'x'))},
                   'dependencies': {'x': [], 'y': ['x']},
                   'client': 'c',
                   'keys': ['y']})

    yield f.write({'op': 'update-graph',
                   'tasks': {'x': dumps_task((inc, 1)),
                             'z': dumps_task((add, 'x', 10))},
                   'dependencies': {'x': [], 'z': ['x']},
                   'client': 'f',
                   'keys': ['z']})

    msg, = yield c.read()
    assert msg['op'] == 'key-in-memory'
    assert msg['key'] == 'y'
    msg, = yield f.read()
    assert msg['op'] == 'key-in-memory'
    assert msg['key'] == 'z'


def test_dumps_function():
    a = dumps_function(inc)
    assert cloudpickle.loads(a)(10) == 11

    b = dumps_function(inc)
    assert a is b

    c = dumps_function(dec)
    assert a != c


def test_dumps_task():
    d = dumps_task((inc, 1))
    assert set(d) == {'function', 'args'}

    f = lambda x, y=2: x + y
    d = dumps_task((apply, f, (1,), {'y': 10}))
    assert cloudpickle.loads(d['function'])(1, 2) == 3
    assert cloudpickle.loads(d['args']) == (1,)
    assert cloudpickle.loads(d['kwargs']) == {'y': 10}

    d = dumps_task((apply, f, (1,)))
    assert cloudpickle.loads(d['function'])(1, 2) == 3
    assert cloudpickle.loads(d['args']) == (1,)
    assert set(d) == {'function', 'args'}


@gen_cluster()
def test_ready_remove_worker(s, a, b):
    s.update_graph(tasks={'x-%d' % i: dumps_task((inc, i)) for i in range(20)},
                   keys=['x-%d' % i for i in range(20)],
                   client='client',
                   dependencies={'x-%d' % i: [] for i in range(20)})

    assert all(len(s.processing[w]) >= s.ncores[w]
                for w in s.ncores)

    s.remove_worker(address=a.address)

    for collection in [s.ncores, s.processing]:
        assert set(collection) == {b.address}
    assert all(len(s.processing[w]) >= s.ncores[w]
                for w in s.ncores)
    assert set(s.processing) == {b.address}


@gen_cluster(client=True, Worker=Nanny)
def test_restart(c, s, a, b):
    futures = c.map(inc, range(20))
    yield _wait(futures)

    yield s.restart()

    for c in [s.processing, s.ncores, s.occupancy]:
        assert len(c) == 2

    for c in [s.processing, s.occupancy]:
        assert not any(v for v in c.values())

    assert not s.tasks
    assert not s.dependencies


@gen_cluster()
def test_broadcast(s, a, b):
    result = yield s.broadcast(msg={'op': 'ping'})
    assert result == {a.address: b'pong', b.address: b'pong'}

    result = yield s.broadcast(msg={'op': 'ping'}, workers=[a.address])
    assert result == {a.address: b'pong'}

    result = yield s.broadcast(msg={'op': 'ping'}, hosts=[a.ip])
    assert result == {a.address: b'pong', b.address: b'pong'}


@gen_cluster(Worker=Nanny)
def test_broadcast_nanny(s, a, b):
    result1 = yield s.broadcast(msg={'op': 'identity'}, nanny=True)
    assert all(d['type'] == 'Nanny' for d in result1.values())

    result2 = yield s.broadcast(msg={'op': 'identity'},
                               workers=[a.worker_address],
                               nanny=True)
    assert len(result2) == 1
    assert first(result2.values())['id'] == a.id

    result3 = yield s.broadcast(msg={'op': 'identity'}, hosts=[a.ip],
                                nanny=True)
    assert result1 == result3


@gen_test()
def test_worker_name():
    s = Scheduler(validate=True)
    s.start(0)
    w = Worker(s.ip, s.port, name='alice')
    yield w._start()
    assert s.worker_info[w.address]['name'] == 'alice'
    assert s.aliases['alice'] == w.address

    with pytest.raises(ValueError):
        w = Worker(s.ip, s.port, name='alice')
        yield w._start()

    yield s.close()
    yield w._close()


@gen_test()
def test_coerce_address():
    s = Scheduler(validate=True)
    s.start(0)
    print("scheduler:", s.address, s.listen_address)
    a = Worker(s.ip, s.port, name='alice')
    b = Worker(s.ip, s.port, name=123)
    c = Worker('127.0.0.1', s.port, name='charlie')
    yield [a._start(), b._start(), c._start()]

    assert s.coerce_address('127.0.0.1:8000') == 'tcp://127.0.0.1:8000'
    assert s.coerce_address('[::1]:8000') == 'tcp://[::1]:8000'
    assert s.coerce_address('tcp://127.0.0.1:8000') == 'tcp://127.0.0.1:8000'
    assert s.coerce_address('tcp://[::1]:8000') == 'tcp://[::1]:8000'
    assert s.coerce_address('localhost:8000') in ('tcp://127.0.0.1:8000', 'tcp://[::1]:8000')
    assert s.coerce_address(u'localhost:8000') in ('tcp://127.0.0.1:8000', 'tcp://[::1]:8000')
    assert s.coerce_address(a.address) == a.address
    # Aliases
    assert s.coerce_address('alice') == a.address
    assert s.coerce_address(123) == b.address
    assert s.coerce_address('charlie') == c.address

    assert s.coerce_hostname('127.0.0.1') == '127.0.0.1'
    assert s.coerce_hostname('alice') == a.ip
    assert s.coerce_hostname(123) == b.ip
    assert s.coerce_hostname('charlie') == c.ip
    assert s.coerce_hostname('jimmy') == 'jimmy'

    assert s.coerce_address('zzzt:8000', resolve=False) == 'tcp://zzzt:8000'

    yield s.close()
    yield [w._close() for w in [a, b, c]]


@pytest.mark.skipif(sys.platform.startswith('win'),
                    reason="file descriptors not really a thing")
@gen_cluster(ncores=[])
def test_file_descriptors_dont_leak(s):
    psutil = pytest.importorskip('psutil')
    proc = psutil.Process()
    before = proc.num_fds()

    w = Worker(s.ip, s.port)

    yield w._start(0)
    yield w._close()

    during = proc.num_fds()

    start = time()
    while proc.num_fds() > before:
        yield gen.sleep(0.01)
        assert time() < start + 5


@gen_cluster()
def test_update_graph_culls(s, a, b):
    s.update_graph(tasks={'x': dumps_task((inc, 1)),
                          'y': dumps_task((inc, 'x')),
                          'z': dumps_task((inc, 2))},
                   keys=['y'],
                   dependencies={'y': 'x', 'x': [], 'z': []},
                   client='client')
    assert 'z' not in s.tasks
    assert 'z' not in s.dependencies


@pytest.mark.skipif(not sys.platform.startswith('linux'),
                    reason="Need 127.0.0.2 to mean localhost")
@gen_cluster(ncores=[('127.0.0.1', 2), ('127.0.0.2', 2), ('127.0.0.1', 1)])
def test_host_health(s, a, b, c):
    start = time()

    for w in [a, b, c]:
        assert w.ip in s.host_info
        assert 0 <= s.host_info[w.ip]['cpu'] <= 100
        assert 0 < s.host_info[w.ip]['memory']
        assert 0 < s.host_info[w.ip]['memory_percent'] < 100

        assert isinstance(s.host_info[w.ip]['last-seen'], (int, float))
        assert -1 < s.worker_info[w.address]['time-delay'] < 1

    assert set(s.host_info) == {'127.0.0.1', '127.0.0.2'}
    assert s.host_info['127.0.0.1']['cores'] == 3
    assert s.host_info['127.0.0.1']['addresses'] == {a.address, c.address}
    assert s.host_info['127.0.0.2']['cores'] == 2
    assert s.host_info['127.0.0.2']['addresses'] == {b.address}

    s.remove_worker(address=a.address)

    assert set(s.host_info) == {'127.0.0.1', '127.0.0.2'}
    assert s.host_info['127.0.0.1']['cores'] == 1
    assert s.host_info['127.0.0.1']['addresses'] == {c.address}
    assert s.host_info['127.0.0.2']['cores'] == 2
    assert s.host_info['127.0.0.2']['addresses'] == {b.address}

    s.remove_worker(address=b.address)

    assert set(s.host_info) == {'127.0.0.1'}
    assert s.host_info['127.0.0.1']['cores'] == 1
    assert s.host_info['127.0.0.1']['addresses'] == {c.address}

    s.remove_worker(address=c.address)

    assert not s.host_info


@gen_cluster(ncores=[])
def test_add_worker_is_idempotent(s):
    s.add_worker(address=alice, ncores=1, resolve_address=False)
    ncores = s.ncores.copy()
    s.add_worker(address=alice, resolve_address=False)
    assert s.ncores == s.ncores


def test_io_loop(loop):
    s = Scheduler(loop=loop, validate=True)
    assert s.io_loop is loop


@gen_cluster(client=True)
def test_transition_story(c, s, a, b):
    x = delayed(inc)(1)
    y = delayed(inc)(x)
    f = c.persist(y)
    yield _wait([f])

    assert s.transition_log

    story = s.transition_story(x.key)
    assert all(line in s.transition_log for line in story)
    assert len(story) < len(s.transition_log)
    assert all(x.key == line[0] or x.key in line[-2] for line in story)

    assert len(s.transition_story(x.key, y.key)) > len(story)


@gen_test()
def test_launch_without_blocked_services():
    from distributed.http import HTTPScheduler
    s = Scheduler(services={('http', 3849): HTTPScheduler})
    s.start(0)

    s2 = Scheduler(services={('http', 3849): HTTPScheduler})
    s2.start(0)

    assert not s2.services

    yield [s.close(), s2.close()]


@gen_cluster(ncores=[], client=True)
def test_scatter_no_workers(c, s):
    with pytest.raises(gen.TimeoutError):
        yield gen.with_timeout(timedelta(seconds=0.1),
                               s.scatter(data={'x': 1}, client='alice'))

    w = Worker(s.ip, s.port, ncores=3)
    yield [c._scatter(data={'x': 1}),
           w._start()]

    assert w.data['x'] == 1
    yield w._close()


@gen_cluster(ncores=[])
def test_scheduler_sees_memory_limits(s):
    w = Worker(s.ip, s.port, ncores=3, memory_limit=12345)
    yield w._start(0)

    assert s.worker_info[w.address]['memory_limit'] == 12345
    yield w._close()


@gen_cluster(client=True, timeout=1000)
def test_retire_workers(c, s, a, b):
    [x] = yield c._scatter([1], workers=a.address)
    [y] = yield c._scatter([list(range(1000))], workers=b.address)

    assert s.workers_to_close() == [a.address]

    workers = yield s.retire_workers()
    assert workers == [a.address]
    assert list(s.ncores) == [b.address]

    assert s.workers_to_close() == []

    assert s.has_what[b.address] == {x.key, y.key}

    workers = yield s.retire_workers()
    assert not workers


@slow
@pytest.mark.skipif(sys.platform.startswith('win'),
                    reason="file descriptors not really a thing")
@gen_cluster(client=True, ncores=[], timeout=240)
def test_file_descriptors(c, s):
    psutil = pytest.importorskip('psutil')
    da = pytest.importorskip('dask.array')
    proc = psutil.Process()
    num_fds_1 = proc.num_fds()

    N = 20
    nannies = [Nanny(s.ip, s.port, loop=s.loop) for i in range(N)]
    yield [n._start() for n in nannies]

    while len(s.ncores) < N:
        yield gen.sleep(0.1)

    num_fds_2 = proc.num_fds()

    yield gen.sleep(0.2)

    num_fds_3 = proc.num_fds()
    assert num_fds_3 == num_fds_2

    x = da.random.normal(10, 1, size=(1000, 1000), chunks=(10, 10))
    x = c.persist(x)
    yield _wait(x)

    num_fds_4 = proc.num_fds()
    assert num_fds_4 < num_fds_3 + N

    y = c.persist(x + x.T)
    yield _wait(y)

    num_fds_5 = proc.num_fds()
    assert num_fds_5 < num_fds_4 + N

    yield gen.sleep(1)

    num_fds_6 = proc.num_fds()
    assert num_fds_6 < num_fds_5 + N

    yield [n._close() for n in nannies]


@gen_cluster(client=True)
def test_learn_occupancy(c, s, a, b):
    futures = c.map(slowinc, range(1000), delay=0.01)
    while not any(s.who_has):
        yield gen.sleep(0.01)

    assert 1 < s.total_occupancy < 40
    for w in [a, b]:
        assert 1 < s.occupancy[w.address] < 20


@gen_cluster(client=True)
def test_learn_occupancy_2(c, s, a, b):
    future = c.map(slowinc, range(1000), delay=0.1)
    while not any(s.who_has):
        yield gen.sleep(0.01)

    assert 50 < s.total_occupancy < 200


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 30)
def test_balance_many_workers(c, s, *workers):
    futures = c.map(slowinc, range(20), delay=0.2)
    yield _wait(futures)
    assert set(map(len, s.has_what.values())) == {0, 1}


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 30)
def test_balance_many_workers_2(c, s, *workers):
    s.extensions['stealing']._pc.callback_time = 100000000
    futures = c.map(slowinc, range(90), delay=0.2)
    yield _wait(futures)
    assert set(map(len, s.has_what.values())) == {3}


@gen_cluster(client=True)
def test_learn_occupancy_multiple_workers(c, s, a, b):
    x = c.submit(slowinc, 1, delay=0.2, workers=a.address)
    yield gen.sleep(0.05)
    futures = c.map(slowinc, range(100), delay=0.2)

    yield _wait(x)

    assert not any(v == 0.5 for vv in s.processing.values() for v in vv)
    s.validate_state()


@gen_cluster(client=True)
def test_include_communication_in_occupancy(c, s, a, b):
    s.task_duration['slowadd'] = 0.001
    x = c.submit(mul, b'0', int(BANDWIDTH), workers=a.address)
    y = c.submit(mul, b'1', int(BANDWIDTH * 1.5), workers=b.address)

    z = c.submit(slowadd, x, y, delay=1)
    while z.key not in s.rprocessing:
        yield gen.sleep(0.01)

    try:
        assert s.processing[b.address][z.key] > 1
    except Exception:
        print("processing:", s.processing)
        print("rprocessing:", s.rprocessing)
        print("task_duration:", s.task_duration)
        print("nbytes:", s.nbytes)
        raise


@gen_cluster(client=True)
def test_worker_arrives_with_processing_data(c, s, a, b):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z])

    while not s.processing:
        yield gen.sleep(0.01)

    w = Worker(s.ip, s.port, ncores=1)
    w.put_key_in_memory(y.key, 3)

    yield w._start()

    start = time()

    while len(s.workers) < 3:
        yield gen.sleep(0.01)

    assert s.task_state[y.key] == 'memory'
    assert s.task_state[x.key] == 'released'
    assert s.task_state[z.key] == 'processing'

    yield w._close()


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)])
def test_worker_breaks_and_returns(c, s, a):
    future = c.submit(slowinc, 1, delay=0.1)
    for i in range(10):
        future = c.submit(slowinc, future, delay=0.1)

    yield _wait(future)

    a.batched_stream.comm.close()

    yield gen.sleep(0.1)
    start = time()
    yield _wait(future)
    end = time()

    assert end - start < 1

    assert frequencies(s.task_state.values()) == {'memory': 1, 'released': 10}


@gen_cluster(client=True, ncores=[])
def test_no_workers_to_memory(c, s):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z])

    while not s.task_state:
        yield gen.sleep(0.01)

    w = Worker(s.ip, s.port, ncores=1)
    w.put_key_in_memory(y.key, 3)

    yield w._start()

    start = time()

    while not s.workers:
        yield gen.sleep(0.01)

    assert s.task_state[y.key] == 'memory'
    assert s.task_state[x.key] == 'released'
    assert s.task_state[z.key] == 'processing'

    yield w._close()


@gen_cluster(client=True)
def test_no_worker_to_memory_restrictions(c, s, a, b):
    x = delayed(slowinc)(1, delay=0.4)
    y = delayed(slowinc)(x, delay=0.4)
    z = delayed(slowinc)(y, delay=0.4)

    yy, zz = c.persist([y, z], workers={(x, y, z): 'alice'})

    while not s.task_state:
        yield gen.sleep(0.01)

    w = Worker(s.ip, s.port, ncores=1, name='alice')
    w.put_key_in_memory(y.key, 3)

    yield w._start()

    while len(s.workers) < 3:
        yield gen.sleep(0.01)
    yield gen.sleep(0.3)

    assert s.task_state[y.key] == 'memory'
    assert s.task_state[x.key] == 'released'
    assert s.task_state[z.key] == 'processing'

    yield w._close()


def test_run_on_scheduler_sync(loop):
    def f(dask_scheduler=None):
        return dask_scheduler.address

    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            address = c.run_on_scheduler(f)
            assert address == s['address']

            with pytest.raises(ZeroDivisionError):
                c.run_on_scheduler(div, 1, 0)


@gen_cluster(client=True)
def test_run_on_scheduler(c, s, a, b):
    def f(dask_scheduler=None):
        return dask_scheduler.address

    response = yield c._run_on_scheduler(f)
    assert response == s.address


@gen_cluster(client=True)
def test_close_worker(c, s, a, b):
    assert len(s.workers) == 2

    yield s.close_worker(worker=a.address)

    assert len(s.workers) == 1
    assert a.address not in s.workers

    yield gen.sleep(0.5)

    assert len(s.workers) == 1


@slow
@gen_cluster(client=True, Worker=Nanny, timeout=20)
def test_close_nanny(c, s, a, b):
    assert len(s.workers) == 2

    assert a.process.is_alive()
    a_worker_address = a.worker_address
    start = time()
    yield s.close_worker(worker=a_worker_address)

    assert len(s.workers) == 1
    assert a_worker_address not in s.workers
    assert not a.is_alive()
    assert a.pid is None

    for i in range(10):
        yield gen.sleep(0.1)
        assert len(s.workers) == 1
        assert not a.is_alive()
        assert a.pid is None

    while a.status != 'closed':
        yield gen.sleep(0.05)
        assert time() < start + 10


@gen_cluster(client=True, timeout=20)
def test_retire_workers_close(c, s, a, b):
    yield s.retire_workers(close=True)
    assert not s.workers


@gen_cluster(client=True, timeout=20, Worker=Nanny)
def test_retire_nannies_close(c, s, a, b):
    nannies = [a, b]
    yield s.retire_workers(close=True, remove=True)
    assert not s.workers

    start = time()

    while any(n.status != 'closed' for n in nannies):
        yield gen.sleep(0.05)
        assert time() < start + 10

    assert not any(n.is_alive() for n in nannies)
    assert not s.workers


@gen_cluster(client=True, ncores=[('127.0.0.1', 2)])
def test_fifo_submission(c, s, w):
    futures = []
    for i in range(20):
        future = c.submit(slowinc, i, delay=0.1, key='inc-%02d' % i)
        futures.append(future)
        yield gen.sleep(0.01)
    yield _wait(futures[-1])
    assert futures[10].status == 'finished'


@gen_test()
def test_scheduler_file():
    with tmpfile() as fn:
        s = Scheduler(scheduler_file=fn)
        s.start(0)
        with open(fn) as f:
            data = json.load(f)
        assert data['address'] == s.address

        c = yield Client(scheduler_file=fn, loop=s.loop, asynchronous=True)
    yield s.close()


@slow
@gen_cluster(client=True, ncores=[])
def test_non_existent_worker(c, s):
    s.add_worker(address='127.0.0.1:5738', ncores=2, nbytes={}, host_info={})
    futures = c.map(inc, range(10))
    yield gen.sleep(4)
    assert not s.workers
    assert all(v == 'no-worker' for v in s.task_state.values())


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_correct_bad_time_estimate(c, s, *workers):
    future = c.submit(slowinc, 1, delay=0)
    yield _wait(future)

    futures = [c.submit(slowinc, future, delay=0.1, pure=False)
               for i in range(20)]

    yield gen.sleep(0.5)

    yield _wait(futures)

    assert all(w.data for w in workers)
