from __future__ import print_function, division, absolute_import

import cloudpickle
from collections import defaultdict, deque
from copy import deepcopy
from datetime import timedelta
from operator import add
import sys
from time import time, sleep

import dask
from dask import delayed
from dask.core import get_deps
from toolz import merge, concat, valmap, first
from tornado.queues import Queue
from tornado.iostream import StreamClosedError
from tornado.gen import TimeoutError
from tornado import gen

import pytest

from distributed import Nanny, Worker
from distributed.batched import BatchedStream
from distributed.core import connect, read, write, rpc, dumps
from distributed.scheduler import (validate_state, decide_worker,
        Scheduler)
from distributed.client import _wait
from distributed.worker import dumps_function, dumps_task
from distributed.utils_test import (inc, ignoring, dec, gen_cluster, gen_test,
        loop)
from distributed.utils import All
from dask.compatibility import apply


alice = 'alice:1234'
bob = 'bob:1234'

stack_duration = defaultdict(lambda: 0)


@gen_cluster()
def test_administration(s, a, b):
    assert isinstance(s.address, str)
    assert s.address_tuple[0] in s.address
    assert s.address in str(s)
    assert str(sum(s.ncores.values())) in repr(s)
    assert str(len(s.ncores)) in repr(s)


@gen_cluster(ncores=[])
def test_update_state(s):
    s.add_worker(address=alice, ncores=1, coerce_address=False)
    s.update_graph(tasks={'x': 1, 'y': dumps_task((inc, 'x'))},
                   keys=['y'],
                   dependencies={'y': 'x', 'x': set()},
                   client='client')

    s.ensure_occupied()
    r = s.transition('x', 'memory', nbytes=10, type=dumps(int),
            compute_start=10, compute_stop=11, worker=alice)
    s.transitions(r)
    s.ensure_occupied()

    assert set(s.processing[alice]) == {'y'}
    assert set(s.rprocessing['y']) == {alice}
    assert not s.ready
    assert s.who_wants == {'y': {'client'}}
    assert s.wants_what == {'client': {'y'}}

    s.update_graph(tasks={'a': 1, 'z': dumps_task((add, 'y', 'a'))},
                   keys=['z'],
                   dependencies={'z': {'y', 'a'}},
                   client='client')


    assert s.tasks == {'x': 1, 'y': dumps_task((inc, 'x')), 'a': 1,
                       'z': dumps_task((add, 'y', 'a'))}
    assert s.dependencies == {'x': set(), 'a': set(), 'y': {'x'}, 'z': {'a', 'y'}}
    assert s.dependents == {'z': set(), 'y': {'z'}, 'a': {'z'}, 'x': {'y'}}

    assert s.waiting == {'z': {'a', 'y'}}
    assert s.waiting_data == {'x': {'y'}, 'y': {'z'}, 'a': {'z'}, 'z': set()}

    assert s.who_wants == {'z': {'client'}, 'y': {'client'}}
    assert s.wants_what == {'client': {'y', 'z'}}

    assert 'a' in s.ready or 'a' in s.processing[alice]


@gen_cluster(ncores=[])
def test_update_state_with_processing(s):
    s.add_worker(address=alice, ncores=1, coerce_address=False)
    s.update_graph(tasks={'x': 1, 'y': dumps_task((inc, 'x')),
                          'z': dumps_task((inc, 'y'))},
                   keys=['z'],
                   dependencies={'y': {'x'}, 'x': set(), 'z': {'y'}},
                   client='client')

    s.ensure_occupied()
    r = s.transition('x', 'memory', nbytes=10, type=dumps(int),
            compute_start=10, compute_stop=11, worker=alice)
    s.transitions(r)
    s.ensure_occupied()

    assert s.waiting == {'z': {'y'}}
    assert s.waiting_data == {'x': {'y'}, 'y': {'z'}, 'z': set()}
    assert list(s.ready) == []

    assert s.who_wants == {'z': {'client'}}
    assert s.wants_what == {'client': {'z'}}

    assert s.who_has == {'x': {alice}}

    s.update_graph(tasks={'a': dumps_task((inc, 'x')), 'b': (add,'a','y'),
                          'c': dumps_task((inc, 'z'))},
                   keys=['b', 'c'],
                   dependencies={'a': {'x'}, 'b': {'a', 'y'}, 'c': {'z'}},
                   client='client')

    assert s.waiting == {'z': {'y'}, 'b': {'a', 'y'}, 'c': {'z'}}
    assert 'a' in s.stacks[alice] or 'a' in s.processing[alice]
    assert not s.ready
    assert s.waiting_data == {'x': {'y', 'a'}, 'y': {'z', 'b'}, 'z': {'c'},
                              'a': {'b'}, 'b': set(), 'c': set()}

    assert s.who_wants == {'b': {'client'}, 'c': {'client'}, 'z': {'client'}}
    assert s.wants_what == {'client': {'b', 'c', 'z'}}


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)])
def test_update_state_respects_data_in_memory(c, s, a):
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


@gen_cluster(ncores=[])
def test_update_state_supports_recomputing_released_results(s):
    s.add_worker(address=alice, ncores=1, coerce_address=False)
    s.update_graph(tasks={'x': 1, 'y': dumps_task((inc, 'x')),
                          'z': dumps_task((inc, 'x'))},
                   keys=['z'],
                   dependencies={'y': {'x'}, 'x': set(), 'z': {'y'}},
                   client='client')

    s.ensure_occupied()
    r = s.transition('x', 'memory', nbytes=10, type=dumps(int),
            compute_start=10, compute_stop=11, worker=alice)
    s.transitions(r)
    s.ensure_occupied()
    r = s.transition('y', 'memory', nbytes=10, type=dumps(int),
            compute_start=10, compute_stop=11, worker=alice)
    s.transitions(r)
    s.ensure_occupied()
    r = s.transition('z', 'memory', nbytes=10, type=dumps(int),
            compute_start=10, compute_stop=11, worker=alice)
    s.transitions(r)
    s.ensure_occupied()

    assert not s.waiting
    assert not s.ready
    assert s.waiting_data == {'z': set()}

    assert s.who_has == {'z': {alice}}

    s.update_graph(tasks={'x': 1, 'y': dumps_task((inc, 'x'))},
                   keys=['y'],
                   dependencies={'y': {'x'}, 'x': set()},
                   client='client')

    assert s.waiting == {'y': {'x'}}
    assert s.waiting_data == {'x': {'y'}, 'y': set(), 'z': set()}
    assert s.who_wants == {'z': {'client'}, 'y': {'client'}}
    assert s.wants_what == {'client': {'y', 'z'}}
    assert set(s.processing[alice]) == {'x'}


def test_decide_worker_with_many_independent_leaves():
    dsk = merge({('y', i): (inc, ('x', i)) for i in range(100)},
                {('x', i): i for i in range(100)})
    dependencies, dependents = get_deps(dsk)
    stacks = {alice: [], bob: []}
    processing = {alice: dict(), bob: dict()}
    who_has = merge({('x', i * 2): {alice} for i in range(50)},
                    {('x', i * 2 + 1): {bob} for i in range(50)})
    nbytes = {k: 0 for k in who_has}
    ncores = {alice: 1, bob: 1}

    for key in dsk:
        worker = decide_worker(dependencies, stacks, stack_duration, processing,
                               who_has, {}, {}, set(), nbytes, ncores, key)
        stacks[worker].append(key)

    nhits = (len([k for k in stacks[alice] if alice in who_has[('x', k[1])]])
             + len([k for k in stacks[bob] if bob in who_has[('x', k[1])]]))

    assert nhits > 90


def test_decide_worker_with_restrictions():
    dependencies = {'x': set()}
    alice, bob, charlie = 'alice:8000', 'bob:8000', 'charlie:8000'
    stacks = {alice: [], bob: [], charlie: []}
    processing = {alice: dict(), bob: dict(), charlie: dict()}
    who_has = {}
    restrictions = {'x': {'alice', 'charlie'}}
    nbytes = {}
    ncores = {alice: 1, bob: 1, charlie: 1}
    result = decide_worker(dependencies, stacks, stack_duration, processing,
                          who_has, {}, restrictions, set(), nbytes, ncores, 'x')
    assert result in {alice, charlie}


    stacks = {alice: [1, 2, 3], bob: [], charlie: [4, 5, 6]}
    result = decide_worker(dependencies, stacks, stack_duration, processing,
                          who_has, {}, restrictions, set(), nbytes, ncores, 'x')
    assert result in {alice, charlie}

    dependencies = {'x': {'y'}}
    who_has = {'y': {bob}}
    nbytes = {'y': 0}
    result = decide_worker(dependencies, stacks, stack_duration, processing,
                          who_has, {}, restrictions, set(), nbytes, ncores, 'x')
    assert result in {alice, charlie}


def test_decide_worker_with_loose_restrictions():
    dependencies = {'x': set()}
    alice, bob, charlie = 'alice:8000', 'bob:8000', 'charlie:8000'
    stacks = {alice: [1, 2, 3], bob: [], charlie: [1]}
    stack_duration = {alice: 3, bob: 0, charlie: 1}
    processing = {alice: dict(), bob: dict(), charlie: dict()}
    who_has = {}
    nbytes = {}
    ncores = {alice: 1, bob: 1, charlie: 1}
    restrictions = {'x': {'alice', 'charlie'}}

    result = decide_worker(dependencies, stacks, stack_duration, processing,
                          who_has, {}, restrictions, set(), nbytes, ncores, 'x')
    assert result == charlie

    result = decide_worker(dependencies, stacks, stack_duration, processing,
                          who_has, {}, restrictions, {'x'}, nbytes, ncores, 'x')
    assert result == charlie

    restrictions = {'x': {'david', 'ethel'}}
    result = decide_worker(dependencies, stacks, stack_duration, processing,
                          who_has, {}, restrictions, set(), nbytes, ncores, 'x')
    assert result is None

    restrictions = {'x': {'david', 'ethel'}}
    result = decide_worker(dependencies, stacks, stack_duration, processing,
                          who_has, {}, restrictions, {'x'}, nbytes, ncores, 'x')

    assert result == bob


def test_decide_worker_without_stacks():
    assert not decide_worker({'x': []}, {}, {}, {}, {}, {}, {}, set(), {}, {},
                             'x')


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


def div(x, y):
    return x / y

@gen_cluster()
def test_scheduler(s, a, b):
    stream = yield connect(s.ip, s.port)
    yield write(stream, {'op': 'register-client', 'client': 'ident'})
    stream = BatchedStream(stream, 10)
    msg = yield read(stream)
    assert msg['op'] == 'stream-start'

    # Test update graph
    yield write(stream, {'op': 'update-graph',
                         'tasks': valmap(dumps_task, {'x': (inc, 1),
                                                      'y': (inc, 'x'),
                                                      'z': (inc, 'y')}),
                         'dependencies': {'x': [],
                                          'y': ['x'],
                                          'z': ['y']},
                         'keys': ['x', 'z'],
                         'client': 'ident'})
    while True:
        msg = yield read(stream)
        if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
            break

    assert a.data.get('x') == 2 or b.data.get('x') == 2

    # Test erring tasks
    yield write(stream, {'op': 'update-graph',
                         'tasks': valmap(dumps_task, {'a': (div, 1, 0),
                                                       'b': (inc, 'a')}),
                         'dependencies': {'a': [],
                                           'b': ['a']},
                         'keys': ['a', 'b'],
                         'client': 'ident'})

    while True:
        msg = yield read(stream)
        if msg['op'] == 'task-erred' and msg['key'] == 'b':
            break

    # Test missing data
    yield write(stream, {'op': 'missing-data', 'keys': ['z']})
    s.ensure_occupied()

    while True:
        msg = yield read(stream)
        if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
            break

    # Test missing data without being informed
    for w in [a, b]:
        if 'z' in w.data:
            del w.data['z']
    yield write(stream, {'op': 'update-graph',
                         'tasks': {'zz': dumps_task((inc, 'z'))},
                         'dependencies': {'zz': ['z']},
                         'keys': ['zz'],
                         'client': 'ident'})
    while True:
        msg = yield read(stream)
        if msg['op'] == 'key-in-memory' and msg['key'] == 'zz':
            break

    write(stream, {'op': 'close'})
    stream.close()


@gen_cluster()
def test_multi_queues(s, a, b):
    sched, report = Queue(), Queue()
    s.handle_queues(sched, report)

    msg = yield report.get()
    assert msg['op'] == 'stream-start'

    # Test update graph
    sched.put_nowait({'op': 'update-graph',
                      'tasks': valmap(dumps_task, {'x': (inc, 1),
                                                   'y': (inc, 'x'),
                                                   'z': (inc, 'y')}),
                      'dependencies': {'x': [],
                                       'y': ['x'],
                                       'z': ['y']},
                      'keys': ['z']})

    while True:
        msg = yield report.get()
        if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
            break

    slen, rlen = len(s.scheduler_queues), len(s.report_queues)
    sched2, report2 = Queue(), Queue()
    s.handle_queues(sched2, report2)
    assert slen + 1 == len(s.scheduler_queues)
    assert rlen + 1 == len(s.report_queues)

    sched2.put_nowait({'op': 'update-graph',
                       'tasks': {'a': dumps_task((inc, 10))},
                       'dependencies': {'a': []},
                       'keys': ['a']})

    for q in [report, report2]:
        while True:
            msg = yield q.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'a':
                break


@gen_cluster()
def test_server(s, a, b):
    stream = yield connect('127.0.0.1', s.port)
    yield write(stream, {'op': 'register-client', 'client': 'ident'})
    stream = BatchedStream(stream, 0)
    stream.send({'op': 'update-graph',
                 'tasks': {'x': dumps_task((inc, 1)),
                           'y': dumps_task((inc, 'x'))},
                 'dependencies': {'x': [], 'y': ['x']},
                 'keys': ['y'],
                 'client': 'ident'})

    while True:
        msg = yield read(stream)
        if msg['op'] == 'key-in-memory' and msg['key'] == 'y':
            break

    stream.send({'op': 'close-stream'})
    msg = yield read(stream)
    assert msg == {'op': 'stream-closed'}
    assert stream.closed()
    stream.close()


@gen_cluster()
def test_remove_client(s, a, b):
    s.add_client(client='ident')
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
    with rpc(ip='127.0.0.1', port=s.port) as r:
        ident = yield r.identity()
        assert ident['type'] == 'Scheduler'


@gen_cluster()
def test_remove_worker_from_scheduler(s, a, b):
    dsk = {('x', i): (inc, i) for i in range(20)}
    s.update_graph(tasks=valmap(dumps_task, dsk), keys=list(dsk),
                   dependencies={k: set() for k in dsk})
    assert s.ready
    assert not any(stack for stack in s.stacks.values())

    assert a.address in s.worker_streams
    s.remove_worker(address=a.address)
    assert a.address not in s.ncores
    assert len(s.ready) + len(s.processing[b.address]) == \
            len(dsk)  # b owns everything
    s.validate_state()


@gen_cluster()
def test_add_worker(s, a, b):
    w = Worker(s.ip, s.port, ncores=3, ip='127.0.0.1')
    w.data['x-5'] = 6
    w.data['y'] = 1
    yield w._start(0)

    dsk = {('x-%d' % i).encode(): (inc, i) for i in range(10)}
    s.update_graph(tasks=valmap(dumps_task, dsk), keys=list(dsk), client='client',
                   dependencies={k: set() for k in dsk})

    s.add_worker(address=w.address, keys=list(w.data),
                 ncores=w.ncores, services=s.services, coerce_address=False)

    s.validate_state()

    assert w.ip in s.host_info
    assert s.host_info[w.ip]['ports'] == set(map(str, [a.port, b.port, w.port]))
    yield w._close()


@gen_cluster()
def test_feed(s, a, b):
    def func(scheduler):
        return dumps((scheduler.processing, scheduler.stacks))

    stream = yield connect(s.ip, s.port)
    yield write(stream, {'op': 'feed',
                         'function': dumps(func),
                         'interval': 0.01})

    for i in range(5):
        response = yield read(stream)
        expected = s.processing, s.stacks
        assert cloudpickle.loads(response) == expected

    stream.close()


@gen_cluster()
def test_feed_setup_teardown(s, a, b):
    def setup(scheduler):
        return 1

    def func(scheduler, state):
        assert state == 1
        return 'OK'

    def teardown(scheduler, state):
        scheduler.flag = 'done'

    stream = yield connect(s.ip, s.port)
    yield write(stream, {'op': 'feed',
                         'function': dumps(func),
                         'setup': dumps(setup),
                         'teardown': dumps(teardown),
                         'interval': 0.01})

    for i in range(5):
        response = yield read(stream)
        assert response == 'OK'

    stream.close()
    start = time()
    while not hasattr(s, 'flag'):
        yield gen.sleep(0.01)
        assert time() - start < 5


@gen_test(timeout=None)
def test_scheduler_as_center():
    s = Scheduler(validate=True)
    done = s.start(0)
    a = Worker('127.0.0.1', s.port, ip='127.0.0.1', ncores=1)
    a.data.update({'x': 1, 'y': 2})
    b = Worker('127.0.0.1', s.port, ip='127.0.0.1', ncores=2)
    b.data.update({'y': 2, 'z': 3})
    c = Worker('127.0.0.1', s.port, ip='127.0.0.1', ncores=3)
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

    with ignoring(StreamClosedError):
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


@gen_cluster(client=True)
def test_delete_callback(c, s, a, b):
    d = yield c._scatter({'x': 1}, workers=a.address)

    assert s.who_has['x'] == {a.address}
    assert s.has_what[a.address] == {'x'}

    s.client_releases_keys(client=c.id, keys=['x'])

    assert 'x' not in s.who_has
    assert 'x' not in s.has_what[a.address]
    assert a.data['x'] == 1  # still in memory
    assert s.deleted_keys == {a.address: {'x'}}
    yield s.clear_data_from_workers()
    assert not s.deleted_keys
    assert not a.data

    assert s._delete_periodic_callback.is_running()


@gen_cluster()
def test_self_aliases(s, a, b):
    a.data['a'] = 1
    s.update_data(who_has={'a': [a.address]},
                  nbytes={'a': 10}, client='client')
    s.update_graph(tasks=valmap(dumps_task, {'a': 'a', 'b': (inc, 'a')}),
                   keys=['b'], client='client',
                   dependencies={'b': ['a']})

    sched, report = Queue(), Queue()
    s.handle_queues(sched, report)
    msg = yield report.get()
    assert msg['op'] == 'stream-start'

    while True:
        msg = yield report.get()
        if msg['op'] == 'key-in-memory' and msg['key'] == 'b':
            break


@gen_cluster()
def test_filtered_communication(s, a, b):
    c = yield connect(ip=s.ip, port=s.port)
    f = yield connect(ip=s.ip, port=s.port)
    yield write(c, {'op': 'register-client', 'client': 'c'})
    yield write(f, {'op': 'register-client', 'client': 'f'})
    yield read(c)
    yield read(f)
    c = BatchedStream(c, 0)
    f = BatchedStream(f, 0)

    assert set(s.streams) == {'c', 'f'}

    yield write(c, {'op': 'update-graph',
                    'tasks': {'x': dumps_task((inc, 1)),
                              'y': dumps_task((inc, 'x'))},
                    'dependencies': {'x': [], 'y': ['x']},
                    'client': 'c',
                    'keys': ['y']})

    yield write(f, {'op': 'update-graph',
                    'tasks': {'x': dumps_task((inc, 1)),
                              'z': dumps_task((add, 'x', 10))},
                    'dependencies': {'x': [], 'z': ['x']},
                    'client': 'f',
                    'keys': ['z']})

    msg = yield read(c)
    assert msg['op'] == 'key-in-memory'
    assert msg['key'] == 'y'
    msg = yield read(f)
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
    s.add_client(client='client')
    s.update_graph(tasks={'x-%d' % i: dumps_task((inc, i)) for i in range(20)},
                   keys=['x-%d' % i for i in range(20)],
                   client='client',
                   dependencies={'x-%d' % i: [] for i in range(20)})

    assert all(len(s.processing[w]) >= s.ncores[w]
                for w in s.ncores)
    assert not any(stack for stack in s.stacks.values())
    assert len(s.ready) + sum(map(len, s.processing.values())) == 20

    s.remove_worker(address=a.address)

    for collection in [s.ncores, s.stacks, s.processing]:
        assert set(collection) == {b.address}
    assert all(len(s.processing[w]) >= s.ncores[w]
                for w in s.ncores)
    assert set(s.processing) == {b.address}
    assert not any(stack for stack in s.stacks.values())
    assert len(s.ready) + sum(map(len, s.processing.values())) == 20


@gen_cluster(Worker=Nanny)
def test_restart(s, a, b):
    s.add_client(client='client')
    s.update_graph(tasks={'x-%d' % i: dumps_task((inc, i)) for i in range(20)},
                   keys=['x-%d' % i for i in range(20)],
                   client='client',
                   dependencies={'x-%d' % i: [] for i in range(20)})

    assert len(s.ready) + sum(map(len, s.processing.values())) == 20
    assert s.ready

    yield s.restart()

    for c in [s.stacks, s.processing, s.ncores]:
        assert len(c) == 2

    for c in [s.stacks, s.processing]:
        assert not any(v for v in c.values())

    assert not s.ready
    assert not s.tasks
    assert not s.dependencies


@gen_cluster()
def test_ready_add_worker(s, a, b):
    s.add_client(client='client')
    s.update_graph(tasks={'x-%d' % i: dumps_task((inc, i)) for i in range(20)},
                   keys=['x-%d' % i for i in range(20)],
                   client='client',
                   dependencies={'x-%d' % i: [] for i in range(20)})

    assert all(len(s.processing[w]) == s.ncores[w]
                for w in s.ncores)
    assert len(s.ready) + sum(map(len, s.processing.values())) == 20

    w = Worker(s.ip, s.port, ncores=3, ip='127.0.0.1')
    w.listen(0)
    s.add_worker(address=w.address, ncores=w.ncores, coerce_address=False)

    assert w.address in s.ncores
    assert all(len(s.processing[w]) == s.ncores[w]
                for w in s.ncores)
    assert len(s.ready) + sum(map(len, s.processing.values())) == 20

    w.scheduler.close_rpc()


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
    a = Worker(s.ip, s.port, name='alice')
    b = Worker(s.ip, s.port, name=123)
    c = Worker(s.ip, s.port, name='charlie', ip='127.0.0.2')
    yield [a._start(), b._start(), c._start()]

    assert s.coerce_address(b'127.0.0.1') == '127.0.0.1'
    assert s.coerce_address(('127.0.0.1', 8000)) == '127.0.0.1:8000'
    assert s.coerce_address(['127.0.0.1', 8000]) == '127.0.0.1:8000'
    assert s.coerce_address([b'127.0.0.1', 8000]) == '127.0.0.1:8000'
    assert s.coerce_address(('127.0.0.1', '8000')) == '127.0.0.1:8000'
    assert s.coerce_address(b'localhost') == '127.0.0.1'
    assert s.coerce_address('localhost') == '127.0.0.1'
    assert s.coerce_address(u'localhost') == '127.0.0.1'
    assert s.coerce_address('localhost:8000') == '127.0.0.1:8000'
    assert s.coerce_address(a.address) == a.address
    assert s.coerce_address(a.address_tuple) == a.address
    assert s.coerce_address(123) == b.address
    assert s.coerce_address('charlie') == c.address

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
    s.add_client(client='client')
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
    assert s.host_info['127.0.0.1']['ports'] == {str(a.port), str(c.port)}
    assert s.host_info['127.0.0.2']['cores'] == 2
    assert s.host_info['127.0.0.2']['ports'] == {str(b.port)}

    s.remove_worker(address=a.address)

    assert set(s.host_info) == {'127.0.0.1', '127.0.0.2'}
    assert s.host_info['127.0.0.1']['cores'] == 1
    assert s.host_info['127.0.0.1']['ports'] == {str(c.port)}
    assert s.host_info['127.0.0.2']['cores'] == 2
    assert s.host_info['127.0.0.2']['ports'] == {str(b.port)}

    s.remove_worker(address=b.address)

    assert set(s.host_info) == {'127.0.0.1'}
    assert s.host_info['127.0.0.1']['cores'] == 1
    assert s.host_info['127.0.0.1']['ports'] == {str(c.port)}

    s.remove_worker(address=c.address)

    assert not s.host_info


@gen_cluster(ncores=[])
def test_add_worker_is_idempotent(s):
    s.add_worker(address=alice, ncores=1, coerce_address=False)
    ncores = s.ncores.copy()
    s.add_worker(address=alice, coerce_address=False)
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
    assert all(x.key == line[0] or x.key in line[-1] for line in story)

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


@gen_cluster(ncores=[])
def test_scatter_no_workers(s):
    with pytest.raises(gen.TimeoutError):
        yield gen.with_timeout(timedelta(seconds=0.1),
                              s.scatter(data={'x': dumps(1)}, client='alice'))

    w = Worker(s.ip, s.port, ncores=3, ip='127.0.0.1')
    yield [s.scatter(data={'x': dumps(1)}, client='alice'),
           w._start()]

    assert w.data['x'] == 1
    yield w._close()


@gen_cluster(ncores=[])
def test_scheduler_sees_memory_limits(s):
    w = Worker(s.ip, s.port, ncores=3, ip='127.0.0.1', memory_limit=12345)
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
