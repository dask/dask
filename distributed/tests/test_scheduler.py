from copy import deepcopy
from operator import add

from dask.core import get_deps
from toolz import merge, concat
from tornado.queues import Queue
import pytest

from distributed import Center, Nanny
from distributed.core import connect, read, write, rpc
from distributed.client import WrappedKey
from distributed.scheduler import (validate_state, heal, update_state,
        decide_worker, assign_many_tasks, heal_missing_data, Scheduler)
from distributed.utils_test import inc


def test_heal():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies = {'x': set(), 'y': {'x'}}
    dependents = {'x': {'y'}, 'y': set()}

    in_memory = set()
    stacks = {'alice': [], 'bob': []}
    processing = {'alice': set(), 'bob': set()}

    waiting = {'x': set(), 'y': {'x'}}
    waiting_data = {'x': {'y'}, 'y': set()}
    finished_results = set()

    local = {k: v for k, v in locals().items() if '@' not in k}

    output = heal(dependencies, dependents, in_memory, stacks, processing,
            {}, {})

    assert output['dependencies'] == dependencies
    assert output['dependents'] == dependents
    assert output['in_memory'] == in_memory
    assert output['processing'] == processing
    assert output['stacks'] == stacks
    assert output['waiting'] == waiting
    assert output['waiting_data'] == waiting_data
    assert output['released'] == set()

    state = {'in_memory': set(),
             'stacks': {'alice': ['x'], 'bob': []},
             'processing': {'alice': set(), 'bob': set()},
             'waiting': {}, 'waiting_data': {}}

    heal(dependencies, dependents, **state)


def test_heal_2():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y'),
           'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b'),
           'result': (add, 'z', 'c')}
    dependencies = {'x': set(), 'y': {'x'}, 'z': {'y'},
                    'a': set(), 'b': {'a'}, 'c': {'b'},
                    'result': {'z', 'c'}}
    dependents = {'x': {'y'}, 'y': {'z'}, 'z': {'result'},
                  'a': {'b'}, 'b': {'c'}, 'c': {'result'},
                  'result': set()}

    state = {'in_memory': {'y', 'a'},  # missing 'b'
             'stacks': {'alice': ['z'], 'bob': []},
             'processing': {'alice': set(), 'bob': set(['c'])},
             'waiting': {}, 'waiting_data': {}}

    output = heal(dependencies, dependents, **state)
    assert output['waiting'] == {'b': set(), 'c': {'b'}, 'result': {'c', 'z'}}
    assert output['waiting_data'] == {'a': {'b'}, 'b': {'c'}, 'c': {'result'},
                                      'y': {'z'}, 'z': {'result'},
                                      'result': set()}
    assert output['in_memory'] == set(['y', 'a'])
    assert output['stacks'] == {'alice': ['z'], 'bob': []}
    assert output['processing'] == {'alice': set(), 'bob': set()}
    assert output['released'] == {'x'}


def test_heal_restarts_leaf_tasks():
    dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b'),
           'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    dependents, dependencies = get_deps(dsk)

    state = {'in_memory': set(),  # missing 'b'
             'stacks': {'alice': ['a'], 'bob': ['x']},
             'processing': {'alice': set(), 'bob': set()},
             'waiting': {}, 'waiting_data': {}}

    del state['stacks']['bob']
    del state['processing']['bob']

    output = heal(dependencies, dependents, **state)
    assert 'x' in output['waiting']


def test_heal_culls():
    dsk = {'a': 1, 'b': (inc, 'a'), 'c': (inc, 'b'),
           'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    dependencies, dependents = get_deps(dsk)

    state = {'in_memory': {'c', 'y'},
             'stacks': {'alice': ['a'], 'bob': []},
             'processing': {'alice': set(), 'bob': set('y')},
             'waiting': {}, 'waiting_data': {}}

    output = heal(dependencies, dependents, **state)
    assert 'a' not in output['stacks']['alice']
    assert output['released'] == {'a', 'b', 'x'}
    assert output['finished_results'] == {'c'}
    assert 'y' not in output['processing']['bob']

    assert output['waiting']['z'] == set()


def test_update_state():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies = {'x': set(), 'y': {'x'}}
    dependents = {'x': {'y'}, 'y': set()}

    waiting = {'y': set()}
    waiting_data = {'x': {'y'}, 'y': set()}

    held_data = {'y'}
    who_has = {'x': {'alice'}}
    processing = set()
    released = set()
    in_play = {'x', 'y'}

    new_dsk = {'a': 1, 'z': (add, 'y', 'a')}
    new_keys = {'z'}

    e_dsk = {'x': 1, 'y': (inc, 'x'), 'a': 1, 'z': (add, 'y', 'a')}
    e_dependencies = {'x': set(), 'a': set(), 'y': {'x'}, 'z': {'a', 'y'}}
    e_dependents = {'z': set(), 'y': {'z'}, 'a': {'z'}, 'x': {'y'}}

    e_waiting = {'y': set(), 'a': set(), 'z': {'a', 'y'}}
    e_waiting_data = {'x': {'y'}, 'y': {'z'}, 'a': {'z'}, 'z': set()}

    e_held_data = {'y', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 who_has, in_play,
                 waiting, waiting_data, new_dsk, new_keys)

    assert dsk == e_dsk
    assert dependencies == e_dependencies
    assert dependents == e_dependents
    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data
    assert in_play == {'x', 'y', 'a', 'z'}


def test_update_state_with_processing():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    dependencies, dependents = get_deps(dsk)

    waiting = {'z': {'y'}}
    waiting_data = {'x': {'y'}, 'y': {'z'}, 'z': set()}

    held_data = {'z'}
    who_has = {'x': {'alice'}}
    processing = {'y'}
    released = set()
    in_play = {'z', 'x', 'y'}

    new_dsk = {'a': (inc, 'x'), 'b': (add, 'a', 'y'), 'c': (inc, 'z')}
    new_keys = {'b', 'c'}

    e_waiting = {'z': {'y'}, 'a': set(), 'b': {'a', 'y'}, 'c': {'z'}}
    e_waiting_data = {'x': {'y', 'a'}, 'y': {'z', 'b'}, 'z': {'c'},
                      'a': {'b'}, 'b': set(), 'c': set()}

    e_held_data = {'b', 'c', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 who_has, in_play,
                 waiting, waiting_data, new_dsk, new_keys)

    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data
    assert in_play == {'x', 'y', 'z', 'a', 'b', 'c'}


def test_update_state_respects_WrappedKeys():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies, dependents = get_deps(dsk)

    waiting = {'y': set()}
    waiting_data = {'x': {'y'}, 'y': set()}

    held_data = {'y'}
    who_has = {'x': {'alice'}}
    processing = set()
    released = set()
    in_play = {'x', 'y'}

    e_dsk = {'x': 1, 'y': (inc, 'x'), 'a': 1, 'z': (add, 'y', 'a')}
    e_dependencies = {'x': set(), 'a': set(), 'y': {'x'}, 'z': {'a', 'y'}}
    e_dependents = {'z': set(), 'y': {'z'}, 'a': {'z'}, 'x': {'y'}}

    e_waiting = {'y': set(), 'a': set(), 'z': {'a', 'y'}}
    e_waiting_data = {'x': {'y'}, 'y': {'z'}, 'a': {'z'}, 'z': set()}

    e_held_data = {'y', 'z'}

    new_dsk = {'z': (add, WrappedKey('y'), 10)}
    a = update_state(*map(deepcopy, [dsk, dependencies, dependents, held_data,
                                     who_has, in_play,
                                     waiting, waiting_data, new_dsk, {'z'}]))
    new_dsk = {'z': (add, 'y', 10)}
    b = update_state(*map(deepcopy, [dsk, dependencies, dependents, held_data,
                                     who_has, in_play,
                                     waiting, waiting_data, new_dsk, {'z'}]))
    assert a == b


def test_update_state_respects_data_in_memory():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies, dependents = get_deps(dsk)

    waiting = dict()
    waiting_data = {'y': set()}

    held_data = {'y'}
    who_has = {'y': {'alice'}}
    processing = set()
    released = {'x'}
    in_play = {'y'}

    new_dsk = {'x': 1, 'y': (inc, 'x'), 'z': (add, 'y', 'x')}
    new_keys = {'z'}

    e_dsk = new_dsk.copy()
    e_waiting = {'z': {'x'}, 'x': set()}
    e_waiting_data = {'x': {'z'}, 'y': {'z'}, 'z': set()}
    e_held_data = {'y', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 who_has, in_play,
                 waiting, waiting_data, new_dsk, new_keys)

    assert dsk == e_dsk
    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data
    assert in_play == {'x', 'y', 'z'}


def test_update_state_supports_recomputing_released_results():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'x')}
    dependencies, dependents = get_deps(dsk)

    waiting = dict()
    waiting_data = {'z': set()}

    held_data = {'z'}
    who_has = {'z': {'alice'}}
    processing = set()
    released = {'x', 'y'}
    in_play = {'z'}

    new_dsk = {'x': 1, 'y': (inc, 'x')}
    new_keys = {'y'}

    e_dsk = dsk.copy()
    e_waiting = {'x': set(), 'y': {'x'}}
    e_waiting_data = {'x': {'y'}, 'y': set(), 'z': set()}
    e_held_data = {'y', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 who_has, in_play,
                 waiting, waiting_data, new_dsk, new_keys)

    assert dsk == e_dsk
    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data
    assert in_play == {'x', 'y', 'z'}


def test_decide_worker_with_many_independent_leaves():
    dsk = merge({('y', i): (inc, ('x', i)) for i in range(100)},
                {('x', i): i for i in range(100)})
    dependencies, dependents = get_deps(dsk)
    stacks = {'alice': [], 'bob': []}
    who_has = merge({('x', i * 2): {'alice'} for i in range(50)},
                    {('x', i * 2 + 1): {'bob'} for i in range(50)})
    nbytes = {k: 0 for k in who_has}

    for key in dsk:
        worker = decide_worker(dependencies, stacks, who_has, {}, nbytes, key)
        stacks[worker].append(key)

    nhits = (len([k for k in stacks['alice'] if 'alice' in who_has[('x', k[1])]])
             + len([k for k in stacks['bob'] if 'bob' in who_has[('x', k[1])]]))

    assert nhits > 90


def test_decide_worker_with_restrictions():
    dependencies = {'x': set()}
    alice, bob, charlie = ('alice', 8000), ('bob', 8000), ('charlie', 8000)
    stacks = {alice: [], bob: [], charlie: []}
    who_has = {}
    restrictions = {'x': {'alice', 'charlie'}}
    nbytes = {}
    result = decide_worker(dependencies, stacks, who_has, restrictions, nbytes, 'x')
    assert result in {alice, charlie}

    stacks = {alice: [1, 2, 3], bob: [], charlie: [4, 5, 6]}
    result = decide_worker(dependencies, stacks, who_has, restrictions, nbytes, 'x')
    assert result in {alice, charlie}

    dependencies = {'x': {'y'}}
    who_has = {'y': {bob}}
    nbytes = {'y': 0}
    result = decide_worker(dependencies, stacks, who_has, restrictions, nbytes, 'x')
    assert result in {alice, charlie}


def test_decide_worker_without_stacks():
    with pytest.raises(ValueError):
        result = decide_worker({'x': []}, [], {}, {}, {}, 'x')


def test_validate_state():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies = {'x': set(), 'y': {'x'}}
    waiting = {'y': {'x'}, 'x': set()}
    dependents = {'x': {'y'}, 'y': set()}
    waiting_data = {'x': {'y'}}
    in_memory = set()
    stacks = {'alice': [], 'bob': []}
    processing = {'alice': set(), 'bob': set()}
    finished_results = set()
    released = set()
    in_play = {'x', 'y'}

    validate_state(**locals())

    in_memory.add('x')
    with pytest.raises(Exception):
        validate_state(**locals())

    del waiting['x']
    with pytest.raises(Exception):
        validate_state(**locals())

    waiting['y'].remove('x')
    validate_state(**locals())

    stacks['alice'].append('y')
    with pytest.raises(Exception):
        validate_state(**locals())

    waiting.pop('y')
    validate_state(**locals())

    stacks['alice'].pop()
    with pytest.raises(Exception):
        validate_state(**locals())

    processing['alice'].add('y')
    validate_state(**locals())

    processing['alice'].pop()
    with pytest.raises(Exception):
        validate_state(**locals())

    in_memory.add('y')
    with pytest.raises(Exception):
        validate_state(**locals())

    finished_results.add('y')
    validate_state(**locals())


def test_assign_many_tasks():
    alice, bob, charlie = ('alice', 8000), ('bob', 8000), ('charlie', 8000)
    dependencies = {'y': {'x'}, 'b': {'a'}, 'x': set(), 'a': set()}
    waiting = {'y': set(), 'b': {'a'}, 'a': set()}
    keyorder = {c: 1 for c in 'abxy'}
    who_has = {'x': {alice}}
    stacks = {alice: [], bob: []}
    nbytes = {}
    restrictions = {}
    keys = ['y', 'a']

    new_stacks = assign_many_tasks(dependencies, waiting, keyorder, who_has,
                                   stacks, restrictions, nbytes, ['y', 'a'])

    assert 'y' in stacks[alice]
    assert 'a' in stacks[alice] + stacks[bob]
    assert 'a' not in waiting
    assert 'y' not in waiting

    assert set(concat(new_stacks.values())) == set(concat(stacks.values()))


def test_assign_many_tasks_with_restrictions():
    alice, bob, charlie = ('alice', 8000), ('bob', 8000), ('charlie', 8000)
    dependencies = {'y': {'x'}, 'b': {'a'}, 'x': set(), 'a': set()}
    waiting = {'y': set(), 'b': {'a'}, 'a': set()}
    keyorder = {c: 1 for c in 'abxy'}
    who_has = {'x': {alice}}
    stacks = {alice: [], bob: []}
    nbytes = {'x': 0}
    restrictions = {'y': {'bob'}, 'a': {'alice'}}
    keys = ['y', 'a']

    new_stacks = assign_many_tasks(dependencies, waiting, keyorder, who_has,
                                   stacks, restrictions, nbytes, ['y', 'a'])

    assert 'y' in stacks[bob]
    assert 'a' in stacks[alice]
    assert 'a' not in waiting
    assert 'y' not in waiting

    assert set(concat(new_stacks.values())) == set(concat(stacks.values()))


def test_fill_missing_data():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    dependencies, dependents = get_deps(dsk)

    waiting = {}
    waiting_data = {'z': set()}

    held_data = {'z'}
    in_memory = {'z'}
    processing = set()
    released = set()
    in_play = {'z'}

    e_waiting = {'x': set(), 'y': {'x'}, 'z': {'y'}}
    e_waiting_data = {'x': {'y'}, 'y': {'z'}, 'z': set()}
    e_in_play = {'x', 'y', 'z'}

    lost = {'z'}
    in_memory.remove('z')
    in_play.remove('z')

    heal_missing_data(dsk, dependencies, dependents, held_data,
                      in_memory, in_play,
                      waiting, waiting_data, lost)

    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert in_play == e_in_play


from distributed.utils_test import cluster, slow, _test_cluster, loop
from distributed.utils import All
from tornado import gen

def div(x, y):
    return x / y

def test_scheduler(loop):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port))
        yield s._sync_center()
        done = s.start()
        sched, report = Queue(), Queue()
        s.handle_queues(sched, report)
        msg = yield report.get()
        assert msg['op'] == 'stream-start'

        # Test update graph
        s.put({'op': 'update-graph',
               'dsk': {'x': (inc, 1),
                       'y': (inc, 'x'),
                       'z': (inc, 'y')},
               'keys': ['z']})
        while True:
            msg = yield report.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
                break

        assert a.data.get('x') == 2 or b.data.get('x') == 2

        # Test erring tasks
        s.put({'op': 'update-graph',
               'dsk': {'a': (div, 1, 0),
                       'b': (inc, 'a')},
               'keys': ['a', 'b']})

        while True:
            msg = yield report.get()
            if msg['op'] == 'task-erred' and msg['key'] == 'b':
                break

        # Test missing data
        s.put({'op': 'missing-data',
               'missing': ['z']})
        while True:
            msg = yield report.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
                break

        # Test missing data without being informed
        for w in [a, b]:
            if 'z' in w.data:
                del w.data['z']
        s.put({'op': 'update-graph',
               'dsk': {'zz': (inc, 'z')},
               'keys': ['zz']})
        while True:
            msg = yield report.get()
            if msg['op'] == 'key-in-memory' and msg['key'] == 'zz':
                break

        s.put({'op': 'close'})
        yield done

    _test_cluster(f, loop)


def test_multi_queues(loop):
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port))
        yield s._sync_center()
        done = s.start()

        sched, report = Queue(), Queue()
        s.handle_queues(sched, report)

        msg = yield report.get()
        assert msg['op'] == 'stream-start'

        # Test update graph
        sched.put_nowait({'op': 'update-graph',
                          'dsk': {'x': (inc, 1),
                                  'y': (inc, 'x'),
                                  'z': (inc, 'y')},
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
                           'dsk': {'a': (inc, 10)},
                           'keys': ['a']})

        for q in [report, report2]:
            while True:
                msg = yield q.get()
                if msg['op'] == 'key-in-memory' and msg['key'] == 'a':
                    break

        sched.put_nowait({'op': 'close'})
        yield done

    _test_cluster(f, loop)


def test_monitor_resources(loop):
    c = Center('127.0.0.1', 8026)
    a = Nanny('127.0.0.1', 8027, 8028, '127.0.0.1', 8026, ncores=2)
    b = Nanny('127.0.0.1', 8029, 8030, '127.0.0.1', 8026, ncores=2)
    c.listen(c.port)
    s = Scheduler((c.ip, c.port), resource_interval=0.01, resource_log_size=3)

    @gen.coroutine
    def f():
        yield a._start()
        yield b._start()
        yield s._sync_center()
        done = s.start()

        try:
            assert s.ncores == {('127.0.0.1', a.worker_port): 2,
                                ('127.0.0.1', b.worker_port): 2}
            assert s.nannies == {(n.ip, n.worker_port): n.port
                                 for n in [a, b]}

            while any(len(v) < 3 for v in s.resource_logs.values()):
                yield gen.sleep(0.01)

            yield gen.sleep(0.1)

            assert set(s.resource_logs) == {(a.ip, a.port), (b.ip, b.port)}
            assert all(len(v) == 3 for v in s.resource_logs.values())

            s.put({'op': 'close'})
            yield done
        finally:
            yield a._close()
            yield b._close()
            c.stop()

    loop.run_sync(f, timeout=30)


def test_server(loop):
    port = 8040
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port))
        yield s._sync_center()
        done = s.start()
        s.listen(port)

        stream = yield connect('127.0.0.1', port)
        yield write(stream, {'op': 'start-control'})
        yield write(stream, {'op': 'update-graph',
                             'dsk': {'x': (inc, 1), 'y': (inc, 'x')},
                             'keys': ['y']})

        while True:
            msg = yield read(stream)
            if msg['op'] == 'key-in-memory' and msg['key'] == 'y':
                break

        yield write(stream, {'op': 'close-stream'})
        msg = yield read(stream)
        assert msg == {'op': 'stream-closed'}
        assert stream.closed()

        s.scheduler_queues[0].put_nowait({'op': 'close'})
        yield done
        s.stop()

    _test_cluster(f, loop)


def test_server_listens_to_other_ops(loop):
    port = 8040
    @gen.coroutine
    def f(c, a, b):
        s = Scheduler((c.ip, c.port))
        yield s._sync_center()
        done = s.start()
        s.listen(port)
        assert s.port == port

        r = rpc(ip='127.0.0.1', port=s.port)
        ident = yield r.identity()
        assert ident['type'] == 'Scheduler'

        s.stop()

    _test_cluster(f, loop)
