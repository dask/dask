from copy import deepcopy
from contextlib import contextmanager
from operator import add, mul
import socket
from time import time, sleep
from threading import Thread

import dask
from dask.core import get_deps
from toolz import merge, concat
import pytest

from distributed import Center, Worker
from distributed.utils import ignoring
from distributed.client import _gather, RemoteData, WrappedKey
from distributed.core import connect_sync, read_sync, write_sync
from distributed.dask import (_get, validate_state, heal, update_state,
        insert_remote_deps, decide_worker, assign_many_tasks)
from distributed.utils_test import cluster

from tornado import gen
from tornado.ioloop import IOLoop


def inc(x):
    return x + 1


def _test_cluster(f):
    @gen.coroutine
    def g():
        c = Center('127.0.0.1', 8017)
        c.listen(c.port)
        a = Worker('127.0.0.1', 8018, c.ip, c.port, ncores=2)
        yield a._start()
        b = Worker('127.0.0.1', 8019, c.ip, c.port, ncores=1)
        yield b._start()

        while len(c.ncores) < 2:
            yield gen.sleep(0.01)

        try:
            yield f(c, a, b)
        finally:
            with ignoring():
                yield a._close()
            with ignoring():
                yield b._close()
            c.stop()

    IOLoop.current().run_sync(g)


def test_scheduler():
    dsk = {'x': 1, 'y': (add, 'x', 10), 'z': (add, (inc, 'y'), 20),
           'a': 1, 'b': (mul, 'a', 10), 'c': (mul, 'b', 20),
           'total': (add, 'c', 'z')}
    keys = ['total', 'c', ['z']]

    @gen.coroutine
    def f(c, a, b):
        result = yield _get(c.ip, c.port, dsk, keys, gather=True)

        expected = dask.async.get_sync(dsk, keys)
        assert tuple(result) == expected
        assert set(a.data) | set(b.data) == {'total', 'c', 'z'}

    _test_cluster(f)


def test_scheduler_errors():
    def mydiv(x, y):
        return x / y
    dsk = {'x': 1, 'y': (mydiv, 'x', 0)}
    keys = 'y'

    @gen.coroutine
    def f(c, a, b):
        try:
            result = yield _get(c.ip, c.port, dsk, keys)
            assert False
        except ZeroDivisionError as e:
            # assert 'mydiv' in str(e)
            pass

    _test_cluster(f)


def test_avoid_computations_for_data_in_memory():
    def bad():
        raise Exception()
    dsk = {'x': (bad,), 'y': (inc, 'x'), 'z': (inc, 'y')}
    keys = 'z'

    @gen.coroutine
    def f(c, a, b):
        a.data['y'] = 10                # manually add 'y' to a
        c.who_has['y'].add(a.address)
        c.has_what[a.address].add('y')

        result = yield _get(c.ip, c.port, dsk, keys)
        assert result.key in a.data or result.key in b.data

    _test_cluster(f)


def test_gather():
    dsk = {'x': 1, 'y': (inc, 'x')}
    keys = 'y'

    @gen.coroutine
    def f(c, a, b):
        result = yield _get(c.ip, c.port, dsk, keys, gather=True)
        assert result == 2

    _test_cluster(f)


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

    output = heal(dependencies, dependents, in_memory, stacks, processing)

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
             'processing': {'alice': set(), 'bob': set()}}

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
             'processing': {'alice': set(), 'bob': set(['c'])}}

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

    state = {'in_memory': {},  # missing 'b'
             'stacks': {'alice': ['a'], 'bob': ['x']},
             'processing': {'alice': set(), 'bob': set()}}

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
             'processing': {'alice': set(), 'bob': set('y')}}

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
    in_memory = {'x'}
    processing = set()
    released = set()

    new_dsk = {'a': 1, 'z': (add, 'y', 'a')}
    new_keys = {'z'}

    e_dsk = {'x': 1, 'y': (inc, 'x'), 'a': 1, 'z': (add, 'y', 'a')}
    e_dependencies = {'x': set(), 'a': set(), 'y': {'x'}, 'z': {'a', 'y'}}
    e_dependents = {'z': set(), 'y': {'z'}, 'a': {'z'}, 'x': {'y'}}

    e_waiting = {'y': set(), 'a': set(), 'z': {'a', 'y'}}
    e_waiting_data = {'x': {'y'}, 'y': {'z'}, 'a': {'z'}, 'z': set()}

    e_held_data = {'y', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 in_memory, processing, released,
                 waiting, waiting_data, new_dsk, new_keys)

    assert dsk == e_dsk
    assert dependencies == e_dependencies
    assert dependents == e_dependents
    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data


def test_update_state_with_processing():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    dependencies, dependents = get_deps(dsk)

    waiting = {'z': {'y'}}
    waiting_data = {'x': {'y'}, 'y': {'z'}, 'z': set()}

    held_data = {'z'}
    in_memory = {'x'}
    processing = {'y'}
    released = set()

    new_dsk = {'a': (inc, 'x'), 'b': (add, 'a', 'y'), 'c': (inc, 'z')}
    new_keys = {'b', 'c'}

    e_waiting = {'z': {'y'}, 'a': set(), 'b': {'a', 'y'}, 'c': {'z'}}
    e_waiting_data = {'x': {'y', 'a'}, 'y': {'z', 'b'}, 'z': {'c'},
                      'a': {'b'}, 'b': set(), 'c': set()}

    e_held_data = {'b', 'c', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 in_memory, processing, released,
                 waiting, waiting_data, new_dsk, new_keys)

    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data


def test_update_state_respects_WrappedKeys():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies, dependents = get_deps(dsk)

    waiting = {'y': set()}
    waiting_data = {'x': {'y'}, 'y': set()}

    held_data = {'y'}
    in_memory = {'x'}
    processing = set()
    released = set()

    e_dsk = {'x': 1, 'y': (inc, 'x'), 'a': 1, 'z': (add, 'y', 'a')}
    e_dependencies = {'x': set(), 'a': set(), 'y': {'x'}, 'z': {'a', 'y'}}
    e_dependents = {'z': set(), 'y': {'z'}, 'a': {'z'}, 'x': {'y'}}

    e_waiting = {'y': set(), 'a': set(), 'z': {'a', 'y'}}
    e_waiting_data = {'x': {'y'}, 'y': {'z'}, 'a': {'z'}, 'z': set()}

    e_held_data = {'y', 'z'}

    new_dsk = {'z': (add, WrappedKey('y'), 10)}
    a = update_state(*map(deepcopy, [dsk, dependencies, dependents, held_data,
                                     in_memory, processing, released,
                                     waiting, waiting_data, new_dsk, {'z'}]))
    new_dsk = {'z': (add, 'y', 10)}
    b = update_state(*map(deepcopy, [dsk, dependencies, dependents, held_data,
                                     in_memory, processing, released,
                                     waiting, waiting_data, new_dsk, {'z'}]))
    assert a == b


def test_update_state_respects_data_in_memory():
    dsk = {'x': 1, 'y': (inc, 'x')}
    dependencies, dependents = get_deps(dsk)

    waiting = dict()
    waiting_data = {'y': set()}

    held_data = {'y'}
    in_memory = {'y'}
    processing = set()
    released = {'x'}

    new_dsk = {'x': 1, 'y': (inc, 'x'), 'z': (add, 'y', 'x')}
    new_keys = {'z'}

    e_dsk = new_dsk.copy()
    e_waiting = {'z': {'x'}, 'x': set()}
    e_waiting_data = {'x': {'z'}, 'y': {'z'}, 'z': set()}
    e_held_data = {'y', 'z'}

    update_state(dsk, dependencies, dependents, held_data,
                 in_memory, processing, released,
                 waiting, waiting_data, new_dsk, new_keys)

    assert dsk == e_dsk
    assert waiting == e_waiting
    assert waiting_data == e_waiting_data
    assert held_data == e_held_data


def test_decide_worker_with_many_independent_leaves():
    dsk = merge({('y', i): (inc, ('x', i)) for i in range(100)},
                {('x', i): i for i in range(100)})
    dependencies, dependents = get_deps(dsk)
    stacks = {'alice': [], 'bob': []}
    who_has = merge({('x', i * 2): {'alice'} for i in range(50)},
                    {('x', i * 2 + 1): {'bob'} for i in range(50)})

    for key in dsk:
        worker = decide_worker(dependencies, stacks, who_has, key)
        stacks[worker].append(key)

    nhits = (len([k for k in stacks['alice'] if 'alice' in who_has[('x', k[1])]])
             + len([k for k in stacks['bob'] if 'bob' in who_has[('x', k[1])]]))

    assert nhits > 90


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


def slowinc(x):
    from time import sleep
    sleep(0.02)
    return x + 1


def test_failing_worker():
    n = 10
    dsk = {('x', i, j): (slowinc, ('x', i, j - 1)) for i in range(4)
                                                   for j in range(1, n)}
    dsk.update({('x', i, 0): i * 10 for i in range(4)})
    dsk['z'] = (sum, [('x', i, n - 1) for i in range(4)])
    keys = 'z'

    with cluster() as (c, [a, b]):
        def kill_a():
            sleep(0.1)
            a['proc'].terminate()

        @gen.coroutine
        def f():
            result = yield _get('127.0.0.1', c['port'], dsk, keys, gather=True)
            assert result == 96

        thread = Thread(target=kill_a)
        thread.start()
        IOLoop.current().run_sync(f)


def test_repeated_computation():
    def func():
        from random import randint
        return randint(0, 100)

    dsk = {'x': (func,)}

    @gen.coroutine
    def f(c, a, b):
        x = yield _get(c.ip, c.port, dsk, 'x', gather=True)
        y = yield _get(c.ip, c.port, dsk, 'x', gather=True)
        assert x == y

    _test_cluster(f)


def test_insert_remote_deps():
    x = RemoteData('x', None, None)
    dsk = {'y': (inc, x)}
    dependencies, dependents = get_deps(dsk)

    dsk, dependencies, depdendents, held_data = insert_remote_deps(dsk, dependencies, dependents)

    assert dsk == {'y': (inc, x.key)}
    assert x.key in dependencies['y']
    assert held_data == {'x'}


def test_RemoteData_interaction():
    @gen.coroutine
    def f(c, a, b):
        a.data['x'] = 10
        c.who_has['x'].add(a.address)
        c.has_what[a.address].add('x')
        x = RemoteData('x', c.ip, c.port)

        dsk = {'y': (inc, x)}

        result = yield _get(c.ip, c.port, dsk, 'y', gather=True)
        assert result == 11
        assert 'x' in a.data  # don't delete input data

    _test_cluster(f)


def test_assign_many_tasks():
    dependencies = {'y': {'x'}, 'b': {'a'}, 'x': set(), 'a': set()}
    waiting = {'y': set(), 'b': {'a'}, 'a': set()}
    keyorder = {c: 1 for c in 'abxy'}
    who_has = {'x': {'alice'}}
    stacks = {'alice': [], 'bob': []}
    keys = ['y', 'a']

    new_stacks = assign_many_tasks(dependencies, waiting, keyorder, who_has,
                                   stacks, ['y', 'a'])

    assert 'y' in stacks['alice']
    assert 'a' in stacks['alice'] + stacks['bob']
    assert 'a' not in waiting
    assert 'y' not in waiting

    assert set(concat(new_stacks.values())) == set(concat(stacks.values()))


def test_get_with_overlapping_keys():
    dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
    keys = 'y'

    @gen.coroutine
    def f(c, a, b):
        dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y')}
        keys = 'z'
        result = yield _get(c.ip, c.port, dsk, keys, gather=True)
        assert result == dask.get(dsk, keys)

        dsk = {'x': 1, 'y': (inc, 'x'), 'z': (inc, 'y'),
               'a': (inc, 'z'), 'b': (add, 'a', 'x')}
        keys = 'b'
        result = yield _get(c.ip, c.port, dsk, keys, gather=True)
        assert result == dask.get(dsk, keys)

    _test_cluster(f)
