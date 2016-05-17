from __future__ import print_function, division, absolute_import

from operator import add
import pytest
import sys
from time import time
from toolz import valmap
from tornado import gen
from tornado.queues import Queue

from dask.core import get_deps
from distributed.executor import _wait
from distributed.worker import dumps_task
from distributed import Nanny
from distributed.utils_test import (gen_cluster, cluster, inc, dec, gen_test,
        div)
from distributed.utils import All, key_split
from distributed.diagnostics.progress import (Progress, SchedulerPlugin,
        AllProgress, MultiProgress, dependent_keys)
from distributed.core import dumps

def test_dependent_keys():
    a, b, c, d, e, f, g = 'abcdefg'
    who_has = {a: [1], b: [1]}
    processing = {'alice': {c}}
    stacks = {'bob': [d]}
    exceptions = {}
    dsk = {a: 1, b: 2, c: (add, a, b), d: (inc, a), e: (add, c, d), f: (inc, e)}
    dependencies, dependeents = get_deps(dsk)

    assert dependent_keys(f, who_has, processing, stacks, dependencies,
            exceptions, complete=False)[0] == {f, e, c, d}

    assert dependent_keys(f, who_has, processing, stacks, dependencies,
            exceptions, complete=True)[0] == {a, b, c, d, e, f}


@gen_cluster()
def test_many_Progresss(s, a, b):
    sched, report = Queue(), Queue(); s.handle_queues(sched, report)
    s.update_graph(tasks=valmap(dumps_task, {'x': (inc, 1),
                                             'y': (inc, 'x'),
                                             'z': (inc, 'y')}),
                   keys=['z'],
                   dependencies={'y': ['x'], 'z': ['y']})

    bars = [Progress(keys=['z'], scheduler=s) for i in range(10)]
    yield [b.setup() for b in bars]

    while True:
        msg = yield report.get()
        if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
            break

    assert all(b.status == 'finished' for b in bars)


@gen_cluster()
def test_multiprogress(s, a, b):
    sched, report = Queue(), Queue(); s.handle_queues(sched, report)
    s.update_graph(tasks=valmap(dumps_task, {'x-1': (inc, 1),
                                             'x-2': (inc, 'x-1'),
                                             'x-3': (inc, 'x-2'),
                                             'y-1': (dec, 'x-3'),
                                             'y-2': (dec, 'y-1')}),
                   keys=['y-2'],
                   dependencies={'x-2': ['x-1'], 'x-3': ['x-2'],
                                 'y-1': ['x-3'], 'y-2': ['y-1']})

    p = MultiProgress(['y-2'], scheduler=s, func=key_split)
    yield p.setup()

    assert p.keys == {'x': {'x-1', 'x-2', 'x-3'},
                      'y': {'y-1', 'y-2'}}

    while True:
        msg = yield report.get()
        if msg['op'] == 'key-in-memory' and msg['key'] == 'x-3':
            break

    assert p.keys == {'x': set(),
                      'y': {'y-1', 'y-2'}}

    while True:
        msg = yield report.get()
        if msg['op'] == 'key-in-memory' and msg['key'] == 'y-2':
            break

    assert p.keys == {'x': set(),
                      'y': set()}

    assert p.status == 'finished'


@gen_cluster()
def test_robust_to_bad_plugin(s, a, b):
    sched, report = Queue(), Queue(); s.handle_queues(sched, report)

    class Bad(SchedulerPlugin):
        def task_finished(self, scheduler, key, worker, nbytes, **kwargs):
            raise Exception()

    bad = Bad()
    s.add_plugin(bad)

    sched.put_nowait({'op': 'update-graph',
                      'tasks': valmap(dumps_task, {'x': (inc, 1),
                                                   'y': (inc, 'x'),
                                                   'z': (inc, 'y')}),
                      'dependencies': {'y': ['x'], 'z': ['y']},
                      'keys': ['z']})

    while True:  # normal execution
        msg = yield report.get()
        if msg['op'] == 'key-in-memory' and msg['key'] == 'z':
            break


def check_bar_completed(capsys, width=40):
    out, err = capsys.readouterr()
    bar, percent, time = [i.strip() for i in out.split('\r')[-1].split('|')]
    assert bar == '[' + '#'*width + ']'
    assert percent == '100% Completed'


@gen_cluster(executor=True, Worker=Nanny)
def test_AllProgress(e, s, a, b):
    x, y, z = e.map(inc, [1, 2, 3])
    xx, yy, zz = e.map(dec, [x, y, z])

    yield _wait([x, y, z])
    p = AllProgress(s)
    assert p.all['inc'] == {x.key, y.key, z.key}
    assert p.in_memory['inc'] == {x.key, y.key, z.key}
    assert p.released == {}
    assert p.erred == {}

    yield _wait([xx, yy, zz])
    assert p.all['dec'] == {xx.key, yy.key, zz.key}
    assert p.in_memory['dec'] == {xx.key, yy.key, zz.key}
    assert p.released == {}
    assert p.erred == {}

    s.client_releases_keys(client=e.id, keys=[x.key, y.key, z.key])
    assert p.released['inc'] == {x.key, y.key, z.key}
    assert p.all['inc'] == {x.key, y.key, z.key}
    assert p.all['dec'] == {xx.key, yy.key, zz.key}

    xxx = e.submit(div, 1, 0)
    yield _wait([xxx])
    assert p.erred == {'div': {xxx.key}}

    s.client_releases_keys(client=e.id, keys=[xx.key, yy.key, zz.key])
    for c in [p.all, p.in_memory, p.released, p.erred]:
        assert 'inc' not in c
        assert 'dec' not in c

    def f(x):
        return x

    for i in range(4):
        future = e.submit(f, i)
    import gc; gc.collect()

    yield gen.sleep(1)

    yield _wait([future])
    assert p.in_memory == {'f': {future.key}}

    yield e._restart()

    for c in [p.all, p.in_memory, p.released, p.erred]:
        assert not c

    x = e.submit(div, 1, 2)
    yield _wait([x])
    p.validate()


@gen_cluster(executor=True, Worker=Nanny)
def test_AllProgress_lost_key(e, s, a, b, timeout=None):
    p = AllProgress(s)
    futures = e.map(inc, range(5))
    yield _wait(futures)
    assert len(p.in_memory['inc']) == 5

    yield a._close()
    yield b._close()

    start = time()
    while len(p.in_memory['inc']) > 0:
        yield gen.sleep(0.1)
        assert time() < start + 2
