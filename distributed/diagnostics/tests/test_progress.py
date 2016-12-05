from __future__ import print_function, division, absolute_import

from operator import add
import pytest
import sys

from toolz import valmap
from tornado import gen
from tornado.queues import Queue

from dask.core import get_deps
from distributed import Nanny
from distributed.client import _wait
from distributed.metrics import time
from distributed.utils_test import (gen_cluster, cluster, inc, dec, gen_test,
        div)
from distributed.utils import All, key_split
from distributed.worker import dumps_task
from distributed.diagnostics.progress import (Progress, SchedulerPlugin,
        AllProgress, MultiProgress, dependent_keys)
from distributed.protocol.pickle import dumps


def test_dependent_keys():
    a, b, c, d, e, f, g = 'abcdefg'
    who_has = {a: [1], b: [1]}
    processing = {'alice': {c}}
    exceptions = {}
    dsk = {a: 1, b: 2, c: (add, a, b), d: (inc, a), e: (add, c, d), f: (inc, e)}
    dependencies, dependents = get_deps(dsk)

    assert dependent_keys(f, who_has, processing, dependencies,
            exceptions, complete=False)[0] == {f, e, c, d}

    assert dependent_keys(f, who_has, processing, dependencies,
            exceptions, complete=True)[0] == {a, b, c, d, e, f}


def f(*args):
    pass

def g(*args):
    pass

def h(*args):
    pass


@gen_cluster(client=True)
def test_many_Progress(c, s, a, b):
    x = c.submit(f, 1)
    y = c.submit(g, x)
    z = c.submit(h, y)

    bars = [Progress(keys=[z], scheduler=s) for i in range(10)]
    yield [b.setup() for b in bars]

    yield z._result()

    start = time()
    while not all(b.status == 'finished' for b in bars):
        yield gen.sleep(0.1)
        assert time() < start + 2


@gen_cluster(client=True)
def test_multiprogress(c, s, a, b):
    x1 = c.submit(f, 1)
    x2 = c.submit(f, x1)
    x3 = c.submit(f, x2)
    y1 = c.submit(g, x3)
    y2 = c.submit(g, y1)

    p = MultiProgress([y2], scheduler=s, complete=True)
    yield p.setup()

    assert p.all_keys == {'f': {f.key for f in [x1, x2, x3]},
                          'g': {f.key for f in [y1, y2]}}

    yield x3._result()

    assert p.keys['f'] == set()

    yield y2._result()

    assert p.keys == {'f': set(),
                      'g': set()}

    assert p.status == 'finished'


@gen_cluster(client=True)
def test_robust_to_bad_plugin(c, s, a, b):
    class Bad(SchedulerPlugin):
        def transition(self, key, start, finish, **kwargs):
            raise Exception()

    bad = Bad()
    s.add_plugin(bad)

    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    result = yield y._result()
    assert result == 3


def check_bar_completed(capsys, width=40):
    out, err = capsys.readouterr()
    bar, percent, time = [i.strip() for i in out.split('\r')[-1].split('|')]
    assert bar == '[' + '#'*width + ']'
    assert percent == '100% Completed'


@gen_cluster(client=True, Worker=Nanny, timeout=None)
def test_AllProgress(c, s, a, b):
    x, y, z = c.map(inc, [1, 2, 3])
    xx, yy, zz = c.map(dec, [x, y, z])

    yield _wait([x, y, z])
    p = AllProgress(s)
    assert p.all['inc'] == {x.key, y.key, z.key}
    assert p.state['memory']['inc'] == {x.key, y.key, z.key}
    assert p.state['released'] == {}
    assert p.state['erred'] == {}
    assert 'inc' in p.nbytes
    assert isinstance(p.nbytes['inc'], int)
    assert p.nbytes['inc'] > 0

    yield _wait([xx, yy, zz])
    assert p.all['dec'] == {xx.key, yy.key, zz.key}
    assert p.state['memory']['dec'] == {xx.key, yy.key, zz.key}
    assert p.state['released'] == {}
    assert p.state['erred'] == {}
    assert p.nbytes['inc'] == p.nbytes['dec']

    t = c.submit(sum, [x, y, z])
    yield t._result()

    keys = {x.key, y.key, z.key}
    del x, y, z
    import gc; gc.collect()

    while any(k in s.who_has for k in keys):
        yield gen.sleep(0.01)

    assert p.state['released']['inc'] == keys
    assert p.all['inc'] == keys
    assert p.all['dec'] == {xx.key, yy.key, zz.key}
    if 'inc' in p.nbytes:
        assert p.nbytes['inc'] == 0

    xxx = c.submit(div, 1, 0)
    yield _wait([xxx])
    assert p.state['erred'] == {'div': {xxx.key}}

    tkey = t.key
    del xx, yy, zz, t
    import gc; gc.collect()

    while tkey in s.task_state:
        yield gen.sleep(0.01)

    for coll in [p.all, p.nbytes] + list(p.state.values()):
        assert 'inc' not in coll
        assert 'dec' not in coll

    def f(x):
        return x

    for i in range(4):
        future = c.submit(f, i)
    import gc; gc.collect()

    yield gen.sleep(1)

    yield _wait([future])
    assert p.state['memory'] == {'f': {future.key}}

    yield c._restart()

    for coll in [p.all] + list(p.state.values()):
        assert not coll

    x = c.submit(div, 1, 2)
    yield _wait([x])
    assert set(p.all) == {'div'}
    assert all(set(d) == {'div'} for d in p.state.values())


@gen_cluster(client=True, Worker=Nanny)
def test_AllProgress_lost_key(c, s, a, b, timeout=None):
    p = AllProgress(s)
    futures = c.map(inc, range(5))
    yield _wait(futures)
    assert len(p.state['memory']['inc']) == 5

    yield a._close()
    yield b._close()

    start = time()
    while len(p.state['memory']['inc']) > 0:
        yield gen.sleep(0.1)
        assert time() < start + 2
