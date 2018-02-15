from distributed.utils_test import gen_cluster, inc
from distributed.diagnostics import GraphLayout
from distributed import wait
from tornado import gen


@gen_cluster(client=True)
def test_basic(c, s, a, b):
    gl = GraphLayout(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)

    yield total

    assert len(gl.x) == len(gl.y) == 6
    assert all(gl.x[f.key] == 0 for f in futures)
    assert gl.x[total.key] == 1
    assert min(gl.y.values()) < gl.y[total.key] < max(gl.y.values())


@gen_cluster(client=True)
def test_construct_after_call(c, s, a, b):
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)

    yield total

    gl = GraphLayout(s)

    assert len(gl.x) == len(gl.y) == 6
    assert all(gl.x[f.key] == 0 for f in futures)
    assert gl.x[total.key] == 1
    assert min(gl.y.values()) < gl.y[total.key] < max(gl.y.values())


@gen_cluster(client=True)
def test_states(c, s, a, b):
    gl = GraphLayout(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)
    del futures

    yield total

    updates = {state for idx, state in gl.state_updates}
    assert 'memory' in updates
    assert 'processing' in updates
    assert 'released' in updates


@gen_cluster(client=True)
def test_release_tasks(c, s, a, b):
    gl = GraphLayout(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)

    yield total
    key = total.key
    del total
    while key in s.tasks:
        yield gen.sleep(0.01)

    assert len(gl.visible_updates) == 1
    assert len(gl.visible_edge_updates) == 5


@gen_cluster(client=True)
def test_forget(c, s, a, b):
    gl = GraphLayout(s)

    futures = c.map(inc, range(10))
    futures = c.map(inc, futures)
    yield wait(futures)
    del futures
    while s.tasks:
        yield gen.sleep(0.01)

    assert not gl.x
    assert not gl.y
    assert not gl.index
    assert not gl.index_edge
