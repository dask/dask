from toolz import frequencies

from distributed.utils_test import gen_cluster, div
from distributed.client import _wait
from distributed.bokeh.task_stream import TaskStreamPlugin


@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 3)
def test_TaskStreamPlugin(c, s, *workers):
    es = TaskStreamPlugin(s)
    assert not es.buffer

    futures = c.map(div, [1] * 10, range(10))
    total = c.submit(sum, futures[1:])
    yield _wait(total)

    assert len(es.buffer) == 11

    workers = dict()

    rects = es.rectangles(0, 10, workers)
    assert all(n == 'div' for n in rects['name'])
    assert all(d > 0 for d in rects['duration'])
    counts = frequencies(rects['color'])
    assert counts['black'] == 1
    assert set(counts.values()) == {9, 1}
    assert len(set(rects['y'])) == 3


    rects = es.rectangles(2, 5, workers)
    assert all(len(L) == 3 for L in rects.values())

    starts = sorted(rects['start'])
    rects = es.rectangles(2, 5, workers=workers,
                          start_boundary=(starts[0] + starts[1]) / 2000)
    assert set(rects['start']).issubset(set(starts[1:]))
