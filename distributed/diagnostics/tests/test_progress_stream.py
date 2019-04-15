from __future__ import print_function, division, absolute_import


import pytest

pytest.importorskip("bokeh")

from dask import delayed
from distributed.client import wait
from distributed.diagnostics.progress_stream import (
    progress_quads,
    nbytes_bar,
    progress_stream,
)
from distributed.utils_test import div, gen_cluster, inc


def test_progress_quads():
    msg = {
        "all": {"inc": 5, "dec": 1, "add": 4},
        "memory": {"inc": 2, "dec": 0, "add": 1},
        "erred": {"inc": 0, "dec": 1, "add": 0},
        "released": {"inc": 1, "dec": 0, "add": 1},
        "processing": {"inc": 1, "dec": 0, "add": 2},
    }

    d = progress_quads(msg, nrows=2)
    color = d.pop("color")
    assert len(set(color)) == 3
    expected = {
        "name": ["inc", "add", "dec"],
        "show-name": ["inc", "add", "dec"],
        "left": [0, 0, 1],
        "right": [0.9, 0.9, 1.9],
        "top": [0, -1, 0],
        "bottom": [-0.8, -1.8, -0.8],
        "all": [5, 4, 1],
        "released": [1, 1, 0],
        "memory": [2, 1, 0],
        "erred": [0, 0, 1],
        "processing": [1, 2, 0],
        "done": ["3 / 5", "2 / 4", "1 / 1"],
        "released-loc": [0.9 * 1 / 5, 0.25 * 0.9, 1.0],
        "memory-loc": [0.9 * 3 / 5, 0.5 * 0.9, 1.0],
        "erred-loc": [0.9 * 3 / 5, 0.5 * 0.9, 1.9],
        "processing-loc": [0.9 * 4 / 5, 1 * 0.9, 1 * 0.9 + 1],
    }
    assert d == expected


def test_progress_quads_too_many():
    keys = ["x-%d" % i for i in range(1000)]
    msg = {
        "all": {k: 1 for k in keys},
        "memory": {k: 0 for k in keys},
        "erred": {k: 0 for k in keys},
        "released": {k: 0 for k in keys},
        "processing": {k: 0 for k in keys},
    }

    d = progress_quads(msg, nrows=6, ncols=3)
    assert len(d["name"]) == 6 * 3


@gen_cluster(client=True)
def test_progress_stream(c, s, a, b):
    futures = c.map(div, [1] * 10, range(10))

    x = 1
    for i in range(5):
        x = delayed(inc)(x)
    future = c.compute(x)

    yield wait(futures + [future])

    comm = yield progress_stream(s.address, interval=0.010)
    msg = yield comm.read()
    nbytes = msg.pop("nbytes")
    assert msg == {
        "all": {"div": 10, "inc": 5},
        "erred": {"div": 1},
        "memory": {"div": 9, "inc": 1},
        "released": {"inc": 4},
        "processing": {},
    }
    assert set(nbytes) == set(msg["all"])
    assert all(v > 0 for v in nbytes.values())

    assert progress_quads(msg)

    yield comm.close()


def test_nbytes_bar():
    nbytes = {"inc": 1000, "dec": 3000}
    expected = {
        "name": ["dec", "inc"],
        "left": [0, 0.75],
        "center": [0.375, 0.875],
        "right": [0.75, 1.0],
        "percent": [75, 25],
        "MB": [0.003, 0.001],
        "text": ["dec", "inc"],
    }

    result = nbytes_bar(nbytes)
    color = result.pop("color")
    assert len(set(color)) == 2
    assert result == expected


def test_progress_quads_many_functions():
    funcnames = ["fn%d" % i for i in range(1000)]
    msg = {
        "all": {fn: 1 for fn in funcnames},
        "memory": {fn: 1 for fn in funcnames},
        "erred": {fn: 0 for fn in funcnames},
        "released": {fn: 0 for fn in funcnames},
        "processing": {fn: 0 for fn in funcnames},
    }

    d = progress_quads(msg, nrows=2)
    color = d.pop("color")
    assert len(set(color)) < 100
