import pytest
import sys
import time
from toolz import first
import threading

from distributed.compatibility import get_thread_identity, WINDOWS
from distributed import metrics
from distributed.profile import (
    process,
    merge,
    create,
    call_stack,
    identifier,
    watch,
    llprocess,
    ll_get_stack,
    plot_data,
)


def test_basic():
    def test_g():
        time.sleep(0.01)

    def test_h():
        time.sleep(0.02)

    def test_f():
        for i in range(100):
            test_g()
            test_h()

    thread = threading.Thread(target=test_f)
    thread.daemon = True
    thread.start()

    state = create()

    for i in range(100):
        time.sleep(0.02)
        frame = sys._current_frames()[thread.ident]
        process(frame, None, state)

    assert state["count"] == 100
    d = state
    while len(d["children"]) == 1:
        d = first(d["children"].values())

    assert d["count"] == 100
    assert "test_f" in str(d["description"])
    g = [c for c in d["children"].values() if "test_g" in str(c["description"])][0]
    h = [c for c in d["children"].values() if "test_h" in str(c["description"])][0]

    assert g["count"] < h["count"]
    assert 95 < g["count"] + h["count"] <= 100

    pd = plot_data(state)
    assert len(set(map(len, pd.values()))) == 1  # all same length
    assert len(set(pd["color"])) > 1  # different colors


@pytest.mark.skipif(
    WINDOWS, reason="no low-level profiler support for Windows available"
)
def test_basic_low_level():
    pytest.importorskip("stacktrace")

    state = create()

    for i in range(100):
        time.sleep(0.02)
        frame = sys._current_frames()[threading.get_ident()]
        llframes = {threading.get_ident(): ll_get_stack(threading.get_ident())}
        for f in llframes.values():
            if f is not None:
                llprocess(f, None, state)

    assert state["count"] == 100
    children = state.get("children")
    assert children
    expected = "<low-level>"
    for k, v in zip(children.keys(), children.values()):
        desc = v.get("description")
        assert desc
        filename = desc.get("filename")
        assert expected in k and filename == expected


def test_merge():
    a1 = {
        "count": 5,
        "identifier": "root",
        "description": "a",
        "children": {
            "b": {
                "count": 3,
                "description": "b-func",
                "identifier": "b",
                "children": {},
            },
            "c": {
                "count": 2,
                "description": "c-func",
                "identifier": "c",
                "children": {},
            },
        },
    }

    a2 = {
        "count": 4,
        "description": "a",
        "identifier": "root",
        "children": {
            "d": {
                "count": 2,
                "description": "d-func",
                "children": {},
                "identifier": "d",
            },
            "c": {
                "count": 2,
                "description": "c-func",
                "children": {},
                "identifier": "c",
            },
        },
    }

    expected = {
        "count": 9,
        "identifier": "root",
        "description": "a",
        "children": {
            "b": {
                "count": 3,
                "description": "b-func",
                "identifier": "b",
                "children": {},
            },
            "d": {
                "count": 2,
                "description": "d-func",
                "identifier": "d",
                "children": {},
            },
            "c": {
                "count": 4,
                "description": "c-func",
                "identifier": "c",
                "children": {},
            },
        },
    }

    assert merge(a1, a2) == expected


def test_merge_empty():
    assert merge() == create()
    assert merge(create()) == create()
    assert merge(create(), create()) == create()


def test_call_stack():
    frame = sys._current_frames()[get_thread_identity()]
    L = call_stack(frame)
    assert isinstance(L, list)
    assert all(isinstance(s, str) for s in L)
    assert "test_call_stack" in str(L[-1])


def test_identifier():
    frame = sys._current_frames()[get_thread_identity()]
    assert identifier(frame) == identifier(frame)
    assert identifier(None) == identifier(None)


def test_watch():
    start = metrics.time()

    def stop():
        return metrics.time() > start + 0.500

    start_threads = threading.active_count()

    log = watch(interval="10ms", cycle="50ms", stop=stop)

    start = metrics.time()  # wait until thread starts up
    while threading.active_count() <= start_threads:
        assert metrics.time() < start + 2
        time.sleep(0.01)

    time.sleep(0.5)
    assert 1 < len(log) < 10

    start = metrics.time()
    while threading.active_count() > start_threads:
        assert metrics.time() < start + 2
        time.sleep(0.01)
