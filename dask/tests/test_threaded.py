from multiprocessing.pool import ThreadPool
import threading
from threading import Thread
from time import time, sleep

import pytest

from dask.context import set_options
from dask.threaded import get
from dask.utils_test import inc, add


def test_get():
    dsk = {'x': 1, 'y': 2, 'z': (inc, 'x'), 'w': (add, 'z', 'y')}
    assert get(dsk, 'w') == 4
    assert get(dsk, ['w', 'z']) == (4, 2)


def test_nested_get():
    dsk = {'x': 1, 'y': 2, 'a': (add, 'x', 'y'), 'b': (sum, ['x', 'y'])}
    assert get(dsk, ['a', 'b']) == (3, 3)


def test_get_without_computation():
    dsk = {'x': 1}
    assert get(dsk, 'x') == 1


def bad(x):
    raise ValueError()


def test_exceptions_rise_to_top():
    dsk = {'x': 1, 'y': (bad, 'x')}
    pytest.raises(ValueError, lambda: get(dsk, 'y'))


def test_reuse_pool():
    pool = ThreadPool()
    with set_options(pool=pool):
        assert get({'x': (inc, 1)}, 'x') == 2
        assert get({'x': (inc, 1)}, 'x') == 2


def test_threaded_within_thread():
    L = []

    def f(i):
        result = get({'x': (lambda: i,)}, 'x', num_workers=2)
        L.append(result)

    before = threading.active_count()

    for i in range(20):
        t = Thread(target=f, args=(1,))
        t.daemon = True
        t.start()
        t.join()
        assert L == [1]
        del L[:]

    start = time()  # wait for most threads to join
    while threading.active_count() > before + 10:
        sleep(0.01)
        assert time() < start + 5


def test_dont_spawn_too_many_threads():
    before = threading.active_count()

    dsk = {('x', i): (lambda: i,) for i in range(10)}
    dsk['x'] = (sum, list(dsk))
    for i in range(20):
        get(dsk, 'x', num_workers=4)

    after = threading.active_count()

    assert after <= before + 8


def test_thread_safety():
    def f(x):
        return 1

    dsk = {'x': (sleep, 0.05), 'y': (f, 'x')}

    L = []

    def test_f():
        L.append(get(dsk, 'y'))

    threads = []
    for i in range(20):
        t = Thread(target=test_f)
        t.daemon = True
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()

    assert L == [1] * 20
