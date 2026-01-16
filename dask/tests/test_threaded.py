from __future__ import annotations

import contextvars
import signal
import sys
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import ThreadPool
from time import sleep, time

import pytest

import dask
from dask.system import CPU_COUNT
from dask.threaded import get
from dask.utils_test import add, inc


def test_get():
    dsk = {"x": 1, "y": 2, "z": (inc, "x"), "w": (add, "z", "y")}
    assert get(dsk, "w") == 4
    assert get(dsk, ["w", "z"]) == (4, 2)


def test_nested_get():
    dsk = {"x": 1, "y": 2, "a": (add, "x", "y"), "b": (sum, ["x", "y"])}
    assert get(dsk, ["a", "b"]) == (3, 3)


def test_get_without_computation():
    dsk = {"x": 1}
    assert get(dsk, "x") == 1


def test_broken_callback():
    from dask.callbacks import Callback

    def _f_ok(*args, **kwargs):
        pass

    def _f_broken(*args, **kwargs):
        raise ValueError("my_exception")

    dsk = {"x": 1}

    with Callback(start=_f_broken, finish=_f_ok):
        with Callback(start=_f_ok, finish=_f_ok):
            with pytest.raises(ValueError, match="my_exception"):
                get(dsk, "x")


def bad(x):
    raise ValueError()


def test_exceptions_rise_to_top():
    dsk = {"x": 1, "y": (bad, "x")}
    pytest.raises(ValueError, lambda: get(dsk, "y"))


@pytest.mark.parametrize("pool_typ", [ThreadPool, ThreadPoolExecutor])
def test_reuse_pool(pool_typ):
    with pool_typ(CPU_COUNT) as pool:
        with dask.config.set(pool=pool):
            assert get({"x": (inc, 1)}, "x") == 2
            assert get({"x": (inc, 1)}, "x") == 2


@pytest.mark.parametrize("pool_typ", [ThreadPool, ThreadPoolExecutor])
def test_pool_kwarg(pool_typ):
    def f():
        sleep(0.01)
        return threading.get_ident()

    dsk = {("x", i): (f,) for i in range(30)}
    dsk["x"] = (len, (set, [("x", i) for i in range(len(dsk))]))

    with pool_typ(3) as pool:
        assert get(dsk, "x", pool=pool) == 3


def test_threaded_within_thread():
    L = []

    def f(i):
        result = get({"x": (lambda: i,)}, "x", num_workers=2)
        L.append(result)

    before = threading.active_count()

    for _ in range(20):
        t = threading.Thread(target=f, args=(1,))
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

    dsk = {("x", i): (lambda i=i: i,) for i in range(10)}
    dsk["x"] = (sum, list(dsk))
    for _ in range(20):
        get(dsk, "x", num_workers=4)

    after = threading.active_count()

    assert after <= before + 8


def test_dont_spawn_too_many_threads_CPU_COUNT():
    before = threading.active_count()

    dsk = {("x", i): (lambda i=i: i,) for i in range(10)}
    dsk["x"] = (sum, list(dsk))
    for _ in range(20):
        get(dsk, "x")

    after = threading.active_count()

    assert after <= before + CPU_COUNT * 2


def test_thread_safety():
    def f(x):
        return 1

    dsk = {"x": (sleep, 0.05), "y": (f, "x")}

    L = []

    def test_f():
        L.append(get(dsk, "y"))

    threads = []
    for _ in range(20):
        t = threading.Thread(target=test_f)
        t.daemon = True
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()

    assert L == [1] * 20


def test_interrupt():
    # Windows implements `queue.get` using polling,
    # which means we can set an exception to interrupt the call to `get`.
    # Python 3 on other platforms requires sending SIGINT to the main thread.
    if sys.platform == "win32":
        from _thread import interrupt_main
    else:
        main_thread = threading.get_ident()

        def interrupt_main() -> None:
            signal.pthread_kill(main_thread, signal.SIGINT)

    in_clog_event = threading.Event()
    clog_event = threading.Event()

    def clog(in_clog_event: threading.Event, clog_event: threading.Event) -> None:
        in_clog_event.set()
        clog_event.wait()

    def interrupt(in_clog_event: threading.Event) -> None:
        in_clog_event.wait()
        interrupt_main()

    dsk = {("x", i): (clog, in_clog_event, clog_event) for i in range(20)}
    dsk["x"] = (len, list(dsk.keys()))

    interrupter = threading.Thread(target=interrupt, args=(in_clog_event,))
    interrupter.start()

    # Use explicitly created ThreadPoolExecutor to avoid leaking threads after
    # the KeyboardInterrupt
    with ThreadPoolExecutor(CPU_COUNT) as pool:
        with pytest.raises(KeyboardInterrupt):
            get(dsk, "x", pool=pool)
        clog_event.set()
    interrupter.join()


def warm_up_thread_pool(num_workers: int | None) -> None:
    """Ensure that there is a warm thread pool.

    With num_workers=None on the default thread, this function warms up the default
    thread pool. With num_workers set or from a different thread, this function spawns
    and warms up a non-default thread pool.

    When this function is called after another test that called get() or compute(), the
    thread pool will be already warm, at least partially (not all threads will
    necessarily be already running).
    """
    parties = num_workers or CPU_COUNT
    barrier = threading.Barrier(parties)
    dsk = {("x", i): (barrier.wait,) for i in range(parties)}
    get(dsk, list(dsk), num_workers=num_workers)


test_ctxvar = contextvars.ContextVar("test_ctxvar", default=42)


@pytest.mark.parametrize("num_workers", [None, 1])
def test_contextvars(num_workers):
    """When the thread pool is cold, contextvars area automatically propagated from the
    caller thread to **new** threads on Python 3.14t by default, as well as on 3.14 with
    the opt-in flag `python -X thread_inherit_context=1`.

    Test that contextvars are always propagated to the tasks, even in absence of the above
    flag and even when the threads were already warm when the context was set.
    """
    warm_up_thread_pool(num_workers)

    dsk = {"x": (test_ctxvar.get,)}
    # Reuse thread pool started by warm_up_thread_pool
    assert get(dsk, "x", num_workers=num_workers) == 42
    tok = test_ctxvar.set(43)
    try:
        assert get(dsk, "x", num_workers=num_workers) == 43
    finally:
        test_ctxvar.reset(tok)


@pytest.mark.parametrize("num_workers", [None, 1])
def test_context_aware_warnings(num_workers):
    """Test that warnings issued from a thread pool can be caught by the main thread,
    even when the thread pool was already warm when entering `warnings.catch_warnings`.
    This also affects `@pytest.mark.filterwarnings` and similar mechanisms.

    This needs special handling on Python 3.14t, which uses context-aware warnings.
    The system is also available opt-in on 3.14 (GIL-enabled) with
       python -X context_aware_warnings=1
    """
    warm_up_thread_pool(num_workers)

    def warn():
        warnings.warn("Hello world", RuntimeWarning, stacklevel=2)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        # Reuse thread pool started by warm_up_thread_pool
        get({"x": (warn,)}, "x", num_workers=num_workers)

    assert len(w) == 1
    assert isinstance(w[0].message, RuntimeWarning)
    assert str(w[0].message) == "Hello world"
