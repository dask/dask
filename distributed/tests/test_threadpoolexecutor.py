from time import sleep
import threading

from distributed.metrics import time
from distributed.threadpoolexecutor import ThreadPoolExecutor, secede, rejoin


def test_tpe():
    with ThreadPoolExecutor(2) as e:
        list(e.map(sleep, [0.01] * 4))

        threads = e._threads.copy()
        assert len(threads) == 2

        def f():
            secede()
            return 1

        assert e.submit(f).result() == 1

        list(e.map(sleep, [0.01] * 4))
        assert len(threads | e._threads) == 3

        start = time()
        while all(t.is_alive() for t in threads):
            sleep(0.01)
            assert time() < start + 1


def test_shutdown_timeout():
    e = ThreadPoolExecutor(1)
    futures = [e.submit(sleep, 0.1 * i) for i in range(1, 3, 1)]
    sleep(0.01)

    start = time()
    e.shutdown()
    end = time()
    assert end - start > 0.1


def test_shutdown_timeout_raises():
    e = ThreadPoolExecutor(1)
    futures = [e.submit(sleep, 0.1 * i) for i in range(1, 3, 1)]
    sleep(0.05)

    start = time()
    e.shutdown(timeout=0.1)
    end = time()
    assert end - start > 0.05


def test_secede_rejoin_busy():
    with ThreadPoolExecutor(2) as e:

        def f():
            assert threading.current_thread() in e._threads
            secede()
            sleep(0.1)
            assert threading.current_thread() not in e._threads
            rejoin()
            assert len(e._threads) == 2
            assert threading.current_thread() in e._threads
            return threading.current_thread()

        future = e.submit(f)
        L = [e.submit(sleep, 0.2) for i in range(10)]
        start = time()
        special_thread = future.result()
        stop = time()

        assert 0.1 < stop - start < 0.3

        assert len(e._threads) == 2
        assert special_thread in e._threads

        def f():
            sleep(0.01)
            return threading.current_thread()

        futures = [e.submit(f) for _ in range(10)]
        assert special_thread in {future.result() for future in futures}


def test_secede_rejoin_quiet():
    with ThreadPoolExecutor(2) as e:

        def f():
            assert threading.current_thread() in e._threads
            secede()
            sleep(0.1)
            assert threading.current_thread() not in e._threads
            rejoin()
            assert len(e._threads) == 2
            assert threading.current_thread() in e._threads
            return threading.current_thread()

        future = e.submit(f)
        result = future.result()
