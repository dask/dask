from time import sleep

from distributed.metrics import time
from distributed.threadpoolexecutor import ThreadPoolExecutor, secede


def test_tpe():
    e = ThreadPoolExecutor(2)
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
