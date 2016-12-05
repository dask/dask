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

    assert len(e._threads) == 1

    list(e.map(sleep, [0.01] * 4))
    assert len(threads | e._threads) == 3

    start = time()
    while all(t.is_alive() for t in threads):
        sleep(0.01)
        assert time() < start + 1

