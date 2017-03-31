from __future__ import print_function, division, absolute_import

from time import sleep

from distributed.system_monitor import SystemMonitor


def test_SystemMonitor():
    sm = SystemMonitor()
    a = sm.update()
    sleep(0.01)
    b = sm.update()

    assert sm.cpu
    assert sm.memory
    assert set(a) == set(b)
    assert all(rb >= 0 for rb in sm.read_bytes)
    assert all(wb >= 0 for wb in sm.write_bytes)
    assert all(len(q) == 2 for q in sm.quantities.values())

    assert 'cpu' in repr(sm)


def test_count():
    sm = SystemMonitor(n=5)
    assert sm.count == 0
    sm.update()
    assert sm.count == 1

    for i in range(10):
        sm.update()

    assert sm.count == 11
    for v in sm.quantities.values():
        assert len(v) == 5


def test_range_query():
    sm = SystemMonitor(n=5)

    assert all(len(v) == 0 for v in sm.range_query(0).values())
    assert all(len(v) == 0 for v in sm.range_query(123).values())

    sm.update()
    sm.update()
    sm.update()

    assert all(len(v) == 3 for v in sm.range_query(0).values())
    assert all(len(v) == 2 for v in sm.range_query(1).values())

    for i in range(10):
        sm.update()

    assert all(len(v) == 3 for v in sm.range_query(10).values())
    assert all(len(v) == 5 for v in sm.range_query(0).values())
