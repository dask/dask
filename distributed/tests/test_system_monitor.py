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
