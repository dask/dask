import json
import pytest

from tornado import gen

from distributed.utils_test import gen_cluster, div
from distributed.diagnostics.scheduler import tasks, workers
from distributed.client import _wait

@gen_cluster(client=True)
def test_tasks(c, s, a, b):
    d = tasks(s)

    assert d['failed'] == 0
    assert d['in-memory'] == 0
    assert d['total'] == 0
    assert d['waiting'] == 0

    L = c.map(div, range(10), range(10))
    yield _wait(L)

    d = tasks(s)
    assert d['failed'] == 1
    assert d['in-memory'] == 9
    assert d['total'] == 10
    assert d['waiting'] == 0


@gen_cluster(client=True)
def test_workers(c, s, a, b):
    d = workers(s)

    assert json.loads(json.dumps(d)) == d

    assert 0 <= d[a.ip]['cpu'] <= 100
    assert 0 <= d[a.ip]['memory']
    assert 0 < d[a.ip]['memory_percent'] < 100
    assert set(map(int, d[a.ip]['ports'])) == {a.port, b.port}
    assert d[a.ip]['processing'] == {}
    # assert d[a.ip]['last-seen'] > 0

    L = c.map(div, range(10), range(10))
    yield _wait(L)

    assert 0 <= d[a.ip]['cpu'] <= 100
    assert 0 <= d[a.ip]['memory']
    assert 0 < d[a.ip]['memory_percent'] < 100
    assert set(map(int, d[a.ip]['ports'])) == {a.port, b.port}
    assert d[a.ip]['processing'] == {}
    try:
        assert 0 <= d[a.ip]['disk-read']
        assert 0 <= d[a.ip]['disk-write']
    except KeyError:
        import psutil
        with pytest.raises(RuntimeError):
            psutil.disk_io_counters()

    assert 0 <= d[a.ip]['network-send']
    assert 0 <= d[a.ip]['network-recv']
