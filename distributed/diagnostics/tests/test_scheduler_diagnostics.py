from distributed.utils_test import gen_cluster, div
from distributed.diagnostics.scheduler import status
from distributed.executor import _wait

@gen_cluster(executor=True)
def test_with_status(e, s, a, b):
    d = status(s)

    assert d['failed'] == 0
    assert d['in-memory'] == 0
    assert d['bytes'] == {a.address: 0, b.address: 0}
    assert d['processing'] == {a.address: {}, b.address: {}}
    assert d['ready'] == 0
    assert d['tasks'] == 0
    assert d['waiting'] == 0

    L = e.map(div, range(10), range(10))
    yield _wait(L)

    d = status(s)
    assert d['failed'] == 1
    assert d['in-memory'] == 9
    assert d['ready'] == 0
    assert d['tasks'] == 10
    assert d['waiting'] == 0
    assert d['processing'] == {a.address: {}, b.address: {}}
    assert set(d['bytes']) == {a.address, b.address}
    assert all(v > 0 for v in d['bytes'].values())
