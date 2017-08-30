import dask.array as da


def test_linear():
    x = da.ones(100, chunks=(10,))
    assert len(x.dask) == 10

    assert len((x + 1).dask) <= 20
    assert len((x + 1 + 1 + 1 + 1).dask) <= 20
    assert len((x + x + 1).dask) <= 20
    assert len((x + 1).sum().dask) < 25
    assert len(((x + x) + (x + 1) + (x + 2)).dask) <= 20


def test_transpose():
    x = da.ones((100, 100), chunks=(50, 50))

    y = x + 1
    z = y + y.T
    assert len(z.dask) <= len(x.dask) * 2
