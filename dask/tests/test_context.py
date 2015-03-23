from dask.context import context
import dask.array as da
import dask

def test_with_get():
    var = [0]

    def myget(dsk, keys, **kwargs):
        var[0] = var[0] + 1
        return dask.get(dsk, keys, **kwargs)

    x = da.ones(10, blockshape=(5,))

    assert x.sum().compute() == 10
    assert var[0] == 0

    with context(get=myget):
        assert x.sum().compute() == 10
    assert var[0] == 1

    # Make sure we've cleaned up
    assert x.sum().compute() == 10
    assert var[0] == 1
