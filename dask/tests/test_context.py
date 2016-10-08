from dask.context import set_options, _globals
import dask.array as da
import dask


def test_with_get():
    var = [0]

    def myget(dsk, keys, **kwargs):
        var[0] = var[0] + 1
        return dask.get(dsk, keys, **kwargs)

    x = da.ones(10, chunks=(5,))

    assert x.sum().compute() == 10
    assert var[0] == 0

    with set_options(get=myget):
        assert x.sum().compute() == 10
    assert var[0] == 1

    # Make sure we've cleaned up
    assert x.sum().compute() == 10
    assert var[0] == 1


def test_set_options_context_manger():
    with set_options(foo='bar'):
        assert _globals['foo'] == 'bar'
    assert _globals['foo'] is None

    try:
        set_options(foo='baz')
        assert _globals['foo'] == 'baz'
    finally:
        del _globals['foo']
