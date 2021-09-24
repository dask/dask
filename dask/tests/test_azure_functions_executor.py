from dask.threaded import get
import dask


def test_basic():
    inc = lambda x: x + 1
    d = {'x': 1, 'y': (inc, 'x')}

    with dask.config.set(backend="azure_functions"):
        y = get(d, 'y')

    assert y == 2
