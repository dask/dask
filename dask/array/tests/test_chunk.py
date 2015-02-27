import numpy as np
from dask.array.chunk import coarsen, trim

def eq(a, b):
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


def test_coarsen():
    x = np.random.randint(10, size=(24, 24))
    y = coarsen(np.sum, x, {0: 2, 1: 4})
    assert y.shape == (12, 6)
    assert y[0, 0] == np.sum(x[:2, :4])


"""
def test_coarsen_on_uneven_shape():
    x = np.random.randint(10, size=(23, 24))
    y = coarsen(np.sum, x, {0: 2, 1: 4})
    assert y.shape == (12, 6)
    assert y[0, 0] == np.sum(x[:2, :4])
    assert eq(y[11, :], x[23, :])
"""
