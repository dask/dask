import dask
import dask.dataframe as dd
from dask.dataframe.utils import assert_eq


def test_Tuple():

    def inc(x):
        return x + 1

    def double(x):
        return x + 2

    def add(x, y):
        return x + y

    # Delayed objects
    a = dask.delayed(inc)(1)
    b = dask.delayed(double)(2)
    c = dask.delayed(add)(a,b)

    tp = dd.Tuple((a,))
    result = tp.compute()
    assert_eq(result, (2,))
    assert_eq(result, tuple((2,)))

    tp = dd.Tuple((a,b))
    result = tp.compute()
    assert_eq(result, (2,4))
    assert_eq(result, tuple((2,4)))

    tp = dd.Tuple(((a,b),c))
    result = tp.compute()
    assert_eq(result, ((2,4),6))
    assert_eq(result, tuple(((2,4),6)))
