import dask.dataframe as dd
import pandas as pd
from dask.dataframe.multi import align
import pandas.util.testing as tm

def test_align():
    A = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6], 'y': list('abdabd')},
                     index=[10, 20, 30, 40, 50, 60])
    a = dd.repartition(A, [10, 40, 60])

    B = pd.DataFrame({'x': [1, 2, 3, 4], 'y': list('abda')},
                     index=[30, 70, 80, 100])
    b = dd.repartition(B, [30, 80, 100])

    (aa, bb), divisions, L = align(a, b)
    assert isinstance(a, dd.DataFrame)
    assert isinstance(b, dd.DataFrame)
    assert divisions == (10, 30, 40, 60, 80, 100)
    assert isinstance(L, list)
    assert len(divisions) == 1 + len(L)
    assert L == [[(aa._name, 0), None],
                 [(aa._name, 1), (bb._name, 0)],
                 [(aa._name, 2), (bb._name, 1)],
                 [None, (bb._name, 2)],
                 [None, (bb._name, 3)]]
