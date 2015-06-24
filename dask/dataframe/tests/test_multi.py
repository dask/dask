import dask.dataframe as dd
import pandas as pd
from dask.dataframe.multi import align, join_indexed_dataframes
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


def test_join_indexed_dataframe_to_indexed_dataframe():
    A = pd.DataFrame({'x': [1, 2, 3, 4, 5, 6]},
                     index=[1, 2, 3, 4, 6, 7])
    a = dd.repartition(A, [1, 4, 7])

    B = pd.DataFrame({'y': list('abcdef')},
                     index=[1, 2, 4, 5, 6, 8])
    b = dd.repartition(B, [1, 2, 5, 8])

    c = join_indexed_dataframes(a, b, how='left')
    assert c.divisions[0] == a.divisions[0]
    assert c.divisions[-1] == a.divisions[-1]
    tm.assert_frame_equal(c.compute(), A.join(B))

    c = join_indexed_dataframes(a, b, how='right')
    assert c.divisions[0] == b.divisions[0]
    assert c.divisions[-1] == b.divisions[-1]
    tm.assert_frame_equal(c.compute(), A.join(B, how='right'))

    c = join_indexed_dataframes(a, b, how='inner')
    assert c.divisions[0] == 1
    assert c.divisions[-1] == 7
    tm.assert_frame_equal(c.compute(), A.join(B, how='inner'))

    c = join_indexed_dataframes(a, b, how='outer')
    assert c.divisions[0] == 1
    assert c.divisions[-1] == 8
    tm.assert_frame_equal(c.compute(), A.join(B, how='outer'))
