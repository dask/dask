import dask.frame as df
import pandas.util.testing as tm
import pandas as pd

def eq(a, b):
    if isinstance(a, df.Frame):
        a = a.compute()
    if isinstance(b, df.Frame):
        b = b.compute()
    if isinstance(a, pd.DataFrame):
        tm.assert_frame_equal(a, b)
        return True
    if isinstance(a, pd.Series):
        tm.assert_series_equal(a, b)
        return True
    assert a == b


def test_frame():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                                  index=[9, 9, 9])}
    d = df.Frame(dsk, 'x', [4, 9])

    result = (d['a'] + 1).compute()
    expected = pd.Series([2, 3, 4, 5, 6, 7, 8, 9, 10],
                        index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
                        name='a')

    assert eq(result, expected)

    assert d['b'].sum().compute() == 4+5+6 + 3+2+1 + 0+0+0
    assert d['b'].max().compute() == 6
