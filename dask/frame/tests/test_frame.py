import dask.frame as df
from dask.frame.core import linecount
import pandas.util.testing as tm
import pandas as pd
from dask.utils import filetext
import dask

def eq(a, b):
    if isinstance(a, df.Frame):
        a = a.compute(get=dask.get)
    if isinstance(b, df.Frame):
        b = b.compute(get=dask.get)
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

    assert eq(d.head(2), dsk[('x', 0)].head(2))
    assert eq(d['a'].head(2), dsk[('x', 0)]['a'].head(2))

    full = d.compute()
    assert eq(d[d['b'] > 2], full[full['b'] > 2])
    assert eq(d[['a', 'b']], full[['a', 'b']])
    assert eq(d.a, full.a)

    assert repr(d) == repr(full.head(3))



text = """
name,amount
Alice,100
Bob,-200
Charlie,300
Dennis,400
Edith,-500
Frank,600
""".strip()


def test_linecount():
    with filetext(text) as fn:
        assert linecount(fn) == 7


def test_read_csv():
    with filetext(text) as fn:
        f = df.read_csv(fn, chunksize=3)
        assert f.npartitions == 2
        assert eq(f, pd.read_csv(fn))

    with filetext(text) as fn:
        f = df.read_csv(fn, chunksize=4)
        assert f.npartitions == 2


def test_set_index():
    dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 2, 6]},
                                  index=[0, 1, 3]),
           ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 5, 8]},
                                  index=[5, 6, 8]),
           ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [9, 1, 8]},
                                  index=[9, 9, 9])}
    d = df.Frame(dsk, 'x', [4, 9])
    full = d.compute()

    d2 = df.core.set_index(d, 'b', npartitions=3, out_chunksize=3)
    assert d2.npartitions == 3
    assert eq(d2, full.set_index('b').sort())

    d3 = df.core.set_index(d, d.b, npartitions=3, out_chunksize=3)
    assert d3.npartitions == 3
    assert eq(d3, full.set_index(full.b))


def test_shuffle():
    f = pd.DataFrame({'name': ['Alice', 'Bob', 'Charlie', 'Dennis'],
                       'balance': [100, 200, 300, 400]})
    f2 = pd.DataFrame({'name': ['Edith', 'Frank', 'George', 'Hannah'],
                        'balance': [500, 600, 700, 800]})
    cache = {('a', 0): f, ('a', 1): f2}
    blockdivs = [2, 3]
    keys = [('a', 0), ('a', 1)]

    result = df.core.shuffle(cache, keys, blockdivs)
    assert eq(cache[result[0]], 0)


def test_shard_df_on_index():
    f = pd.DataFrame({'a': [0, 10, 20, 30, 40], 'b': [5, 4 ,3, 2, 1]},
                      index=[1, 2, 3, 4, 4])

    result = list(df.core.shard_df_on_index(f, [2, 7]))
    assert eq(result[0], f.loc[[1]])
    assert eq(result[1], f.loc[[2, 3, 4]])
    assert eq(result[2], pd.DataFrame(columns=['a', 'b'], dtype=f.dtypes))
