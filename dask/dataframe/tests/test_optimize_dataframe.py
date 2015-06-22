from operator import getitem
from toolz import valmap
import bcolz
from dask.dataframe.optimize import rewrite_rules, dataframe_from_ctable
import dask.dataframe as dd
import pandas as pd

dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                              index=[9, 9, 9])}
dfs = list(dsk.values())


def test_column_optimizations_with_bcolz_and_rewrite():
    bc = bcolz.ctable([[1, 2, 3], [10, 20, 30]], names=['a', 'b'])
    func = lambda x: x
    for cols in [None, 'abc', ['abc']]:
        dsk2 = dict((('x', i),
                     (func,
                       (getitem,
                         (dataframe_from_ctable, bc, slice(0, 2), cols, {}),
                         (list, ['a', 'b']))))
                for i in [1, 2, 3])

        expected = dict((('x', i), (func, (dataframe_from_ctable,
                                     bc, slice(0, 2), (list, ['a', 'b']), {})))
                for i in [1, 2, 3])
        result = valmap(rewrite_rules.rewrite, dsk2)

        assert result == expected


def test_fast_functions():
    df = dd.DataFrame(dsk, 'x', ['a', 'b'], [None, None])
    e = df.a + df.b
    assert len(e.dask) > 6

    assert len(dd.optimize(e.dask, e._keys())) == 6
