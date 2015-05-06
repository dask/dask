from operator import getitem
from toolz import valmap
import bcolz
from pframe import pframe
from dask.dataframe.optimize import rewrite_rules, dataframe_from_ctable
import pandas as pd

dsk = {('x', 0): pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]},
                              index=[0, 1, 3]),
       ('x', 1): pd.DataFrame({'a': [4, 5, 6], 'b': [3, 2, 1]},
                              index=[5, 6, 8]),
       ('x', 2): pd.DataFrame({'a': [7, 8, 9], 'b': [0, 0, 0]},
                              index=[9, 9, 9])}
dfs = list(dsk.values())
pf = pframe(like=dfs[0], divisions=[5])
for df in dfs:
    pf.append(df)



def test_column_optimizations_with_pframe_and_rewrite():
    dsk2 = dict((('x', i), (getitem,
                             (pframe.get_partition, pf, i),
                             (list, ['a', 'b'])))
            for i in [1, 2, 3])

    expected = dict((('x', i),
                     (pframe.get_partition, pf, i, (list, ['a', 'b'])))
            for i in [1, 2, 3])
    result = valmap(rewrite_rules.rewrite, dsk2)

    assert result == expected


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


