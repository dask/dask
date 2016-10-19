import pytest
from distutils.version import LooseVersion
from operator import getitem
from toolz import merge
from dask.dataframe.optimize import dataframe_from_ctable
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
    bcolz = pytest.importorskip('bcolz')

    bc = bcolz.ctable([[1, 2, 3], [10, 20, 30]], names=['a', 'b'])
    for cols in [None, 'abc', ['abc']]:
        dsk2 = merge(dict((('x', i),
                          (dataframe_from_ctable, bc, slice(0, 2), cols, {}))
                          for i in [1, 2, 3]),
                     dict((('y', i),
                          (getitem, ('x', i), ['a', 'b']))
                          for i in [1, 2, 3]))

        expected = dict((('y', i), (dataframe_from_ctable,
                                    bc, slice(0, 2), ['a', 'b'], {}))
                        for i in [1, 2, 3])

        result = dd.optimize(dsk2, [('y', i) for i in [1, 2, 3]])
        assert result == expected


def test_castra_column_store():
    castra = pytest.importorskip('castra')
    blosc = pytest.importorskip('blosc')
    if (LooseVersion(blosc.__version__) == '1.3.0' or
            LooseVersion(castra.__version__) < '0.1.8'):
        pytest.skip()

    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})

    with castra.Castra(template=df) as c:
        c.extend(df)

        df = c.to_dask()

        df2 = df[['x']]

        dsk = dd.optimize(df2.dask, df2._keys())

        assert dsk == {(df2._name, 0): (castra.Castra.load_partition, c,
                                        '0--2', ['x'])}
        df3 = df.index
        dsk = dd.optimize(df3.dask, df3._keys())
        assert dsk == {(df3._name, 0): (castra.Castra.load_index, c, '0--2')}
