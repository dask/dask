import pandas as pd
import pytest

import dask
import dask.dataframe as dd
from dask.dataframe.core import Scalar
from dask.dataframe.utils import assert_eq
from dask.highlevelgraph import HighLevelGraph


def test_fuse_ave_width():
    df = pd.DataFrame({"x": range(10)})
    df = dd.from_pandas(df, npartitions=5)

    s = (df.x + 1) + (df.x + 2)

    with dask.config.set({"optimization.fuse.ave-width": 4}):
        a = s.__dask_optimize__(s.dask, s.__dask_keys__())

    b = s.__dask_optimize__(s.dask, s.__dask_keys__())

    assert len(a) <= 15
    assert len(b) <= 15


def test_optimize_blockwise():
    from dask.array.optimization import optimize_blockwise

    df = pd.DataFrame({"x": range(10), "y": range(10)})
    ddf = dd.from_pandas(df, npartitions=2)

    for i in range(10):
        ddf["x"] = ddf.x + 1 + ddf.y

    graph = optimize_blockwise(ddf.dask)

    assert len(graph) <= 4


def test_optimization_requires_hlg():
    df = pd.DataFrame({"x": range(10)})
    df = dd.from_pandas(df, npartitions=5) + 1

    dsk = df.dask.to_dict()
    assert isinstance(dsk, dict)

    with pytest.raises(TypeError, match="high-level graphs"):
        df.__dask_optimize__(dsk, df.__dask_keys__())

    # Ensure DataFrames constructed from low-level graphs still work
    df_from_lowlevel = dd.DataFrame(dsk, df._name, df._meta, df.divisions)
    assert isinstance(df_from_lowlevel.dask, HighLevelGraph)
    # ^ `_Frame.__init__` converts to HLG
    assert tuple(df_from_lowlevel.dask.layers) == df_from_lowlevel.__dask_layers__()
    (df_opt,) = dask.optimize(df_from_lowlevel)
    assert_eq(df, df_opt)

    # Ensure Series constructed from low-level graphs still work
    s = df.x
    dsk = s.dask.to_dict()
    s_from_lowlevel = dd.Series(dsk, s._name, s._meta, s.divisions)
    assert isinstance(s_from_lowlevel.dask, HighLevelGraph)
    # ^ `_Frame.__init__` converts to HLG
    assert tuple(s_from_lowlevel.dask.layers) == s_from_lowlevel.__dask_layers__()
    (s_opt,) = dask.optimize(s_from_lowlevel)
    assert_eq(s, s_opt)

    # Ensure Scalars constructed from low-level graphs still work
    sc = s.sum()
    dsk = sc.dask.to_dict()
    sc_from_lowlevel = Scalar(dsk, sc._name, sc._meta)
    assert isinstance(sc_from_lowlevel.dask, HighLevelGraph)
    # ^ `Scalar.__init__` converts to HLG
    assert tuple(sc_from_lowlevel.dask.layers) == sc_from_lowlevel.__dask_layers__()
    (sc_opt,) = dask.optimize(sc_from_lowlevel)
    assert_eq(sc, sc_opt)
