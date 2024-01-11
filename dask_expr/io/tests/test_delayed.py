import numpy as np
from dask import delayed

from dask_expr import from_delayed
from dask_expr.tests._util import _backend_library, assert_eq

pd = _backend_library()


def test_from_delayed():
    pdf = pd.DataFrame(
        data=np.random.normal(size=(10, 4)), columns=["a", "b", "c", "d"]
    )
    parts = [pdf.iloc[:1], pdf.iloc[1:3], pdf.iloc[3:6], pdf.iloc[6:10]]
    dfs = [delayed(parts.__getitem__)(i) for i in range(4)]

    df = from_delayed(dfs, meta=pdf.head(0), divisions=None)
    assert_eq(df, pdf)

    divisions = tuple([p.index[0] for p in parts] + [parts[-1].index[-1]])
    df = from_delayed(dfs, meta=pdf.head(0), divisions=divisions)
    assert_eq(df, pdf)


def test_from_delayed_dask():
    df = pd.DataFrame(data=np.random.normal(size=(10, 4)), columns=list("abcd"))
    parts = [df.iloc[:1], df.iloc[1:3], df.iloc[3:6], df.iloc[6:10]]
    dfs = [delayed(parts.__getitem__)(i) for i in range(4)]
    meta = dfs[0].compute()

    my_len = lambda x: pd.Series([len(x)])

    for divisions in [None, [0, 1, 3, 6, 10]]:
        ddf = from_delayed(dfs, meta=meta, divisions=divisions)
        assert_eq(ddf, df)
        assert list(ddf.map_partitions(my_len).compute()) == [1, 2, 3, 4]
        assert ddf.known_divisions == (divisions is not None)

        s = from_delayed([d.a for d in dfs], meta=meta.a, divisions=divisions)
        assert_eq(s, df.a)
        assert list(s.map_partitions(my_len).compute()) == [1, 2, 3, 4]
        assert ddf.known_divisions == (divisions is not None)
