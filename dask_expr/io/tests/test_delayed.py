import numpy as np
from dask import delayed

from dask_expr import from_delayed
from dask_expr.tests._util import _backend_library, assert_eq

lib = _backend_library()


def test_from_delayed():
    pdf = lib.DataFrame(
        data=np.random.normal(size=(10, 4)), columns=["a", "b", "c", "d"]
    )
    parts = [pdf.iloc[:1], pdf.iloc[1:3], pdf.iloc[3:6], pdf.iloc[6:10]]
    dfs = [delayed(parts.__getitem__)(i) for i in range(4)]

    df = from_delayed(dfs, meta=pdf.head(0), divisions=None)
    assert_eq(df, pdf)

    divisions = tuple([p.index[0] for p in parts] + [parts[-1].index[-1]])
    df = from_delayed(dfs, meta=pdf.head(0), divisions=divisions)
    assert_eq(df, pdf)
