import dask.array as da
import numpy as np
import pytest

from dask_expr import Index, from_pandas
from dask_expr.tests._util import _backend_library, assert_eq

pd = _backend_library()


@pytest.fixture
def pdf():
    pdf = pd.DataFrame({"x": range(100)})
    pdf["y"] = pdf.x // 7  # Not unique; duplicates span different partitions
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=10)


def test_ufunc(df, pdf):
    ufunc = "conj"
    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    pandas_type = pdf.__class__
    dask_type = df.__class__

    assert isinstance(dafunc(df), dask_type)
    assert_eq(dafunc(df), npfunc(pdf))

    if isinstance(npfunc, np.ufunc):
        assert isinstance(npfunc(df), dask_type)
    else:
        assert isinstance(npfunc(df), pandas_type)
    assert_eq(npfunc(df), npfunc(pdf))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(pdf), pandas_type)
    assert_eq(dafunc(df), npfunc(pdf))

    assert isinstance(dafunc(df.index), Index)
    assert_eq(dafunc(df.index), npfunc(pdf.index))

    if isinstance(npfunc, np.ufunc):
        assert isinstance(npfunc(df.index), Index)
    else:
        assert isinstance(npfunc(df.index), pd.Index)

    assert_eq(npfunc(df.index), npfunc(pdf.index))


def test_ufunc_with_2args(pdf, df):
    ufunc = "logaddexp"
    dafunc = getattr(da, ufunc)
    npfunc = getattr(np, ufunc)

    pandas_type = pdf.__class__
    pdf2 = pdf.sort_index(ascending=False)
    dask_type = df.__class__
    df2 = from_pandas(pdf2, npartitions=8)
    # applying Dask ufunc doesn't trigger computation
    assert isinstance(dafunc(df, df2), dask_type)
    assert_eq(dafunc(df, df2), npfunc(pdf, pdf2))

    # should be fine with pandas as a second arg, too
    assert isinstance(dafunc(df, pdf2), dask_type)
    assert_eq(dafunc(df, pdf2), npfunc(pdf, pdf2))

    # applying NumPy ufunc is lazy
    if isinstance(npfunc, np.ufunc):
        assert isinstance(npfunc(df, df2), dask_type)
        assert isinstance(npfunc(df, pdf2), dask_type)
    else:
        assert isinstance(npfunc(df, df2), pandas_type)
        assert isinstance(npfunc(df, pdf2), pandas_type)

    assert_eq(npfunc(df, df2), npfunc(pdf, pdf2))
    assert_eq(npfunc(df, pdf), npfunc(pdf, pdf2))

    # applying Dask ufunc to normal Series triggers computation
    assert isinstance(dafunc(pdf, pdf2), pandas_type)
    assert_eq(dafunc(pdf, pdf2), npfunc(pdf, pdf2))


@pytest.mark.parametrize(
    "ufunc",
    [
        np.mean,
        np.std,
        np.sum,
        np.cumsum,
        np.cumprod,
        np.var,
        np.min,
        np.max,
        np.all,
        np.any,
        np.prod,
    ],
)
def test_reducers(pdf, df, ufunc):
    assert_eq(ufunc(pdf, axis=0), ufunc(df, axis=0))


def test_clip(pdf, df):
    assert_eq(np.clip(pdf, 10, 20), np.clip(df, 10, 20))
