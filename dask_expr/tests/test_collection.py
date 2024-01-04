from __future__ import annotations

import fnmatch
import io
import operator
import pickle
from datetime import timedelta

import dask
import numpy as np
import pytest
from dask.dataframe._compat import PANDAS_GE_210
from dask.dataframe.utils import UNKNOWN_CATEGORIES
from dask.utils import M

from dask_expr import (
    Series,
    expr,
    from_pandas,
    is_scalar,
    isna,
    optimize,
    to_datetime,
    to_numeric,
    to_timedelta,
)
from dask_expr._expr import are_co_aligned
from dask_expr._reductions import Len
from dask_expr._shuffle import Shuffle
from dask_expr.datasets import timeseries
from dask_expr.tests._util import _backend_library, assert_eq, xfail_gpu

# Set DataFrame backend for this module
lib = _backend_library()


@pytest.fixture
def pdf():
    pdf = lib.DataFrame({"x": range(100)})
    pdf["y"] = pdf.x // 7  # Not unique; duplicates span different partitions
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=10)


def test_del(pdf, df):
    pdf = pdf.copy()

    # Check __delitem__
    del pdf["x"]
    del df["x"]
    assert_eq(pdf, df)


@pytest.mark.parametrize("verbose", (True, False, None))
@pytest.mark.parametrize("buf", (None, io.StringIO))
@pytest.mark.parametrize("memory_usage", (True, False, None))
def test_info(df, verbose, buf, memory_usage):
    if buf is not None:
        buf = buf()
    kwargs = {
        k: v
        for k, v in (("verbose", verbose), ("buf", buf), ("memory_usage", memory_usage))
        if v is not None
    }

    ret = df.info(**kwargs)

    if buf and not verbose and not memory_usage:
        expected = (
            "<class 'dask_expr.DataFrame'>\n"
            "Columns: 2 entries, x to y\n"
            "dtypes: int64(2)"
        )
        assert buf.getvalue() == expected
    elif buf and verbose and not memory_usage:
        expected = (
            "<class 'dask_expr.DataFrame'>\n"
            "RangeIndex: 100 entries, 0 to 99\n"
            "Data columns (total 2 columns):\n"
            " #   Column  Non-Null Count  Dtype\n"
            "---  ------  --------------  -----\n"
            " 0   x       100 non-null      int64\n"
            " 1   y       100 non-null      int64\n"
            "dtypes: int64(2)"
        )
        assert buf.getvalue() == expected
    elif buf and not verbose and memory_usage:
        expected = (
            "<class 'dask_expr.DataFrame'>\n"
            "Columns: 2 entries, x to y\n"
            "dtypes: int64(2)\n"
            "memory usage: *\n"
        )
        assert fnmatch.fnmatch(buf.getvalue(), expected)
    elif all((buf, verbose, memory_usage)):
        expected = (
            "<class 'dask_expr.DataFrame'>\n"
            "RangeIndex: 100 entries, 0 to 99\n"
            "Data columns (total 2 columns):\n"
            " #   Column  Non-Null Count  Dtype\n"
            "---  ------  --------------  -----\n"
            " 0   x       100 non-null      int64\n"
            " 1   y       100 non-null      int64\n"
            "dtypes: int64(2)\n"
            "memory usage: *\n"
        )
        assert fnmatch.fnmatch(buf.getvalue(), expected)
    elif buf is None:
        assert ret is None
    else:
        raise NotImplementedError(f"Case not covered for kwargs: {kwargs}")


def test_setitem(pdf, df):
    pdf = pdf.copy()
    pdf["z"] = pdf.x + pdf.y

    df["z"] = df.x + df.y

    assert "z" in df.columns
    assert_eq(df, pdf)


def test_series_product(pdf, df):
    assert_eq(pdf.x.product(), df.x.product())
    assert_eq(pdf.y.product(), df.y.product())


@xfail_gpu("https://github.com/rapidsai/cudf/issues/10271")
def test_explode():
    pdf = lib.DataFrame({"a": [[1, 2], [3, 4]]})
    df = from_pandas(pdf)
    assert_eq(pdf.explode(column="a"), df.explode(column="a"))
    assert_eq(pdf.a.explode(), df.a.explode())


@xfail_gpu("https://github.com/rapidsai/cudf/issues/10271")
def test_explode_simplify(pdf):
    pdf["z"] = 1
    df = from_pandas(pdf)
    q = df.explode(column="x")["y"]
    result = optimize(q, fuse=False)
    expected = optimize(df[["x", "y"]], fuse=False).explode(column="x")["y"]
    assert result._name == expected._name


def test_meta_divisions_name():
    a = lib.DataFrame({"x": [1, 2, 3, 4], "y": [1.0, 2.0, 3.0, 4.0]})
    df = 2 * from_pandas(a, npartitions=2)
    assert list(df.columns) == list(a.columns)
    assert df.npartitions == 2

    assert np.isscalar(df.x.sum()._meta)
    assert df.x.sum().npartitions == 1

    assert "mul" in df._name
    assert "sum" in df.sum()._name


def test_meta_blockwise():
    a = lib.DataFrame({"x": [1, 2, 3, 4], "y": [1.0, 2.0, 3.0, 4.0]})
    b = lib.DataFrame({"z": [1, 2, 3, 4], "y": [1.0, 2.0, 3.0, 4.0]})

    aa = from_pandas(a, npartitions=2)
    bb = from_pandas(b, npartitions=2)

    cc = 2 * aa - 3 * bb
    assert set(cc.columns) == {"x", "y", "z"}


def test_dask(pdf, df):
    assert (df.x + df.y).npartitions == 10
    z = (df.x + df.y).sum()

    assert assert_eq(z, (pdf.x + pdf.y).sum())


@pytest.mark.parametrize(
    "func",
    [
        M.max,
        M.min,
        M.any,
        M.all,
        M.sum,
        M.prod,
        M.count,
        M.mean,
        M.std,
        M.var,
        pytest.param(
            M.idxmin, marks=xfail_gpu("https://github.com/rapidsai/cudf/issues/9602")
        ),
        pytest.param(
            M.idxmax, marks=xfail_gpu("https://github.com/rapidsai/cudf/issues/9602")
        ),
        pytest.param(
            lambda df: df.size,
            marks=pytest.mark.skip(reason="scalars don't work yet"),
        ),
    ],
)
def test_reductions(func, pdf, df):
    result = func(df)
    assert result.known_divisions
    assert_eq(result, func(pdf))
    result = func(df.x)
    assert not result.known_divisions
    assert_eq(result, func(pdf.x))
    # check_dtype False because sub-selection of columns that is pushed through
    # is not reflected in the meta calculation
    assert_eq(func(df)["x"], func(pdf)["x"], check_dtype=False)


def test_reduction_on_empty_df():
    pdf = lib.DataFrame()
    df = from_pandas(pdf)
    assert_eq(df.sum(), pdf.sum())


@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.parametrize(
    "skipna",
    [
        True,
        pytest.param(
            False, marks=xfail_gpu("cudf requires skipna=True when nulls are present.")
        ),
    ],
)
@pytest.mark.parametrize("ddof", [1, 2])
def test_std_kwargs(axis, skipna, ddof):
    pdf = lib.DataFrame(
        {"x": range(30), "y": [1, 2, None] * 10, "z": ["dog", "cat"] * 15}
    )
    df = from_pandas(pdf, npartitions=3)
    assert_eq(
        pdf.std(axis=axis, skipna=skipna, ddof=ddof, numeric_only=True),
        df.std(axis=axis, skipna=skipna, ddof=ddof, numeric_only=True),
    )


@pytest.mark.parametrize("func", ["cumsum", "cumprod", "cummin", "cummax"])
def test_cumulative_methods(df, pdf, func):
    assert_eq(getattr(df, func)(), getattr(pdf, func)(), check_dtype=False)
    assert_eq(getattr(df.x, func)(), getattr(pdf.x, func)())

    q = getattr(df, func)()["x"]
    assert q.simplify()._name == getattr(df.x, func)().simplify()._name

    pdf.loc[slice(None, None, 2), "x"] = np.nan
    df = from_pandas(pdf, npartitions=10)
    assert_eq(
        getattr(df, func)(skipna=False),
        getattr(pdf, func)(skipna=False),
        check_dtype=False,
    )
    assert_eq(getattr(df.x, func)(skipna=False), getattr(pdf.x, func)(skipna=False))

    df = from_pandas(pdf, npartitions=10)
    df["new"] = getattr(df.x, func)()
    pdf["new"] = getattr(pdf.x, func)()

    assert_eq(df, pdf)


def test_bool(df):
    conditions = [df, df["x"], df == df, df["x"] == df["x"]]
    for cond in conditions:
        with pytest.raises(ValueError):
            bool(cond)


@xfail_gpu("nbytes not supported by cudf")
def test_nbytes(pdf, df):
    with pytest.raises(NotImplementedError, match="nbytes is not implemented"):
        df.nbytes
    assert_eq(df.x.nbytes, pdf.x.nbytes)


def test_mode():
    pdf = lib.DataFrame({"x": [1, 2, 3, 1, 2]})
    df = from_pandas(pdf, npartitions=3)

    assert_eq(df.x.mode(), pdf.x.mode(), check_names=False)


def test_value_counts(df, pdf):
    with pytest.raises(
        AttributeError, match="'DataFrame' object has no attribute 'value_counts'"
    ):
        df.value_counts()
    assert_eq(df.x.value_counts(), pdf.x.value_counts().astype("int64"))


def test_dropna(pdf):
    pdf.loc[0, "y"] = np.nan
    df = from_pandas(pdf)
    assert_eq(df.dropna(), pdf.dropna())
    assert_eq(df.dropna(how="all"), pdf.dropna(how="all"))
    assert_eq(df.y.dropna(), pdf.y.dropna())


def test_value_counts_with_dropna():
    pdf = lib.DataFrame({"x": [1, 2, 1, 3, np.nan, 1, 4]})
    df = from_pandas(pdf, npartitions=3)
    result = df.x.value_counts(dropna=False)
    expected = pdf.x.value_counts(dropna=False)
    assert_eq(result, expected)


def test_fillna():
    pdf = lib.DataFrame({"x": [1, 2, None, None, 5, 6]})
    df = from_pandas(pdf, npartitions=2)
    actual = df.fillna(value=100)
    expected = pdf.fillna(value=100)
    assert_eq(actual, expected)


@pytest.mark.parametrize("limit", (None, 1, 2))
@pytest.mark.parametrize("how", ("ffill", "bfill"))
@pytest.mark.parametrize("axis", ("index", "columns", 0, 1))
def test_ffill_and_bfill(limit, axis, how):
    if limit is None:
        pytest.xfail("Need to determine partition size for Fill.before <= frame size")
    if axis in (1, "columns"):
        pytest.xfail("bfill/ffill not implemented for axis 1")
    pdf = lib.DataFrame({"x": [1, 2, None, None, 5, 6]})
    df = from_pandas(pdf, npartitions=2)
    actual = getattr(df, how)(axis=axis, limit=limit)
    expected = getattr(pdf, how)(axis=axis, limit=limit)
    assert_eq(actual, expected)


def test_series_map_meta():
    ser = lib.Series(
        ["".join(np.random.choice(["a", "b", "c"], size=3)) for x in range(100)]
    )

    mapper = lib.Series(np.random.randint(50, size=len(ser)))
    expected = ser.map(mapper)
    dask_base = from_pandas(ser, npartitions=5)
    dask_map = from_pandas(mapper, npartitions=5)
    result = dask_base.map(dask_map)
    assert_eq(expected, result)


@pytest.mark.parametrize("periods", (1, 2))
@pytest.mark.parametrize("freq", (None, "1h", timedelta(hours=1)))
@pytest.mark.parametrize("axis", ("index", 0, "columns", 1))
def test_shift(pdf, df, periods, freq, axis):
    if freq and axis in ("columns", 1):
        pytest.skip(reason="Neither dask or pandas supports freq w/ axis 1 shift")

    if freq is not None:
        pdf["time"] = lib.date_range("2000-01-01", "2000-01-02", periods=len(pdf))
        pdf = pdf.set_index("time", drop=True)
        df = from_pandas(pdf, npartitions=df.npartitions)

    actual = df.shift(periods=periods, axis=axis, freq=freq)
    expected = pdf.shift(periods=periods, axis=axis, freq=freq)
    assert_eq(actual, expected)


def test_memory_usage(pdf):
    # Results are not equal with RangeIndex because pandas has one RangeIndex while
    # we have one RangeIndex per partition
    pdf.index = np.arange(len(pdf))
    df = from_pandas(pdf)
    assert_eq(df.memory_usage(), pdf.memory_usage())
    assert_eq(df.memory_usage(index=False), pdf.memory_usage(index=False))
    assert_eq(df.x.memory_usage(), pdf.x.memory_usage())
    assert_eq(df.x.memory_usage(index=False), pdf.x.memory_usage(index=False))
    assert_eq(df.index.memory_usage(), pdf.index.memory_usage())
    with pytest.raises(TypeError, match="got an unexpected keyword"):
        df.index.memory_usage(index=True)


@pytest.mark.parametrize("func", [M.nlargest, M.nsmallest])
def test_nlargest_nsmallest(df, pdf, func):
    assert_eq(func(df, n=5, columns="x"), func(pdf, n=5, columns="x"))
    assert_eq(func(df.x, n=5), func(pdf.x, n=5))
    with pytest.raises(TypeError, match="got an unexpected keyword argument"):
        func(df.x, n=5, columns="foo")


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.x > 10,
        lambda df: df.x.gt(10),
        lambda df: df.x + 20 > df.y,
        lambda df: 10 < df.x,
        lambda df: df.x.lt(10),
        lambda df: 10 <= df.x,
        lambda df: df.x.le(10),
        lambda df: 10 == df.x,
        lambda df: df.x.eq(10),
        lambda df: df.x < df.y,
        lambda df: df.lt(df),
        lambda df: df.x.lt(df.y),
        lambda df: df.x > df.y,
        lambda df: df.gt(df),
        lambda df: df.x.gt(df.y),
        lambda df: df.x == df.y,
        lambda df: df.eq(df),
        lambda df: df.x.eq(df.y),
        lambda df: df.x != df.y,
        lambda df: df.ne(df),
        lambda df: df.x.ne(df.y),
    ],
)
def test_conditionals(func, pdf, df):
    assert_eq(func(pdf), func(df), check_names=False)


@pytest.mark.parametrize("axis", ("index", 0, "columns", 1, None))
@pytest.mark.parametrize("periods", (1, 2, None))
def test_diff(pdf, df, axis, periods):
    kwargs = {k: v for k, v in (("periods", periods), ("axis", axis)) if v}

    actual = df.diff(**kwargs)
    expected = pdf.diff(**kwargs)
    assert_eq(expected, actual)

    # Check projections
    expected = df[["x"]].diff(**kwargs)
    actual = df.diff(**kwargs)[["x"]]

    # no optimization on axis 1
    if axis in ("columns", 1):
        assert actual._name == actual.simplify()._name
    else:
        assert actual.simplify()._name == expected.simplify()._name


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.x & df.y,
        lambda df: df.x.__rand__(df.y),
        lambda df: df.x | df.y,
        lambda df: df.x.__ror__(df.y),
        lambda df: df.x ^ df.y,
        lambda df: df.x.__rxor__(df.y),
    ],
)
def test_boolean_operators(func):
    pdf = lib.DataFrame(
        {"x": [True, False, True, False], "y": [True, False, False, False]}
    )
    df = from_pandas(pdf)
    assert_eq(func(pdf), func(df))


@pytest.mark.parametrize("axis", ("columns", 1, "index", 0))
@pytest.mark.parametrize("level", (None, 0, "x"))
@pytest.mark.parametrize("fill_value", (None, 1))
@pytest.mark.parametrize(
    "op",
    (
        "add",
        "sub",
        "mul",
        "div",
        "divide",
        "truediv",
        "floordiv",
        "mod",
        "pow",
        "radd",
        "rsub",
        "rmul",
        "rdiv",
        "rtruediv",
        "rfloordiv",
        "rmod",
        "rpow",
    ),
)
@pytest.mark.parametrize("series", (True, False))
@pytest.mark.parametrize("other", ("series", "dataframe", 1))
def test_method_operators(pdf, df, axis, level, fill_value, op, series, other):
    kwargs = {
        k: v
        for k, v in (("axis", axis), ("level", level), ("fill_value", fill_value))
        if v is not None
    }

    if level is not None:
        with pytest.raises(NotImplementedError, match="level must be None"):
            getattr(df, op)(other=df, **kwargs)
        return

    if other == "series":
        pother = pdf.x
        other = df.x
    elif other == "dataframe":
        pother = pdf
        other = df
    else:
        pother = other

    if isinstance(other, Series) and axis in (1, "columns"):
        with pytest.raises(ValueError, match=f"Unable to {op} dd.Series with axis=1"):
            getattr(df, op)(other=other, **kwargs)

    elif isinstance(other, Series) and axis in (0, "index") and fill_value:
        msg = f"fill_value {fill_value} not supported"
        with pytest.raises(NotImplementedError, match=msg):
            getattr(df, op)(other=other, **kwargs)

    else:
        expected = getattr(pdf, op)(other=pother, **kwargs)
        actual = getattr(df, op)(other=other, **kwargs)
        assert_eq(expected, actual)


@pytest.mark.parametrize(
    "func",
    [
        lambda df: ~df,
        lambda df: ~df.x,
        lambda df: -df.z,
        lambda df: +df.z,
        lambda df: -df,
        lambda df: +df,
    ],
)
def test_unary_operators(func):
    pdf = lib.DataFrame(
        {"x": [True, False, True, False], "y": [True, False, False, False], "z": 1}
    )
    df = from_pandas(pdf)
    assert_eq(func(pdf), func(df))


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.x + df.y,
        lambda df: 2 * df.x,
        lambda df: df.x * df.y,
        lambda df: df.x - df.y,
        lambda df: df.x**2,
    ],
)
def test_binary_operator(pdf, df, func):
    assert_eq(func(pdf), func(df))


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df[(df.x > 10) | (df.x < 5)],
        lambda df: df[(df.x > 7) & (df.x < 10)],
    ],
)
def test_and_or(func, pdf, df):
    assert_eq(func(pdf), func(df), check_names=False)


@xfail_gpu("period_range not supported by cudf")
@pytest.mark.parametrize("how", ["start", "end"])
def test_to_timestamp(pdf, how):
    pdf.index = lib.period_range("2019-12-31", freq="D", periods=len(pdf))
    df = from_pandas(pdf)
    assert_eq(df.to_timestamp(how=how), pdf.to_timestamp(how=how))
    assert_eq(df.x.to_timestamp(how=how), pdf.x.to_timestamp(how=how))


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.astype(int),
        lambda df: df.clip(lower=10, upper=50),
        lambda df: df.x.clip(lower=10, upper=50),
        lambda df: df.x.between(left=10, right=50),
        lambda df: df.x.map(lambda x: x + 1),
        lambda df: df[df.x > 5],
        lambda df: df.assign(a=df.x + df.y, b=df.x - df.y),
        lambda df: df.assign(a=df.x + df.y, b=lambda x: x.a + 1),
        lambda df: df.replace(to_replace=1, value=1000),
        lambda df: df.x.replace(to_replace=1, value=1000),
        lambda df: df.isna(),
        lambda df: isna(df),
        lambda df: isna(df.x),
        lambda df: df.x.isna(),
        lambda df: df.isnull(),
        lambda df: df.x.isnull(),
        lambda df: df.mask(df.x == 10, 42),
        lambda df: df.mask(df.x == 10),
        lambda df: df.mask(lambda df: df.x % 2 == 0, 42),
        lambda df: df.mask(df.x == 10, df + 2),
        lambda df: df.mask(df.x == 10, lambda df: df + 2),
        lambda df: df.x.mask(df.x == 10, 42),
        lambda df: df.abs(),
        lambda df: df.x.abs(),
        lambda df: df.where(df.x == 10, 42),
        lambda df: df.where(df.x == 10),
        lambda df: df.where(lambda df: df.x % 2 == 0, 42),
        lambda df: df.where(df.x == 10, df + 2),
        lambda df: df.where(df.x == 10, lambda df: df + 2),
        lambda df: df.x.where(df.x == 10, 42),
        lambda df: df.rename(columns={"x": "xx"}),
        lambda df: df.rename(columns={"x": "xx"}).xx,
        lambda df: df.rename(columns={"x": "xx"})[["xx"]],
        lambda df: df.x.rename(index="hello"),
        lambda df: df.x.rename(index=df.x),
        lambda df: df.x.rename(index=("hello",)),
        lambda df: df.x.to_frame(),
        lambda df: df.drop(columns="x"),
        lambda df: df.drop(axis=1, labels=["x"]),
        lambda df: df.x.index.to_series(),
        lambda df: df.x.index.to_frame(),
        lambda df: df.x.index.to_series(name="abc"),
        lambda df: df.x.index.to_frame(name="abc"),
        lambda df: df.eval("z=x+y"),
        lambda df: df.select_dtypes(include="integer"),
        lambda df: df.add_prefix(prefix="2_"),
        lambda df: df.add_suffix(suffix="_2"),
        lambda df: df.query("x > 10"),
    ],
)
def test_blockwise(func, pdf, df):
    assert_eq(func(pdf), func(df))


def test_add_prefix():
    df = lib.DataFrame({"x": [1, 2, 3, 4, 5], "y": [4, 5, 6, 7, 8]})
    ddf = from_pandas(df, npartitions=2)
    assert_eq(ddf.add_prefix("abc"), df.add_prefix("abc"))
    assert_eq(ddf.x.add_prefix("abc"), df.x.add_prefix("abc"))


def test_add_suffix():
    df = lib.DataFrame({"x": [1, 2, 3, 4, 5], "y": [4, 5, 6, 7, 8]})
    ddf = from_pandas(df, npartitions=2)
    assert_eq(ddf.add_suffix("abc"), df.add_suffix("abc"))
    assert_eq(ddf.x.add_suffix("abc"), df.x.add_suffix("abc"))


def test_rename(pdf, df):
    q = df.x.rename({1: 2})
    assert q.divisions[0] is None
    assert_eq(q, pdf.x.rename({1: 2}))

    q = df.x.rename(lambda x: x)
    assert q.divisions[0] is None
    assert_eq(q, pdf.x.rename(lambda x: x))

    q = df.x.rename({1: 2}, sorted_index=True)
    assert q.divisions[0] is not None
    assert_eq(q, pdf.x.rename({1: 2}))

    q = df.x.rename(lambda x: x, sorted_index=True)
    assert q.divisions[0] is not None
    assert_eq(q, pdf.x.rename(lambda x: x))

    with pytest.raises(ValueError, match="non-monotonic"):
        df.x.rename({0: 200}, sorted_index=True).divisions


def test_isna(pdf):
    pdf.iloc[list(range(0, len(pdf), 2)), 0] = np.nan
    df = from_pandas(pdf, npartitions=10)
    assert_eq(isna(df.x), lib.isna(pdf.x))
    assert_eq(isna(df), lib.isna(pdf))
    assert_eq(isna(pdf.x), lib.isna(pdf.x))


def test_abs_errors():
    df = lib.DataFrame(
        {
            "A": [1, -2, 3, -4, 5],
            "C": ["a", "b", "c", "d", "e"],
        }
    )
    ddf = from_pandas(df, npartitions=2)
    pytest.raises((TypeError, NotImplementedError, ValueError), lambda: ddf.C.abs())
    pytest.raises((TypeError, NotImplementedError), lambda: ddf.abs())


def test_to_datetime():
    pdf = lib.DataFrame({"year": [2015, 2016], "month": [2, 3], "day": [4, 5]})
    df = from_pandas(pdf, npartitions=2)
    expected = lib.to_datetime(pdf)
    result = to_datetime(df)
    assert_eq(result, expected)

    ps = lib.Series(["2018-10-26 12:00:00", "2018-10-26 13:00:15"])
    ds = from_pandas(ps, npartitions=2)
    expected = lib.to_datetime(ps)
    result = to_datetime(ds)
    assert_eq(result, expected)

    with pytest.raises(TypeError, match="arg must be a Series or a DataFrame"):
        to_datetime(1490195805)


def test_to_numeric(pdf, df):
    pdf.x = pdf.x.astype("str")
    expected = lib.to_numeric(pdf.x)
    df.x = df.x.astype("str")
    result = to_numeric(df.x)
    assert_eq(result, expected)

    with pytest.raises(TypeError, match="arg must be a Series"):
        to_numeric("1.0")


def test_to_timedelta(pdf, df):
    expected = lib.to_timedelta(pdf.x)
    result = to_timedelta(df.x)
    assert_eq(result, expected)

    with pytest.raises(TypeError, match="arg must be a Series"):
        to_timedelta("1.0")


def test_drop_not_implemented(pdf, df):
    msg = "Drop currently only works for axis=1 or when columns is not None"
    with pytest.raises(NotImplementedError, match=msg):
        df.drop(axis=0, labels=[0])


@xfail_gpu("func not supported by cudf")
@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.apply(lambda row, x, y=10: row * x + y, x=2),
        lambda df: df.index.map(lambda x: x + 1),
        pytest.param(
            lambda df: df.map(lambda x: x + 1),
            marks=pytest.mark.skipif(
                not PANDAS_GE_210, reason="Only available from 2.1"
            ),
        ),
        lambda df: df.combine_first(df),
        lambda df: df.x.combine_first(df.y),
    ],
)
def test_blockwise_pandas_only(func, pdf, df):
    assert_eq(func(pdf), func(df))


def test_map_meta(pdf, df):
    expected = pdf.x.map(lambda x: x + 1)
    result = df.x.map(lambda x: x + 1, meta=expected.iloc[:0])
    assert_eq(result, expected)

    result = df.x.map(df.x + 1, meta=expected.iloc[:0])
    assert_eq(result, expected)

    result = df.x.map(df.x + 1, meta=("x", "int64"))
    assert_eq(result, expected)

    result = df.x.map((df.x + 1).compute(), meta=("x", "int64"))
    assert_eq(result, expected)

    pdf.index.name = "a"
    df = from_pandas(pdf, npartitions=10)
    result = df.x.map(lambda x: x + 1, meta=("x", "int64"))
    assert_eq(result, expected)


def test_simplify_add_suffix_add_prefix(df, pdf):
    result = df.add_prefix("2_")["2_x"].simplify()
    expected = df[["x"]].simplify().add_prefix("2_")["2_x"]
    assert result._name == expected._name
    assert_eq(result, pdf.add_prefix("2_")["2_x"])

    result = df.add_suffix("_2")["x_2"].simplify()
    expected = df[["x"]].simplify().add_suffix("_2")["x_2"]
    assert result._name == expected._name
    assert_eq(result, pdf.add_suffix("_2")["x_2"])


@xfail_gpu("rename_axis not supported by cudf")
def test_rename_axis(pdf):
    pdf.index.name = "a"
    pdf.columns.name = "b"
    df = from_pandas(pdf, npartitions=10)
    assert_eq(df.rename_axis(index="dummy"), pdf.rename_axis(index="dummy"))
    assert_eq(df.rename_axis(columns="dummy"), pdf.rename_axis(columns="dummy"))
    assert_eq(df.x.rename_axis(index="dummy"), pdf.x.rename_axis(index="dummy"))


def test_series_name(pdf, df):
    pser = pdf.x
    ser = df.x
    assert ser.name == pser.name
    ser.name = "y"
    assert ser.name == "y"


def test_isin(df, pdf):
    values = [1, 2]
    assert_eq(pdf.isin(values), df.isin(values))
    assert_eq(pdf.x.isin(values), df.x.isin(values))


def test_round(pdf):
    pdf += 0.5555
    df = from_pandas(pdf)
    assert_eq(df.round(decimals=1), pdf.round(decimals=1))
    assert_eq(df.x.round(decimals=1), pdf.x.round(decimals=1))


def test_repr(df):
    assert "+ 1" in str(df + 1)
    assert "+ 1" in repr(df + 1)

    s = (df["x"] + 1).sum(skipna=False).expr
    assert '["x"]' in str(s) or "['x']" in str(s)
    assert "+ 1" in str(s)
    assert "sum(skipna=False)" in str(s)


@xfail_gpu("combine_first not supported by cudf")
def test_combine_first_simplify(pdf):
    df = from_pandas(pdf)
    pdf2 = pdf.rename(columns={"y": "z"})
    df2 = from_pandas(pdf2)

    q = df.combine_first(df2)[["z", "y"]]
    result = q.simplify()
    expected = df[["y"]].simplify().combine_first(df2[["z"]].simplify())[["z", "y"]]
    assert result._name == expected._name
    assert_eq(result, pdf.combine_first(pdf2)[["z", "y"]])


def test_rename_traverse_filter(df):
    result = df.rename(columns={"x": "xx"})[["xx"]].simplify()
    expected = df[["x"]].simplify().rename(columns={"x": "xx"})
    assert str(result) == str(expected)


def test_columns_traverse_filters(pdf):
    pdf = pdf.copy()
    pdf["z"] = pdf["x"]
    df = from_pandas(pdf, npartitions=10)
    result = df[df.x > 5].y.optimize(fuse=False)
    df_opt = df[["x", "y"]].simplify()
    expected = df_opt.y[df_opt.x > 5]

    assert result._name == expected._name


def test_clip_traverse_filters(df):
    result = df.clip(lower=10).y.simplify()
    expected = df.y.simplify().clip(lower=10)

    assert result._name == expected._name

    result = df.clip(lower=10)[["x", "y"]].simplify()
    expected = df.clip(lower=10)

    assert result._name == expected._name

    arg = df.clip(lower=10)[["x"]]
    result = arg.simplify()
    expected = df[["x"]].simplify().clip(lower=10)

    assert result._name == expected._name


@pytest.mark.parametrize("projection", ["zz", ["zz"], ["zz", "x"], "zz"])
@pytest.mark.parametrize("subset", ["x", ["x"]])
def test_drop_duplicates_subset_simplify(pdf, subset, projection):
    pdf["zz"] = 1
    df = from_pandas(pdf)
    result = df.drop_duplicates(subset=subset)[projection].simplify()
    expected = df[["x", "zz"]].simplify().drop_duplicates(subset=subset)[projection]

    assert str(result) == str(expected)


def test_rename_columns():
    # Multi-index columns
    pdf = lib.DataFrame({("A", "0"): [1, 2, 2, 3], ("B", 1): [1, 2, 3, 4]})
    df = from_pandas(pdf, npartitions=2)

    df.columns = ["x", "y"]
    pdf.columns = ["x", "y"]
    lib.testing.assert_index_equal(df.columns, lib.Index(["x", "y"]))
    lib.testing.assert_index_equal(df._meta.columns, lib.Index(["x", "y"]))


def test_columns_named_divisions_and_meta():
    df = lib.DataFrame(
        {"_meta": [1, 2, 3, 4], "divisions": ["a", "b", "c", "d"]},
        index=[0, 1, 3, 5],
    )
    ddf = from_pandas(df, npartitions=2)

    assert ddf.divisions == (0, 3, 5)
    assert_eq(ddf["divisions"], df.divisions)
    assert all(ddf._meta.columns == ["_meta", "divisions"])
    assert_eq(ddf["_meta"], df._meta)


def test_broadcast(pdf, df):
    assert_eq(
        df + df.sum(),
        pdf + pdf.sum(),
    )
    assert_eq(
        df.x + df.x.sum(),
        pdf.x + pdf.x.sum(),
    )


def test_persist(pdf, df):
    a = df + 2
    b = a.persist()

    assert_eq(a, b)
    assert len(a.__dask_graph__()) > len(b.__dask_graph__())

    assert len(b.__dask_graph__()) == b.npartitions

    assert_eq(b.y.sum(), (pdf + 2).y.sum())


def test_index(pdf, df):
    assert_eq(df.index, pdf.index)
    assert_eq(df.x.index, pdf.x.index)
    df.index = df.index.astype("float64")
    pdf.index = pdf.index.astype("float64")
    assert_eq(df, pdf)
    with pytest.raises(AssertionError, match="aligned"):
        df.index = df.repartition(npartitions=2).index


@pytest.mark.parametrize("drop", [True, False])
def test_reset_index(pdf, df, drop):
    assert_eq(df.reset_index(drop=drop), pdf.reset_index(drop=drop), check_index=False)
    assert_eq(
        df.x.reset_index(drop=drop), pdf.x.reset_index(drop=drop), check_index=False
    )


def test_head(pdf, df):
    assert_eq(df.head(compute=False), pdf.head())
    assert_eq(df.head(compute=False, n=7), pdf.head(n=7))

    assert df.head(compute=False).npartitions == 1
    assert_eq(df.index.head(compute=False, n=7), pdf.index[0:7])


def test_head_down(df):
    result = (df.x + df.y + 1).head(compute=False)
    optimized = result.simplify()

    assert_eq(result, optimized)

    assert not isinstance(optimized.expr, expr.Head)


def test_head_head(df):
    a = df.head(compute=False).head(compute=False)
    b = df.head(compute=False)

    assert a.optimize()._name == b.optimize()._name


def test_head_npartitions(df):
    full = df.compute()
    length = len(full)

    assert_eq(df.head(5, npartitions=2), full.head(5))
    assert_eq(df.head(5, npartitions=2, compute=False), full.head(5))
    assert_eq(df.head(5, npartitions=-1), full.head(5))
    assert_eq(df.head(7, npartitions=-1), full.head(7))
    assert_eq(df.head(2, npartitions=-1), full.head(2))
    with pytest.raises(ValueError):
        df.head(2, npartitions=length + 1)


def test_head_tail_repartition(df):
    q = df.head(compute=False).repartition(npartitions=1).optimize()
    expected = df.head(compute=False).optimize()
    assert q._name == expected._name

    q = df.tail(compute=False).repartition(npartitions=1).optimize()
    expected = df.tail(compute=False).optimize()
    assert q._name == expected._name


def test_tail(pdf, df):
    assert_eq(df.tail(compute=False), pdf.tail())
    assert_eq(df.tail(compute=False, n=7), pdf.tail(n=7))

    assert df.tail(compute=False).npartitions == 1
    assert_eq(df.index.tail(compute=False, n=7), pdf.index[-7:])


def test_tail_down(df):
    result = (df.x + df.y + 1).tail(compute=False)
    optimized = optimize(result)

    assert_eq(result, optimized)

    assert not isinstance(optimized.expr, expr.Tail)


def test_tail_tail(df):
    a = df.tail(compute=False).tail(compute=False)
    b = df.tail(compute=False)

    assert a.optimize()._name == b.optimize()._name


def test_tail_repartition(df):
    a = df.repartition(npartitions=10).tail()
    b = df.tail()
    assert_eq(a, b)


def test_projection_stacking(df):
    result = df[["x", "y"]]["x"]
    optimized = result.simplify()
    expected = df["x"].simplify()

    assert optimized._name == expected._name


def test_projection_stacking_coercion(pdf):
    df = from_pandas(pdf)
    assert_eq(df.x[0], pdf.x[0], check_divisions=False)
    assert_eq(df.x[[0]], pdf.x[[0]], check_divisions=False)


def test_projection_pushdown_dim_0(pdf, df):
    result = (df[["x"]] + df["x"].sum(skipna=False))["x"]
    expected = (pdf[["x"]] + pdf["x"].sum(skipna=False))["x"]
    assert_eq(result, expected)
    assert_eq(result.optimize(), expected)


def test_remove_unnecessary_projections(df):
    result = (df + 1)[df.columns]
    optimized = result.simplify()
    expected = df + 1

    assert optimized._name == expected._name

    result = (df[["x"]] + 1)[["x"]]
    optimized = result.simplify()
    expected = df[["x"]].simplify() + 1

    assert optimized._name == expected._name


def test_substitute():
    pdf = lib.DataFrame(
        {
            "a": range(100),
            "b": range(100),
            "c": range(100),
        }
    )
    df = from_pandas(pdf, npartitions=3)
    df = df.expr

    result = (df + 1).substitute(1, 2)
    expected = df + 2
    assert result._name == expected._name

    result = df["a"].substitute(df["a"], df["b"])
    expected = df["b"]
    assert result._name == expected._name

    result = (df["a"] - df["b"]).substitute(df["b"], df["c"])
    expected = df["a"] - df["c"]
    assert result._name == expected._name

    result = df["a"].substitute(3, 4)
    expected = from_pandas(pdf, npartitions=4).a
    assert result._name == expected._name

    result = (df["a"].sum() + 5).substitute(df["a"], df["b"]).substitute(5, 6)
    expected = df["b"].sum() + 6
    assert result._name == expected._name


def test_substitute_parameters(df):
    pdf = lib.DataFrame(
        {
            "a": range(100),
            "b": range(100),
            "c": range(100),
            "index": range(100, 0, -1),
        }
    )
    df = from_pandas(pdf, npartitions=3, sort=True)

    result = df.substitute_parameters({"sort": False})
    assert result._name != df._name
    assert result._name == from_pandas(pdf, npartitions=3, sort=False)._name

    result = df.substitute_parameters({"npartitions": 2, "sort": False})
    assert result._name != df._name
    assert result._name == from_pandas(pdf, npartitions=2, sort=False)._name

    df2 = df[["a", "b"]]
    result = df2.substitute_parameters({"columns": "c"})
    assert result._name != df2._name
    assert result._name == df["c"]._name


def test_from_pandas(pdf):
    df = from_pandas(pdf, npartitions=3)
    assert df.npartitions == 3
    assert "pandas" in df._name


def test_copy(df):
    original = df.copy()
    columns = tuple(original.columns)

    df["z"] = df.x + df.y

    assert tuple(original.columns) == columns
    assert "z" not in original.columns


def test_partitions(pdf, df):
    assert_eq(df.partitions[0], pdf.iloc[:10])
    assert_eq(df.partitions[1], pdf.iloc[10:20])
    assert_eq(df.partitions[1:3], pdf.iloc[10:30])
    assert_eq(df.partitions[[3, 4]], pdf.iloc[30:50])
    assert_eq(df.partitions[-1], pdf.iloc[90:])

    out = (df + 1).partitions[0].simplify()
    assert isinstance(out.expr, expr.Add)
    assert out.expr.left._partitions == [0]

    # Check culling
    out = optimize(df.partitions[1])
    assert len(out.dask) == 1
    assert_eq(out, pdf.iloc[10:20])


def test_get_partition(pdf, df):
    assert_eq(df.get_partition(0), pdf.iloc[:10])
    assert_eq(df.get_partition(1), pdf.iloc[10:20])
    assert_eq(df.get_partition(-1), pdf.iloc[90:])
    assert_eq(df.x.get_partition(0), pdf.x.iloc[:10])


def test_column_getattr(df):
    df = df.expr
    assert df.x._name == df["x"]._name

    with pytest.raises(AttributeError):
        df.foo


def test_serialization(pdf, df):
    before = pickle.dumps(df)

    assert len(before) < 200 + len(pickle.dumps(pdf))

    part = df.partitions[0].compute()
    assert (
        len(pickle.dumps(df.__dask_graph__()))
        < 1000 + len(pickle.dumps(part)) * df.npartitions
    )

    after = pickle.dumps(df)

    assert before == after  # caching doesn't affect serialization

    assert pickle.loads(before)._name == pickle.loads(after)._name
    assert_eq(pickle.loads(before), pickle.loads(after))


@xfail_gpu("Cannot apply lambda function in cudf")
def test_size_optimized(df):
    expr = (df.x + 1).apply(lambda x: x).size
    out = optimize(expr)
    expected = optimize(df.x.size)
    assert out._name == expected._name

    expr = (df + 1).apply(lambda x: x).size
    out = optimize(expr)
    expected = optimize(df.size)
    assert out._name == expected._name


def test_apply_infer_columns():
    df = lib.DataFrame({"x": [1, 2, 3, 4], "y": [10, 20, 30, 40]})
    ddf = from_pandas(df, npartitions=2)

    def return_df(x):
        return lib.Series([x.sum(), x.mean()], index=["sum", "mean"])

    result = ddf.apply(return_df, axis=1)
    assert_eq(result.columns, lib.Index(["sum", "mean"]))
    assert_eq(result, df.apply(return_df, axis=1))


@pytest.mark.parametrize("fuse", [True, False])
def test_tree_repr(fuse):
    s = from_pandas(lib.Series(range(10))).expr.tree_repr()
    assert ("<pandas>" in s) or ("<series>" in s)

    df = timeseries()
    expr = ((df.x + 1).sum(skipna=False) + df.y.mean()).expr

    # Check result before optimization
    s = expr.tree_repr()
    assert "Sum:" in s
    assert "Add:" in s
    assert "Mean:" in s
    assert "AlignPartitions:" not in s
    assert str(df.seed) in s.lower()

    # Check result after optimization
    optimized = expr.optimize(fuse=fuse)
    s = str(optimized.tree_repr())
    assert "Sum(Chunk):" in s
    assert "Sum(TreeReduce): split_every=False" in s
    assert "Add:" in s
    assert "Mean:" not in s
    assert "AlignPartitions:" not in s
    assert "True" not in s
    assert "None" not in s
    assert "skipna=False" in s
    assert str(df.seed) in s.lower()
    if fuse:
        assert "Fused" in s
        assert "|" in s


def test_simple_graphs(df):
    expr = (df + 1).expr
    graph = expr.__dask_graph__()

    assert graph[(expr._name, 0)] == (operator.add, (df.expr._name, 0), 1)


def test_values():
    from dask.array.utils import assert_eq

    pdf = lib.DataFrame(
        {"x": ["a", "b", "c", "d"], "y": [2, 3, 4, 5]},
        index=lib.Index([1.0, 2.0, 3.0, 4.0], name="ind"),
    )

    df = from_pandas(pdf, 2)

    assert_eq(df.values, pdf.values)
    assert_eq(df.x.values, pdf.x.values)
    assert_eq(df.y.values, pdf.y.values)
    assert_eq(df.index.values, pdf.index.values)


def test_depth(df):
    assert df._depth() == 1
    assert (df + 1)._depth() == 2
    assert ((df.x + 1) + df.y)._depth() == 4


def test_partitions_nested(df):
    a = expr.Partitions(expr.Partitions(df.expr, [2, 4, 6]), [0, 2])
    b = expr.Partitions(df.expr, [2, 6])

    assert a.optimize()._name == b.optimize()._name


@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("npartitions", [7, 12])
def test_repartition_npartitions(pdf, npartitions, sort):
    df = from_pandas(pdf, sort=sort) + 1
    df2 = df.repartition(npartitions=npartitions)
    assert df2.npartitions == npartitions
    assert_eq(df, df2)


@pytest.mark.parametrize("opt", [True, False])
def test_repartition_divisions(df, opt):
    end = df.divisions[-1] + 100
    stride = end // (df.npartitions + 2)
    divisions = tuple(range(0, end, stride))
    df2 = (df + 1).repartition(divisions=divisions, force=True)["x"]
    df2 = optimize(df2) if opt else df2
    assert df2.divisions == divisions
    assert_eq((df + 1)["x"], df2)

    # Check partitions
    for p, part in enumerate(dask.compute(list(df2.index.partitions))[0]):
        if len(part):
            assert part.min() >= df2.divisions[p]
            assert part.max() < df2.divisions[p + 1]


def test_repartition_no_op(df):
    result = df.repartition(divisions=df.divisions).optimize()
    assert result._name == df._name


def test_repartition_partition_size(df):
    df2 = df.repartition(partition_size="0.25kb")
    assert df2.npartitions == 20
    assert_eq(df, df2, check_divisions=False)
    assert all(div is None for div in df2.divisions)

    df2 = df.repartition(partition_size="1kb")
    assert df2.npartitions == 4
    assert_eq(df, df2)
    assert all(div is not None for div in df2.divisions)


def test_len(df, pdf):
    df2 = df[["x"]] + 1
    assert len(df2) == len(pdf)

    assert len(df[df.x > 5]) == len(pdf[pdf.x > 5])

    first = df2.partitions[0].compute()
    assert len(df2.partitions[0]) == len(first)

    assert isinstance(Len(df2.expr).optimize(), expr.Literal)
    assert isinstance(expr.Lengths(df2.expr).optimize(), expr.Literal)


def test_astype_simplify(df, pdf):
    q = df.astype({"x": "float64", "y": "float64"})["x"]
    result = q.simplify()
    expected = df["x"].simplify().astype({"x": "float64"})
    assert result._name == expected._name
    assert_eq(q, pdf.astype({"x": "float64", "y": "float64"})["x"])

    q = df.astype({"y": "float64"})["x"]
    result = q.simplify()
    expected = df["x"].simplify()
    assert result._name == expected._name

    q = df.astype("float64")["x"]
    result = q.simplify()
    expected = df["x"].simplify().astype("float64")
    assert result._name == expected._name


@pytest.mark.parametrize("split_out", [1, True])
def test_drop_duplicates(df, pdf, split_out):
    with dask.config.set({"dataframe.shuffle.method": "tasks"}):
        assert_eq(
            df.drop_duplicates(split_out=split_out),
            pdf.drop_duplicates(),
            check_index=split_out is not True,
        )
        assert_eq(
            df.drop_duplicates(ignore_index=True, split_out=split_out),
            pdf.drop_duplicates(ignore_index=True),
            check_index=split_out is not True,
        )
        assert_eq(
            df.drop_duplicates(subset=["y"], split_out=split_out),
            pdf.drop_duplicates(subset=["y"]),
            check_index=split_out is not True,
        )
        assert_eq(
            df.y.drop_duplicates(split_out=split_out),
            pdf.y.drop_duplicates(),
            check_index=split_out is not True,
        )

        actual = df.set_index("y").index.drop_duplicates(split_out=split_out)
        if split_out is True:
            actual = actual.compute().sort_values()  # shuffle is unordered
        assert_eq(
            actual,
            pdf.set_index("y").index.drop_duplicates(),
        )

    with pytest.raises(KeyError, match="'a'"):
        df.drop_duplicates(subset=["a"], split_out=split_out)

    with pytest.raises(TypeError, match="got an unexpected keyword argument"):
        df.x.drop_duplicates(subset=["a"], split_out=split_out)


def test_drop_duplicates_split_out(df, pdf):
    q = df.drop_duplicates(subset=["x"])
    assert len(list(q.optimize().find_operations(Shuffle))) > 0
    assert_eq(q, pdf.drop_duplicates(subset=["x"]))

    q = df.x.drop_duplicates()
    assert len(list(q.optimize().find_operations(Shuffle))) > 0
    assert_eq(q, pdf.x.drop_duplicates())


def test_walk(df):
    df2 = df[df["x"] > 1][["y"]] + 1
    assert all(isinstance(ex, expr.Expr) for ex in df2.walk())
    exprs = {e._name for e in set(df2.walk())}
    assert df.expr._name in exprs
    assert df["x"].expr._name in exprs
    assert (df["x"] > 1).expr._name in exprs
    assert 1 not in exprs


def test_find_operations(df):
    df2 = df[df["x"] > 1][["y"]] + 1

    filters = list(df2.find_operations(expr.Filter))
    assert len(filters) == 1

    projections = list(df2.find_operations(expr.Projection))
    assert len(projections) == 2

    adds = list(df2.find_operations(expr.Add))
    assert len(adds) == 1
    assert next(iter(adds))._name == df2._name

    both = list(df2.find_operations((expr.Add, expr.Filter)))
    assert len(both) == 2


@pytest.mark.parametrize("subset", ["x", ["x"]])
def test_dropna_simplify(pdf, subset):
    pdf["z"] = 1
    df = from_pandas(pdf)
    q = df.dropna(subset=subset)["y"]
    result = q.simplify()
    expected = df[["x", "y"]].simplify().dropna(subset=subset)["y"]
    assert result._name == expected._name
    assert_eq(q, pdf.dropna(subset=subset)["y"])


def test_series_slice_getitem(df, pdf):
    with pytest.raises(NotImplementedError):
        df.x[:4]


def test_dir(df):
    assert all(c in dir(df) for c in df.columns)
    assert "sum" in dir(df)
    assert "sum" in dir(df.x)
    assert "sum" in dir(df.index)


@pytest.mark.parametrize(
    "func, args",
    [
        ("replace", (1, 2)),
        ("isin", ([1, 2],)),
        ("clip", (0, 5)),
        ("isna", ()),
        ("round", ()),
        ("abs", ()),
        ("fillna", ({"x": 1})),
        # ("map", (lambda x: x+1, )),  # add in when pandas 2.1 is out
    ],
)
@pytest.mark.parametrize("indexer", ["x", ["x"]])
def test_simplify_up_blockwise(df, pdf, func, args, indexer):
    q = getattr(df, func)(*args)[indexer]
    result = q.simplify()
    expected = getattr(df[indexer].simplify(), func)(*args)
    assert result._name == expected._name

    assert_eq(q, getattr(pdf, func)(*args)[indexer])

    q = getattr(df, func)(*args)[["x", "y"]]
    result = q.simplify()
    expected = getattr(df.simplify(), func)(*args)
    assert result._name == expected._name


def test_isin_as_predicate(df, pdf):
    result = df[df.x.isin([10])]
    assert_eq(result, pdf[pdf.x.isin([10])])


def test_sample(df):
    result = df.sample(frac=0.5)

    assert_eq(result, result)

    result = df.sample(frac=0.5, random_state=1234)
    expected = df.sample(frac=0.5, random_state=1234)
    assert_eq(result, expected)


@xfail_gpu("align not supported by cudf")
def test_align(df, pdf):
    result_1, result_2 = df.align(df)
    pdf_result_1, pdf_result_2 = pdf.align(pdf)
    assert_eq(result_1, pdf_result_1)
    assert_eq(result_2, pdf_result_2)

    result_1, result_2 = df.x.align(df.x)
    pdf_result_1, pdf_result_2 = pdf.x.align(pdf.x)
    assert_eq(result_1, pdf_result_1)
    assert_eq(result_2, pdf_result_2)


@xfail_gpu("align not supported by cudf")
def test_align_different_partitions():
    pdf = lib.DataFrame({"a": [11, 12, 31, 1, 2, 3], "b": [1, 2, 3, 4, 5, 6]})
    df = from_pandas(pdf, npartitions=2)
    pdf2 = lib.DataFrame(
        {"a": [11, 12, 31, 1, 2, 3], "b": [1, 2, 3, 4, 5, 6]},
        index=[-2, -1, 0, 1, 2, 3],
    )
    df2 = from_pandas(pdf2, npartitions=2)
    result_1, result_2 = df.align(df2)
    pdf_result_1, pdf_result_2 = pdf.align(pdf2)
    assert_eq(result_1, pdf_result_1)
    assert_eq(result_2, pdf_result_2)


@xfail_gpu("align not supported by cudf")
def test_align_unknown_partitions_same_root():
    pdf = lib.DataFrame({"a": 1}, index=[3, 2, 1])
    df = from_pandas(pdf, npartitions=2, sort=False)
    result_1, result_2 = df.align(df)
    pdf_result_1, pdf_result_2 = pdf.align(pdf)
    assert_eq(result_1, pdf_result_1)
    assert_eq(result_2, pdf_result_2)


@xfail_gpu(reason="align not supported by cudf")
def test_unknown_partitions_different_root():
    pdf = lib.DataFrame({"a": 1}, index=[3, 2, 1])
    df = from_pandas(pdf, npartitions=2, sort=False)
    pdf2 = lib.DataFrame({"a": 1}, index=[4, 3, 2, 1])
    df2 = from_pandas(pdf2, npartitions=2, sort=False)
    with pytest.raises(ValueError, match="Not all divisions"):
        df.align(df2)


@pytest.mark.parametrize("dropna", [False, True])
def test_nunique(pdf, dropna):
    pdf["z"] = pdf.y.astype(float)
    pdf.loc[9:12, "z"] = np.nan  # Spans two partitions
    df = from_pandas(pdf, npartitions=10)

    assert_eq(
        df.nunique(dropna=dropna),
        pdf.nunique(dropna=dropna),
    )
    assert_eq(
        df.nunique(axis=1, dropna=dropna),
        pdf.nunique(axis=1, dropna=dropna),
    )
    assert_eq(
        df.z.nunique(dropna=dropna),
        pdf.z.nunique(dropna=dropna),
    )
    assert_eq(
        df.set_index("z").index.nunique(dropna=dropna),
        pdf.set_index("z").index.nunique(dropna=dropna),
    )


@xfail_gpu("compute_hll_array doesn't work for cudf")
def test_nunique_approx(df, pdf):
    actual = df.nunique_approx().compute()
    assert 99 < actual < 101

    actual = df.y.nunique_approx().compute()
    expect = pdf.y.nunique()
    assert expect * 0.99 < actual < expect * 1.01

    actual = df.set_index("y").index.nunique_approx().compute()
    expect = pdf.set_index("y").index.nunique()
    assert expect * 0.99 < actual < expect * 1.01


def test_memory_usage_per_partition(df):
    expected = lib.Series(part.compute().memory_usage().sum() for part in df.partitions)
    result = df.memory_usage_per_partition()
    assert_eq(expected, result, check_index=False)

    expected = lib.Series(part.x.compute().memory_usage() for part in df.partitions)
    result = df.x.memory_usage_per_partition()
    assert_eq(expected, result, check_index=False)


def test_assign_simplify(pdf):
    df = from_pandas(pdf)
    df2 = from_pandas(pdf)
    df["new"] = df.x > 1
    result = df[["x", "new"]].simplify()
    expected = df2[["x"]].assign(new=df2.x > 1).simplify()
    assert result._name == expected._name

    pdf["new"] = pdf.x > 1
    assert_eq(pdf[["x", "new"]], result)


def test_assign_simplify_new_column_not_needed(pdf):
    df = from_pandas(pdf)
    df2 = from_pandas(pdf)
    df["new"] = df.x > 1
    result = df[["x"]].simplify()
    expected = df2[["x"]].simplify()
    assert result._name == expected._name

    pdf["new"] = pdf.x > 1
    assert_eq(result, pdf[["x"]])


def test_assign_simplify_series(pdf):
    df = from_pandas(pdf)
    df2 = from_pandas(pdf)
    df["new"] = df.x > 1
    result = df.new.simplify()
    expected = df2[[]].assign(new=df2.x > 1).new.simplify()
    assert result._name == expected._name


@xfail_gpu("assign function not supported by cudf")
def test_assign_non_series_inputs(df, pdf):
    assert_eq(df.assign(a=lambda x: x.x * 2), pdf.assign(a=lambda x: x.x * 2))
    assert_eq(df.assign(a=2), pdf.assign(a=2))
    assert_eq(df.assign(a=df.x.sum()), pdf.assign(a=pdf.x.sum()))

    assert_eq(df.assign(a=lambda x: x.x * 2).y, pdf.assign(a=lambda x: x.x * 2).y)
    assert_eq(df.assign(a=lambda x: x.x * 2).a, pdf.assign(a=lambda x: x.x * 2).a)


def test_are_co_aligned(pdf, df):
    df2 = df.reset_index()
    assert are_co_aligned(df.expr, df2.expr)
    assert are_co_aligned(df.expr, df2.sum().expr)
    assert not are_co_aligned(df.expr, df2.repartition(npartitions=2).expr)

    assert are_co_aligned(df.expr, df.sum().expr)
    assert are_co_aligned((df + df.sum()).expr, df.sum().expr)

    pdf = pdf.assign(z=1)
    df3 = from_pandas(pdf, npartitions=10)
    assert not are_co_aligned(df.expr, df3.expr)
    assert are_co_aligned(df.expr, df3.sum().expr)

    merged = df.merge(df2)
    merged_first = merged.reset_index()
    merged_second = merged.rename(columns={"x": "a"})
    assert are_co_aligned(merged_first.expr, merged_second.expr)
    assert not are_co_aligned(merged_first.expr, df.expr)


def test_assign_different_roots():
    pdf = lib.DataFrame(
        list(range(100)), index=list(range(1000, 0, -10)), columns=["x"]
    )
    pdf2 = lib.DataFrame(list(range(100)), index=list(range(100, 0, -1)), columns=["x"])
    df = from_pandas(pdf, npartitions=10, sort=False)
    df2 = from_pandas(pdf2, npartitions=10, sort=False)

    with pytest.raises(NotImplementedError, match="different base"):
        df["new"] = df2.x


def test_assign_pandas_inputs(df, pdf):
    assert_eq(df.assign(a=pdf.x), pdf.assign(a=pdf.x))


@xfail_gpu()
def test_astype_categories(df):
    result = df.astype("category")
    assert_eq(result.x._meta.cat.categories, lib.Index([UNKNOWN_CATEGORIES]))
    assert_eq(result.y._meta.cat.categories, lib.Index([UNKNOWN_CATEGORIES]))


def test_drop_simplify(df):
    q = df.drop(columns=["x"])[["y"]]
    result = q.simplify()
    expected = df[["y"]].simplify()
    assert result._name == expected._name


def test_op_align():
    pdf = lib.DataFrame({"x": [1, 2, 3], "y": 1})
    df = from_pandas(pdf, npartitions=2)

    pdf2 = lib.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": 1})
    df2 = from_pandas(pdf2, npartitions=2)

    assert_eq(df - df2, pdf - pdf2)


def test_can_co_align(df, pdf):
    q = (df.x + df.y).optimize(fuse=False)
    assert_eq(q, pdf.x + pdf.y)

    pdf["z"] = 100
    df2 = from_pandas(pdf, npartitions=df.npartitions * 2)
    assert df.npartitions != df2.npartitions
    assert_eq(df.x + df.y, pdf.x + pdf.y)


def test_avoid_alignment():
    from dask_expr._align import AlignPartitions

    a = lib.DataFrame({"x": range(100)})
    da = from_pandas(a, npartitions=4)

    b = lib.DataFrame({"y": range(100)})
    b["z"] = b.y * 2
    db = from_pandas(b, npartitions=3)

    # Give correct results even when misaligned
    assert_eq(a.x + b.y, da.x + db.y)

    assert not any(isinstance(ex, AlignPartitions) for ex in (db.y + db.z).walk())
    assert not any(isinstance(ex, AlignPartitions) for ex in (da.x + db.y.sum()).walk())


def test_len_shuffle_repartition(df, pdf):
    df2 = df.set_index("x")
    assert isinstance(Len(df2.expr).optimize(), expr.Literal)
    result = len(df2)
    assert result == len(pdf.set_index("x"))

    df2 = df.repartition(npartitions=3)
    assert isinstance(Len(df2.expr).optimize(), expr.Literal)
    result = len(df2)
    assert result == len(df)

    df2 = df.shuffle("x")
    assert isinstance(Len(df2.expr).optimize(), expr.Literal)
    result = len(df2)
    assert result == len(df)


def test_columns_setter(df, pdf):
    df.columns = ["a", "b"]
    result = df[["a"]]
    pdf.columns = ["a", "b"]
    expecetd = pdf[["a"]]
    assert_eq(result, expecetd)

    with pytest.raises(ValueError, match="Length mismatch"):
        df.columns = [1, 2, 3]


def test_filter_pushdown(df, pdf):
    indexer = df.x > 5
    result = df.replace(1, 5)[indexer].optimize(fuse=False)
    expected = df[indexer].replace(1, 5)
    assert result._name == expected._name

    # Don't do anything here
    df = df.replace(1, 5)
    result = df[df.x > 5].optimize(fuse=False)
    expected = df[df.x > 5]
    assert result._name == expected._name

    pdf["z"] = 1
    df = from_pandas(pdf, npartitions=10)
    df2 = df.replace(1, 5)
    result = df2[df2.x > 5][["x", "y"]].optimize(fuse=False)
    df_opt = df[["x", "y"]].simplify().replace(1, 5)
    expected = df_opt[df_opt.x > 5]
    assert result._name == expected._name


def test_shape(df, pdf):
    result = df.shape
    assert result[0]._name == (df.size / 2)._name
    assert assert_eq(result[0], pdf.shape[0])
    assert result[1] == pdf.shape[1]

    result = df.x.shape
    assert result[0]._name == (df.x.size)._name
    assert assert_eq(result[0], pdf.shape[0])

    result = df[[]].shape
    assert assert_eq(result[0], pdf[[]].shape[0])
    assert assert_eq(result[1], pdf[[]].shape[1])


def test_size(df, pdf):
    assert_eq(df.size, pdf.size)


def test_drop_duplicates_groupby(pdf):
    pdf["z"] = 1
    df = from_pandas(pdf, npartitions=10)
    df = df.drop_duplicates(subset="x")
    query = df.groupby("y").z.count()
    expected = pdf.drop_duplicates(subset="x").groupby("y").z.count()
    assert_eq(query, expected)


def test_expression_bool_raises(df):
    with pytest.raises(ValueError, match="The truth value"):
        bool(df.expr)


def test_expr_is_scalar(df):
    assert not is_scalar(df.expr)
    with pytest.raises(ValueError, match="The truth value"):
        df.expr.x in df.expr.columns  # noqa: B015


def test_replace_filtered_combine_similar():
    from dask_expr._expr import Filter, Replace

    pdf = lib.DataFrame({"a": [1, 2, 3, 4, 5, 6], "b": 1, "c": 2})

    df = from_pandas(pdf.copy(), npartitions=2)
    df = df[df.a > 3]
    df = df.replace(5, 4)
    df["new"] = df.a + df.b

    pdf = pdf[pdf.a > 3]
    pdf = pdf.replace(5, 4)
    pdf["new"] = pdf.a + pdf.b

    df = df.optimize(fuse=False)
    assert_eq(df, pdf)  # Check result

    # Check that Replace expressions always operate on
    # Filter expressions (and not the other way around)
    similar = list(df.find_operations(Replace))
    assert all(isinstance(op.frame, Filter) for op in similar)


def test_quantile_frame(df, pdf):
    assert_eq(df.quantile(), lib.Series([49.0, 7.0], index=["x", "y"], name=0.5))
    assert df.quantile().divisions == ("x", "y")
    assert_eq(df.quantile(q=[0.5]), lib.DataFrame({"x": [49.0], "y": 7.0}, index=[0.5]))
    assert_eq(
        df.quantile(q=[0.5], numeric_only=True),
        lib.DataFrame({"x": [49.0], "y": 7.0}, index=[0.5]),
    )

    pdf["z"] = 1
    df = from_pandas(pdf, npartitions=10)
    q = df.quantile(q=[0.5])[["x", "z"]]
    assert q.optimize()._name == df[["x", "z"]].quantile(q=[0.5]).optimize()._name

    q = df.quantile(q=0.5)[["x", "z"]]
    assert (
        q.optimize()._name
        == df[["x", "z"]].quantile(q=0.5)[["x", "z"]].optimize()._name
    )


def test_quantile(df):
    assert_eq(df.x.quantile(), 49.0)
    assert_eq(df.x.quantile(method="dask"), 49.0)
    assert_eq(
        df.x.quantile(q=[0.2, 0.8]),
        lib.Series([19.0, 79.0], index=[0.2, 0.8], name="x"),
    )
    assert_eq(
        df.x.index.quantile(q=[0.2, 0.8]),
        lib.Series([19.0, 79.0], index=[0.2, 0.8]),
    )

    with pytest.raises(AssertionError):
        df.x.quantile(q=[]).compute()

    ser = from_pandas(lib.Series(["a", "b", "c"]), npartitions=2)
    with pytest.raises(TypeError, match="on non-numeric"):
        ser.quantile()


def test_dtype(df, pdf):
    assert df.x.dtype == pdf.x.dtype
    assert df.index.dtype == pdf.index.dtype
    assert_eq(df.dtypes, pdf.dtypes)


def test_isnull():
    pdf = lib.DataFrame(
        {
            "A": range(10),
            "B": [None] * 10,
            "C": [1] * 4 + [None] * 4 + [2] * 2,
        }
    )
    df = from_pandas(pdf, npartitions=2)
    assert_eq(df.notnull(), pdf.notnull())
    assert_eq(df.isnull(), pdf.isnull())
    for c in pdf.columns:
        assert_eq(df[c].isnull(), pdf[c].isnull())


def test_scalar_to_series():
    sc = from_pandas(lib.Series([1])).sum()
    ss1 = sc.to_series()
    ss2 = sc.to_series("xxx")
    assert_eq(ss1, lib.Series([1]))
    assert_eq(ss2, lib.Series([1], index=["xxx"]))


def test_keys(df, pdf):
    assert_eq(df.keys(), pdf.keys())  # Alias for DataFrame.columns
    assert_eq(df.x.keys(), pdf.x.keys())  # Alias for Series.index


@pytest.mark.parametrize("data_freq, divs1", [("B", False), ("D", True), ("h", True)])
def test_shift_with_freq_datetime(pdf, data_freq, divs1):
    pdf.index = lib.date_range(start="2020-01-01", periods=len(pdf), freq=data_freq)
    df = from_pandas(pdf, npartitions=4)
    for freq, divs2 in [("s", True), ("W", False), (lib.Timedelta(10, unit="h"), True)]:
        for d, p in [(df, pdf), (df.x, pdf.x)]:
            res = d.shift(2, freq=freq)
            assert_eq(res, p.shift(2, freq=freq))
            assert res.known_divisions == divs2

    res = df.index.shift(2)
    assert_eq(res, df.index.shift(2))
    assert res.known_divisions == divs1


@pytest.mark.parametrize("data_freq,divs", [("D", True), ("h", True)])
def test_shift_with_freq_period_index(pdf, data_freq, divs):
    pdf.index = lib.period_range(start="2020-01-01", periods=len(pdf), freq=data_freq)
    df = from_pandas(pdf, npartitions=4)
    for d, p in [(df, pdf), (df.x, pdf.x)]:
        res = d.shift(2, freq=data_freq)
        assert_eq(res, p.shift(2, freq=data_freq))
        assert res.known_divisions == divs
    res = df.index.shift(2)
    assert_eq(res, df.index.shift(2))
    assert res.known_divisions == divs

    with pytest.raises(TypeError, match="argument is not supported"):
        df.index.shift(2, freq="D")


@pytest.mark.parametrize("data_freq", ["min", "D", "h"])
def test_shift_with_freq_TimedeltaIndex(pdf, data_freq):
    pdf.index = lib.timedelta_range("1 day", periods=len(pdf), freq=data_freq)
    df = from_pandas(pdf, npartitions=4)
    for freq in ["s", lib.Timedelta(10, unit="h")]:
        for d, p in [(df, pdf), (df.x, pdf.x), (df.index, pdf.index)]:
            res = d.shift(2, freq=freq)
            assert_eq(res, p.shift(2, freq=freq))
            assert res.known_divisions
    # Index shifts also work with freq=None
    res = df.index.shift(2)
    assert_eq(res, df.index.shift(2))
    assert res.known_divisions


def test_iter(df, pdf):
    assert_eq(list(df), list(pdf))  # column names


def test_items(df, pdf):
    expect = list(pdf.items())
    actual = list(df.items())
    assert len(expect) == len(actual)
    for (expect_name, expect_col), (actual_name, actual_col) in zip(expect, actual):
        assert expect_name == actual_name
        assert_eq(expect_col, actual_col)


def test_index_index(df):
    with pytest.raises(NotImplementedError, match="has no"):
        df.index.index


def test_axes(df, pdf):
    assert len(df.axes) == len(pdf.axes)
    [assert_eq(d, p) for d, p in zip(df.axes, pdf.axes)]
    assert len(df.x.axes) == len(pdf.x.axes)
    assert_eq(df.x.axes[0], pdf.x.axes[0])
