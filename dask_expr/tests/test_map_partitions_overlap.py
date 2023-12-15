import pytest
from dask.dataframe._compat import PANDAS_GE_200

from dask_expr import from_pandas, map_partitions
from dask_expr.tests._util import _backend_library, assert_eq

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


def test_map_partitions(df):
    def combine_x_y(x, y, foo=None):
        assert foo == "bar"
        return x + y

    df2 = df.map_partitions(combine_x_y, df + 1, foo="bar")
    assert_eq(df2, df + (df + 1))

    df2 = map_partitions(combine_x_y, df, df + 1, foo="bar")
    assert_eq(df2, df + (df + 1))


def test_map_partitions_broadcast(df):
    def combine_x_y(x, y, val, foo=None):
        assert foo == "bar"
        return x + y + val

    df2 = df.map_partitions(combine_x_y, df["x"].sum(), 123, foo="bar")
    assert_eq(df2, df + df["x"].sum() + 123)
    assert_eq(df2.optimize(), df + df["x"].sum() + 123)


@pytest.mark.parametrize("opt", [True, False])
def test_map_partitions_merge(opt):
    # Make simple left & right dfs
    pdf1 = lib.DataFrame({"x": range(20), "y": range(20)})
    df1 = from_pandas(pdf1, 2)
    pdf2 = lib.DataFrame({"x": range(0, 20, 2), "z": range(10)})
    df2 = from_pandas(pdf2, 1)

    # Partition-wise merge with map_partitions
    df3 = df1.map_partitions(
        lambda l, r: l.merge(r, on="x"),
        df2,
        enforce_metadata=False,
        clear_divisions=True,
    )

    # Check result with/without fusion
    expect = pdf1.merge(pdf2, on="x")
    df3 = (df3.optimize() if opt else df3)[list(expect.columns)]
    if not PANDAS_GE_200:
        df3 = df3.reset_index(drop=True)
    assert_eq(df3, expect, check_index=False)


def test_map_overlap():
    def func(x):
        x = x + x.sum()
        return x

    idx = lib.date_range("2020-01-01", periods=5, freq="D")
    pdf = lib.DataFrame(1, index=idx, columns=["a"])
    df = from_pandas(pdf, npartitions=2)

    result = df.map_overlap(func, before=0, after="2D")
    expected = lib.DataFrame([5, 5, 5, 3, 3], index=idx, columns=["a"])
    assert_eq(result, expected)
    result = df.map_overlap(func, before=0, after=1)
    assert_eq(result, expected)

    # Bug in dask/dask
    # result = df.map_overlap(func, before=0, after="1D")
    # expected = lib.DataFrame([4, 4, 4, 3, 3], index=idx, columns=["a"])
    # assert_eq(result, expected)

    result = df.map_overlap(func, before="2D", after=0)
    expected = lib.DataFrame(4, index=idx, columns=["a"])
    assert_eq(result, expected, check_index=False)

    result = df.map_overlap(func, before=1, after=0)
    assert_eq(result, expected, check_index=False)


def test_map_overlap_raises():
    def func(x):
        x = x + x.sum()
        return x

    idx = lib.date_range("2020-01-01", periods=5, freq="D")
    pdf = lib.DataFrame(1, index=idx, columns=["a"])
    df = from_pandas(pdf, npartitions=2)

    with pytest.raises(NotImplementedError, match="is less than"):
        df.map_overlap(func, before=5, after=0).compute()

    with pytest.raises(NotImplementedError, match="is less than"):
        df.map_overlap(func, before=0, after=5).compute()

    with pytest.raises(NotImplementedError, match="is less than"):
        df.map_overlap(func, before="5D", after=0).compute()

    with pytest.raises(NotImplementedError, match="is less than"):
        df.map_overlap(func, before=0, after="5D").compute()

    with pytest.raises(ValueError, match="positive"):
        df.map_overlap(func, before=-1, after=5).compute()

    with pytest.raises(ValueError, match="positive"):
        df.map_overlap(func, before=1, after=-5).compute()


@pytest.mark.parametrize("npartitions", [1, 4])
def test_map_overlap(npartitions, pdf, df):
    def shifted_sum(df, before, after, c=0):
        a = df.shift(before)
        b = df.shift(-after)
        return df + a + b + c

    for before, after in [(0, 3), (3, 0), (3, 3), (0, 0)]:
        # DataFrame
        res = df.map_overlap(shifted_sum, before, after, before, after, c=2)
        sol = shifted_sum(pdf, before, after, c=2)
        assert_eq(res, sol)

        # Series
        res = df.x.map_overlap(shifted_sum, before, after, before, after, c=2)
        sol = shifted_sum(pdf.x, before, after, c=2)
        assert_eq(res, sol)
