import pytest

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library, assert_eq

lib = _backend_library()


@pytest.fixture
def pdf():
    pdf = lib.DataFrame({"x": range(20)})
    pdf["y"] = pdf.x * 10.0
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=4)


def test_iloc(df, pdf):
    assert_eq(df.iloc[:, 1], pdf.iloc[:, 1])
    assert_eq(df.iloc[:, [1]], pdf.iloc[:, [1]])
    assert_eq(df.iloc[:, [0, 1]], pdf.iloc[:, [0, 1]])
    assert_eq(df.iloc[:, []], pdf.iloc[:, []])


def test_iloc_errors(df):
    with pytest.raises(NotImplementedError):
        df.iloc[1]
    with pytest.raises(NotImplementedError):
        df.iloc[1, 1]
    with pytest.raises(ValueError, match="Too many"):
        df.iloc[(1, 2, 3)]


def test_loc(df, pdf):
    assert_eq(df.loc[:, "x"], pdf.loc[:, "x"])
    assert_eq(df.loc[:, ["x"]], pdf.loc[:, ["x"]])
    assert_eq(df.loc[:, []], pdf.loc[:, []])

    assert_eq(df.loc[df.y == 20, "x"], pdf.loc[pdf.y == 20, "x"])
    assert_eq(df.loc[df.y == 20, ["x"]], pdf.loc[pdf.y == 20, ["x"]])


def test_loc_errors(df):
    with pytest.raises(NotImplementedError):
        df.loc[1, "x"]
    with pytest.raises(ValueError, match="Too many"):
        df.iloc[(1, 2, 3)]


def test_loc_slice(pdf):
    pdf.columns = [10, 20]
    # FIXME can't just update df.columns; see
    #       https://github.com/dask-contrib/dask-expr/issues/485
    df = from_pandas(pdf, npartitions=10)

    assert_eq(df.loc[:, :15], pdf.loc[:, :15])
    assert_eq(df.loc[:, 15:], pdf.loc[:, 15:])
    assert_eq(df.loc[:, 25:], pdf.loc[:, 25:])  # no columns
    assert_eq(df.loc[:, ::-1], pdf.loc[:, ::-1])


def test_iloc_slice(df, pdf):
    assert_eq(df.iloc[:, :1], pdf.iloc[:, :1])
    assert_eq(df.iloc[:, 1:], pdf.iloc[:, 1:])
    assert_eq(df.iloc[:, 99:], pdf.iloc[:, 99:])  # no columns
    assert_eq(df.iloc[:, ::-1], pdf.iloc[:, ::-1])


@pytest.mark.parametrize("loc", [False, True])
@pytest.mark.parametrize(
    "update",
    [
        False,
        pytest.param(
            True,
            marks=pytest.mark.xfail(
                reason="https://github.com/dask-contrib/dask-expr/issues/485"
            ),
        ),
    ],
)
def test_columns_dtype_on_empty_slice(df, pdf, loc, update):
    pdf.columns = [10, 20]
    if update:
        df.columns = [10, 20]
    else:
        df = from_pandas(pdf, npartitions=10)

    assert df.columns.dtype == pdf.columns.dtype
    assert df.compute().columns.dtype == pdf.columns.dtype
    assert_eq(df, pdf)

    if loc:
        df = df.loc[:, []]
        pdf = pdf.loc[:, []]
    else:
        df = df[[]]
        pdf = pdf[[]]

    assert df.columns.dtype == pdf.columns.dtype
    assert df.compute().columns.dtype == pdf.columns.dtype
    assert_eq(df, pdf)
