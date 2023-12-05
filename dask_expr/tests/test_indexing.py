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

    assert_eq(df.loc[df.y == 20, "x"], pdf.loc[pdf.y == 20, "x"])
    assert_eq(df.loc[df.y == 20, ["x"]], pdf.loc[pdf.y == 20, ["x"]])


def test_loc_errors(df):
    with pytest.raises(NotImplementedError):
        df.loc[1, "x"]
    with pytest.raises(ValueError, match="Too many"):
        df.iloc[(1, 2, 3)]
