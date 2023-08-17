import numpy as np
import pytest
from dask.dataframe import assert_eq

from dask_expr import Len, concat, from_pandas
from dask_expr.tests._util import _backend_library

# Set DataFrame backend for this module
lib = _backend_library()


@pytest.fixture
def pdf():
    pdf = lib.DataFrame({"x": range(100)})
    pdf["y"] = pdf.x * 10.0
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=10)


def test_concat_str(df):
    result = str(concat([df, df], join="inner"))
    expected = "<dask_expr.expr.DataFrame: expr=Concat(frames=[df, df], join=inner)>"
    assert result == expected


def test_concat(pdf, df):
    result = concat([df, df])
    expected = lib.concat([pdf, pdf])
    assert_eq(result, expected)
    assert all(div is None for div in result.divisions)


def test_concat_pdf(pdf, df):
    result = concat([df, pdf])
    expected = lib.concat([pdf, pdf])
    assert_eq(result, expected)
    assert all(div is None for div in result.divisions)


def test_concat_divisions(pdf, df):
    pdf2 = pdf.set_index(np.arange(200, 300))
    df2 = from_pandas(pdf2, npartitions=10)
    result = concat([df, df2])
    expected = lib.concat([pdf, pdf2])
    assert_eq(result, expected)
    assert not any(div is None for div in result.divisions)


@pytest.mark.parametrize("join", ["right", "left"])
def test_invalid_joins(join):
    with pytest.raises(ValueError, match="'join' must be"):
        concat([df, df], join=join)


def test_concat_invalid():
    with pytest.raises(TypeError, match="dfs must"):
        concat(df)
    with pytest.raises(ValueError, match="No objects to"):
        concat([])


def test_concat_one_object(df, pdf):
    result = concat([df])
    expected = lib.concat([pdf])
    assert_eq(result, expected)
    assert not any(div is None for div in result.divisions)


def test_concat_one_no_columns(df, pdf):
    result = concat([df, df[[]]])
    expected = lib.concat([pdf, pdf[[]]])
    assert_eq(result, expected)


def test_concat_simplify(pdf, df):
    pdf2 = pdf.copy()
    pdf2["z"] = 1
    df2 = from_pandas(pdf2)
    q = concat([df, df2])[["z", "x"]]
    result = q.simplify()
    expected = concat([df[["x"]], df2[["x", "z"]]]).simplify()[["z", "x"]]
    assert result._name == expected._name

    assert_eq(q, lib.concat([pdf, pdf2])[["z", "x"]])


def test_concat_simplify_projection_not_added(pdf, df):
    pdf2 = pdf.copy()
    pdf2["z"] = 1
    df2 = from_pandas(pdf2)
    q = concat([df, df2])[["y", "x"]]
    result = q.simplify()
    expected = concat([df, df2[["x", "y"]]]).simplify()[["y", "x"]]
    assert result._name == expected._name

    assert_eq(q, lib.concat([pdf, pdf2])[["y", "x"]])


def test_concat_axis_one_co_aligned(pdf, df):
    df2 = df.add_suffix("_2")
    pdf2 = pdf.add_suffix("_2")
    assert_eq(concat([df, df2], axis=1), lib.concat([pdf, pdf2], axis=1))


def test_concat_axis_one_all_divisions_unknown(pdf):
    pdf = pdf.sort_values(by="x", ascending=False, ignore_index=True)
    df = from_pandas(pdf, npartitions=2, sort=False)
    pdf2 = pdf.add_suffix("_2")
    df2 = from_pandas(pdf2, npartitions=2, sort=False)
    with pytest.warns(UserWarning):
        assert_eq(concat([df, df2], axis=1), lib.concat([pdf, pdf2], axis=1))
    assert_eq(
        concat([df, df2], axis=1, ignore_unknown_divisions=True),
        lib.concat([pdf, pdf2], axis=1),
    )


def test_concat_axis_one_drop_dfs_not_selected(pdf, df):
    df2 = df.add_suffix("_2")
    pdf2 = pdf.add_suffix("_2")
    df3 = df.add_suffix("_3")
    pdf3 = pdf.add_suffix("_3")
    result = concat([df, df2, df3], axis=1)[["x", "y", "x_2"]].simplify()
    expected = concat([df, df2[["x_2"]]], axis=1).simplify()
    assert result._name == expected._name
    assert_eq(result, lib.concat([pdf, pdf2, pdf3], axis=1)[["x", "y", "x_2"]])


def test_concat_index(df, pdf):
    df2 = from_pandas(pdf, npartitions=3)
    result = concat([df, df2])
    expected = lib.concat([pdf, pdf])
    assert_eq(len(result), len(expected))

    query = Len(result.expr).optimize(fuse=False)
    expected = (0 + Len(df.expr) + Len(df2.expr)).optimize(fuse=False)
    assert query._name == expected._name
