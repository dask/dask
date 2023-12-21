import numpy as np
import pytest

from dask_expr import DataFrame, Len, Series, concat, from_pandas
from dask_expr.tests._util import _backend_library, assert_eq

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


def test_concat_ignore_order():
    pdf1 = lib.DataFrame(
        {
            "x": lib.Categorical(
                ["a", "b", "c", "a"], categories=["a", "b", "c"], ordered=True
            )
        }
    )
    ddf1 = from_pandas(pdf1, 2)
    pdf2 = lib.DataFrame(
        {
            "x": lib.Categorical(
                ["c", "b", "a"], categories=["c", "b", "a"], ordered=True
            )
        }
    )
    ddf2 = from_pandas(pdf2, 2)
    expected = lib.concat([pdf1, pdf2])
    expected["x"] = expected["x"].astype("category")
    result = concat([ddf1, ddf2], ignore_order=True)
    assert_eq(result, expected)


def test_concat_index(df, pdf):
    df2 = from_pandas(pdf, npartitions=3)
    result = concat([df, df2])
    expected = lib.concat([pdf, pdf])
    assert_eq(len(result), len(expected))

    query = Len(result.expr).optimize(fuse=False)
    expected = (0 + Len(df.expr) + Len(df2.expr)).optimize(fuse=False)
    assert query._name == expected._name


def test_concat_one_series(df):
    c = concat([df.x], axis=0)
    assert isinstance(c, Series)

    c = concat([df.x], axis=1)
    assert isinstance(c, DataFrame)


def test_concat_dataframe_empty():
    df = lib.DataFrame({"a": [100, 200, 300]}, dtype="int64")
    empty_df = lib.DataFrame([], dtype="int64")
    df_concat = lib.concat([df, empty_df])

    ddf = from_pandas(df, npartitions=1)
    empty_ddf = from_pandas(empty_df, npartitions=1)
    ddf_concat = concat([ddf, empty_ddf])
    assert_eq(df_concat, ddf_concat)


def test_concat_after_merge():
    pdf1 = lib.DataFrame(
        {"x": range(10), "y": [1, 2, 3, 4, 5] * 2, "z": ["cat", "dog"] * 5}
    )
    pdf2 = lib.DataFrame(
        {"i": range(10), "j": [2, 3, 4, 5, 6] * 2, "k": ["bird", "dog"] * 5}
    )
    _pdf1 = pdf1[pdf1["z"] == "cat"].merge(pdf2, left_on="y", right_on="j")
    _pdf2 = pdf1[pdf1["z"] == "dog"].merge(pdf2, left_on="y", right_on="j")
    ptotal = concat([_pdf1, _pdf2])

    df1 = from_pandas(pdf1, npartitions=2)
    df2 = from_pandas(pdf2, npartitions=2)
    _df1 = df1[df1["z"] == "cat"].merge(df2, left_on="y", right_on="j")
    _df2 = df1[df1["z"] == "dog"].merge(df2, left_on="y", right_on="j")
    total = concat([_df1, _df2])

    assert_eq(total, ptotal, check_index=False)


def test_concat_series(pdf):
    pdf["z"] = 1
    df = from_pandas(pdf, npartitions=5)
    q = concat([df.y, df.x, df.z], axis=1)[["x", "y"]]
    df2 = df[["x", "y"]]
    expected = concat([df2.y, df2.x], axis=1)[["x", "y"]]
    assert q.optimize(fuse=False)._name == expected.optimize(fuse=False)._name
    assert_eq(q, lib.concat([pdf.y, pdf.x, pdf.z], axis=1)[["x", "y"]])
