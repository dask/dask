import numpy as np
import pandas as pd
import pytest
from dask.dataframe import assert_eq

from dask_expr import concat, from_pandas


@pytest.fixture
def pdf():
    pdf = pd.DataFrame({"x": range(100)})
    pdf["y"] = pdf.x * 10.0
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=10)


def test_concat(pdf, df):
    result = concat([df, df])
    expected = pd.concat([pdf, pdf])
    assert_eq(result, expected)
    assert all(div is None for div in result.divisions)


def test_concat_pdf(pdf, df):
    result = concat([df, pdf])
    expected = pd.concat([pdf, pdf])
    assert_eq(result, expected)
    assert all(div is None for div in result.divisions)


def test_concat_divisions(pdf, df):
    pdf2 = pdf.set_index(np.arange(200, 300))
    df2 = from_pandas(pdf2, npartitions=10)
    result = concat([df, df2])
    expected = pd.concat([pdf, pdf2])
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
    expected = pd.concat([pdf])
    assert_eq(result, expected)
    assert not any(div is None for div in result.divisions)


def test_concat_one_no_columns(df, pdf):
    result = concat([df, df[[]]])
    expected = pd.concat([pdf, pdf[[]]])
    assert_eq(result, expected)
