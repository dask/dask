from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pandas.tests.extension.decimal.array import DecimalDtype

import dask.dataframe as dd
from dask.dataframe._pyarrow import (
    is_object_string_dataframe,
    is_object_string_dtype,
    is_object_string_index,
    is_object_string_series,
    is_pyarrow_string_dtype,
)
from dask.dataframe.utils import assert_eq

pa = pytest.importorskip("pyarrow")


@pytest.mark.parametrize(
    "dtype,expected",
    [
        (object, False),
        (str, False),
        (np.dtype(int), False),
        (np.dtype(float), False),
        (pd.StringDtype("python"), False),
        (DecimalDtype(), False),
        (pa.int64(), False),
        (pa.float64(), False),
        (pd.StringDtype("pyarrow"), True),
        (pa.string(), True),
    ],
)
def test_is_pyarrow_string_dtype(dtype, expected):
    if isinstance(dtype, pa.DataType):
        dtype = pd.ArrowDtype(dtype)
    assert is_pyarrow_string_dtype(dtype) is expected


@pytest.mark.parametrize(
    "dtype,expected",
    [
        (object, True),
        (str, True),
        (np.dtype(int), False),
        (np.dtype(float), False),
        (pd.StringDtype("python"), True),
        (DecimalDtype(), False),
        (pa.int64(), False),
        (pa.float64(), False),
        (pa.string(), False),
        (pd.StringDtype("pyarrow"), False),
    ],
)
def test_is_object_string_dtype(dtype, expected):
    if isinstance(dtype, pa.DataType):
        dtype = pd.ArrowDtype(dtype)
    assert is_object_string_dtype(dtype) is expected


@pytest.mark.parametrize(
    "series,expected",
    [
        # Bare object dtype without values available: conservatively
        # treated as string-like (matches pandas' `is_string_dtype(dtype)`)
        (pd.Series(["a", "b"], dtype=object), True),
        # Object column holding bools + NA (GH#12176): pandas' value-aware
        # `is_string_dtype` says this is *not* a string column
        (pd.Series([True, np.nan], dtype=object), False),
        # Object column of mixed non-string objects
        (pd.Series([1, "a"], dtype=object), False),
        # Categorical dtype also reports dtype.kind == "O", but must never be
        # treated as string-like just because its categories happen to be
        # strings -- pandas' `is_string_dtype(series)` would otherwise
        # inspect the categories and misclassify it.
        (pd.Series(["x", "y", "z"], dtype="category"), False),
    ],
)
def test_is_object_string_dtype_value_aware(series, expected):
    # When the actual data (`obj`) is supplied, is_object_string_dtype should
    # defer to pandas' value-aware classification instead of assuming any
    # bare `object` dtype is string-like.
    assert is_object_string_dtype(series.dtype, series) is expected


def test_from_pandas_does_not_convert_non_string_object_column():
    # Regression test for https://github.com/dask/dask/issues/12176
    # An object column containing booleans (and NA) must not be coerced to
    # a pyarrow string dtype -- pandas itself would never call this a
    # string column. The conversion decision is made once from the full
    # in-memory frame (see `FromPandas._pyarrow_string_dtypes`), so `_meta`
    # and the computed result must agree.
    pdf = pd.DataFrame(
        {
            "c0": [0.0, np.nan, 0.0, 1.0],
            "c1": pd.array([True, True, None, None], dtype=object),
        }
    )
    ddf = dd.from_pandas(pdf, npartitions=2)

    assert ddf._meta["c1"].dtype == object
    result = ddf.compute()
    assert result["c1"].dtype == object
    assert_eq(ddf, pdf)


@pytest.mark.parametrize(
    "index,expected",
    [
        (pd.Index(["a", "b"], dtype=object), True),
        (pd.Index(["a", "b"], dtype="string[python]"), True),
        # Prior to pandas=1.4, Index couldn't contain extension dtypes
        (pd.Index(["a", "b"], dtype="string[pyarrow]"), False),
        (pd.Index([1, 2], dtype=int), False),
        (pd.Index([1, 2], dtype=float), False),
        (pd.Series(["a", "b"], dtype=object), False),
        (
            pd.MultiIndex.from_arrays(
                [
                    pd.Index(["a", "a"], dtype="string[pyarrow]"),
                    pd.Index(["a", "b"], dtype=object),
                ]
            ),
            True,
        ),
        # Prior to pandas=1.4, Index couldn't contain extension dtypes
        (
            pd.MultiIndex.from_arrays(
                [
                    pd.Index(["a", "a"], dtype="string[pyarrow]"),
                    pd.Index(["a", "b"], dtype="string[pyarrow]"),
                ]
            ),
            False,
        ),
        (
            pd.MultiIndex.from_arrays(
                [pd.Index(["a", "a"], dtype=object), pd.Index([1, 2], dtype=int)]
            ),
            True,
        ),
        (
            pd.MultiIndex.from_arrays(
                [pd.Index([1, 1], dtype=int), pd.Index([1, 2], dtype=float)]
            ),
            False,
        ),
    ],
)
def test_is_object_string_index(index, expected):
    assert is_object_string_index(index) is expected


@pytest.mark.parametrize(
    "series,expected",
    [
        (pd.Series(["a", "b"], dtype=object), True),
        (pd.Series(["a", "b"], dtype="string[python]"), True),
        (pd.Series(["a", "b"], dtype="string[pyarrow]"), False),
        (pd.Series([1, 2], dtype=int), False),
        (pd.Series([1, 2], dtype=float), False),
        (
            pd.Series([1, 2], dtype=float, index=pd.Index(["a", "b"], dtype=object)),
            True,
        ),
        (
            pd.Series(
                [1, 2], dtype=float, index=pd.Index(["a", "b"], dtype="string[pyarrow]")
            ),
            False,
        ),
        (pd.Index(["a", "b"], dtype=object), False),
    ],
)
def test_is_object_string_series(series, expected):
    assert is_object_string_series(series) is expected


@pytest.mark.parametrize(
    "series,expected",
    [
        (pd.DataFrame({"x": ["a", "b"]}, dtype=object), True),
        (pd.DataFrame({"x": ["a", "b"]}, dtype="string[python]"), True),
        (pd.DataFrame({"x": ["a", "b"]}, dtype="string[pyarrow]"), False),
        (pd.DataFrame({"x": [1, 2]}, dtype=int), False),
        (pd.DataFrame({"x": [1, 2]}, dtype=float), False),
        (
            pd.DataFrame(
                {"x": [1, 2]}, dtype=float, index=pd.Index(["a", "b"], dtype=object)
            ),
            True,
        ),
        (
            pd.DataFrame(
                {"x": [1, 2]},
                dtype=float,
                index=pd.Index(["a", "b"], dtype="string[pyarrow]"),
            ),
            False,
        ),
        (pd.Series({"x": ["a", "b"]}, dtype=object), False),
        (pd.Index({"x": ["a", "b"]}, dtype=object), False),
        (
            pd.MultiIndex.from_arrays(
                [pd.Index(["a", "a"], dtype=object), pd.Index(["a", "b"], dtype=object)]
            ),
            False,
        ),
        (
            pd.MultiIndex.from_arrays(
                [
                    pd.Index(["a", "a"], dtype="string[python]"),
                    pd.Index(["a", "b"], dtype="string[pyarrow]"),
                ]
            ),
            False,
        ),
        (
            pd.MultiIndex.from_arrays(
                [
                    pd.Index(["a", "a"], dtype=object),
                    pd.Index(["a", "b"], dtype="string[pyarrow]"),
                ]
            ),
            False,
        ),
    ],
)
def tests_is_object_string_dataframe(series, expected):
    assert is_object_string_dataframe(series) is expected
