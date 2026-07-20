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
        # Plain strings, no missing values.
        (pd.Series(["a", "b"], dtype=object), True),
        # String column with missing values (`None`/`nan`): this is the
        # *primary* case `dataframe.convert-string` exists for, and it must
        # keep converting -- pandas' own `is_string_dtype(series)` actually
        # returns False here on pandas>=3 (it calls
        # `is_string_array(..., skipna=False)` internally), which is
        # exactly why dask uses its own value-aware classifier
        # (`_is_object_string_like`) instead of pandas'.
        (pd.Series(["a", None], dtype=object), True),
        (pd.Series(["a", np.nan], dtype=object), True),
        # All-NA / empty object columns: no values contradict a string
        # classification, and dask's pre-GH#12176 dtype-only check always
        # converted every `object` dtype regardless of content -- so these
        # are treated as string-like too, preserving that prior behavior.
        (pd.Series([None, None], dtype=object), True),
        (pd.Series([], dtype=object), True),
        # Object column holding bools + NA (GH#12176): must NOT convert --
        # pandas itself would never call this a string column.
        (pd.Series([True, np.nan], dtype=object), False),
        # Object column of mixed non-string objects
        (pd.Series([1, "a"], dtype=object), False),
        # Categorical dtype also reports dtype.kind == "O", but must never be
        # treated as string-like just because its categories happen to be
        # strings -- inspecting values would otherwise look at the
        # *categories* and misclassify it.
        (pd.Series(["x", "y", "z"], dtype="category"), False),
    ],
)
def test_is_object_string_dtype_value_aware(series, expected):
    # When the actual data (`obj`) is supplied, is_object_string_dtype should
    # defer to dask's own value-aware classification instead of assuming any
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


@pytest.mark.parametrize("missing", [None, np.nan])
def test_from_pandas_still_converts_string_column_with_missing_values(missing):
    # Regression test for the trap this fix could easily fall into: a
    # string column with missing values is the *primary* case
    # `dataframe.convert-string` is meant to handle, and it must keep
    # converting to `string[pyarrow]` even though pandas' own
    # `is_string_dtype(series)` returns False for it on pandas>=3 (see
    # `_is_object_string_like`'s docstring). Both `_meta` and the computed
    # partitions must agree it converted.
    pdf = pd.DataFrame({"c0": ["a", "b", "c", missing]})
    ddf = dd.from_pandas(pdf, npartitions=2)

    assert is_pyarrow_string_dtype(ddf._meta["c0"].dtype)
    result = ddf.compute()
    assert is_pyarrow_string_dtype(result["c0"].dtype)


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
