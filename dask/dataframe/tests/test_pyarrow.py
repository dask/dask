from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pandas.tests.extension.decimal.array import DecimalDtype

from dask.dataframe._pyarrow import (
    is_object_string_dataframe,
    is_object_string_dtype,
    is_object_string_index,
    is_object_string_series,
    is_pyarrow_string_dtype,
    to_pyarrow_string,
)

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

@pytest.mark.parametrize(
    "data,should_convert",
    [
        (["a", "b", None], True),
        (["a", "b"], True),
        ([], True),
        ([None, None], True),
        (["1", 1, None], False),
        ([1, 2], False),
        ([1, "1"], False),
        ([True, False], False),
        ([True, "a"], False),
    ],
)
def test_to_pyarrow_string_skips_mixed_object(data, should_convert):
    s = pd.Series(data, dtype=object)
    result = to_pyarrow_string(s)
    if should_convert:
        assert is_pyarrow_string_dtype(result.dtype)
    else:
        assert result.dtype == object
        assert result.tolist() == s.tolist()


def test_to_pyarrow_string_preserves_pure_string_object_dataframe():
    df = pd.DataFrame(
        {
            "pure": pd.Series(["a", "b", None], dtype=object),
            "mixed": pd.Series(["1", 1, None], dtype=object),
            "ints": pd.Series([1, 2, 3], dtype=int),
        }
    )
    result = to_pyarrow_string(df)
    assert is_pyarrow_string_dtype(result["pure"].dtype)
    assert result["mixed"].dtype == object
    assert result["mixed"].tolist() == ["1", 1, None]
    assert pd.api.types.is_integer_dtype(result["ints"].dtype)
