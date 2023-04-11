from datetime import date, time

import numpy as np
import pandas as pd
import pytest
from _decimal import Decimal
from pandas.tests.extension.decimal.array import DecimalDtype

import dask.dataframe as dd
from dask.dataframe import assert_eq
from dask.dataframe._compat import PANDAS_GT_140, PANDAS_GT_150
from dask.dataframe._pyarrow import (
    is_object_string_dataframe,
    is_object_string_dtype,
    is_object_string_index,
    is_object_string_series,
    is_pyarrow_string_dtype,
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
        pytest.param(
            pa.int64(),
            False,
            marks=pytest.mark.skipif(not PANDAS_GT_150, reason="Needs pd.ArrowDtype"),
        ),
        pytest.param(
            pa.float64(),
            False,
            marks=pytest.mark.skipif(not PANDAS_GT_150, reason="Needs pd.ArrowDtype"),
        ),
        (pd.StringDtype("pyarrow"), True),
        pytest.param(
            pa.string(),
            True,
            marks=pytest.mark.skipif(not PANDAS_GT_150, reason="Needs pd.ArrowDtype"),
        ),
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
        pytest.param(
            pa.int64(),
            False,
            marks=pytest.mark.skipif(not PANDAS_GT_150, reason="Needs pd.ArrowDtype"),
        ),
        pytest.param(
            pa.float64(),
            False,
            marks=pytest.mark.skipif(not PANDAS_GT_150, reason="Needs pd.ArrowDtype"),
        ),
        (pd.StringDtype("pyarrow"), False),
        pytest.param(
            pa.string(),
            False,
            marks=pytest.mark.skipif(not PANDAS_GT_150, reason="Needs pd.ArrowDtype"),
        ),
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
        (
            pd.Index(["a", "b"], dtype="string[pyarrow]"),
            False if PANDAS_GT_140 else True,
        ),
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
            False if PANDAS_GT_140 else True,
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
            # Prior to pandas=1.4, Index couldn't contain extension dtypes
            False if PANDAS_GT_140 else True,
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
            # Prior to pandas=1.4, Index couldn't contain extension dtypes
            False if PANDAS_GT_140 else True,
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
    "data, arrow_dtype",
    [
        (["a", "b"], pa.string()),
        ([b"a", b"b"], pa.binary()),
        # Should probably fix upstream, https://github.com/pandas-dev/pandas/issues/52590
        # (["a", "b"], pa.large_string()),
        # ([b"a", b"b"], pa.large_binary()),
        ([1, 2], pa.int64()),
        ([1, 2], pa.float64()),
        ([1, 2], pa.uint64()),
        ([date(2022, 1, 1), date(1999, 12, 31)], pa.date32()),
        (
            [pd.Timestamp("2022-01-01"), pd.Timestamp("2023-01-02")],
            pa.timestamp(unit="ns"),
        ),
        ([Decimal("5"), Decimal("6.24")], pa.decimal128(10, 2)),
        ([pd.Timedelta("1 day"), pd.Timedelta("20 days")], pa.duration("ns")),
        ([time(12, 0), time(0, 12)], pa.time64("ns")),
    ],
)
def test_set_index_pyarrow_dtype(data, arrow_dtype):
    dtype = pd.ArrowDtype(arrow_dtype)
    pdf = pd.DataFrame({"a": 1, "arrow_col": pd.Series(data, dtype=dtype)})
    ddf = dd.from_pandas(pdf, npartitions=2)
    pdf_result = pdf.set_index("arrow_col")
    ddf_result = ddf.set_index("arrow_col")
    assert_eq(ddf_result, pdf_result)
