import numpy as np
import pandas as pd
import pytest

pa = pytest.importorskip("pyarrow")

import contextlib

import dask
import dask.dataframe as dd
from dask.dataframe._compat import PANDAS_GT_140, PANDAS_GT_150
from dask.dataframe._pyarrow import (
    PYARROW_STRINGS_AVAILABLE,
    is_object_string_dataframe,
    is_object_string_dtype,
    is_object_string_index,
    is_object_string_series,
    is_pyarrow_string_dtype,
)

pytestmark = pytest.mark.skipif(
    not PYARROW_STRINGS_AVAILABLE, reason="Requires pyarrow strings"
)
pa = pytest.importorskip("pyarrow")
# NOTE: The `importorskip` below is already covered by the `not PYARROW_STRINGS_AVAILABLE`
# module-level marker above. We add the `importorskip` below because it's applied before
# markers, which makes `pytest.mark.parametrize` here easier to work with.
pytest.importorskip("pandas", minversion="1.3.0", reason="Requires pyarrow strings")


@pytest.mark.parametrize(
    "dtype,expected",
    [
        (object, False),
        (str, False),
        (np.dtype(int), False),
        (np.dtype(float), False),
        (pd.StringDtype("python"), False),
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
    ],
)
def tests_is_object_string_dataframe(series, expected):
    assert is_object_string_dataframe(series) is expected


@pytest.mark.parametrize(
    "engine,setting,expected_type",
    [
        ("pyarrow", True, pd.StringDtype(storage="pyarrow")),
        ("pyarrow", False, object),
        ("fastparquet", True, None),  # will raise an exception
        ("fastparquet", False, object),
    ],
)
def test_read_parquet_convert_string(tmpdir, engine, setting, expected_type):
    """Test that string dtypes are converted with dd.read_parquet and
    dataframe.convert_string=True"""

    df = pd.DataFrame(
        {
            "A": ["def", "abc", "ghi"],
            "B": [5, 2, 3],
        }
    )
    path = str(tmpdir.join("test.parquet"))
    df.to_parquet(path)

    ctx = contextlib.nullcontext()
    success = True
    if engine == "fastparquet" and setting:
        ctx = pytest.raises(ValueError, match="`convert_strings` is not supported")
        success = False

    with dask.config.set({"dataframe.convert_string": setting}):
        with ctx:
            ddf = dd.read_parquet(path, engine=engine)
        if success:
            assert expected_type == ddf.A.dtype
            assert ddf.A.dtype == ddf.compute().A.dtype
            # make sure we didn't convert types in __init__, the only operation
            # was read_parquet
            assert len(ddf.dask.layers) == 1
