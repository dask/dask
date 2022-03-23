import string

import numpy as np
import pandas as pd
from packaging.version import parse as parse_version

PANDAS_VERSION = parse_version(pd.__version__)
PANDAS_GT_104 = PANDAS_VERSION >= parse_version("1.0.4")
PANDAS_GT_110 = PANDAS_VERSION >= parse_version("1.1.0")
PANDAS_GT_120 = PANDAS_VERSION >= parse_version("1.2.0")
PANDAS_GT_121 = PANDAS_VERSION >= parse_version("1.2.1")
PANDAS_GT_130 = PANDAS_VERSION >= parse_version("1.3.0")
PANDAS_GT_131 = PANDAS_VERSION >= parse_version("1.3.1")
PANDAS_GT_133 = PANDAS_VERSION >= parse_version("1.3.3")
PANDAS_GT_140 = PANDAS_VERSION >= parse_version("1.4.0")
# FIXME: Using `.release` below as versions like `1.5.0.dev0+268.gbe8d1ec880`
# are less than `1.5.0` with `packaging.version`. Update to use `parse_version("1.5.0")`
# below once `pandas=1.5.0` is released
PANDAS_GT_150 = PANDAS_VERSION.release >= (1, 5, 0)

import pandas.testing as tm


def assert_categorical_equal(left, right, *args, **kwargs):
    tm.assert_extension_array_equal(left, right, *args, **kwargs)
    assert pd.api.types.is_categorical_dtype(
        left.dtype
    ), f"{left} is not categorical dtype"
    assert pd.api.types.is_categorical_dtype(
        right.dtype
    ), f"{right} is not categorical dtype"


def assert_numpy_array_equal(left, right):
    left_na = pd.isna(left)
    right_na = pd.isna(right)
    np.testing.assert_array_equal(left_na, right_na)

    left_valid = left[~left_na]
    right_valid = right[~right_na]
    np.testing.assert_array_equal(left_valid, right_valid)


def makeDataFrame():
    data = np.random.randn(30, 4)
    index = list(string.ascii_letters)[:30]
    return pd.DataFrame(data, index=index, columns=list("ABCD"))


def makeTimeDataFrame():
    data = makeDataFrame()
    data.index = makeDateIndex()
    return data


def makeTimeSeries():
    return makeTimeDataFrame()["A"]


def makeDateIndex(k=30, freq="B"):
    return pd.date_range("2000", periods=k, freq=freq)


def makeTimedeltaIndex(k=30, freq="D"):
    return pd.timedelta_range("1 day", periods=k, freq=freq)


def makeMissingDataframe():
    df = makeDataFrame()
    data = df.values
    data = np.where(data > 1, np.nan, data)
    return pd.DataFrame(data, index=df.index, columns=df.columns)


def makeMixedDataFrame():
    df = pd.DataFrame(
        {
            "A": [0.0, 1, 2, 3, 4],
            "B": [0.0, 1, 0, 1, 0],
            "C": [f"foo{i}" for i in range(5)],
            "D": pd.date_range("2009-01-01", periods=5),
        }
    )
    return df
