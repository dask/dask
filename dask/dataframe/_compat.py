import string
from distutils.version import LooseVersion

import numpy as np
import pandas as pd


PANDAS_VERSION = LooseVersion(pd.__version__)
PANDAS_GT_0240 = PANDAS_VERSION >= LooseVersion("0.24.0")
PANDAS_GT_0250 = PANDAS_VERSION >= LooseVersion("0.25.0")
PANDAS_GT_100 = PANDAS_VERSION >= LooseVersion("1.0.0")
HAS_INT_NA = PANDAS_GT_0240


if PANDAS_GT_100:
    import pandas.testing as tm  # noqa: F401
else:
    import pandas.util.testing as tm  # noqa: F401


def assert_categorical_equal(left, right, *args, **kwargs):
    if PANDAS_GT_100:
        tm.assert_extension_array_equal(left, right, *args, **kwargs)
        assert pd.api.types.is_categorical_dtype(
            left.dtype
        ), "{} is not categorical dtype".format(left)
        assert pd.api.types.is_categorical_dtype(
            right.dtype
        ), "{} is not categorical dtype".format(right)
    else:
        return tm.assert_categorical_equal(left, right, *args, **kwargs)


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
            "C": ["foo{}".format(i) for i in range(5)],
            "D": pd.date_range("2009-01-01", periods=5),
        }
    )
    return df
