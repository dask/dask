import numpy as np
import pytest

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library, assert_eq

# Set DataFrame backend for this module
lib = _backend_library()


@pytest.fixture
def pdf():
    pdf = lib.DataFrame(
        {
            "x": [None, 0, 1, 2, 3, 4] * 2,
            "ts": [
                lib.Timestamp("2017-05-09 00:00:00.006000"),
                lib.Timestamp("2017-05-09 00:00:00.006000"),
                lib.Timestamp("2017-05-09 07:56:23.858694"),
                lib.Timestamp("2017-05-09 05:59:58.938999"),
                None,
                None,
            ]
            * 2,
            "td": [
                np.timedelta64(3, "D"),
                np.timedelta64(1, "D"),
                None,
                None,
                np.timedelta64(3, "D"),
                np.timedelta64(1, "D"),
            ]
            * 2,
            "y": "a",
        }
    )
    yield pdf


@pytest.fixture
def df(pdf):
    yield from_pandas(pdf, npartitions=2)


def test_describe_series(df, pdf):
    assert_eq(df.x.describe(), pdf.x.describe())
    assert_eq(df.y.describe(), pdf.y.describe())
    assert_eq(df.ts.describe(), pdf.ts.describe().drop("mean"))
    assert_eq(df.td.describe(), pdf.td.describe())
