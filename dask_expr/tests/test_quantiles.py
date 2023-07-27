from dask.dataframe import assert_eq

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library

# Set DataFrame backend for this module
lib = _backend_library()


def test_repartition_quantiles():
    pdf = lib.DataFrame({"a": [1, 2, 3, 4, 5, 15, 7, 8, 9, 10, 11], "d": 3})
    df = from_pandas(pdf, npartitions=5)
    result = df.a._repartition_quantiles(npartitions=5)
    expected = lib.Series(
        [1, 1, 3, 7, 9, 15], index=[0, 0.2, 0.4, 0.6, 0.8, 1], name="a"
    )
    assert_eq(result, expected)

    result = df.a._repartition_quantiles(npartitions=4)
    expected = lib.Series([1, 2, 5, 8, 15], index=[0, 0.25, 0.5, 0.75, 1], name="a")
    assert_eq(result, expected)
