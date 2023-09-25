import pytest
from dask.dataframe import assert_eq

from dask_expr import from_pandas
from dask_expr.tests._util import _backend_library

lib = _backend_library()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"npartitions": 2},
        {"npartitions": 4},
        {"divisions": (0, 1, 79)},
        {"partition_size": "1kb"},
    ],
)
def test_repartition_combine_similar(kwargs):
    pdf = lib.DataFrame({"x": [1, 2, 3, 4, 5, 6, 7, 8] * 10, "y": 1, "z": 2})
    df = from_pandas(pdf, npartitions=3)
    query = df.repartition(**kwargs)
    query["new"] = query.x + query.y
    result = query.optimize(fuse=False)

    expected = df.repartition(**kwargs).optimize(fuse=False)
    arg1 = expected.x
    arg2 = expected.y
    expected["new"] = arg1 + arg2
    assert result._name == expected._name

    expected_pdf = pdf.copy()
    expected_pdf["new"] = expected_pdf.x + expected_pdf.y
    assert_eq(result, expected_pdf)
