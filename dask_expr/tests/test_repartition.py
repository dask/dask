import numpy as np
import pytest

from dask_expr import Repartition, from_pandas, repartition
from dask_expr.tests._util import _backend_library, assert_eq

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


@pytest.mark.parametrize("type_ctor", [lambda o: o, tuple, list])
def test_repartition_noop(type_ctor):
    pdf = lib.DataFrame({"x": [1, 2, 4, 5], "y": [6, 7, 8, 9]}, index=[-1, 0, 2, 7])
    df = from_pandas(pdf, npartitions=2)
    ds = df.x

    def assert_not_repartitions(expr, fuse=False):
        repartitions = [
            x for x in expr.optimize(fuse=fuse).walk() if isinstance(x, Repartition)
        ]
        assert len(repartitions) == 0

    # DataFrame method
    df2 = df.repartition(divisions=type_ctor(df.divisions))
    assert_not_repartitions(df2.expr)

    # Top-level dask.dataframe method
    df3 = repartition(df, divisions=type_ctor(df.divisions))
    assert_not_repartitions(df3.expr)

    # Series method
    ds2 = ds.repartition(divisions=type_ctor(ds.divisions))
    assert_not_repartitions(ds2.expr)

    # Top-level dask.dataframe method applied to a Series
    ds3 = repartition(ds, divisions=type_ctor(ds.divisions))
    assert_not_repartitions(ds3.expr)


def test_repartition_freq():
    ts = lib.date_range("2015-01-01 00:00", "2015-05-01 23:50", freq="10min")
    pdf = lib.DataFrame(
        np.random.randint(0, 100, size=(len(ts), 4)), columns=list("ABCD"), index=ts
    )
    df = from_pandas(pdf, npartitions=1).repartition(freq="MS")

    assert_eq(df, pdf)

    assert df.divisions == (
        lib.Timestamp("2015-1-1 00:00:00"),
        lib.Timestamp("2015-2-1 00:00:00"),
        lib.Timestamp("2015-3-1 00:00:00"),
        lib.Timestamp("2015-4-1 00:00:00"),
        lib.Timestamp("2015-5-1 00:00:00"),
        lib.Timestamp("2015-5-1 23:50:00"),
    )

    assert df.npartitions == 5


def test_repartition_freq_errors():
    pdf = lib.DataFrame({"x": [1, 2, 3]})
    df = from_pandas(pdf, npartitions=1)
    with pytest.raises(TypeError, match="for timeseries"):
        df.repartition(freq="1s")


def test_repartition_empty_partitions_dtype():
    pdf = lib.DataFrame({"x": [1, 2, 3, 4, 5, 6, 7, 8]})
    df = from_pandas(pdf, npartitions=4)
    assert_eq(
        df[df.x < 5].repartition(npartitions=1),
        pdf[pdf.x < 5],
    )
