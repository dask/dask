import pandas as pd

import dask.operation.dataframe as opdd
from dask.dataframe.utils import assert_eq


class MyIOFunc:
    def __init__(self, columns=None):
        self.columns = columns

    def project_columns(self, columns):
        return MyIOFunc(columns)

    def __call__(self, t):
        size = t[0] + 1
        x = t[1]
        df = pd.DataFrame({"A": [x] * size, "B": [10] * size})
        if self.columns is None:
            return df
        return df[self.columns]


def test_creation_operation_from_map():

    ddf = opdd.from_map(
        MyIOFunc(),
        enumerate([0, 1, 2]),
        label="myfunc",
        enforce_metadata=True,
    )

    expect = pd.DataFrame(
        {
            "A": [0, 1, 1, 2, 2, 2],
            "B": [10] * 6,
        },
        index=[0, 0, 1, 0, 1, 2],
    )

    assert_eq(ddf["A"].compute(), expect["A"])
    assert_eq(ddf.compute(), expect)


def test_creation_operation_from_pandas():

    expect = pd.DataFrame(
        {
            "A": [0, 1, 1, 2, 2, 2],
            "B": [10] * 6,
        },
        index=[0, 0, 1, 0, 1, 2],
    )

    ddf = opdd.from_pandas(expect, 2)

    assert_eq(ddf["A"].compute(), expect["A"])
    assert_eq(ddf.compute(), expect)


def test_creation_fusion():
    from dask.operation.dataframe.core import optimize

    # Creation followed by partitionwise operations
    ddf = opdd.from_map(
        MyIOFunc(),
        enumerate([0]),
        label="myfunc",
        enforce_metadata=True,
    )
    ddf += 1
    ddf.assign(new=ddf["B"])

    # Materialized dict should only have a single (fused) task
    dsk = optimize(ddf.operation).dask.to_dict()
    assert len(dsk) == 1


def test_basic():
    expect = pd.DataFrame(
        {
            "A": [0, 1, 1, 2, 2, 2],
            "B": [10] * 6,
        },
        index=[0, 0, 1, 0, 1, 2],
    )
    ddf = opdd.from_pandas(expect, 2)

    expect = (expect[expect["A"] > 0][["B"]] - 5).assign(new=1)
    ddf = (ddf[ddf["A"] > 0][["B"]] - 5).assign(new=1)

    assert_eq(ddf.head(npartitions=-1), expect.head())


def test_repartition_divisions():
    expect = pd.DataFrame({"A": range(100), "B": [0, 1] * 50})
    ddf = opdd.from_pandas(expect.copy(), 5)
    expect += 1
    ddf += 1

    ddf = ddf.repartition(divisions=(0, 50, 99))
    assert ddf.divisions == (0, 50, 99)
    assert_eq(ddf.compute(), expect)
