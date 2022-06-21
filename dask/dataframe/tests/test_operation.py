import pandas as pd

import dask.dataframe as dd
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


def test_creation_operation():

    ddf = dd.from_map(
        MyIOFunc(),
        enumerate([0, 1, 2]),
        label="myfunc",
        enforce_metadata=True,
        use_operation_api=True,
    )

    expect = pd.DataFrame(
        {
            "A": [0, 1, 1, 2, 2, 2],
            "B": [10] * 6,
        },
        index=[0, 0, 1, 0, 1, 2],
    )

    assert_eq(ddf["A"], expect["A"])
    assert_eq(ddf, expect)


def test_creation_fusion():
    from dask.dataframe.operation import optimize

    # Creation followed by partitionwise operations
    ddf = dd.from_map(
        MyIOFunc(),
        enumerate([0]),
        label="myfunc",
        enforce_metadata=True,
        use_operation_api=True,
    )
    ddf += 1
    ddf.assign(new=ddf["B"])

    # Materialized dict should only have a single (fused) task
    dsk = optimize(ddf.operation).dask.to_dict()
    assert len(dsk) == 1
