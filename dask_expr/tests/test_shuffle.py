import dask
import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq

from dask_expr import from_pandas


@pytest.mark.parametrize("ignore_index", [True, False])
@pytest.mark.parametrize("npartitions", [3, 6])
def test_disk_shuffle(ignore_index, npartitions):
    pdf = pd.DataFrame({"x": list(range(20)) * 5, "y": range(100)})
    df = from_pandas(pdf, npartitions=4)
    df2 = df.shuffle(
        "x",
        backend="disk",
        npartitions=npartitions,
        ignore_index=ignore_index,
    )

    # Check that the output partition count is correct
    assert df2.npartitions == (npartitions or df.npartitions)

    # Check the computed (re-ordered) result
    assert_eq(df, df2, check_index=not ignore_index, check_divisions=False)

    # Check that df was really partitioned by "x"
    unique = []
    for part in dask.compute(list(df2["x"].partitions))[0]:
        unique.extend(part.unique().tolist())
    # If any values of "x" can be found in multiple
    # partitions, then `len(unique)` will be >20
    assert sorted(unique) == list(range(20))


@pytest.mark.parametrize("ignore_index", [True, False])
@pytest.mark.parametrize("npartitions", [3, 12])
@pytest.mark.parametrize("max_branch", [32, 8])
def test_task_shuffle(ignore_index, npartitions, max_branch):
    pdf = pd.DataFrame({"x": list(range(20)) * 5, "y": range(100)})
    df = from_pandas(pdf, npartitions=10)
    df2 = df.shuffle(
        "x",
        backend="tasks",
        npartitions=npartitions,
        ignore_index=ignore_index,
        max_branch=max_branch,
    )

    # Check that the output partition count is correct
    assert df2.npartitions == (npartitions or df.npartitions)

    # Check the computed (re-ordered) result
    assert_eq(df, df2, check_index=not ignore_index, check_divisions=False)

    # Check that df was really partitioned by "x"
    unique = []
    for part in dask.compute(list(df2["x"].partitions))[0]:
        unique.extend(part.unique().tolist())
    # If any values of "x" can be found in multiple
    # partitions, then `len(unique)` will be >20
    assert sorted(unique) == list(range(20))


@pytest.mark.parametrize("npartitions", [3, 12])
@pytest.mark.parametrize("max_branch", [32, 8])
def test_task_shuffle_index(npartitions, max_branch):
    pdf = pd.DataFrame({"x": list(range(20)) * 5, "y": range(100)}).set_index("x")
    df = from_pandas(pdf, npartitions=10)
    df2 = df.shuffle(
        "x",
        backend="tasks",
        npartitions=npartitions,
        max_branch=max_branch,
    )

    # Check that the output partition count is correct
    assert df2.npartitions == (npartitions or df.npartitions)

    # Check the computed (re-ordered) result
    assert_eq(df, df2, check_divisions=False)

    # Check that df was really partitioned by "x"
    unique = []
    for part in dask.compute(list(df2.index.partitions))[0]:
        unique.extend(part.unique().tolist())
    # If any values of "x" can be found in multiple
    # partitions, then `len(unique)` will be >20
    assert sorted(unique) == list(range(20))
