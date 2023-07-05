import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq

from dask_expr import from_pandas
from dask_expr.expr import Blockwise
from dask_expr.io import FromPandas


@pytest.fixture
def pdf():
    return pd.DataFrame({"x": list(range(20)) * 5, "y": range(100)})


@pytest.fixture
def df(pdf):
    return from_pandas(pdf, npartitions=10)


@pytest.mark.parametrize("ignore_index", [True, False])
@pytest.mark.parametrize("npartitions", [3, 6])
def test_disk_shuffle(ignore_index, npartitions, df):
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

    # Check that df was really partitioned by "x".
    # If any values of "x" can be found in multiple
    # partitions, this will fail
    df3 = df2["x"].map_partitions(lambda x: x.drop_duplicates())
    assert sorted(df3.compute().tolist()) == list(range(20))

    # Check `partitions` after shuffle
    a = df2.partitions[1]
    b = df.shuffle(
        "y",
        backend="disk",
        npartitions=npartitions,
        ignore_index=ignore_index,
    ).partitions[1]
    assert set(a["x"].compute()).issubset(b["y"].compute())

    # Check for culling
    assert len(a.optimize().dask) < len(df2.optimize().dask)


@pytest.mark.parametrize("ignore_index", [True, False])
@pytest.mark.parametrize("npartitions", [8, 12])
@pytest.mark.parametrize("max_branch", [32, 6])
def test_task_shuffle(ignore_index, npartitions, max_branch, df):
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

    # Check that df was really partitioned by "x".
    # If any values of "x" can be found in multiple
    # partitions, this will fail
    df3 = df2["x"].map_partitions(lambda x: x.drop_duplicates())
    assert sorted(df3.compute().tolist()) == list(range(20))

    # Check `partitions` after shuffle
    a = df2.partitions[1]
    b = df.shuffle(
        "y",
        backend="tasks",
        npartitions=npartitions,
        ignore_index=ignore_index,
    ).partitions[1]
    assert set(a["x"].compute()).issubset(b["y"].compute())

    # Check for culling
    assert len(a.optimize().dask) < len(df2.optimize().dask)


@pytest.mark.parametrize("npartitions", [3, 12])
@pytest.mark.parametrize("max_branch", [32, 8])
def test_task_shuffle_index(npartitions, max_branch, pdf):
    pdf = pdf.set_index("x")
    df = from_pandas(pdf, 10)

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

    # Check that df was really partitioned by "x".
    # If any values of "x" can be found in multiple
    # partitions, this will fail
    df3 = df2.index.map_partitions(lambda x: x.drop_duplicates())
    assert sorted(df3.compute().tolist()) == list(range(20))


def test_shuffle_column_projection(df):
    df2 = df.shuffle("x")[["x"]].simplify()

    assert "y" not in df2.expr.operands[0].columns


def test_shuffle_reductions(df):
    assert df.shuffle("x").sum().optimize()._name == df.sum()._name


@pytest.mark.xfail(reason="Shuffle can't see the reduction through the Projection")
def test_shuffle_reductions_after_projection(df):
    assert df.shuffle("x").y.sum().optimize()._name == df.y.sum()._name


def test_set_index(df, pdf):
    assert_eq(df.set_index("x"), pdf.set_index("x"))
    assert_eq(df.set_index(df.x), pdf.set_index(pdf.x))

    with pytest.raises(TypeError, match="can't be of type DataFrame"):
        df.set_index(df)


def test_set_index_pre_sorted(pdf):
    pdf = pdf.sort_values(by="y", ignore_index=True)
    df = from_pandas(pdf, npartitions=10)
    q = df.set_index("y")
    assert_eq(q, pdf.set_index("y"))
    result = q.simplify().expr
    assert all(isinstance(expr, (Blockwise, FromPandas)) for expr in result.walk())
    q = df.set_index(df.y)
    assert_eq(q, pdf.set_index(pdf.y))
    result = q.simplify().expr
    assert all(isinstance(expr, (Blockwise, FromPandas)) for expr in result.walk())
