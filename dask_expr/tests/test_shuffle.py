import pytest
from dask.dataframe.utils import assert_eq

from dask_expr import SetIndexBlockwise, from_pandas
from dask_expr._expr import Blockwise
from dask_expr.io import FromPandas
from dask_expr.tests._util import _backend_library

# Set DataFrame backend for this module
lib = _backend_library()


@pytest.fixture
def pdf():
    return lib.DataFrame({"x": list(range(20)) * 5, "y": range(100)})


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
    assert df.shuffle("x").sum().simplify()._name == df.sum()._name


@pytest.mark.xfail(reason="Shuffle can't see the reduction through the Projection")
def test_shuffle_reductions_after_projection(df):
    assert df.shuffle("x").y.sum().simplify()._name == df.y.sum()._name


@pytest.mark.parametrize("partition_size", [128e6, 128e5])
@pytest.mark.parametrize("upsample", [1.0, 2.0])
def test_set_index(df, pdf, upsample, partition_size):
    assert_eq(
        df.set_index("x", upsample=upsample, partition_size=partition_size),
        pdf.set_index("x"),
    )
    assert_eq(
        df.set_index(df.x, upsample=upsample, partition_size=partition_size),
        pdf.set_index(pdf.x),
    )

    with pytest.raises(TypeError, match="can't be of type DataFrame"):
        df.set_index(df)


def test_set_index_sorted(pdf):
    pdf = pdf.sort_values(by="y", ignore_index=True)
    pdf["z"] = pdf["x"]
    df = from_pandas(pdf, npartitions=10)
    q = df.set_index("y", sorted=True)
    assert_eq(q, pdf.set_index("y"))
    result = q.simplify().expr
    assert all(
        isinstance(expr, (SetIndexBlockwise, FromPandas)) for expr in result.walk()
    )

    q = df.set_index("y", sorted=True)["x"]
    assert_eq(q, pdf.set_index("y")["x"])
    result = q.optimize(fuse=False)
    expected = df[["x", "y"]].set_index("y", sorted=True)["x"].simplify()
    assert result._name == expected._name


def test_set_index_pre_sorted(pdf):
    pdf = pdf.sort_values(by="y", ignore_index=True)
    pdf["z"] = pdf["x"]
    df = from_pandas(pdf, npartitions=10)
    q = df.set_index("y")
    assert_eq(q, pdf.set_index("y"))
    result = q.optimize(fuse=False).expr
    assert all(isinstance(expr, (Blockwise, FromPandas)) for expr in result.walk())
    q = df.set_index(df.y)
    assert_eq(q, pdf.set_index(pdf.y))
    result = q.optimize(fuse=False).expr
    assert all(isinstance(expr, (Blockwise, FromPandas)) for expr in result.walk())

    q = df.set_index("y")["x"].optimize(fuse=False)
    expected = df[["x", "y"]].set_index("y")["x"].optimize(fuse=False)
    assert q._name == expected._name


def test_set_index_repartition(df, pdf):
    result = df.set_index("x", npartitions=2)
    assert result.npartitions == 2
    assert result.optimize(fuse=False).npartitions == 2
    assert_eq(result, pdf.set_index("x"))


def test_set_index_simplify(df, pdf):
    q = df.set_index("x")["y"].optimize(fuse=False)
    expected = df[["x", "y"]].set_index("x")["y"].optimize(fuse=False)
    assert q._name == expected._name

    q = df.set_index(df.x)["y"].optimize(fuse=False)
    expected = df[["y"]].set_index(df.x)["y"].optimize(fuse=False)
    assert q._name == expected._name


def test_set_index_without_sort(df, pdf):
    result = df.set_index("y", sort=False)
    assert_eq(result, pdf.set_index("y"))

    result = result.optimize(fuse=False)
    assert all(isinstance(ex, (FromPandas, Blockwise)) for ex in result.walk())

    result = df.set_index(df.y, sort=False)
    assert_eq(result, pdf.set_index(pdf.y))

    with pytest.raises(ValueError, match="Specifying npartitions with sort=False"):
        df.set_index("y", sort=False, npartitions=20)

    with pytest.raises(
        ValueError, match="Specifying npartitions with sort=False or sorted=True"
    ):
        df.set_index("y", sorted=True, npartitions=20)


def test_sort_values(df, pdf):
    assert_eq(df.sort_values("x"), pdf.sort_values("x"))
    assert_eq(df.sort_values("x", npartitions=2), pdf.sort_values("x"))
    pdf.iloc[5, 0] = -10
    df = from_pandas(pdf, npartitions=10)
    assert_eq(df.sort_values("x", upsample=2.0), pdf.sort_values("x"))

    with pytest.raises(NotImplementedError, match="a single boolean for ascending"):
        df.sort_values(by=["x", "y"], ascending=[True, True])
    with pytest.raises(NotImplementedError, match="sorting by named columns"):
        df.sort_values(by=1)

    with pytest.raises(ValueError, match="must be either 'first' or 'last'"):
        df.sort_values(by="x", na_position="bla")


def test_sort_values_optimize(df, pdf):
    q = df.sort_values("x")["y"].optimize(fuse=False)
    expected = df[["x", "y"]].sort_values("x")["y"].optimize(fuse=False)
    assert q._name == expected._name

    q = df.sort_values("x")["x"].optimize(fuse=False)
    expected = df[["x"]].sort_values("x")["x"].optimize(fuse=False)
    assert q._name == expected._name


def test_set_index_single_partition(pdf):
    df = from_pandas(pdf, npartitions=1)
    assert_eq(df.set_index("x"), pdf.set_index("x"))


def test_sort_values_descending(df, pdf):
    assert_eq(
        df.sort_values(by="y", ascending=False),
        pdf.sort_values(by="y", ascending=False),
        sort_results=False,
    )


def test_sort_head_nlargest(df):
    a = df.sort_values("x", ascending=False).head(10, compute=False).expr
    b = df.nlargest(10, columns=["x"]).expr
    assert a.optimize()._name == b.optimize()._name

    a = df.sort_values("x", ascending=True).head(10, compute=False).expr
    b = df.nsmallest(10, columns=["x"]).expr
    assert a.optimize()._name == b.optimize()._name

    a = df.sort_values("x", ascending=False).tail(10, compute=False).expr
    b = df.nsmallest(10, columns=["x"]).expr
    assert a.optimize()._name == b.optimize()._name

    a = df.sort_values("x", ascending=True).tail(10, compute=False).expr
    b = df.nlargest(10, columns=["x"]).expr
    assert a.optimize()._name == b.optimize()._name


def test_set_index_head_nlargest(df, pdf):
    a = df.set_index("x").head(10, compute=False).expr
    b = df.nsmallest(10, columns="x").set_index("x").expr
    assert a.optimize()._name == b.optimize()._name

    a = df.set_index("x").tail(10, compute=False).expr
    b = df.nlargest(10, columns="x").set_index("x").expr
    assert a.optimize()._name == b.optimize()._name

    # These still work, even if we haven't optimized them yet
    df.set_index(df.x).head(3)
    # df.set_index([df.x, df.y]).head(3)


def test_filter_sort(df):
    a = df.sort_values("x")
    a = a[a.y > 40]

    b = df[df.y > 40]
    b = b.sort_values("x")

    assert a.optimize()._name == b.optimize()._name
