from collections import OrderedDict

import dask
import pytest

from dask_expr import SetIndexBlockwise, from_pandas
from dask_expr._expr import Blockwise
from dask_expr._repartition import RepartitionToFewer
from dask_expr._shuffle import TaskShuffle, divisions_lru
from dask_expr.io import FromPandas
from dask_expr.tests._util import _backend_library, assert_eq

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

    q = df.set_index(["y", "z"], sorted=True)[[]]
    assert_eq(q, pdf.set_index(["y", "z"])[[]])
    result = q.optimize(fuse=False)
    expected = df[["y", "z"]].set_index(["y", "z"], sorted=True)[[]].simplify()
    assert result._name == expected._name

    with pytest.raises(TypeError, match="not supported by set_index"):
        df.set_index([df["y"], df["x"]], sorted=True)

    with pytest.raises(NotImplementedError, match="requires sorted=True"):
        df.set_index(["y", "z"], sorted=False)


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


def test_set_index_numeric_columns():
    pdf = lib.DataFrame(
        {
            0: list("ABAABBABAA"),
            1: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            2: [1, 2, 3, 2, 1, 3, 2, 4, 2, 3],
        }
    )
    ddf = from_pandas(pdf, 3)
    assert_eq(ddf.set_index(0), pdf.set_index(0))


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


@pytest.mark.parametrize("shuffle", [None, "tasks"])
def test_sort_values(df, pdf, shuffle):
    assert_eq(df.sort_values("x", shuffle_method=shuffle), pdf.sort_values("x"))
    assert_eq(
        df.sort_values("x", shuffle_method=shuffle, npartitions=2), pdf.sort_values("x")
    )
    pdf.iloc[5, 0] = -10
    df = from_pandas(pdf, npartitions=10)
    assert_eq(
        df.sort_values("x", shuffle_method=shuffle, upsample=2.0), pdf.sort_values("x")
    )

    with pytest.raises(NotImplementedError, match="a single boolean for ascending"):
        df.sort_values(by=["x", "y"], shuffle_method=shuffle, ascending=[True, True])
    with pytest.raises(NotImplementedError, match="sorting by named columns"):
        df.sort_values(by=1, shuffle_method=shuffle)

    with pytest.raises(ValueError, match="must be either 'first' or 'last'"):
        df.sort_values(by="x", shuffle_method=shuffle, na_position="bla")


@pytest.mark.parametrize("shuffle", [None, "tasks"])
def test_sort_values_temporary_column_dropped(shuffle):
    pdf = lib.DataFrame(
        {"x": range(10), "y": [1, 2, 3, 4, 5] * 2, "z": ["cat", "dog"] * 5}
    )
    df = from_pandas(pdf, npartitions=2)
    _sorted = df.sort_values(["z"], shuffle_method=shuffle)
    result = _sorted.compute()
    assert "_partitions" not in result.columns


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


def test_set_index_list(df, pdf):
    assert_eq(df.set_index(["x"]), pdf.set_index(["x"]))


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


def test_sort_head_nlargest_string(pdf):
    pdf["z"] = "a" + pdf.x.map(str)
    df = from_pandas(pdf, npartitions=5)
    a = df.sort_values("z", ascending=False).head(10, compute=False)
    assert_eq(a, pdf.sort_values("z", ascending=False).head(10))

    a = df.sort_values("z", ascending=True).head(10, compute=False)
    assert_eq(a, pdf.sort_values("z", ascending=True).head(10))

    a = df.sort_values("z", ascending=False).tail(10, compute=False)
    assert_eq(a, pdf.sort_values("z", ascending=False).tail(10))

    a = df.sort_values("z", ascending=True).tail(10, compute=False)
    assert_eq(a, pdf.sort_values("z", ascending=True).tail(10))


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


def test_set_index_head_nlargest_string(pdf):
    pdf["z"] = "a" + pdf.x.map(str)
    df = from_pandas(pdf, npartitions=5)
    print(df.dtypes)

    a = df.set_index("z").head(10, compute=False)
    assert_eq(a, pdf.set_index("z").sort_index().head(10))

    a = df.set_index("z").tail(10, compute=False)
    assert_eq(a, pdf.set_index("z").sort_index().tail(10))


def test_filter_sort(df):
    a = df.sort_values("x")
    a = a[a.y > 40]

    b = df[df.y > 40]
    b = b.sort_values("x")

    assert a.optimize()._name == b.optimize()._name


def test_sort_values_add():
    pdf = lib.DataFrame({"x": [1, 2, 3, 0, 1, 2, 4, 5], "y": 1})
    df = from_pandas(pdf, npartitions=2, sort=False)
    with dask.config.set({"dataframe.shuffle.method": "tasks"}):
        df = df.sort_values("x")
        df["z"] = df.x + df.y
        pdf = pdf.sort_values("x")
        pdf["z"] = pdf.x + pdf.y
        assert_eq(df, pdf, sort_results=False)


@pytest.mark.parametrize("null_value", [None, lib.NaT, lib.NA])
def test_index_nulls(null_value):
    "Setting the index with some non-numeric null raises error"
    df = lib.DataFrame(
        {"numeric": [1, 2, 3, 4], "non_numeric": ["foo", "bar", "foo", "bar"]}
    )
    ddf = from_pandas(df, npartitions=2)
    with pytest.raises(NotImplementedError, match="presence of nulls"):
        ddf.set_index(
            ddf["non_numeric"].map({"foo": "foo", "bar": null_value})
        ).compute()


def test_set_index_sort_values_shuffle_options(df, pdf):
    q = df.set_index("x", shuffle_method="tasks", max_branch=10)
    shuffle = list(q.optimize().find_operations(TaskShuffle))[0]
    assert shuffle.options == {"max_branch": 10}
    assert_eq(q, pdf.set_index("x"))

    q = df.sort_values("x", shuffle_method="tasks", max_branch=10)
    sorter = list(q.optimize().find_operations(TaskShuffle))[0]
    assert sorter.options == {"max_branch": 10}
    assert_eq(q, pdf)


def test_set_index_predicate_pushdown(df, pdf):
    pdf = pdf.set_index("x")
    query = df.set_index("x")
    result = query[query.y > 5]
    expected = pdf[pdf.y > 5]
    assert_eq(result, expected)
    expected_query = df[df.y > 5].set_index("x").optimize()
    assert expected_query._name == result.optimize()._name

    result = query[query.index > 5]
    assert_eq(result, pdf[pdf.index > 5])

    result = query[(query.index > 5) & (query.y > -1)]
    assert_eq(result, pdf[(pdf.index > 5) & (pdf.y > -1)])


def test_set_index_with_explicit_divisions():
    pdf = lib.DataFrame({"x": [4, 1, 2, 5]}, index=[10, 20, 30, 40])

    df = from_pandas(pdf, npartitions=2)

    result = df.set_index("x", divisions=[1, 3, 5])
    assert result.divisions == (1, 3, 5)
    assert_eq(result, pdf.set_index("x"))


def test_set_index_npartitions_changes(pdf):
    df = from_pandas(pdf, npartitions=30)
    result = df.set_index("x", shuffle_method="disk")
    assert result.npartitions == result.optimize().npartitions
    assert_eq(result, pdf.set_index("x"))


def test_set_index_sorted_divisions(df):
    with pytest.raises(ValueError, match="must be the same length"):
        df.set_index("x", divisions=(1, 2, 3), sorted=True)


def test_set_index_sort_values_one_partition(pdf):
    divisions_lru.data = OrderedDict()
    df = from_pandas(pdf, sort=False)
    query = df.sort_values("x").optimize(fuse=False)
    assert query.divisions == (None, None)
    assert_eq(pdf.sort_values("x"), query, sort_results=False)
    assert len(divisions_lru) == 0

    df = from_pandas(pdf, sort=False)
    query = df.set_index("x").optimize(fuse=False)
    assert query.divisions == (None, None)
    assert_eq(pdf.set_index("x"), query)
    assert len(divisions_lru) == 0

    df = from_pandas(pdf, sort=False, npartitions=2)
    query = df.set_index("x", npartitions=1).optimize(fuse=False)
    assert query.divisions == (None, None)
    assert_eq(pdf.set_index("x"), query)
    assert len(divisions_lru) == 0
    assert len(list(query.expr.find_operations(RepartitionToFewer))) > 0


def test_shuffle(df, pdf):
    result = df.shuffle(df.x)
    assert result.npartitions == df.npartitions
    assert_eq(result, pdf)

    result = df.shuffle(df[["x"]])
    assert result.npartitions == df.npartitions
    assert_eq(result, pdf)

    result = df[["y"]].shuffle(df[["x"]])
    assert result.npartitions == df.npartitions
    assert_eq(result, pdf[["y"]])

    with pytest.raises(TypeError, match="index must be aligned"):
        df.shuffle(df.x.repartition(npartitions=2))

    result = df.shuffle(df.x, npartitions=2)
    assert result.npartitions == 2
    assert_eq(result, pdf)
