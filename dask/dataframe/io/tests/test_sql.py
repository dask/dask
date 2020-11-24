from contextlib import contextmanager
import io

import pytest

# import dask
from dask.dataframe.io.sql import read_sql_table
from dask.dataframe.utils import assert_eq, PANDAS_GT_0240
from dask.utils import tmpfile

pd = pytest.importorskip("pandas")
dd = pytest.importorskip("dask.dataframe")
pytest.importorskip("sqlalchemy")
pytest.importorskip("sqlite3")
np = pytest.importorskip("numpy")


data = """
name,number,age,negish
Alice,0,33,-5
Bob,1,40,-3
Chris,2,22,3
Dora,3,16,5
Edith,4,53,0
Francis,5,30,0
Garreth,6,20,0
"""

df = pd.read_csv(io.StringIO(data), index_col="number")


@pytest.yield_fixture
def db():
    with tmpfile() as f:
        uri = "sqlite:///%s" % f
        df.to_sql("test", uri, index=True, if_exists="replace")
        yield uri


def test_empty(db):
    from sqlalchemy import create_engine, MetaData, Table, Column, Integer

    with tmpfile() as f:
        uri = "sqlite:///%s" % f
        metadata = MetaData()
        engine = create_engine(uri)
        table = Table(
            "empty_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("col2", Integer),
        )
        metadata.create_all(engine)

        dask_df = read_sql_table(table.name, uri, index_col="id", npartitions=1)
        assert dask_df.index.name == "id"
        assert dask_df.col2.dtype == np.dtype("int64")
        pd_dataframe = dask_df.compute()
        assert pd_dataframe.empty is True


def test_passing_engine_as_uri_raises_helpful_error(db):
    # https://github.com/dask/dask/issues/6473
    from sqlalchemy import create_engine

    df = pd.DataFrame([{"i": i, "s": str(i) * 2} for i in range(4)])
    ddf = dd.from_pandas(df, npartitions=2)

    with tmpfile() as f:
        db = "sqlite:///%s" % f
        engine = create_engine(db)
        with pytest.raises(ValueError, match="Expected URI to be a string"):
            ddf.to_sql("test", engine, if_exists="replace")


@pytest.mark.skip(
    reason="Requires a postgres server. Sqlite does not support multiple schemas."
)
def test_empty_other_schema():
    from sqlalchemy import create_engine, MetaData, Table, Column, Integer, event, DDL

    # Database configurations.
    pg_host = "localhost"
    pg_port = "5432"
    pg_user = "user"
    pg_pass = "pass"
    pg_db = "db"
    db_url = "postgresql://%s:%s@%s:%s/%s" % (pg_user, pg_pass, pg_host, pg_port, pg_db)

    # Create an empty table in a different schema.
    table_name = "empty_table"
    schema_name = "other_schema"
    engine = create_engine(db_url)
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("col2", Integer),
        schema=schema_name,
    )
    # Create the schema and the table.
    event.listen(
        metadata, "before_create", DDL("CREATE SCHEMA IF NOT EXISTS %s" % schema_name)
    )
    metadata.create_all(engine)

    # Read the empty table from the other schema.
    dask_df = read_sql_table(
        table.name, db_url, index_col="id", schema=table.schema, npartitions=1
    )

    # Validate that the retrieved table is empty.
    assert dask_df.index.name == "id"
    assert dask_df.col2.dtype == np.dtype("int64")
    pd_dataframe = dask_df.compute()
    assert pd_dataframe.empty is True

    # Drop the schema and the table.
    engine.execute("DROP SCHEMA IF EXISTS %s CASCADE" % schema_name)


def test_needs_rational(db):
    import datetime

    now = datetime.datetime.now()
    d = datetime.timedelta(seconds=1)
    df = pd.DataFrame(
        {
            "a": list("ghjkl"),
            "b": [now + i * d for i in range(5)],
            "c": [True, True, False, True, True],
        }
    )
    df = df.append(
        [
            {"a": "x", "b": now + d * 1000, "c": None},
            {"a": None, "b": now + d * 1001, "c": None},
        ]
    )
    with tmpfile() as f:
        uri = "sqlite:///%s" % f
        df.to_sql("test", uri, index=False, if_exists="replace")

        # one partition contains NULL
        data = read_sql_table("test", uri, npartitions=2, index_col="b")
        df2 = df.set_index("b")
        assert_eq(data, df2.astype({"c": bool}))  # bools are coerced

        # one partition contains NULL, but big enough head
        data = read_sql_table("test", uri, npartitions=2, index_col="b", head_rows=12)
        df2 = df.set_index("b")
        assert_eq(data, df2)

        # empty partitions
        data = read_sql_table("test", uri, npartitions=20, index_col="b")
        part = data.get_partition(12).compute()
        assert part.dtypes.tolist() == ["O", bool]
        assert part.empty
        df2 = df.set_index("b")
        assert_eq(data, df2.astype({"c": bool}))

        # explicit meta
        data = read_sql_table("test", uri, npartitions=2, index_col="b", meta=df2[:0])
        part = data.get_partition(1).compute()
        assert part.dtypes.tolist() == ["O", "O"]
        df2 = df.set_index("b")
        assert_eq(data, df2)


def test_simple(db):
    # single chunk
    data = read_sql_table("test", db, npartitions=2, index_col="number").compute()
    assert (data.name == df.name).all()
    assert data.index.name == "number"
    assert_eq(data, df)


def test_npartitions(db):
    data = read_sql_table(
        "test", db, columns=list(df.columns), npartitions=2, index_col="number"
    )
    assert len(data.divisions) == 3
    assert (data.name.compute() == df.name).all()
    data = read_sql_table(
        "test", db, columns=["name"], npartitions=6, index_col="number"
    )
    assert_eq(data, df[["name"]])
    data = read_sql_table(
        "test",
        db,
        columns=list(df.columns),
        bytes_per_chunk="2 GiB",
        index_col="number",
    )
    assert data.npartitions == 1
    assert (data.name.compute() == df.name).all()

    data_1 = read_sql_table(
        "test",
        db,
        columns=list(df.columns),
        bytes_per_chunk=2 ** 30,
        index_col="number",
        head_rows=1,
    )
    assert data_1.npartitions == 1
    assert (data_1.name.compute() == df.name).all()

    data = read_sql_table(
        "test",
        db,
        columns=list(df.columns),
        bytes_per_chunk=250,
        index_col="number",
        head_rows=1,
    )
    assert data.npartitions == 2


def test_divisions(db):
    data = read_sql_table(
        "test", db, columns=["name"], divisions=[0, 2, 4], index_col="number"
    )
    assert data.divisions == (0, 2, 4)
    assert data.index.max().compute() == 4
    assert_eq(data, df[["name"]][df.index <= 4])


def test_division_or_partition(db):
    with pytest.raises(TypeError):
        read_sql_table(
            "test",
            db,
            columns=["name"],
            index_col="number",
            divisions=[0, 2, 4],
            npartitions=3,
        )

    out = read_sql_table("test", db, index_col="number", bytes_per_chunk=100)
    m = out.map_partitions(
        lambda d: d.memory_usage(deep=True, index=True).sum()
    ).compute()
    assert (50 < m).all() and (m < 200).all()
    assert_eq(out, df)


def test_meta(db):
    data = read_sql_table(
        "test", db, index_col="number", meta=dd.from_pandas(df, npartitions=1)
    ).compute()
    assert (data.name == df.name).all()
    assert data.index.name == "number"
    assert_eq(data, df)


def test_meta_no_head_rows(db):
    data = read_sql_table(
        "test",
        db,
        index_col="number",
        meta=dd.from_pandas(df, npartitions=1),
        npartitions=2,
        head_rows=0,
    )
    assert len(data.divisions) == 3
    data = data.compute()
    assert (data.name == df.name).all()
    assert data.index.name == "number"
    assert_eq(data, df)

    data = read_sql_table(
        "test",
        db,
        index_col="number",
        meta=dd.from_pandas(df, npartitions=1),
        divisions=[0, 3, 6],
        head_rows=0,
    )
    assert len(data.divisions) == 3
    data = data.compute()
    assert (data.name == df.name).all()
    assert data.index.name == "number"
    assert_eq(data, df)


def test_no_meta_no_head_rows(db):
    with pytest.raises(ValueError):
        read_sql_table("test", db, index_col="number", head_rows=0, npartitions=1)


def test_range(db):
    data = read_sql_table("test", db, npartitions=2, index_col="number", limits=[1, 4])
    assert data.index.min().compute() == 1
    assert data.index.max().compute() == 4


def test_datetimes():
    import datetime

    now = datetime.datetime.now()
    d = datetime.timedelta(seconds=1)
    df = pd.DataFrame(
        {"a": list("ghjkl"), "b": [now + i * d for i in range(2, -3, -1)]}
    )
    with tmpfile() as f:
        uri = "sqlite:///%s" % f
        df.to_sql("test", uri, index=False, if_exists="replace")
        data = read_sql_table("test", uri, npartitions=2, index_col="b")
        assert data.index.dtype.kind == "M"
        assert data.divisions[0] == df.b.min()
        df2 = df.set_index("b")
        assert_eq(data.map_partitions(lambda x: x.sort_index()), df2.sort_index())


def test_with_func(db):
    from sqlalchemy import sql

    index = sql.func.abs(sql.column("negish")).label("abs")

    # function for the index, get all columns
    data = read_sql_table("test", db, npartitions=2, index_col=index)
    assert data.divisions[0] == 0
    part = data.get_partition(0).compute()
    assert (part.index == 0).all()

    # now an arith op for one column too; it's name will be 'age'
    data = read_sql_table(
        "test",
        db,
        npartitions=2,
        index_col=index,
        columns=[index, -(sql.column("age"))],
    )
    assert (data.age.compute() < 0).all()

    # a column that would have no name, give it a label
    index = (-(sql.column("negish"))).label("index")
    data = read_sql_table(
        "test", db, npartitions=2, index_col=index, columns=["negish", "age"]
    )
    d = data.compute()
    assert (-d.index == d["negish"]).all()


def test_no_nameless_index(db):
    from sqlalchemy import sql

    index = -(sql.column("negish"))
    with pytest.raises(ValueError):
        read_sql_table(
            "test", db, npartitions=2, index_col=index, columns=["negish", "age", index]
        )

    index = sql.func.abs(sql.column("negish"))

    # function for the index, get all columns
    with pytest.raises(ValueError):
        read_sql_table("test", db, npartitions=2, index_col=index)


def test_select_from_select(db):
    from sqlalchemy import sql

    s1 = sql.select([sql.column("number"), sql.column("name")]).select_from(
        sql.table("test")
    )
    out = read_sql_table(s1, db, npartitions=2, index_col="number")
    assert_eq(out, df[["name"]])


def test_extra_connection_engine_keywords(capsys, db):
    data = read_sql_table(
        "test", db, npartitions=2, index_col="number", engine_kwargs={"echo": False}
    ).compute()
    # no captured message from the stdout with the echo=False parameter (this is the default)
    out, err = capsys.readouterr()
    assert "SELECT" not in out
    assert_eq(data, df)
    # with the echo=True sqlalchemy parameter, you should get all SQL queries in the stdout
    data = read_sql_table(
        "test", db, npartitions=2, index_col="number", engine_kwargs={"echo": True}
    ).compute()
    out, err = capsys.readouterr()
    assert "WHERE test.number >= ? AND test.number < ?" in out
    assert "WHERE test.number >= ? AND test.number <= ?" in out
    assert_eq(data, df)


def test_no_character_index_without_divisions(db):

    # attempt to read the sql table with a character index and no divisions
    with pytest.raises(TypeError):
        read_sql_table("test", db, npartitions=2, index_col="name", divisions=None)


@contextmanager
def tmp_db_uri():
    with tmpfile() as f:
        yield "sqlite:///%s" % f


@pytest.mark.parametrize("npartitions", (1, 2))
@pytest.mark.parametrize("parallel", (False, True))
def test_to_sql(npartitions, parallel):
    df_by_age = df.set_index("age")
    df_appended = pd.concat(
        [
            df,
            df,
        ]
    )

    ddf = dd.from_pandas(df, npartitions)
    ddf_by_age = ddf.set_index("age")

    # Simple round trip test: use existing "number" index_col
    with tmp_db_uri() as uri:
        ddf.to_sql("test", uri, parallel=parallel)
        result = read_sql_table("test", uri, "number")
        assert_eq(df, result)

    # Test writing no index, and reading back in with one of the other columns as index (`read_sql_table` requires
    # an index_col)
    with tmp_db_uri() as uri:
        ddf.to_sql("test", uri, parallel=parallel, index=False)

        result = read_sql_table("test", uri, "negish")
        assert_eq(df.set_index("negish"), result)

        result = read_sql_table("test", uri, "age")
        assert_eq(df_by_age, result)

    # Index by "age" instead
    with tmp_db_uri() as uri:
        ddf_by_age.to_sql("test", uri, parallel=parallel)
        result = read_sql_table("test", uri, "age")
        assert_eq(df_by_age, result)

    # Index column can't have "object" dtype if no partitions are provided
    with tmp_db_uri() as uri:
        ddf.set_index("name").to_sql("test", uri)
        with pytest.raises(
            TypeError,
            match='Provided index column is of type "object".  If divisions is not provided the index column type must be numeric or datetime.',  # noqa: E501
        ):
            read_sql_table("test", uri, "name")

    # Test various "if_exists" values
    with tmp_db_uri() as uri:
        ddf.to_sql("test", uri)

        # Writing a table that already exists fails
        with pytest.raises(ValueError, match="Table 'test' already exists"):
            ddf.to_sql("test", uri)

        ddf.to_sql("test", uri, parallel=parallel, if_exists="append")
        result = read_sql_table("test", uri, "number")

        assert_eq(df_appended, result)

        ddf_by_age.to_sql("test", uri, parallel=parallel, if_exists="replace")
        result = read_sql_table("test", uri, "age")
        assert_eq(df_by_age, result)

    # Verify number of partitions returned, when compute=False
    with tmp_db_uri() as uri:
        result = ddf.to_sql("test", uri, parallel=parallel, compute=False)

        # the first result is from the "meta" insert
        actual = len(result.compute())

        assert actual == npartitions


def test_to_sql_kwargs():
    ddf = dd.from_pandas(df, 2)
    with tmp_db_uri() as uri:
        # "method" keyword is allowed iff pandas>=0.24.0
        if PANDAS_GT_0240:
            ddf.to_sql("test", uri, method="multi")
        else:
            with pytest.raises(
                NotImplementedError,
                match=r"'method' requires pandas>=0.24.0. You have version 0.23.\d",
            ):
                ddf.to_sql("test", uri, method="multi")

        # Other, unknown keywords always disallowed
        with pytest.raises(
            TypeError, match="to_sql\\(\\) got an unexpected keyword argument 'unknown'"
        ):
            ddf.to_sql("test", uri, unknown=None)
