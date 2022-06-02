import signal

import pytest

from dask.datasets import timeseries

dd = pytest.importorskip("dask.dataframe")
pyspark = pytest.importorskip("pyspark")

from dask.dataframe.utils import assert_eq

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark_session():
    # Spark registers a global signal handler that can cause problems elsewhere
    # in the test suite. In particular, the handler fails if the spark session
    # is stopped (a bug in pyspark).
    prev = signal.getsignal(signal.SIGINT)
    spark = (
        pyspark.sql.SparkSession.builder.master("local")
        .appName("Dask Testing")
        .getOrCreate()
    )
    yield spark

    spark.stop()
    # Make sure we get rid of the signal once we leave stop the session.
    signal.signal(signal.SIGINT, prev)


@pytest.mark.parametrize("npartitions", (1, 5, 10))
@pytest.mark.parametrize("engine", ("pyarrow", "fastparquet"))
def test_roundtrip_parquet_spark_to_dask(spark_session, npartitions, tmpdir, engine):
    tmpdir = str(tmpdir)
    # TODO: pyspark auto-converts timezones -- round-tripping timestamps
    # is tricky, so drop timeseries for now.
    pdf = timeseries(freq="1H").compute().reset_index(drop=True)
    sdf = spark_session.createDataFrame(pdf)
    # not overwriting any data, but spark complains if the directory
    # already exists and we don't set overwrite
    sdf.repartition(npartitions).write.parquet(tmpdir, mode="overwrite")

    # TODO: fastparquet requires the glob, pyarrow does not.
    ddf = dd.read_parquet(tmpdir + "/**.parquet", engine=engine)
    assert ddf.npartitions == npartitions

    assert_eq(ddf, pdf, check_index=False)


@pytest.mark.parametrize("engine", ("pyarrow", "fastparquet"))
def test_roundtrip_hive_parquet_spark_to_dask(spark_session, tmpdir, engine):
    tmpdir = str(tmpdir)
    # TODO: pyspark auto-converts timezones -- round-tripping timestamps
    # is tricky, so drop timeseries for now.
    pdf = timeseries(freq="1H").compute().reset_index(drop=True)
    sdf = spark_session.createDataFrame(pdf)
    # not overwriting any data, but spark complains if the directory
    # already exists and we don't set overwrite
    sdf.write.parquet(tmpdir, mode="overwrite", partitionBy="name")

    # TODO: fastparquet requires the glob, pyarrow does not.
    ddf = dd.read_parquet(tmpdir + "/**.parquet", engine=engine)

    # Partitioning can change the column order. This is mostly okay,
    # but we sort them here to ease comparison
    ddf = ddf.compute().sort_index(axis=1)
    # Dask automatically converts hive-partitioned columns to categories.
    # This is fine, but convert back to strings for comparison.
    ddf = ddf.assign(name=ddf.name.astype("str"))

    assert_eq(ddf, pdf, check_index=False)


@pytest.mark.parametrize("npartitions", (1, 5, 10))
@pytest.mark.parametrize("engine", ("pyarrow", "fastparquet"))
def test_roundtrip_parquet_dask_to_spark(spark_session, npartitions, tmpdir, engine):
    tmpdir = str(tmpdir)
    # TODO: pyspark auto-converts timezones -- round-tripping timestamps
    # is tricky, so drop timeseries for now.
    ddf = (
        timeseries(freq="1H")
        .repartition(npartitions=npartitions)
        .reset_index(drop=True)
    )
    ddf.to_parquet(tmpdir, engine=engine, write_index=False)

    sdf = spark_session.read.parquet(tmpdir)

    assert_eq(sdf.toPandas(), ddf, check_index=False)
