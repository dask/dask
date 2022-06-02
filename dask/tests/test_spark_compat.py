import pytest

from dask.datasets import timeseries

dd = pytest.importorskip("dask.dataframe")
pyspark = pytest.importorskip("pyspark")

from dask.dataframe.utils import assert_eq

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark_session():
    spark = (
        pyspark.sql.SparkSession.builder.master("local")
        .appName("Dask Testing")
        .getOrCreate()
    )
    yield spark

    spark.stop()


@pytest.mark.parametrize("npartitions", (1, 5, 10))
@pytest.mark.parametrize("engine", ("pyarrow", "fastparquet"))
def test_roundtrip_parquet_spark_to_dask(spark_session, npartitions, tmpdir, engine):
    tmpdir = str(tmpdir)
    # TODO: pyspark auto-converts timezones -- round-tripping timestamps
    # is tricky, so drop timeseries for now.
    pdf = timeseries(freq="1H").compute().reset_index(drop=True)
    sdf = spark_session.createDataFrame(pdf)
    (
        sdf.repartition(npartitions).write
        # not overwriting any data, but spark complains if the directory
        # already exists and we don't set overwrite
        .parquet(tmpdir, mode="overwrite")
    )

    # TODO: fastparquet requires the glob, pyarrow does not.
    ddf = dd.read_parquet(tmpdir + "/**.parquet", engine=engine)
    assert ddf.npartitions == npartitions

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
