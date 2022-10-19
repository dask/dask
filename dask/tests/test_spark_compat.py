import signal
import sys
import threading

import pytest

from dask.datasets import timeseries

dd = pytest.importorskip("dask.dataframe")
pyspark = pytest.importorskip("pyspark")
pytest.importorskip("pyarrow")
pytest.importorskip("fastparquet")

from dask.dataframe.utils import assert_eq

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="Unnecessary, and hard to get spark working on non-linux platforms",
)

# pyspark auto-converts timezones -- round-tripping timestamps is easier if
# we set everything to UTC.
pdf = timeseries(freq="1H").compute()
pdf.index = pdf.index.tz_localize("UTC")
pdf = pdf.reset_index()


@pytest.fixture(scope="module")
def spark_session():
    # Spark registers a global signal handler that can cause problems elsewhere
    # in the test suite. In particular, the handler fails if the spark session
    # is stopped (a bug in pyspark).
    prev = signal.getsignal(signal.SIGINT)
    # Create a spark session. Note that we set the timezone to UTC to avoid
    # conversion to local time when reading parquet files.
    spark = (
        pyspark.sql.SparkSession.builder.master("local")
        .appName("Dask Testing")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield spark

    spark.stop()
    # Make sure we get rid of the signal once we leave stop the session.
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, prev)


@pytest.mark.parametrize("npartitions", (1, 5, 10))
@pytest.mark.parametrize("engine", ("pyarrow", "fastparquet"))
def test_roundtrip_parquet_spark_to_dask(spark_session, npartitions, tmpdir, engine):
    tmpdir = str(tmpdir)

    sdf = spark_session.createDataFrame(pdf)
    # We are not overwriting any data, but spark complains if the directory
    # already exists (as tmpdir does) and we don't set overwrite
    sdf.repartition(npartitions).write.parquet(tmpdir, mode="overwrite")

    ddf = dd.read_parquet(tmpdir, engine=engine)
    # Papercut: pandas TZ localization doesn't survive roundtrip
    ddf = ddf.assign(timestamp=ddf.timestamp.dt.tz_localize("UTC"))
    assert ddf.npartitions == npartitions

    assert_eq(ddf, pdf, check_index=False)


@pytest.mark.parametrize("engine", ("pyarrow", "fastparquet"))
def test_roundtrip_hive_parquet_spark_to_dask(spark_session, tmpdir, engine):
    tmpdir = str(tmpdir)

    sdf = spark_session.createDataFrame(pdf)
    # not overwriting any data, but spark complains if the directory
    # already exists and we don't set overwrite
    sdf.write.parquet(tmpdir, mode="overwrite", partitionBy="name")

    ddf = dd.read_parquet(tmpdir, engine=engine)
    # Papercut: pandas TZ localization doesn't survive roundtrip
    ddf = ddf.assign(timestamp=ddf.timestamp.dt.tz_localize("UTC"))

    # Partitioning can change the column order. This is mostly okay,
    # but we sort them here to ease comparison
    ddf = ddf.compute().sort_index(axis=1)
    # Dask automatically converts hive-partitioned columns to categories.
    # This is fine, but convert back to strings for comparison.
    ddf = ddf.assign(name=ddf.name.astype("str"))

    assert_eq(ddf, pdf.sort_index(axis=1), check_index=False)


@pytest.mark.parametrize("npartitions", (1, 5, 10))
@pytest.mark.parametrize("engine", ("pyarrow", "fastparquet"))
def test_roundtrip_parquet_dask_to_spark(spark_session, npartitions, tmpdir, engine):
    tmpdir = str(tmpdir)
    ddf = dd.from_pandas(pdf, npartitions=npartitions)

    # Papercut: https://github.com/dask/fastparquet/issues/646#issuecomment-885614324
    kwargs = {"times": "int96"} if engine == "fastparquet" else {}

    ddf.to_parquet(tmpdir, engine=engine, write_index=False, **kwargs)

    sdf = spark_session.read.parquet(tmpdir)
    sdf = sdf.toPandas()

    # Papercut: pandas TZ localization doesn't survive roundtrip
    sdf = sdf.assign(timestamp=sdf.timestamp.dt.tz_localize("UTC"))

    assert_eq(sdf, ddf, check_index=False)
