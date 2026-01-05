.. _dataframe.parquet:

Dask Dataframe and Parquet
==========================

.. currentmodule:: dask.dataframe

Parquet_ is a popular, columnar file format designed for efficient data storage
and retrieval. Dask dataframe includes :func:`read_parquet` and
:func:`to_parquet` functions/methods for reading and writing parquet files
respectively. Here we document these methods, and provide some tips and best
practices.

Parquet I/O requires ``pyarrow`` to be installed.


Reading Parquet Files
---------------------

.. autosummary::
    read_parquet

Dask dataframe provides a :func:`read_parquet` function for reading one or more
parquet files. Its first argument is one of:

- A path to a single parquet file
- A path to a directory of parquet files (files with ``.parquet`` or ``.parq`` extension)
- A `glob string`_ expanding to one or more parquet file paths
- A list of parquet file paths

These paths can be local, or point to some remote filesystem (for example S3_
or GCS_) by prepending the path with a protocol.

.. code-block:: python

    >>> import dask.dataframe as dd

    # Load a single local parquet file
    >>> df = dd.read_parquet("path/to/mydata.parquet")

    # Load a directory of local parquet files
    >>> df = dd.read_parquet("path/to/my/parquet/")

    # Load a directory of parquet files from S3
    >>> df = dd.read_parquet("s3://bucket-name/my/parquet/")

Note that for remote filesystems you may need to configure credentials. When
possible we recommend handling these external to Dask through
filesystem-specific configuration files/environment variables. For example, you
may wish to store S3 credentials using the `AWS credentials file`_.
Alternatively, you can pass configuration on to the fsspec_ backend through the
``storage_options`` keyword argument:

.. code-block:: python

   >>> df = dd.read_parquet(
   ...      "s3://bucket-name/my/parquet/",
   ...      storage_options={"anon": True}  # passed to `s3fs.S3FileSystem`
   ... )

For more information on connecting to remote data, see
:doc:`how-to/connect-to-remote-data`.

:func:`read_parquet` has many configuration options affecting both behavior and
performance. Here we highlight a few common options.

Metadata
~~~~~~~~

When :func:`read_parquet` is used to read *multiple files*, it first loads
metadata about the files in the dataset. This metadata may include:

- The dataset schema
- How the dataset is partitioned into files, and those files into row-groups

Some parquet datasets include a ``_metadata`` file which aggregates per-file
metadata into a single location. For small-to-medium sized datasets this *may*
be useful because it makes accessing the row-group metadata possible without
reading parts of *every* file in the dataset. Row-group metadata allows Dask to
split large files into smaller in-memory partitions, and merge many small files
into larger partitions, possibly leading to higher performance.

However, for large datasets the ``_metadata`` file can be problematic because
it may be too large for a single endpoint to parse! If this is true, you can
disable loading the ``_metadata`` file by specifying
``ignore_metadata_file=True``.

.. code-block:: python

   >>> df = dd.read_parquet(
   ...      "s3://bucket-name/my/parquet/",
   ...      ignore_metadata_file=True  # don't read the _metadata file
   ... )

Partition Size
~~~~~~~~~~~~~~

By default, Dask will use metadata from the first parquet file in the dataset
to infer whether or not it is safe load each file individually as a partition
in the Dask dataframe. If the uncompressed byte size of the parquet data
exceeds ``blocksize`` (which is 256 MiB by default), then each partition will
correspond to a range of parquet row-groups instead of the entire file.

For best performance, use files that can be individually mapped to good
dataframe partition sizes, and set ``blocksize`` accordingly. If individual
files need to be divided into multiple row-group ranges, and the dataset
does not contain a ``_metadata`` file, Dask will need to load all footer
metadata up-front.

We recommend aiming for 100-300 MiB in-memory size per file once loaded into
pandas. Oversized partitions can lead to excessive memory usage on a single
worker, while undersized partitions can lead to poor performance as the
overhead of Dask dominates.

If you know your parquet dataset comprises oversized files, you can pass
``split_row_groups='adaptive'`` to ensure that Dask will attempt to keep
each partition under the ``blocksize`` limit. Note that partitions may
still exceed ``blocksize`` if one or more row-groups are too large.

Column Selection
~~~~~~~~~~~~~~~~

When loading parquet data, sometimes you don't need all the columns available
in the dataset. In this case, you likely want to specify the subset of columns
you need via the ``columns`` keyword argument. This is beneficial for a few
reasons:

- It lets Dask read less data from the backing filesystem, reducing IO costs
- It lets Dask load less data into memory, reducing memory usage

.. code-block:: python

    >>> dd.read_parquet(
    ...     "s3://path/to/myparquet/",
    ...     columns=["a", "b", "c"]  # Only read columns 'a', 'b', and 'c'
    ... )

Calculating Divisions
~~~~~~~~~~~~~~~~~~~~~

By default, :func:`read_parquet` will **not** produce a collection with
known divisions. However, you can pass ``calculate_divisions=True`` to
tell Dask that you want to use row-group statistics from the footer
metadata (or global ``_metadata`` file) to calculate the divisions at
graph-creation time. Using this option will not produce known
divisions if any of the necessary row-group statistics are missing,
or if no index column is detected. Using the ``index`` argument is the
best way to ensure that the desired field will be treated as the index.

.. code-block:: python

    >>> dd.read_parquet(
    ...     "s3://path/to/myparquet/",
    ...     index="timestamp",  # Specify a specific index column
    ...     calculate_divisions=True,  # Calculate divisions from metadata
    ... )

Although using ``calculate_divisions=True`` does not require any *real*
data to be read from the parquet file(s), it does require Dask to load
and process metadata for every row-group in the dataset. For this reason,
calculating divisions should be avoided for large datasets without a
global ``_metadata`` file. This is especially true for remote storage.

For more information about divisions, see :ref:`dataframe.design`.

Writing
-------

.. autosummary::
    to_parquet
    DataFrame.to_parquet

Dask dataframe provides a :func:`to_parquet` function and method for writing
parquet files.

In its simplest usage, this takes a path to the directory in which to write the
dataset. This path may be local, or point to some remote filesystem (for
example S3_ or GCS_) by prepending the path with a protocol.

.. code-block:: python

    # Write to a local directory
    >>> df.to_parquet("path/to/my/parquet/")

    # Write to S3
    >>> df.to_parquet("s3://bucket-name/my/parquet/")

Note that for remote filesystems you may need to configure credentials. When
possible we recommend handling these external to Dask through
filesystem-specific configuration files/environment variables For example, you
may wish to store S3 credentials using the `AWS credentials file`_.
Alternatively, you can pass configuration on to the fsspec_ backend through the
``storage_options`` keyword argument:

.. code-block:: python

   >>> df.to_parquet(
   ...     "s3://bucket-name/my/parquet/",
   ...     storage_options={"anon": True}  # passed to `s3fs.S3FileSystem`
   ... )

For more information on connecting to remote data, see
:doc:`how-to/connect-to-remote-data`.

Dask will write one file per Dask dataframe partition to this directory. To
optimize access for downstream consumers, we recommend aiming for an in-memory
size of 100-300 MiB per partition. This helps balance worker memory usage
against Dask overhead. You may find the
:meth:`DataFrame.memory_usage_per_partition` method useful for determining if
your data is partitioned optimally.

:func:`to_parquet` has many configuration options affecting both behavior and
performance. Here we highlight a few common options.

Metadata
~~~~~~~~

In order to improve *read* performance, Dask can optionally write out
a global ``_metadata`` file at write time by aggregating the row-group
metadata from every file in the dataset. While potentially useful at
read time, the generation of this file may result in excessive memory
usage at scale (and potentially killed Dask workers). As such,
enabling the writing of this file is only recommended for small to
moderate dataset sizes.

.. code-block:: python

   >>> df.to_parquet(
   ...     "s3://bucket-name/my/parquet/",
   ...     write_metadata_file=True  # enable writing the _metadata file
   ... )

File Names
~~~~~~~~~~

Unless the `partition_on` option is used (see :doc:`dataframe-hive`),
:func:`to_parquet` will write one file per Dask dataframe partition to the
output directory. By default these files will have names like
``part.0.parquet``, ``part.1.parquet``, etc. If you wish to alter this naming
scheme, you can use the ``name_function`` keyword argument. This takes a
function with the signature ``name_function(partition: int) -> str``, taking
the partition index for each Dask dataframe partition and returning a string to
use as the filename. Note that names returned must sort in the same order as
their partition indices.

.. code-block:: python

    >>> df.npartitions  # 3 partitions (0, 1, and 2)
    3

    >>> df.to_parquet("/path/to/output", name_function=lambda i: f"data-{i}.parquet")

    >>> os.listdir("/path/to/parquet")
    ["data-0.parquet", "data-1.parquet", "data-2.parquet"]

Hive Partitioning
~~~~~~~~~~~~~~~~~

It is sometimes useful to write a parquet dataset with a hive-like directory scheme
(e.g. ``'/year=2022/month=12/day=25'``). :func:`to_parquet` will automatically
produce a dataset with this kind of directory structure when the ``partition_on``
option is used. In most cases, :func:`to_parquet` will handle hive partitioning
automatically. See :doc:`dataframe-hive` for more information.

.. _parquet: https://parquet.apache.org/
.. _glob string: https://docs.python.org/3/library/glob.html
.. _fsspec: https://filesystem-spec.readthedocs.io
.. _s3: https://aws.amazon.com/s3/
.. _gcs: https://cloud.google.com/storage
.. _AWS credentials file: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#shared-credentials-file>`_.
