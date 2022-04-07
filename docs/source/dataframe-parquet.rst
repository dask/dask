Dask Dataframe and Parquet
==========================

.. currentmodule:: dask.dataframe

Parquet_ is a popular, columnar file format designed for efficient data storage
and retrieval. Dask dataframe includes `read_parquet` and `to_parquet`
functions/methods for reading and writing parquet files respectively. Here we
document these methods, and provide some tips and best practices.

Reading Parquet Files
---------------------

.. autosummary::
    read_parquet

Dask dataframe provides a `read_parquet` function for reading parquet files and
datasets. Its first argument is one of:

- A path to a single parquet file
- A path to a directory of parquet files (files with ``.parquet`` or ``.parq`` extension)
- A `glob string`_ expanding to one or more parquet file paths
- A list of parquet file paths

These paths can be local, or point to some remote filesystem (e.g. S3, HDFS,
...) by prepending the path with a protocol.

.. code-block:: python

    >>> import dask.dataframe as dd

    # Load a single local parquet file
    >>> df = dd.read_parquet("path/to/mydata.parquet")

    # Load a directory of local parquet files
    >>> df = dd.read_parquet("path/to/my/parquet/")

    # Load a directory of parquet files from S3
    >>> df = dd.read_parquet("s3://bucket-name/my/parquet/")

Note that for remote filesystems you many need to configure credentials. When
possible we recommend handling these external to Dask through
filesystem-specific configuration files/environment variables (e.g. the
``~/.aws/config`` file for S3). Alternatively, you can pass configuration on to
the fsspec_ backend through the ``storage_options`` kwarg:

.. code-block:: python

   >>> df = dd.read_parquet(
   ...      "s3://bucket-name/my/parquet/",
   ...      storage_options={'anon': True}  # passed to `s3fs.S3FileSystem`
   ... )

`read_parquet` has many configuration options affecting both behavior and
performance. Here we highlight a few common options.

Engine
~~~~~~

`read_parquet` supports two backend engines - ``fastparquet`` and ``pyarrow``.
For historical reasons this defaults to ``fastparquet`` (if installed), and
falls back to ``pyarrow``. We recommend using ``pyarrow`` when possible. This
can be explicitly set by passing ``engine="pyarrow"``, but if ``fastparquet``
is not installed this configuration is unnecessary since it will automatically
fallback to ``pyarrow``.

.. code-block:: python

   >>> df = dd.read_parquet(
   ...      "s3://bucket-name/my/parquet/",
   ...      engine="pyarrow"  # explicitly specify the pyarrow engine
   ... )

Metadata
~~~~~~~~

When `read_parquet` is called, it first loads metadata about the file(s). This
metadata may include:

- The dataset schema
- How the dataset is partitioned into files (and those files into row-groups)

Some parquet datasets include a ``_metadata`` file which aggregates per-file
metadata into a single location. For small-to-medium sized datasets this *may*
be useful - it makes accessing the row-group metadata possible without reading
parts of *every* file in the dataset. Row-group metadata allows dask to split
large files into smaller in-memory partitions, and merge many small files into
larger partitions, possibly leading to higher performance.

However, for large datasets the ``_metadata`` file can be problematic - it may
be too large for a single endpoint to parse! If this is true, you can disable
loading the ``_metadata`` file by specifying ``ignore_metadata_file=True``.

.. code-block:: python

   >>> df = dd.read_parquet(
   ...      "s3://bucket-name/my/parquet/",
   ...      ignore_metadata_file=True  # don't read the _metadata file
   ... )

If no ``_metadata`` file is present, dask will load each parquet file
individually as a partition in the larger dask dataframe (unless only a single
file is specified). This is performant provided all files are of reasonable
size (We recommend aiming for 10-250 MiB in-memory size per file once loaded
into pandas). Too large files can lead to excessive memory usage on a single
worker, while too small files can lead to poor performance as the overhead of
dask dominates. If you need to read a parquet dataset composed of many large
files (and lacking a ``_metadata`` file), you can pass
``gather_statistics=True`` to have dask load the row-group metadata from all
files in the dataset. Note that this can be *extremely* slow on some systems.

Column Selection
~~~~~~~~~~~~~~~~

When loading parquet data, sometimes you don't need all the columns available
in the dataset. In this case, you may want to specify the subset of columns you
need via the ``columns`` kwarg. This is beneficial for two reasons:

- It lets dask read less data from the backing filesystem, reducing IO costs
- It lets dask load less data into memory, reducing memory usage

.. code-block:: python

    >>> dd.read_parquet(
    ...     "s3://path/to/myparquet/",
    ...     columns=["a", "b", "c"]  # Only read columns 'a', 'b', and 'c'
    ... )


Writing
-------

.. autosummary::
    to_parquet
    DataFrame.to_parquet

Dask dataframe provides a `to_parquet` function (as well as a method) for
writing parquet files and datasets.

In its simplest usage, this takes a path to the directory in which to write the
dataset. Just as with `read_parquet`, this path may be local, or point to some
remote filesystem (e.g. S3, HDFS, ...) by prepending the path with a protocol.

.. code-block:: python

    # Write to a local directory
    >>> df.to_parquet("path/to/my/parquet/")

    # Write to S3
    >>> df.to_parquet("s3://bucket-name/my/parquet/")

Note that for remote filesystems you many need to configure credentials. When
possible we recommend handling these external to Dask through
filesystem-specific configuration files/environment variables (e.g. the
``~/.aws/config`` file for S3). Alternatively, you can pass configuration on to
the fsspec_ backend through the ``storage_options`` kwarg:

.. code-block:: python

   >>> df.to_parquet(
   ...     "s3://bucket-name/my/parquet/",
   ...     storage_options={'anon': True}  # passed to `s3fs.S3FileSystem`
   ... )

Dask will write one file per dask dataframe partition to this directory. To
optimize access for downstream consumers, we recommend aiming for an in-memory
size of 10-250 MiB per partition. This helps balance worker memory usage
against dask overhead. You may find the `DataFrame.memory_usage_per_partition`
method useful for determining if your data is partitioned optimally.

`to_parquet` has many configuration options affecting both behavior and
performance. Here we highlight a few common options.

Engine
~~~~~~

`to_parquet` supports two backend engines - ``fastparquet`` and ``pyarrow``.
For historical reasons this defaults to ``fastparquet`` (if installed), and
falls back to ``pyarrow``. We recommend using ``pyarrow`` when possible. This
can be explicitly set by passing ``engine="pyarrow"``, but if ``fastparquet``
is not installed this configuration is unnecessary since it will automatically
fallback to ``pyarrow``.

.. code-block:: python

   >>> df.to_parquet(
   ...     "s3://bucket-name/my/parquet/",
   ...     engine="pyarrow"  # explicitly specify the pyarrow engine
   ... )

Metadata
~~~~~~~~

By default dask will write a ``_metadata`` file aggregating row-group metadata
from all files together. While potentially useful when reading data later with
Dask, for large datasets the generation of this file may result in excessive
memory usage (and potentially killed dask workers). As such, we generally
recommend disabling creation of this file by passing in
``write_metadata_file=False``.

.. code-block:: python

   >>> df.to_parquet(
   ...     "s3://bucket-name/my/parquet/",
   ...     ignore_metadata_file=False  # disable writing the metadata file
   ... )

File Names
~~~~~~~~~~

`to_parquet` will write one file per dask dataframe partition to the output
directory. By default these files will have names like ``part.0.parquet``,
``part.1.parquet``, etc. If you wish to alter this naming scheme, you can use
the ``name_function`` keyword argument. This takes a function with the
signature ``name_function(partition: int) -> str``, taking the partition index
for each dask dataframe partition and returning a string to use as the
filename. Note that names returned must sort in the same order as their
partition indices.

.. code-block:: python

    >>> df.npartitions  # 3 partitions (0, 1, and 2)
    3

    >>> df.to_parquet("/path/to/output", name_function=lambda i: f"data-{i}.parquet")

    >>> os.listdir("/path/to/parquet")
    ["data-0.parquet", "data-1.parquet", "data-2.parquet"]

.. _parquet: https://parquet.apache.org/
.. _glob string: https://docs.python.org/3/library/glob.html
.. _fsspec: https://filesystem-spec.readthedocs.io
.. _s3: https://aws.amazon.com/s3/
.. _hdfs: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
