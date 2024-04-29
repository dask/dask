.. _dataframe.hive:

Using Hive Partitioning with Dask
=================================

.. currentmodule:: dask.dataframe

It is sometimes useful to write your dataset with a hive-like directory scheme.
For example, if your dataframe contains ``'year'`` and ``'semester'`` columns,
a hive-based directory structure might look something like the following::

    output-path/
    ├── year=2022/
    │   ├── semester=fall/
    │   │   └── part.0.parquet
    │   └── semester=spring/
    │       ├── part.0.parquet
    │       └── part.1.parquet
    └── year=2023/
        └── semester=fall/
            └── part.1.parquet

The use of this self-describing structure implies that all rows within
the ``'output-path/year=2022/semester=fall/'`` directory will contain
the value ``2022`` in the ``'year'`` column and the value ``'fall'``
in the ``'semester'`` column.

The primary advantage of generating a hive-partitioned dataset
is that certain IO filters can be applied by :func:`read_parquet`
without the need to parse any file metadata. In other words,
the following command will typically be faster when the dataset
is already hive-partitioned on the ``'year'`` column.

.. code-block:: python

    >>> dd.read_parquet("output-path", filters=[("year", ">", 2022)])

Writing Parquet Data with Hive Partitioning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Dask's :func:`to_parquet` function will produce a hive-partitioned
directory scheme automatically when the ``partition_on`` option is used.

.. code-block:: python

    >>> df.to_parquet("output-path", partition_on=["year", "semester"])

    >>> os.listdir("output-path")
    ["year=2022", "year=2023"]

    >>> os.listdir("output-path/year=2022")
    ["semester=fall", "semester=spring"]

    >>> os.listdir("output-path/year=2022/semester=spring")
    ['part.0.parquet', 'part.1.parquet']


It is important to recognize that Dask will **not** aggregate the
data files written within each of the leaf directories. This is
because each of the DataFrame partitions is written independently 
during the execution of the :func:`to_parquet` task graph. In order 
to write out data for partition `i`, the partition-i write task will
perform a `groupby` operation on columns ``["year", "semester"]``,
and then each distinct group will be written to the corresponding
directory using the file name ``'part.{i}.parquet'``. Therefore, it
is possible for a hive-partitioned write to produce a large number
of files in every leaf directory (one for each DataFrame partition).

If your application requires you to produce a single parquet file
for each hive partition, one possible solution is to sort or shuffle
on the partitioning columns before calling :func:`to_parquet`.

.. code-block:: python

    >>> partition_on = ["year", "semester"]

    >>> df.shuffle(on=partition_on).to_parquet(partition_on=partition_on)

Using a global shuffle like this is extremely expensive, and should be
avoided whenever possible. However, it is also guaranteed to produce
the minimum number of files, which may be worth the sacrifice at times.


Reading Parquet Data with Hive Partitioning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In most cases, :func:`read_parquet` will process hive-partitioned
data automatically. By default, all hive-partitioned columns will
be interpreted as categorical columns.

.. code-block:: python

    >>> ddf = dd.read_parquet("output-path", columns=["year", "semester"])

    >>> ddf
    Dask DataFrame Structure:
                            year         semester
    npartitions=4                                  
                category[known]  category[known]
                            ...              ...
                            ...              ...
                            ...              ...
                            ...              ...
    Dask Name: read-parquet, 1 graph layer

    >>> ddf.compute()
    year semester
    0  2022     fall
    1  2022     fall
    2  2022     fall
    3  2022   spring
    4  2022   spring
    5  2022   spring
    6  2023     fall
    7  2023     fall


Defining a Custom Partitioning Schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to specify a custom schema for the hive-partitioned columns. The columns
will then be read using the specified types and not as `category`.

.. code-block:: python

    >>> schema = pa.schema([("year", pa.int16()), ("semester", pa.string())])

    >>> ddf2 = dd.read_parquet(
    ...     path,
    ...     columns=["year", "semester"],
    ...     dataset={"partitioning": {"flavor": "hive", "schema": schema}}
    ... )
    Dask DataFrame Structure:
                    year semester
    npartitions=4                
                int16   object
                    ...      ...
                    ...      ...
                    ...      ...
                    ...      ...


If any of your hive-partitioned columns contain null values, you
**must** specify the partitioning schema in this way.

Although it is not required, we also recommend that you specify
the partitioning schema if you need to partition on high-cardinality
columns. This is because the default ``'category'`` dtype will
track the known categories in a way that can significantly increase
the overall memory footprint of your Dask collection. In fact,
:func:`read_parquet` already clears the "known categories" of other
columns for this same reason (see :doc:`dataframe-categoricals`).


Best Practices
--------------

Although hive partitioning can sometimes improve read performance
by simplifying filtering, it can also lead to degraded performance
and errors in other cases.


Avoid High Cardinality
~~~~~~~~~~~~~~~~~~~~~~

A good rule of thumb is to avoid partitioning on `float` columns,
or any column containing many unique values (i.e. high cardinality).

The most common cause of poor user experience with hive partitioning
is high-cardinality of the partitioning column(s). For example, if 
you try to partition on a column with millions of unique values, then
`:func:`to_parquet`` will need to generate millions of directories. The
management of these directories is likely to put strain on the file
system, and the need for many small files within each directory
is sure to compound the issue.


Use Simple Data Types for Partitioning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since hive-partitioned data is "self describing," we suggest that
you avoid partitioning on complex data types, and opt for integer
or string-based data types whenever possible. If your data type
cannot be easily inferred from the string value used to define the
directory name, then the IO engine may struggle to parse the values.

For example, directly partitioning on a column with a ``datetime64``
dtype might produce a directory name like the following::

    output-path/
    ├── date=2022-01-01 00:00:00/
    ├── date=2022-02-01 00:00:00/
    ├── ...
    └── date=2022-12-01 00:00:00/

These directory names will not be correctly interpreted as
``datetime64`` values, and are even considered illegal on Windows
systems. For more-reliable behavior, we recommend that such a column
be decomposed into one or more "simple" columns. For example, one
could easily use ``'date'`` to construct ``'year'``, ``'month'``,
and ``'day'`` columns (as needed).


Aggregate Files at Read Time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::
    The ``aggregate_files`` argument is currently listed as
    experimental. However, there are currently no plans to remove
    the argument or change it's behavior in a future release.

Since hive-partitioning will typically produce a large number of
small files, :func:`read_parquet` performance will usually benefit
from proper usage of the ``aggregate_files`` argument. Take the
following dataset for example::

    dataset-path/
    ├── region=1/
    │   ├── section=a/
    │   │   └── 01.parquet
    │   │   └── 02.parquet
    │   │   └── 03.parquet
    │   ├── section=b/
    │   └── └── 04.parquet
    │   └── └── 05.parquet
    └── region=2/
        ├── section=a/
        │   ├── 06.parquet
        │   ├── 07.parquet
        │   ├── 08.parquet

If we set ``aggregate_files=True`` for this case, we are telling Dask
that any of the parquet data files may be aggregated into the same output
DataFrame partition. If, instead, we specify the name of a partitioning
column (e.g. ``'region'`` or ``'section'``), we allow the aggregation of
any two files sharing a file path up to, and including, the corresponding
directory name. For example, if ``aggregate_files`` is set to ``'section'``,
``04.parquet`` and ``05.parquet`` may be aggregated together, but
``03.parquet`` and ``04.parquet`` cannot be. If, however, ``aggregate_files``
is set to ``'region'``, ``04.parquet`` may be aggregated with ``05.parquet``,
**and** ``03.parquet`` may be aggregated with ``04.parquet``.

Using ``aggregate_files`` will typically improve performance by making it
more likely for DataFrame partitions to approach the size specified by the
``blocksize`` argument. In contrast, default behavior may produce a large
number of partitions that are much smaller than ``blocksize``.
