Dask Dataframe and SQL
======================

SQL is a method for executing tabular computation on database servers.
Similar operations can be done on Dask Dataframes. Users commonly wish
to link the two together.

This document describes the connection between Dask and SQL-databases
and serves to clarify several of the questions that we commonly
receive from users.

.. contents::
    :local:
    :depth: 1
    :backlinks: top

Does Dask implement SQL?
------------------------

The short answer is "no". Dask has no parser or query planner for SQL
queries. However, the Pandas API, which is largely identical for
Dask Dataframes, has many analogues to SQL operations. A good
description for mapping SQL onto Pandas syntax can be found in the
`pandas docs`_.

.. _pandas docs: https://pandas.pydata.org/docs/getting_started/comparison/comparison_with_sql.html

The following packages may be of interest

- `blazingSQL`_, part of the Rapids project, implements SQL queries using ``cuDF``
  and Dask, for execution on CUDA/GPU-enabled hardware, including referencing
  externally-stored data

- `pandasql`_ allows executing SQL queries on a pandas table by writing the data to
  ``SQLite``, which may be useful for small toy examples (this package has not been
  maintained for some time)

.. _blazingSQL: https://docs.blazingdb.com/docs
.. _pandasql: https://github.com/yhat/pandasql/

Database or Dask?
-----------------

A database server is able to process tabular data and produce results just like
Dask Dataframe. Why would you choose to use one over the other?

These days a database server can be a sharded/distributed system, capable of
handling tables with millions of rows. Most database implementations are
geared towards row-wise retrieval and (atomic) updates of small subset of a
table. Configuring a database to be fast for a particular
sort of query can be challenging, but assuming all your data is already in the
database, it may well be the best solution - particularly if you understand
something about SQL query plan optimisation. A SQL implementation can
very efficiently analyse a query to only extract a small part of a table
for consideration, when the rest is excluded by conditionals.

Dask is much more flexible than a database, and designed explicitly
to work with larger-than-memory datasets, in parallel, and potentially distributed
across a cluster. If your workflow is not well suited to SQL, use dask. If
your database server struggles with volume, dask may do better. It
would be best to profile your queries
(and keep in mind other users of the resources!). If you need
to combine data from different sources, dask may be your best option.

You may find the dask API easier to use than writing SQL (if you
are already used to Pandas), and the diagnostic feedback more useful.
These points can debatably be in Dask's favour.

Loading from SQL with read_sql_table
------------------------------------

Dask allows you to build dataframes from SQL tables and queries using the
function :func:`dask.dataframe.read_sql_table`, based on the `Pandas version`_,
sharing most arguments, and using SQLAlchemy for the actual handling of the
queries. You may need to install additional driver packages for your chosen
database server.

.. _Pandas version: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_table.html

Since Dask is designed to work with larger-than-memory datasets, or be distributed
on a cluster, the following are the main differences versus Pandas to watch out for

- Dask does not support arbitrary text queries, only whole tables and SQLAlchemy
  `sql expressions`_

- the engine argument must be a `URI string`_, not an SQLAlchemy engine/connection

- partitioning information is *required*, which can be as simple as providing
  an index column argument, or can be more explicit (see below)

- the chunksize argument is not used, since the partitioning must be via an
  index column

.. _URI string: https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls
.. _sql expressions: https://docs.sqlalchemy.org/en/13/core/tutorial.html

If you need something more flexible than this, or the
method fails for you (e.g., on type inference), then skip to the next section.

Why the differences
^^^^^^^^^^^^^^^^^^^

Dask is intended to make processing large volumes of data possible, including
potentially distributing that processing across a cluster. For the retrieval of
data from SQL servers, this means that the query must be partitionable: that
each partition can be fetched independently of others and not dependent on
some global state, and that the definitions of the tasks must be serialisable,
i.e., can be represented as a stream of bytes communicated to workers.

The constraints mean that we cannot directly accept SQLAlchemy engines
or connection objects, since they have internal state (buffers, etc.)
that cannot be serialised. A `URI string`_  must be used, which can be
recreated into a fresh engine on the workers.
Similarly, we cannot accommodate chunked queries
which rely on the internal state of a database cursor; nor LIMIT/OFFSET
queries, which are not guaranteed to be repeatable, and involve scanning
the whole query on th server (which is very inefficient).

**If** your data is small enough not to require Dask's out-of-core and/or
distributed capabilities, then you are probably better to use Pandas or SQLAlchemy
directly.

Index Column
^^^^^^^^^^^^

We need a way to turn a single main query into sub-queries for each
partition. For most reasonable database tables, there should be an obvious
column which can be used for partitioning - it is probably numeric,
and should certainly be indexed in the database. The latter condition
is important, since many simultaneous queries will hit your server once
Dask starts to compute.

By providing just a column name for the index argument, you imply that the
column is numeric, and Dask guesses a reasonable partitioning by evenly
splitting the space between minimum and maximum values into ``npartitions``
intervals. You can also provide the max/min that you would like to
consider so that Dask doesn't need to query for these. Alternatively,
you can have Dask fetch the first few row (5 by default) and use
them to guess the typical bytes/row, and base the partitioning size on
this. Needless to say, the results will vary a lot for tables that are
not uncommonly homogenous.

Specific partitioning
^^^^^^^^^^^^^^^^^^^^^

In some cases, you may have a very good idea of how to partition the data,
for example based on a column that has a finite number of unique values
or categories. This enables using string columns, or anything with a
natural ordering, for the index column, not only numerical types.

In this case, you would provide a specific set of ``divisions``,
the start/end values of the index column for each partition. For example,
if a column happened to contain a random ID in hex string format, then you
could specify 16 partitions with

.. code-block:: python

    df = read_sql_table("mytable", divisions=list("0123456789abcdefh"),
                        index_col="hexID")

so the first partition would have IDs with values ``"0" <= hexID < "1"``, i.e.,
leading character "0".

SQLAlchemy expressions
^^^^^^^^^^^^^^^^^^^^^^

Since we only send the database connection URI and not the engine object,
we cannot rely on SQLAlchemy's table class inference and ORM to conduct queries. However, we can
use the "select" `sql expressions`_, which only get formatted into a text query at
the point of execution.

.. code-block:: python

    from sqlalchemy import sql
    number = sql.column("number")
    name = sql.column("name")
    s1 = sql.select([
            number, name, sql.func.length(name).label("lenname")
        ]
        ).select_from(sql.table("test"))
    data = read_sql_table(
        "test", db, npartitions=2, index_col=number
    )

Here we have also demonstrated the use of the function ``length`` to
perform an operation server-side. Note that it is necessary to *label* such
operations, but you can use them for the index column (by name or expression),
so long as it is also
in the set of selected columns. If using for the index/partitioning, the
column should still be indexed in the database, for performance.
One of the most important functions to consider is ``cast`` to specify the
output data type or conversion in the database, if pandas is having
trouble inferring the data type.

You should be warned, that SQLAlchemy expressions take some time to get
used to, and you can practice with Pandas first, reading only the first small
chunk of a query, until things look right. You can find a more complete
object-oriented example in `this gist`_

.. _this gist: https://gist.github.com/quasiben/08a7f291039db2b04c2e28e1a6c21e3b

Load from SQL, manual approaches
--------------------------------

If ``read_sql_table`` is not sufficient for your needs, you can try one of
the following methods.

Delayed functions
^^^^^^^^^^^^^^^^^

Often you know more about your data and server than the generic approach above
allows. Indeed, some database-like servers may simply not be supported by
SQLAlchemy, or provide an alternate API which is better optimised
(`snowflake example`_).

.. _snowflake example: https://www.saturncloud.io/s/snowflake-and-dask/

If you already have a way to fetch data from the database in partitions,
then you can wrap this function in :func:`dask.delayed` and construct a
dataframe this way. It might look something like

.. code-block:: python

   from dask import delayed
   import dask.dataframe as dd

   @delayed
   def fetch_partition(part):
       conn = establish_connection()
       df = fetch_query(base_query.format(part))
       return df.astype(known_types)

    ddf = dd.from_delayed([fetch_partition(part) for part in parts],
                          meta=known_types,
                          divisions=div_from_parts(parts))

Where you must provide your own functions for setting up a connection to the server,
your own query, and a way to format that query to be specific to each partition.
For example, you might have ranges or specific unique values with a WHERE
clause. The ``known_types`` here is used to transform the dataframe partition and provide
a ``meta``, to help for consistency and avoid Dask having to analyse one partition
up front to guess the columns/types; you may also want to explicitly set the index.
Furthermore, it is a good idea to provide
``divisions`` (the start/end of each partition in the index column), if possible,
since you likely know these from the subqueries you are constructing.

Stream via client
^^^^^^^^^^^^^^^^^

In some cases, the workers may not have access to data, but the client does;
or the initial loading time of the data is not important, so long as the
dataset is then held in cluster memory and available for dask-dataframe
queries. It is possible to construct the dataframe by uploading chunks of
data from the client:

See a complete example of how to do this `here`_

.. _here: https://stackoverflow.com/questions/62818473/why-dasks-read-sql-table-requires-a-index-col-parameter/62821858#62821858


Access data files directly
^^^^^^^^^^^^^^^^^^^^^^^^^^

Some database systems such as Apache Hive store their data in a location
and format that may be directly accessible to Dask, such as parquet files
on S3 or HDFS. In cases where your SQL query would read whole datasets and pass
them to Dask, the streaming of data from the database is very likely the
bottleneck, and it's probably faster to read the source data files directly.

Query pushdown?
---------------

If you define a query based on a database table, then only use some columns
of the output, you may expect that Dask is able to tell the database server
to only send some of the table's data. Dask is not currently able to
do this "pushdown" optimisation, and you would need to change your query using
the SQL expression syntax.
We may be able to resolve this in the future (:issue:`6388`).

If the divisions on your dataframe are well defined, then selections on the
index may successfully avoid reading irrelevant partitions.
