Dask Dataframe and SQL
======================

SQL is a method for executing tabular computation on database servers.
Simlar operations can be done on Dask Dataframes. Users commonly wish
to link the two:

- perform SQL-like operations on Dask Dataframes

- load data from databases using SQL, and process the results in Dask

- save the output of Dask commputations in a database

- use the Dask API to specify queries, but execute some or part of the
  query on the database

- which is the best tool for my workload, SQL or dask?

This document describes the connection between Dask and SQL-databases
and serves to clarify several of the questions that we commonly
receive from users.

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

Loading from SQL with read_sql_table
------------------------------------

Dask allows you to build dataframes from SQL tables and queries using the
function :func:`dask.dataframe.read_sql_table`, based on the `Pandas version`_,
sharing most arguments, and using SQLAlchemy for the actual handling of the
queries.

.. _Pandas version: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_table.html

Since Dask is designed to work with larger-than-memory datasets, or be distributed
on a cluster, the following are the main differences to watch out for

- Dask does not support arbitray text queries, only whole tables and SQLAlchemy
  `sql expressions`_

- the engine argument must be a `URI string`_, not an SQLAlchemy engine/connection

- partitioning information is *required*, which can be as simple as providing
  an index column argument, or can be more explicit (see below)

- the chunksize argument is not used, since the partitioning must be via an
  index column

.. _URI string: https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls
.. _sql expressions: https://docs.sqlalchemy.org/en/13/core/tutorial.html

If you need something more flexible than this, then skip to the next section.

**Why the differences**

**Index Column**

**Specific partitioning**

**SQLAlchemy expressions**

Load from SQL, manual approaches
--------------------------------

**Delayed functions**

Often you know more about your data and server than the generic approach above
allows. Indeed, some database-like servers may simply not be supported by
SQLAlchemy, or provide an alternate API which is better optimised.

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

Where you must provide your own code for setting up a connection to the server,
your own query, and a way to format that query to be specific to each partition.
For example, you might have ranges or specific unique values with a WHERE
clause. The ``known_types`` to transform the dataframe partition and provide
a meta will help for consistency and avoid Dask having to analyse one partition
up front the guess the columns/types; you may also want to explicitly set the index.
It is also a good idea to provide
``divisions`` (the start/end of each partition in the index column), if possible,
since you likely know these from the subqueries you are constructing.

**Stream via client**

**Access data files directly**

Some database systems such as Apache Hive store their data in a location
and format that may be directly accessible to Dask, such as parquet files
on S3. In cases where your SQL query would read whole datasets and pass
them to Dask, it's probably faster to read the source data files directly
(the output from the database is very likely the bottleneck).

Query pushdown?
---------------

If you define a query based on a database table, then only use some columns
of the output, you may expect that Dask is able to tell the database server
to only send some of the table's data. Dask is not currently able to
do this "pushdown" optimisation, and you would need to change your query.
We may be able to resolve this in the future (:issue:`6388`).

If the divisions on your dataframe are well defined, then selections on the
index may successfullly only

Database or Dask?
-----------------

A database server is able to process tabular data and roduce results just like
Dask Dataframe. Why would you choose to use one over the other?

These days a database server can be a sharded/distributed system, capable of
handling tables with millions of rows. Most database implementations are
geared towards row-wise retreival and (atomic) updates of s amll subset of a
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
would be best to profile (and keep in mind other users of the resources!).
If you need
to combine data from different sources, dask may be your only options

You may find the dask API easier to use than writing SQL (if you
are already used to Pandas), and the diagnostic feedback more unseful.
These points can debatably be in Dask's favour.
