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

The following packages may be of interest:
- `blazingSQL`_, part of the Rapids project, implements SQL queries using ``cuDF``
 and Dask, for execution on CUDA/GPU-enabled hardware, including referencing
 externally-stored data
- `pandasql`_ allows executing SQL queries on a pandas tabel by writing the data to
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
on a cluster, the following are the main differences to watch out for:

- the engine argument must be a string, not an SQLAlchemy engine (which cannot
 be serialised)
- partitioning information is *required*, which can be as simple as providing
 an index column argument, or can be more explicit (see below)
- the chunksize argument is not used, since the partitioning must be via an
 index column

If you need something more flexible than this, then skip to the next section.

**Why the differences**

**Index Column**

**Specific partitioning**

**SQLAlchemy expressions**

Load from SQL, manual approaches
--------------------------------

**Delayed functions**

**Stream via client**

Query pushdown?
---------------

Database or Dask?
-----------------
