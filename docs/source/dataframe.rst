DataFrame
=========

A Dask DataFrame is a large parallel dataframe composed of many smaller Pandas
dataframes, split along the index.  These pandas dataframes may live on disk
for larger-than-memory computing on a single machine, or on many different
machines in a cluster.

Dask.dataframe implements a commonly used subset of the Pandas_ interface
including elementwise operations, reductions, grouping operations, joins,
timeseries algorithms, and more.  It copies the Pandas interface for these
operations exactly and so should be very familiar to Pandas users.  Because
Dask.dataframe operations merely coordinate Pandas operations they usually
exhibit similar performance characteristics as are found in Pandas.

.. _Pandas: http://pandas.pydata.org/

.. toctree::
   :maxdepth: 1

   dataframe-overview.rst
   dataframe-create.rst
   dataframe-api.rst

Other topics

.. toctree::
   :maxdepth: 1

   dataframe-design.rst
   dataframe-groupby.rst
