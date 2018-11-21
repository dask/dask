Subclass DataFrames
===================

There are a few projects that subclass or replicate the functionality of Pandas
objects:

-  GeoPandas: for Geospatial analytics
-  PyGDF: for data analysis on GPUs
-  ...

These projects may also want to produce parallel variants of themselves with
Dask, and may want to reuse some of the code in Dask DataFrame.
This document describes how to do this.  It is intended for maintainers of
these libraries and not for general users.


Implement dask, name, meta, and divisions
-----------------------------------------

You will need to implement ``._meta``, ``.dask``, ``.divisions``, and
``._name`` as defined in the :doc:`DataFrame design docs <dataframe-design>`.


Extend Dispatched Methods
-------------------------

If you are going to pass around Pandas-like objects that are not normal Pandas
objects, then we ask you to extend a few dispatched methods.

make_meta
~~~~~~~~~

This function returns an empty version of one of your non-Dask objects, given a
non-empty non-Dask object:

.. code-block:: python

   from dask.dataframe import make_meta

   @make_meta.register(MyDataFrame)
   def make_meta_dataframe(df):
       return df.head(0)


   @make_meta.register(MySeries)
   def make_meta_series(s):
       return s.head(0)


   @make_meta.register(MyIndex)
   def make_meta_index(ind):
       return ind[:0]


Additionally, you should create a similar function that returns a non-empty
version of your non-Dask DataFrame objects filled with a few rows of
representative or random data.  This is used to guess types when they are not
provided.  It should expect an empty version of your object with columns,
dtypes, index name, and it should return a non-empty version:

.. code-block:: python

   from dask.dataframe.utils import meta_nonempty

   @meta_nonempty.register(MyDataFrame)
   def meta_nonempty_dataframe(df):
       ...
       return MyDataFrame(..., columns=df.columns,
                          index=MyIndex(..., name=df.index.name)


   @meta_nonempty.register(MySeries)
   def meta_nonempty_series(s):
       ...


   @meta_nonempty.register(MyIndex)
   def meta_nonempty_index(ind):
       ...


get_parallel_type
~~~~~~~~~~~~~~~~~

Given a non-Dask DataFrame object, return the Dask equivalent:

.. code-block:: python

   from dask.dataframe.core import get_parallel_type

   @get_parallel_type.register(MyDataFrame)
   def get_parallel_type_dataframe(df):
       return MyDaskDataFrame


   @get_parallel_type.register(MySeries)
   def get_parallel_type_series(s):
       return MyDaskSeries


   @get_parallel_type.register(MyIndex)
   def get_parallel_type_index(ind):
       return MyDaskIndex


concat
~~~~~~

Concatenate many of your non-Dask DataFrame objects together.  It should expect
a list of your objects (homogeneously typed):

.. code-block:: python

   from dask.dataframe.methods import concat_dispatch

   @concat_dispatch.register((MyDataFrame, MySeries, MyIndex))
   def concat_pandas(dfs, axis=0, join='outer', uniform=False, filter_warning=True):
       ...


Extension Arrays
----------------

Rather than subclassing Pandas DataFrames, you may be interested in extending
Pandas with `Extension Arrays <https://pandas.pydata.org/pandas-docs/stable/extending.html>`_.

API support for extension arrays isn't in Dask DataFrame yet (though this would
be a good contribution), but many of the complications above will go away if
your objects are genuinely Pandas DataFrames, rather than a subclass.
