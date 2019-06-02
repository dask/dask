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


.. _extensionarrays:

Extension Arrays
----------------

Rather than subclassing Pandas DataFrames, you may be interested in extending
Pandas with `Extension Arrays
<https://pandas.pydata.org/pandas-docs/stable/extending.html>`_.

All of the first-party extension arrays (those implemented in pandas itself)
are supported directly by dask.

Developers implementing third-party extension arrays (outside of pandas) will
need to do register their ``ExtensionDtype`` with Dask so that it works
correctly in ``dask.dataframe``.

For example, we'll register the *test-only* ``DecimalDtype`` from pandas
test suite.

.. code-block:: python

   from decimal import Decimal
   from dask.dataframe.extensions import make_array_nonempty, make_scalar
   from pandas.tests.extension.decimal import DecimalArray, DecimalDtype

   @make_array_nonempty.register(DecimalDtype)
   def _(dtype):
       return DecimalArray._from_sequence([Decimal('0'), Decimal('NaN')],
                                          dtype=dtype)


   @make_scalar.register(Decimal)
   def _(x):
      return Decimal('1')


Internally, Dask will use this to create a small dummy Series for tracking
metadata through operations.

.. code-block:: python

   >>> make_array_nonempty(DecimalDtype())
   <DecimalArray>
   [Decimal('0'), Decimal('NaN')]
   Length: 2, dtype: decimal

So you (or your users) can now create and store a dask ``DataFrame`` or
``Series`` with your extension array contained within.

.. code-block:: python

   >>> from decimal import Decimal
   >>> import dask.dataframe as dd
   >>> import pandas as pd
   >>> from pandas.tests.extension.decimal import DecimalArray
   >>> ser = pd.Series(DecimalArray([Decimal('0.0')] * 10))
   >>> dser = dd.from_pandas(ser, 3)
   >>> dser
   Dask Series Structure:
   npartitions=3
   0    decimal
   4        ...
   8        ...
   9        ...
   dtype: decimal
   Dask Name: from_pandas, 3 tasks

Notice the ``decimal`` dtype.

.. _dataframe.accessors:

Accessors
---------

Many extension arrays expose their functionality on Series or DataFrame objects
using accessors. Dask provides decorators to register accessors similar to pandas. See
`the pandas documentation on accessors <http://pandas.pydata.org/pandas-docs/stable/development/extending.html#registering-custom-accessors>`_
for more.

.. currentmodule:: dask.dataframe

.. autofunction:: dask.dataframe.extensions.register_dataframe_accessor
.. autofunction:: dask.dataframe.extensions.register_series_accessor
.. autofunction:: dask.dataframe.extensions.register_index_accessor
