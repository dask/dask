.. _dataframe.indexing:

Indexing into Dask DataFrames
=============================

Dask DataFrame supports some of Pandas' indexing behavior.

.. currentmodule:: dask.dataframe

.. autosummary::

   DataFrame.iloc
   DataFrame.loc

Label-based Indexing
--------------------

Just like Pandas, Dask DataFrame supports label-based indexing with the ``.loc``
accessor for selecting rows or columns, and ``__getitem__`` (square brackets)
for selecting just columns.

.. note::

   To select rows, the DataFrame's divisions must be known (see
   :ref:`dataframe.design` and :ref:`dataframe.performance` for more information.)

.. code-block:: python

   >>> import dask.dataframe as dd
   >>> import pandas as pd
   >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [3, 4, 5]},
   ...                   index=['a', 'b', 'c'])
   >>> ddf = dd.from_pandas(df, npartitions=2)
   >>> ddf
   Dask DataFrame Structure:
                      A      B
   npartitions=1
   a              int64  int64
   c                ...    ...
   Dask Name: from_pandas, 1 tasks

Selecting columns:

.. code-block:: python

   >>> ddf[['B', 'A']]
   Dask DataFrame Structure:
                      B      A
   npartitions=1
   a              int64  int64
   c                ...    ...
   Dask Name: getitem, 2 tasks


Selecting a single column reduces to a Dask Series:

.. code-block:: python

   >>> ddf['A']
   Dask Series Structure:
   npartitions=1
   a    int64
   c      ...
   Name: A, dtype: int64
   Dask Name: getitem, 2 tasks

Slicing rows and (optionally) columns with ``.loc``:

.. code-block:: python

   >>> ddf.loc[['b', 'c'], ['A']]
   Dask DataFrame Structure:
                      A
   npartitions=1
   b              int64
   c                ...
   Dask Name: loc, 2 tasks
   
   >>> ddf.loc[df["A"] > 1, ["B"]]
   Dask DataFrame Structure:
                      B
   npartitions=1
   a              int64
   c                ...
   Dask Name: try_loc, 2 tasks

   >>> ddf.loc[lambda df: df["A"] > 1, ["B"]]
   Dask DataFrame Structure:
                      B
   npartitions=1
   a              int64
   c                ...
   Dask Name: try_loc, 2 tasks

Dask DataFrame supports Pandas' `partial-string indexing <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#partial-string-indexing>`_:

.. code-block:: python

   >>> ts = dd.demo.make_timeseries()
   >>> ts
   Dask DataFrame Structure:
                      id    name        x        y
   npartitions=11
   2000-01-31      int64  object  float64  float64
   2000-02-29        ...     ...      ...      ...
   ...               ...     ...      ...      ...
   2000-11-30        ...     ...      ...      ...
   2000-12-31        ...     ...      ...      ...
   Dask Name: make-timeseries, 11 tasks

   >>> ts.loc['2000-02-12']
   Dask DataFrame Structure:
                                     id    name        x        y
   npartitions=1
   2000-02-12 00:00:00.000000000  int64  object  float64  float64
   2000-02-12 23:59:59.999999999    ...     ...      ...      ...
   Dask Name: loc, 12 tasks


Positional Indexing
-------------------

Dask DataFrame does not track the length of partitions, making positional
indexing with ``.iloc`` inefficient for selecting rows. :meth:`DataFrame.iloc`
only supports indexers where the row indexer is ``slice(None)`` (which ``:`` is
a shorthand for.)

.. code-block:: python

   >>> ddf.iloc[:, [1, 0]]
   Dask DataFrame Structure:
                      B      A
   npartitions=1
   a              int64  int64
   c                ...    ...
   Dask Name: iloc, 2 tasks

Trying to select specific rows with ``iloc`` will raise an exception:

.. code-block:: python

   >>> ddf.iloc[[0, 2], [1]]
   Traceback (most recent call last)
     File "<stdin>", line 1, in <module>
   ValueError: 'DataFrame.iloc' does not support slicing rows. The indexer must be a 2-tuple whose first item is 'slice(None)'.
