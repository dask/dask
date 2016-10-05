Internal Design
===============

Dask dataframes coordinate many Pandas DataFrames/Series arranged along an
index. We define a ``dask.dataframe`` object with the following components:

- A dask graph with a special set of keys designating partitions, such as
  ``('x', 0), ('x', 1), ...``.
- A name to identify which keys in the dask graph refer to this dataframe, such
  as ``'x'``.
- An empty pandas object containing appropriate metadata (e.g.  column names,
  dtypes, etc...).
- A sequence of partition boundaries along the index, called ``divisions``.

Metadata
--------

Many dataframe operations rely on knowing the name and dtype of columns. To
keep track of this information, all ``dask.dataframe`` objects have a ``_meta``
attribute which contains an empty pandas object with the same dtypes and names.
For example:

.. code-block:: python

   >>> df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
   >>> ddf = dd.from_pandas(df, npartitions=2)
   >>> ddf._meta
   Empty DataFrame
   Columns: [a, b]
   Index: []
   >>> ddf._meta.dtypes
   a     int64
   b    object
   dtype: object

Internally ``dask.dataframe`` does its best to propagate this information
through all operations, so most of the time a user shouldn't have to worry
about this.  Usually this is done by evaluating the operation on a small sample
of fake data, which can be found on the ``_meta_nonempty`` attribute:

.. code-block:: python

   >>> ddf._meta_nonempty
      a    b
   0  1  foo
   1  1  foo

Sometimes this operation may fail in user defined functions (e.g. when using
``DataFrame.apply``), or may be prohibitively expensive.  For these cases, many
functions support an optional ``meta`` keyword, which allows specifying the
metadata directly, avoiding the inference step. For convenience, this supports
several options:

- A pandas object with appropriate dtypes and names. If not empty, an empty
  slice will be taken:

.. code-block:: python

  >>> ddf.map_partitions(foo, meta=pd.DataFrame({'a': [1], 'b': [2]}))

- A description of the appropriate names and dtypes. This can take several forms:

    * A ``dict`` of ``{name: dtype}`` or an iterable of ``(name, dtype)``
      specifies a dataframe
    * A tuple of ``(name, dtype)`` specifies a series
    * A dtype object or string (e.g. ``'f8'``) specifies a scalar

This keyword is available on all functions/methods that take user provided
callables (e.g. ``DataFrame.map_partitions``, ``DataFrame.apply``, etc...), as
well as many creation functions (e.g. ``dd.from_delayed``).

Partitions
----------

Internally a dask dataframe is split into many partitions, and each partition
is one pandas dataframe.  These dataframes are split vertically along the
index.  When our index is sorted and we know the values of the divisions of our
partitions, then we can be clever and efficient with expensive algorithms (e.g.
groupby's, joins, etc...).

For example, if we have a time-series index then our partitions might be
divided by month.  All of January will live in one partition while all of
February will live in the next.  In these cases operations like ``loc``,
``groupby``, and ``join/merge`` along the index can be *much* more efficient
than would otherwise be possible in parallel.  You can view the number of
partitions and divisions of your dataframe with the following fields:

.. code-block:: python

   >>> df.npartitions
   4
   >>> df.divisions
   ['2015-01-01', '2015-02-01', '2015-03-01', '2015-04-01', '2015-04-31']

Divisions includes the minimum value of every partition's index and the maximum
value of the last partition's index.  In the example above if the user searches
for a specific datetime range then we know which partitions we need to inspect
and which we can drop:

.. code-block:: python

   >>> df.loc['2015-01-20': '2015-02-10']  # Must inspect first two partitions

Often we do not have such information about our partitions.  When reading CSV
files for example we do not know, without extra user input, how the data is
divided.  In this case ``.divisions`` will be all ``None``:

.. code-block:: python

   >>> df.divisions
   [None, None, None, None, None]

In these cases any operation that requires a cleanly partitioned dataframe with known divisions will have to perform a sort.  This can generally achieved by calling ``df.set_index(...)``.
