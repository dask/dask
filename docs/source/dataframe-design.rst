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

1. A pandas object with appropriate dtypes and names. If not empty, an empty
   slice will be taken:

.. code-block:: python

  >>> ddf.map_partitions(foo, meta=pd.DataFrame({'a': [1], 'b': [2]}))

2. A description of the appropriate names and dtypes. This can take several forms:

    * A ``dict`` of ``{name: dtype}`` or an iterable of ``(name, dtype)``
      specifies a dataframe
    * A tuple of ``(name, dtype)`` specifies a series
    * A dtype object or string (e.g. ``'f8'``) specifies a scalar

This keyword is available on all functions/methods that take user provided
callables (e.g. ``DataFrame.map_partitions``, ``DataFrame.apply``, etc...), as
well as many creation functions (e.g. ``dd.from_delayed``).

Categoricals
------------

Dask dataframe divides `categorical data`_ into two types:

- Known categoricals have the ``categories`` known statically (on the ``_meta``
  attribute). Each partition **must** have the same categories as found on the
  ``_meta`` attribute.
- Unknown categoricals don't know the categories statically, and may have
  different categories in each partition.  Internally, unknown categoricals are
  indicated by the presence of ``dd.utils.UNKNOWN_CATEGORIES`` in the
  categories on the ``_meta`` attribute.  Since most dataframe operations
  propogate the categories, the known/unknown status should propogate through
  operations (similar to how ``NaN`` propagates).

For metadata specified as a description (option 2 above), unknown categoricals
are created.

Certain operations are only available for known categoricals. For example,
``df.col.cat.categories`` would only work if ``df.col`` has known categories,
since the categorical mapping is only known statically on the metadata of known
categoricals.

The known/unknown status for a categorical column can be found using the
``known`` property on the categorical accessor:

.. code-block:: python

    >>> ddf.col.cat.known
    False

Additionally, an unknown categorical can be converted to known using
``.cat.as_known()``. If you have multiple categorical columns in a dataframe,
you may instead want to use ``df.categorize(columns=...)``, which will convert
all specified columns to known categoricals. Since getting the categories
requires a full scan of the data, using ``df.categorize()`` is more efficient
than calling ``.cat.as_known()`` for each column (which would result in
multiple scans).

.. code-block:: python

    >>> col_known = ddf.col.cat.as_known()  # use for single column
    >>> col_known.cat.known
    True
    >>> ddf_known = ddf.categorize()        # use for multiple columns
    >>> ddf_known.col.cat.known
    True

To convert a known categorical to an unknown categorical, there is also the
``.cat.as_unknown()`` method. This requires no computation, as it's just a
change in the metadata.

Non-categorical columns can be converted to categoricals in a few different
ways:

.. code-block:: python

    # astype operates lazily, and results in unknown categoricals
    ddf = ddf.astype({'mycol': 'category', ...})
    # or
    ddf['mycol'] = ddf.mycol.astype('category')

    # categorize requires computation, and results in known categoricals
    ddf = ddf.categorize(columns=['mycol', ...])

Additionally, with pandas 0.19.2 and up ``dd.read_csv`` and ``dd.read_table``
can read data directly into unknown categorical columns by specifying a column
dtype as ``'category'``:

.. code-block:: python

    >>> ddf = dd.read_csv(..., dtype={col_name: 'category'})

.. _`categorical data`: http://pandas.pydata.org/pandas-docs/stable/categorical.html

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

In these cases any operation that requires a cleanly partitioned dataframe with
known divisions will have to perform a sort.  This can generally achieved by
calling ``df.set_index(...)``.
