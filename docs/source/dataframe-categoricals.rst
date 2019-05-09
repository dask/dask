Categoricals
============

Dask DataFrame divides `categorical data`_ into two types:

- Known categoricals have the ``categories`` known statically (on the ``_meta``
  attribute).  Each partition **must** have the same categories as found on the
  ``_meta`` attribute
- Unknown categoricals don't know the categories statically, and may have
  different categories in each partition.  Internally, unknown categoricals are
  indicated by the presence of ``dd.utils.UNKNOWN_CATEGORIES`` in the
  categories on the ``_meta`` attribute.  Since most DataFrame operations
  propagate the categories, the known/unknown status should propagate through
  operations (similar to how ``NaN`` propagates)

For metadata specified as a description (option 2 above), unknown categoricals
are created.

Certain operations are only available for known categoricals.  For example,
``df.col.cat.categories`` would only work if ``df.col`` has known categories,
since the categorical mapping is only known statically on the metadata of known
categoricals.

The known/unknown status for a categorical column can be found using the
``known`` property on the categorical accessor:

.. code-block:: python

    >>> ddf.col.cat.known
    False

Additionally, an unknown categorical can be converted to known using
``.cat.as_known()``.  If you have multiple categorical columns in a DataFrame,
you may instead want to use ``df.categorize(columns=...)``, which will convert
all specified columns to known categoricals.  Since getting the categories
requires a full scan of the data, using ``df.categorize()`` is more efficient
than calling ``.cat.as_known()`` for each column (which would result in
multiple scans):

.. code-block:: python

    >>> col_known = ddf.col.cat.as_known()  # use for single column
    >>> col_known.cat.known
    True
    >>> ddf_known = ddf.categorize()        # use for multiple columns
    >>> ddf_known.col.cat.known
    True

To convert a known categorical to an unknown categorical, there is also the
``.cat.as_unknown()`` method. This requires no computation as it's just a
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

Additionally, with Pandas 0.19.2 and up, ``dd.read_csv`` and ``dd.read_table``
can read data directly into unknown categorical columns by specifying a column
dtype as ``'category'``:

.. code-block:: python

    >>> ddf = dd.read_csv(..., dtype={col_name: 'category'})

.. _`categorical data`: https://pandas.pydata.org/pandas-docs/stable/categorical.html

Moreover, with Pandas 0.21.0 and up, ``dd.read_csv`` and ``dd.read_table`` can read
data directly into *known* categoricals by specifying instances of
``pd.api.types.CategoricalDtype``:

.. code-block:: python

    >>> dtype = {'col': pd.api.types.CategoricalDtype(['a', 'b', 'c'])}
    >>> ddf = dd.read_csv(..., dtype=dtype)
