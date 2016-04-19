Partitions
==========

Internally a dask dataframe is split into many partitions, and each partition is
one pandas dataframe.  These dataframes are split vertically along the index.
When our index is sorted and we know the values of the divisions of our
partitions, then we can be clever and efficient.

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
