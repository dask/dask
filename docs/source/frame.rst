Dask Frames
===========

Dask.frame is not ready for public use.

.. image:: images/frame.png
   :width: 30%
   :align: right
   :alt: A dask frame

Dask frame implements a blocked DataFrame as a sequence of in-memory Pandas
DataFrames, partitioned along the index.

Partitioning along the index is good because it tells us which blocks hold
which data.  This makes efficient implementations of complex operations like
join and arbitrary groupby possible in a blocked setting as long as we
join/group along the index.

Partitioning along the index is also hard because, when we re-index, we need to
shuffle all of our data between blocks.  This is tricky to code up and
expensive to compute.


Relevant Metadata
-----------------

Dask ``Frame`` objects contain the following data:

*  dask graph - The task dependency graph necessary to compute the frame
*  name - string like ``'f'`` that is the prefix to all keys to define this frame
   like ``('f', 0)``
*  columns - list of column names to improve usability and error checking
*  divisions - tuple of index values on which to partition our blocks

The ``divisions`` attribute, analagous to ``chunks`` in ``dask.array`` is
particularly important.  The values in divisions determine a partitioning of
left-inclusive / right-exclusive ranges on the index::

    divisions -- (10, 20, 40)
    ranges    -- (-oo, 10), [10, 20), [20, 40), [40, oo)


Example Partitioning
--------------------

Consider the following dataset of twelve entries in an account book separated
into three dataframes of size four.  We would like to organize them into
partitions arranged by name::

        name, balance                       name, balance
        Alice, 100                          Alice, 100
        Bob, 200                            Alice, 300
        Alice, 300                          Alice, 600
        Frank, 400                          Alice, 700
                                            Alice, 900
        name, balance
        Dan, 500                            name, balance
        Alice, 600       -> Shuffle ->      Bob, 200
        Alice, 700                          Dan, 500
        Charlie, 800                        Bob, 1200
                                            Charlie, 800
        name, balance
        Alice, 900                          name, balance
        Edith, 1000                         Frank, 400
        Frank, 1100                         Edith, 1000
        Bob, 1200                           Frank, 1100

Notice a few things

1.  On the right Records are now organized by name; given any name it is
    obvious in which block it belongs.
2.  Blocks are roughly the same size (though not exactly).  We prefer evenly
    sized blocks over preditable partition values, (e.g. A, B, C).  Because
    this dataset has many Alices we have a block just for her.
3.  The blocks don't need to be sorted internally

Our divisions in this case are ``['Bob', 'Edith']``


Shuffle
=======

Much of the complex bits of dask.frame are about shuffling records to obtain
this nice arrangement of records along an index.  We do this in two stages

1.  Find good values on which to partition our data
    (e.g. find, ``['Bob', 'Edith']``
2.  Shuffle records from old blocks to new blocks


Find partition values by approximate quantiles
----------------------------------------------

The problem of finding approximate values that regularly divide our data is
exactly the problem of approximate quantiles.  This problem is somewhat
difficult due to the blocked nature of our storage, but has decent solutions.

Currently we compute percentiles/quantiles on the new index of each block and
then merge these together intelligently.


Shard and Recombine
-------------------

Once we know good partition values we need to shard sections out of the old
blocks and then reconstruct those shards in new blocks.  We do this by
leveraging in-core pandas segmentation on each block, and then using a special
appendable on-disk data structure, pframe_.


Supported API
-------------

Dask frame supports the following API from Pandas

* Trivially parallelizable (fast):
    *  Elementwise operations:  ``df.x + df.y``
    *  Row-wise selections:  ``df[df.x > 0]``
    *  Loc:  ``df.loc[4.0:10.5]``
    *  Common aggregations:  ``df.x.max()``
    *  Is in:  ``df[df.x.isin([1, 2, 3])]``
* Cleverly parallelizable (also fast):
    *  groupby-aggregate (with common aggregations): ``df.groupby(df.x).y.max()``
    *  value_counts:  ``df.x.value_counts``
    *  Drop duplicates:  ``df.x.drop_duplicates()``
* Requires shuffle (slow-ish, unless on index)
    *  Set index:  ``df.set_index(df.x)``
    *  groupby-apply (with anything):  ``df.groupby(df.x).apply(myfunc)``
* Ingest
    *  ``pd.read_csv``  (in all its glory)

Dask frame also introduces some new API

* Requires full dataset read, but otherwise fast
    *  Approximate quantiles:  ``df.x.quantiles([25, 50, 75])``
    *  Convert object dtypes to categoricals:  ``df.categorize()``
* Ingest
    *  Read from bcolz (efficient on-disk column-store):
      ``from_bcolz(x, index='mycol', categorize=True)``


.. _Chest: http://github.com/ContinuumIO/chest
.. _pframe: pframe.html
