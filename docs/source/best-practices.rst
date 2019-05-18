Best Practices
==============

It is easy to get started with Dask's APIs, but using them *well* requires some
experience. This page contains suggestions for best practices, and includes
solutions to common problems.

This document specifically focuses on best practices that are shared among all
of the Dask APIs.  Readers may first want to investigate one of the
API-specific Best Practices documents first.

-  :doc:`Arrays <array-best-practices>`
-  :doc:`DataFrames <dataframe-best-practices>`
-  :doc:`Delayed <delayed-best-practices>`


Start Small
-----------

Parallelism brings extra complexity and overhead.
Sometimes it's necessary for larger problems, but often it's not.
Before adding a parallel computing system like Dask to your workload you may
want to first try some alternatives:

-   **Use better algorithms or data structures**:  NumPy, Pandas, Scikit-Learn
    may have faster functions for what you're trying to do.  It may be worth
    consulting with an expert or reading through their docs again to find a
    better pre-built algorithm.

-   **Better file formats**:  Efficient binary formats that support random
    access can often help you manage larger-than-memory datasets efficiently and
    simply.  See the `Store Data Efficiently`_ section below.

-   **Compiled code**:  Compiling your Python code with Numba or Cython might
    make parallelism unnecessary.  Or you might use the multi-core parallelism
    available within those libraries.

-   **Sampling**:  Even if you have a lot of data, there might not be much
    advantage from using all of it.  By sampling intelligently you might be able
    to derive the same insight from a much more manageable subset.

-   **Profile**:  If you're trying to speed up slow code it's important that
    you first understand why it is slow.  Modest time investments in profiling
    your code can help you to identify what is slowing you down.  This
    information can help you make better decisions about if parallelism is likely
    to help, or if other approaches are likely to be more effective.


Use The Dashboard
-----------------

Dask's dashboard helps you to understand the state of your workers.
This information can help to guide you to efficient solutions.
In parallel and distributed computing there are new costs to be aware of and so
your old intuition may no longer be true.  Working with the dashboard can help
you relearn about what is fast and slow and how to deal with it.

See :doc:`Documentation on Dask's dashboard <diagnostics-distributed>` for more
information.


Avoid Very Large Partitions
---------------------------

Your chunks of data should be small enough so that many of them fit in a
worker's available memory at once.  You often control this when you select
partition size in Dask DataFrame or chunk size in Dask Array.

Dask will likely manipulate as many chunks in parallel on one machine as you
have cores on that machine.  So if you have 1 GB chunks and ten
cores, then Dask is likely to use *at least* 10 GB of memory.  Additionally,
it's common for Dask to have 2-3 times as many chunks available to work on so
that it always has something to work on.

If you have a machine with 100 GB and 10 cores, then you might want to choose
chunks in the 1GB range.  You have space for ten chunks per core which gives
Dask a healthy margin, without having tasks that are too small

Note that you also want to avoid chunk sizes that are too small.  See the next
section for details.


Avoid Very Large Graphs
-----------------------

Dask workloads are composed of *tasks*.
A task is a Python function, like ``np.sum`` applied onto a Python object,
like a Pandas dataframe or NumPy array.  If you are working with Dask
collections with many partitions, then every operation you do, like ``x + 1``
likely generates many tasks, at least as many as partitions in your collection.

Every task comes with some overhead.  This is somewhere between 200us and 1ms.
If you have a computation with thousands of tasks this is fine, there will be
about a second of overhead, and that may not trouble you.

However when you have very large graphs with millions of tasks then this may
become troublesome, both because overhead is now in the 10 minutes to hours
range, and also because the overhead of dealing with such a large graph can
start to overwhelm the scheduler.

There are a few things you can do to address this:

-   Build smaller graphs.  You can do this by ...

    -  **Increasing your chunk size:**  If you have a 1000 GB of data and are using
       10 MB chunks, then you have 100,000 partitions.  Every operation on such
       a collection will generate at least 100,000 tasks.

       However if you increase your chunksize to 1 GB or even a few GB then you
       reduce the overhead by orders of magnitude.  This requires that your
       workers have much more than 1 GB of memory, but that's typical for larger
       workloads.

    -  **Fusing operations together:** Dask will do a bit of this on its own, but you
       can help it.  If you have a very complex operation with dozens of
       sub-operations, maybe you can pack that into a single Python function
       and use a function like ``da.map_blocks`` or ``dd.map_partitions``.

       In general, the more administrative work you can move into your functions
       the better.  That way the Dask scheduler doesn't need to think about all
       of the fine-grained operations.

    -  **Breaking up your computation:** For very large workloads you may also want to
       try sending smaller chunks to Dask at a time.  For example if you're
       processing a petabyte of data but find that Dask is only happy with 100
       TB, maybe you can break up your computation into ten pieces and submit
       them one after the other.


Learn Techniques For Customization
----------------------------------

The high level Dask collections (array, dataframe, bag) include common
operations that follow standard Python APIs from NumPy and Pandas.
However, many Python workloads are complex and may require operations that are
not included in these high level APIs.

Fortunately, there are many options to support custom workloads:

-   All collections have a ``map_partitions`` or ``map_blocks`` function, that
    applies a user provided function across every Pandas dataframe or NumPy array
    in the collection.  Because Dask collections are made up of normal Python
    objects, it's often quite easy to map custom functions across partitions of a
    dataset without much modification.

    .. code-block:: python

       df.map_partitions(my_custom_func)

-   More complex ``map_*`` functions.  Sometimes your custom behavior isn't
    embarrassingly parallel, but requires more advanced communication.  For
    example maybe you need to communicate a little bit of information from one
    partition to the next, or maybe you want to build a custom aggregation.

    Dask collections include methods for these as well.

-   For even more complex workloads you can convert your collections into
    individual blocks, and arrange those blocks as you like using Dask Delayed.
    There is usually a ``to_delayed`` method on every collection.

.. currentmodule:: dask.dataframe

.. autosummary::

    map_partitions
    rolling.map_overlap
    Aggregation

.. currentmodule:: dask.array

.. autosummary::

    blockwise
    map_blocks
    map_overlap
    reduction


Stop Using Dask When No Longer Needed
-------------------------------------

In many workloads it is common to use Dask to read in a large amount of data,
reduce it down, and then iterate on a much smaller amount of data.  For this
latter stage on smaller data it may make sense to stop using Dask, and start
using normal Python again.

.. code-block:: python

   df = dd.read_parquet("lots-of-data-*.parquet")
   df = df.groupby('name').mean()  # reduce data significantly
   df = df.compute()               # continue on with Pandas/NumPy


Persist When You Can
--------------------

Accessing data from RAM is often much faster than accessing it from disk.
Once you have your dataset in a clean state that both:

1.  Fits in memory
2.  Is clean enough that you will want to try many different analyses

Then it is a good time to *persist* your data in RAM

.. code-block:: python

    df = dd.read_parquet("lots-of-data-*.parquet")
    df = df.fillna(...)  # clean up things lazily
    df = df[df.name == 'Alice']  # get down to a more reasonable size

    df = df.persist()  # trigger computation, persist in distributed RAM

Note that this is only relevant if you are on a distributed machine (otherwise,
as mentioned above, you should probably continue on without Dask).


Store Data Efficiently
----------------------

As your ability to compute increases you will likely find that data access and
I/O take up a larger portion of your total time.  Additionally, parallel
computing will often add new constraints to how your store your data,
particularly around providing random access to blocks of your data that are in
line with how you plan to compute on it.

For example ...

-   For compression you'll probably find that you drop gzip and bz2, and embrace
    newer systems like lz4, snappy, and Z-Standard that provide better
    performance and random access.
-   For storage formats you may find that you want self-describing formats that
    are optimized for random access, metadata storage, and binary encoding like
    Parquet, ORC, Zarr, HDF5, GeoTIFF and so on
-   When working on the cloud you may find that some older formats like HDF5 may
    not work well
-   You may want to partition or chunk your data in ways that align well to
    common queries.  In Dask DataFrame this might mean choosing a column to
    sort by for fast selection and joins.  For Dask dataframe this might mean
    choosing chunk sizes that are aligned with your access patterns and
    algorithms.
