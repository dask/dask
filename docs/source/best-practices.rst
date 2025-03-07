Dask Best Practices
===================

.. meta::
    :description: This is a short overview of Dask best practices. This document specifically focuses on best practices that are shared among all of the Dask APIs. Readers may first want to investigate one of the API-specific Best Practices documents first.

It is easy to get started with Dask's APIs, but using them *well* requires some
experience. This page contains suggestions for Dask best practices and
includes solutions to common Dask problems.

This document specifically focuses on best practices that are shared among all
of the Dask APIs.  Readers may first want to investigate one of the
API-specific Best Practices documents first.

-  :doc:`Arrays <array-best-practices>`
-  :doc:`DataFrames <dataframe-best-practices>`
-  :doc:`Delayed <delayed-best-practices>`

.. raw:: html

    <iframe width="560" height="315" src="https://www.youtube.com/embed/Gvuf6gSGA9M" title="YouTube video player" frameborder="0" style="margin: 0 auto 20px auto; display: block;" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>


Start Small
-----------

Parallelism brings extra complexity and overhead.
Sometimes it's necessary for larger problems, but often it's not.
Before adding a parallel computing system like Dask to your workload you may
want to first try some alternatives:

-   **Use better algorithms or data structures**:  NumPy, pandas, Scikit-learn
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

See :doc:`Documentation on Dask's dashboard <dashboard>` for more
information.


Avoid Very Large Partitions
---------------------------

Your chunks of data should be small enough so that many of them fit in a
worker's available memory at once.  You often control this when you select
partition size in Dask DataFrame (see :ref:`DataFrame Partitions <dataframe-design-partitions>`)
or chunk size in Dask Array (see :doc:`Array Chunks <array-chunks>`).

Dask will likely manipulate as many chunks in parallel on one machine as you
have cores on that machine.  So if you have 1 GB chunks and ten
cores, then Dask is likely to use *at least* 10 GB of memory.  Additionally,
it's common for Dask to have 2-3 times as many chunks available to work on so
that it always has something to work on.

If you have a machine with 100 GB and 10 cores, then you might want to choose
chunks in the 1GB range.  You have space for ten chunks per core which gives
Dask a healthy margin, without having tasks that are too small.

Note that you also want to avoid chunk sizes that are too small.  See the next
section for details. For a more detailed guide to choosing chunk sizes for
Dask Arrays, see this blog post on `Choosing good chunk sizes <https://blog.dask.org/2021/11/02/choosing-dask-chunk-sizes>`_.


Avoid Very Large Graphs
-----------------------

Dask workloads are composed of *tasks*.
A task is a Python function, like ``np.sum`` applied onto a Python object,
like a pandas DataFrame or NumPy array.  If you are working with Dask
collections with many partitions, then every operation you do, like ``x + 1``
likely generates many tasks, at least as many as partitions in your collection.

Every task comes with some overhead.  This is somewhere between 200us and 1ms.
If you have a computation with thousands of tasks this is fine, there will be
about a second of overhead, and that may not trouble you.

However when you have very large graphs with millions of tasks then this may
become troublesome, both because overhead is now in the 10 minutes to hours
range, and also because the overhead of dealing with such a large graph can
start to overwhelm the scheduler.

You can build smaller graphs by:

-  **Increasing your chunk size:**  If you have a 1,000 GB of data and are using
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

The high level Dask collections (array, DataFrame, bag) include common
operations that follow standard Python APIs from NumPy and pandas.
However, many Python workloads are complex and may require operations that are
not included in these high level APIs.

Fortunately, there are many options to support custom workloads:

-   All collections have a ``map_partitions`` or ``map_blocks`` function, that
    applies a user provided function across every pandas DataFrame or NumPy array
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
    map_overlap
    groupby.Aggregation

.. currentmodule:: dask.array

.. autosummary::

    blockwise
    map_blocks
    map_overlap
    reduction

Store Data Efficiently
----------------------

As your ability to compute increases you will likely find that data access and
I/O take up a larger portion of your total time.  Additionally, parallel
computing will often add new constraints to how your store your data,
particularly around providing random access to blocks of your data that are in
line with how you plan to compute on it.

For example:

-   For compression you'll probably find that you drop gzip and bz2, and embrace
    newer systems like lz4, snappy, and Z-Standard that provide better
    performance and random access.
-   For storage formats you may find that you want self-describing formats that
    are optimized for random access, metadata storage, and binary encoding like
    `Parquet <https://parquet.apache.org/>`_, `ORC <https://orc.apache.org/>`_,
    `Zarr <https://zarr.readthedocs.io/en/stable/>`_,
    `HDF5 <https://portal.hdfgroup.org/display/HDF5/HDF5>`_, and
    `GeoTIFF <https://en.wikipedia.org/wiki/GeoTIFF>`_.
-   When working on the cloud you may find that some older formats like HDF5 may
    not work as well.
-   You may want to partition or chunk your data in ways that align well to
    common queries.  In Dask DataFrame this might mean choosing a column to
    sort by for fast selection and joins.  For Dask Array this might mean
    choosing chunk sizes that are aligned with your access patterns and
    algorithms.

Processes, Threads and VM sizes
-------------------------------

If you're doing mostly numeric work with Numpy, pandas, Scikit-learn, Numba,
and other libraries that release the `GIL <https://docs.python.org/3/glossary.html#term-global-interpreter-lock>`_, then use mostly threads.  If you're
doing work on text data or Python collections like lists and dicts then use
mostly processes.

If you're on larger machines with a high thread count (much greater than 4),
then you should probably split things up into at least a few processes
regardless. Python can be highly productive with about 4 threads per process
with numeric work, but not 50 threads.

This is advise that generalizes to cloud computing and picking appropriate VM
instance sizes. There is a lot of nuance to picking the _perfect_ instance but
a good starting point is a 1:4 CPU to RAM ratio with one Worker instance per VM. You can adjust from there given your workload.

For more information on threads, processes, and how to configure them in Dask, see
:doc:`the scheduler documentation <scheduling>`.


Load Data with Dask
-------------------

A common anti-pattern we see is people creating large Python objects like a DataFrame
or an Array on the client (i.e. their local machine) outside of Dask and then embedding
them into the computation. This means that Dask has to send these objects over the network
multiple times instead of just passing pointers to the data.

This incurs a lot of overhead and slows down a computation quite significantly, especially
so if the network connection between the client and the scheduler is slow. It can
also overload the scheduler so that it errors with out of memory errors. Instead, you
should use Dask methods to load the data and use Dask to control the results.

Here are some common patterns to avoid and nicer alternatives:


.. tab-set::

   .. tab-item:: DataFrames
       :sync: dataframe

       We are using Dask to read a parquet dataset before appending a set of pandas
       DataFrames to it. We are loading the csv files into memory before sending the
       data to Dask.

       .. code-block:: python

          ddf = dd.read_parquet(...)

          pandas_dfs = []
          for fn in filenames:
              pandas_dfs(pandas.read_csv(fn))     # Read locally with pandas
          ddf = dd.concat([ddf] + pandas_dfs)     # Give to Dask

       Instead, we can use Dask to read the csv files directly, keeping all data
       on the cluster.

       .. code-block:: python

           ddf = dd.read_parquet(...)
           ddf2 = dd.read_csv(filenames)
           ddf = dd.concat([ddf, ddf2])


   .. tab-item:: Arrays
       :sync: array

       We are using NumPy to create an in-memory array before handing it over to
       Dask, forcing Dask to embed the array into the task graph instead of handling
       pointers to the data.

       .. code-block:: python

           f = h5py.File(...)

           x = np.asarray(f["x"])  # Get data as a NumPy array locally
           x = da.from_array(x)   # Hand NumPy array to Dask

       Instead, we can use Dask to read the file directly, keeping all data
       on the cluster.

       .. code-block:: python

            f = h5py.File(...)
            x = da.from_array(f["x"])  # Let Dask do the reading

   .. tab-item:: Delayed
       :sync: delayed

       We are using pandas to read a large CSV file before building a Graph
       with delayed to parallelize a computation on the data.

       .. code-block:: python

           @dask.delayed
           def process(a, b):
               ...

           df = pandas.read_csv("some-large-file.csv")  # Create large object locally
           results = []
           for item in L:
               result = process(item, df)  # include df in every delayed call
               results.append(result)

       Instead, we can use delayed to read the data as well. This avoid embedding the
       large file into the graph, Dask can just pass a reference to the delayed
       object around.

       .. code-block:: python

           @dask.delayed
           def process(a, b):
              ...

           df = dask.delayed(pandas.read_csv)("some-large-file.csv")  # Let Dask build object
           results = []
           for item in L:
              result = process(item, df)  # include pointer to df in every delayed call
              results.append(result)

Embedding large objects like pandas DataFrames or Arrays into the computation is a
frequent pain point for Dask users. It adds a significant delay until the scheduler
has received and is able to start the computation and stresses the scheduler during
the computation.

Using Dask to load these objects instead avoids these issues and improves the performance
of a computation significantly.

Avoid calling compute repeatedly
--------------------------------

Calling ``compute`` will block the execution on the client until the Dask computation
completes. A pattern we regularly see is users calling ``compute`` in a loop or sequentially
on slightly different queries.

This prohibits Dask from parallelizing different computations on the cluster and from
sharing intermediate results between different queries.

.. code-block:: python

    foo = ...
    results = []
    for i in range(...):
         results.append(foo.select(...).compute())

This holds execution every time that the iteration arrives at the compute call computing
one query at a time.

.. code-block:: python

    foo = ...
    results = []
    for i in range(...):
         results.append(foo.select(...))  # no compute here
    results = dask.compute(*results)

This allows Dask to compute the shared parts of the computation (like the
``foo`` object above) only once, rather than once per ``compute`` call and allows
Dask to parallelize across the different selects as well instead of running
them sequentially.
