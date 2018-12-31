User Interfaces
===============

Dask supports several user interfaces:

-  High-Level
    -  :doc:`Arrays <array>`: Parallel NumPy
    -  :doc:`Bags <bag>`: Parallel lists
    -  :doc:`DataFrames <dataframe>`: Parallel Pandas
    -  `Machine Learning <https://ml.dask.org>`_ : Parallel Scikit-Learn
    -  Others from external projects, like `XArray <https://xarray.pydata.org>`_
-  Low-Level
    -  :doc:`Delayed <delayed>`: Parallel function evaluation
    -  :doc:`Futures <futures>`: Real-time parallel function evaluation

Each of these user interfaces employs the same underlying parallel computing
machinery, and so has the same scaling, diagnostics, resilience, and so on, but
each provides a different set of parallel algorithms and programming style.

This document helps you to decide which user interface best suits your needs,
and gives some general information that applies to all interfaces.
The pages linked above give more information about each interface in greater
depth.

High-Level Collections
----------------------

Many people who start using Dask are explicitly looking for a scalable version of
NumPy, Pandas, or Scikit-Learn.  For these situations, the starting point within
Dask is usually fairly clear.  If you want scalable NumPy arrays, then start with Dask
array; if you want scalable Pandas DataFrames, then start with Dask DataFrame, and so on.

These high-level interfaces copy the standard interface with slight variations.
These interfaces automatically parallelize over larger datasets for you for a
large subset of the API from the original project.

.. code-block:: python

   # Arrays
   import dask.array as da
   x = da.random.uniform(low=0, high=10, size=(10000, 10000),  # normal numpy code
                         chunks=(1000, 1000))  # break into chunks of size 1000x1000

   y = x + x.T - x.mean(axis=0)  # Use normal syntax for high level algorithms

   # DataFrames
   import dask.dataframe as dd
   df = dd.read_csv('2018-*-*.csv', parse_dates='timestamp',  # normal Pandas code
                    blocksize=64000000)  # break text into 64MB chunks

   s = df.groupby('name').balance.mean()  # Use normal syntax for high level algorithms

   # Bags / lists
   import dask.bag as db
   b = db.read_text('*.json').map(json.loads)
   total = (b.filter(lambda d: d['name'] == 'Alice')
             .map(lambda d: d['balance'])
             .sum())

It is important to remember that, while APIs may be similar, some differences do
exist.  Additionally, the performance of some algorithms may differ from their
in-memory counterparts due to the advantages and disadvantages of parallel
programming.  Some thought and attention is still required when using Dask.


Low-Level Interfaces
--------------------

Often when parallelizing existing code bases or building custom algorithms, you
run into code that is parallelizable, but isn't just a big DataFrame or array.
Consider the for-loopy code below:

.. code-block:: python

   results = []
   for a in A:
       for b in B:
           if a < b:
               c = f(a, b)
           else:
               c = g(a, b)
           results.append(c)

There is potential parallelism in this code (the many calls to ``f`` and ``g``
can be done in parallel), but it's not clear how to rewrite it into a big
array or DataFrame so that it can use a higher-level API.  Even if you could
rewrite it into one of these paradigms, it's not clear that this would be a
good idea.  Much of the meaning would likely be lost in translation, and this
process would become much more difficult for more complex systems.

Instead, Dask's lower-level APIs let you write parallel code one function call
at a time within the context of your existing for loops.  A common solution
here is to use :doc:`Dask delayed <delayed>` to wrap individual function calls
into a lazily constructed task graph:

.. code-block:: python

   import dask

   lazy_results = []
   for a in A:
       for b in B:
           if a < b:
               c = dask.delayed(f)(a, b)  # add lazy task
           else:
               c = dask.delayed(g)(a, b)  # add lazy task
           lazy_results.append(c)

   results = dask.compute(*lazy_results)  # compute all in parallel


Combining High- and Low-Level Interfaces
----------------------------------------

It is common to combine high- and low-level interfaces.
For example, you might use Dask array/bag/dataframe to load in data and do
initial pre-processing, then switch to Dask delayed for a custom algorithm that
is specific to your domain, then switch back to Dask array/dataframe to clean
up and store results.  Understanding both sets of user interfaces, and how
to switch between them, can be a productive combination.

.. code-block:: python

   # Convert to a list of delayed Pandas dataframes
   delayed_values = df.to_delayed()

   # Manipulate delayed values arbitrarily as you like

   # Convert many delayed Pandas DataFrames back to a single Dask DataFrame
   df = dd.from_delayed(delayed_values)


Laziness and Computing
----------------------

Most Dask user interfaces are *lazy*, meaning that they do not evaluate until
you explicitly ask for a result using the ``compute`` method:

.. code-block:: python

   # This array syntax doesn't cause computation
   y = x + x.T - x.mean(axis=0)

   # Trigger computation by explicitly calling the compute method
   y = y.compute()

If you have multiple results that you want to compute at the same time, use the
``dask.compute`` function.  This can share intermediate results and so be more
efficient:

.. code-block:: python

   # compute multiple results at the same time with the compute function
   min, max = dask.compute(y.min(), y.max())

Note that the ``compute()`` function returns in-memory results.  It converts
Dask DataFrames to Pandas DataFrames, Dask arrays to NumPy arrays, and Dask
bags to lists.  *You should only call compute on results that will fit
comfortably in memory*.  If your result does not fit in memory, then you might
consider writing it to disk instead.

.. code-block:: python

   # Write larger results out to disk rather than store them in memory
   my_dask_dataframe.to_parquet('myfile.parquet')
   my_dask_array.to_hdf5('myfile.hdf5')
   my_dask_bag.to_textfiles('myfile.*.txt')


Persist into Distributed Memory
-------------------------------

Alternatively, if you are on a cluster, then you may want to trigger a
computation and store the results in distributed memory.  In this case you do
not want to call ``compute``, which would create a single Pandas, NumPy, or
list result. Instead, you want to call ``persist``, which returns a new Dask
object that points to actively computing, or already computed results spread
around your cluster's memory.

.. code-block:: python

   # Compute returns an in-memory non-Dask object
   y = y.compute()

   # Persist returns an in-memory Dask object that uses distributed storage if available
   y = y.persist()

This is common to see after data loading an preprocessing steps, but before
rapid iteration, exploration, or complex algorithms.  For example, we might read
in a lot of data, filter down to a more manageable subset, and then persist
data into memory so that we can iterate quickly.

.. code-block:: python

   import dask.dataframe as dd
   df = dd.read_parquet('...')
   df = df[df.name == 'Alice']  # select important subset of data
   df = df.persist()  # trigger computation in the background

   # These are all relatively fast now that the relevant data is in memory
   df.groupby(df.id).balance.sum().compute()   # explore data quickly
   df.groupby(df.id).balance.mean().compute()  # explore data quickly
   df.id.nunique()                             # explore data quickly


Lazy vs Immediate
-----------------

As mentioned above, most Dask workloads are lazy, that is, they don't start any
work until you explicitly trigger them with a call to ``compute()``.
However, sometimes you *do* want to submit work as quickly as possible, track it
over time, submit new work or cancel work depending on partial results, and so
on.  This can be useful when tracking or responding to real-time events,
handling streaming data, or when building complex and adaptive algorithms.

For these situations, people typically turn to the :doc:`futures interface
<futures>` which is a low-level interface like Dask delayed, but operates
immediately rather than lazily.

Here is the same example with Dask delayed and Dask futures to illustrate the
difference.

Delayed: Lazy
~~~~~~~~~~~~~

.. code-block:: python

   @dask.delayed
   def inc(x):
       return x + 1

   @dask.delayed
   def add(x, y):
       return x + y

   a = inc(1)       # no work has happened yet
   b = inc(2)       # no work has happened yet
   c = add(a, b)    # no work has happened yet

   c = c.compute()  # This triggers all of the above computations


Futures: Immediate
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from dask.distributed import Client
   client = Client()

   def inc(x):
       return x + 1

   def add(x, y):
       return x + y

   a = client.submit(inc, 1)     # work starts immediately
   b = client.submit(inc, 2)     # work starts immediately
   c = client.submit(add, a, b)  # work starts immediately

   c = c.result()                # block until work finishes, then gather result

You can also trigger work with the high-level collections using the
``persist`` function.  This will cause work to happen in the background when
using the distributed scheduler.


Combining Interfaces
--------------------

There are established ways to combine the interfaces above:

1.  The high-level interfaces (array, bag, dataframe) have a ``to_delayed``
    method that can convert to a sequence (or grid) of Dask delayed objects

    .. code-block:: python

       delayeds = df.to_delayed()

2.  The high-level interfaces (array, bag, dataframe) have a ``from_delayed``
    method that can convert from either Delayed *or* Future objects

    .. code-block:: python

       df = dd.from_delayed(delayeds)
       df = dd.from_delayed(futures)

3.  The ``Client.compute`` method converts Delayed objects into Futures

    .. code-block:: python

       futures = client.compute(delayeds)

4.  The ``dask.distributed.futures_of`` function gathers futures from
    persisted collections

    .. code-block:: python

       from dask.distributed import futures_of

       df = df.persist()  # start computation in the background
       futures = futures_of(df)

5.  The Dask.delayed object converts Futures into delayed objects

    .. code-block:: python

       delayed_value = dask.delayed(future)

The approaches above should suffice to convert any interface into any other.
We often see some anti-patterns that do not work as well:

1.  Calling low-level APIs (delayed or futures) on high-level objects (like
    Dask arrays or DataFrames). This downgrades those objects to their NumPy or
    Pandas equivalents, which may not be desired.
    Often people are looking for APIs like ``dask.array.map_blocks`` or
    ``dask.dataframe.map_partitions`` instead.
2.  Calling ``compute()`` on Future objects.
    Often people want the ``.result()`` method instead.
3.  Calling NumPy/Pandas functions on high-level Dask objects or
    high-level Dask functions on NumPy/Pandas objects

Conclusion
----------

Most people who use Dask start with only one of the interfaces above but
eventually learn how to use a few interfaces together.  This helps them
leverage the sophisticated algorithms in the high-level interfaces while also
working around tricky problems with the low-level interfaces.

For more information, see the documentation for the particular user interfaces
below:

-  High Level
    -  :doc:`Arrays <array>`: Parallel NumPy
    -  :doc:`Bags <bag>`: Parallel lists
    -  :doc:`DataFrames <dataframe>`: Parallel Pandas
    -  `Machine Learning <https://ml.dask.org>`_ : Parallel Scikit-Learn
    -  Others from external projects, like `XArray <https://xarray.pydata.org>`_
-  Low Level
    -  :doc:`Delayed <delayed>`: Parallel function evaluation
    -  :doc:`Futures <futures>`: Real-time parallel function evaluation
