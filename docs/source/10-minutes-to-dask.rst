10 Minutes to Dask
==================

.. meta::
    :description: This is a short overview of Dask geared towards new users. Additional Dask information can be found in the rest of the Dask documentation.

This is a short overview of Dask geared towards new users.
There is much more information contained in the rest of the documentation.

.. figure:: images/dask-overview.svg
   :alt: Dask overview. Dask is composed of three parts: collections, task graphs, and schedulers.
   :align: center

   High level collections are used to generate task graphs which can be executed by schedulers on a single machine or a cluster.

We normally import Dask as follows:

.. code-block:: python

   >>> import numpy as np
   >>> import pandas as pd

   >>> import dask.dataframe as dd
   >>> import dask.array as da
   >>> import dask.bag as db

Based on the type of data you are working with, you might not need all of these.

Creating a Dask Object
----------------------

You can create a Dask object from scratch by supplying existing data and optionally
including information about how the chunks should be structured.

.. tabs::

   .. group-tab:: DataFrame

      See :doc:`dataframe`.

      .. jupyter-execute::
        :hide-code:

         import pandas as pd
         import numpy as np
         import dask.dataframe as dd
         import dask.array as da
         import dask.bag as db

      .. jupyter-execute::

         index = pd.date_range("2021-09-01", periods=2400, freq="1H")
         df = pd.DataFrame({"a": np.arange(2400), "b": list("abcaddbe" * 300)}, index=index)
         ddf = dd.from_pandas(df, npartitions=10)
         ddf

      Now we have a Dask DataFrame with 2 columns and 2400 rows composed of 10 partitions where
      each partition has 240 rows. Each partition represents a piece of the data.

      Here are some key properties of an DataFrame:

      .. jupyter-execute::

         # check the index values covered by each partition
         ddf.divisions

      .. jupyter-execute::

         # access a particular partition
         ddf.partitions[1]

   .. group-tab:: Array

      See :doc:`array`.

      .. jupyter-execute::

         data = np.arange(100_000).reshape(200, 500)
         a = da.from_array(data, chunks=(100, 100))
         a

      Now we have a 2D array with the shape (200, 500) composed of 10 chunks where
      each chunk has the shape (100, 100). Each chunk represents a piece of the data.

      Here are some key properties of a Dask Array:

      .. jupyter-execute::

         # inspect the chunks
         a.chunks

      .. jupyter-execute::
            
         # access a particular block of data
         a.blocks[1, 3]

   .. group-tab:: Bag

      See :doc:`bag`.

      .. jupyter-execute::

         b = db.from_sequence([1, 2, 3, 4, 5, 6, 2, 1], npartitions=2)
         b

      Now we have a sequence with 8 items composed of 2 partitions where each partition
      has 4 items in it. Each partition represents a piece of the data.


Indexing
--------

Indexing Dask collections feels just like slicing NumPy arrays or pandas DataFrame.

.. tabs::

   .. group-tab:: DataFrame

      .. jupyter-execute::

         ddf.b

      .. jupyter-execute::

         ddf["2021-10-01": "2021-10-09 5:00"]

   .. group-tab:: Array

    .. jupyter-execute::

       a[:50, 200]

   .. group-tab:: Bag

      A Bag is an unordered collection allowing repeats. So it is like a list, but it doesn’t
      guarantee an ordering among elements. There is no way to index Bags since they are
      not ordered.


Computation
-----------

Dask is lazily evaluated. The result from a computation isn't computed until
you ask for it. Instead, a Dask task graph for the computation is produced.

Anytime you have a Dask object and you want to get the result, call ``compute``:

.. tabs::

   .. group-tab:: DataFrame

      .. jupyter-execute::

         ddf["2021-10-01": "2021-10-09 5:00"].compute()

   .. group-tab:: Array

      .. jupyter-execute::

         a[:50, 200].compute()

   .. group-tab:: Bag

      .. jupyter-execute::

        b.compute()


Methods
-------

Dask collections match existing numpy and pandas methods, so they should feel familiar.
Call the method to set up the task graph, and then call ``compute`` to get the result.

.. tabs::

   .. group-tab:: DataFrame

      .. jupyter-execute::

         ddf.a.mean()

      .. jupyter-execute::

         ddf.a.mean().compute()

      .. jupyter-execute::

         ddf.b.unique()

      .. jupyter-execute::

         ddf.b.unique().compute()

      Methods can be chained together just like in pandas

      .. jupyter-execute::

         result = ddf["2021-10-01": "2021-10-09 5:00"].a.cumsum() - 100
         result

      .. jupyter-execute::

         result.compute()

   .. group-tab:: Array

      .. jupyter-execute::

         a.mean()

      .. jupyter-execute::

         a.mean().compute()

      .. jupyter-execute::         

         np.sin(a)

      .. jupyter-execute::

         np.sin(a).compute()

      .. jupyter-execute::
         
         a.T

      .. jupyter-execute::

         a.T.compute()

      Methods can be chained together just like in NumPy

      .. jupyter-execute::

         a_max = a.max(axis=1)[::-1] + 10
         a_max

      .. jupyter-execute::

         a_max[:10].compute()

   .. group-tab:: Bag

      Dask Bag implements operations like ``map``, ``filter``, ``fold``, and
      ``groupby`` on collections of generic Python objects.

      .. jupyter-execute::

         b.filter(lambda x: x % 2)

      .. jupyter-execute::

         b.filter(lambda x: x % 2).compute()

      .. jupyter-execute::

         b.distinct()
      
      .. jupyter-execute::

         b.distinct().compute()

      Methods can be chained together.

      .. jupyter-execute::

         c = db.zip(b, b.map(lambda x: x * 10))
         c

      .. jupyter-execute::

         c.compute()


Visualize the Task Graph
------------------------

So far we've been setting up computations and calling ``compute``. In addition to
triggering computation, we can inspect the task graph to figure out what's going on.

.. tabs::

   .. group-tab:: DataFrame

      .. code-block:: python

         >>> result.dask
         HighLevelGraph with 7 layers.
         <dask.highlevelgraph.HighLevelGraph object at 0x7f129df7a9d0>
         1. from_pandas-0b850a81e4dfe2d272df4dc718065116
         2. loc-fb7ada1e5ba8f343678fdc54a36e9b3e
         3. getitem-55d10498f88fc709e600e2c6054a0625
         4. series-cumsum-map-131dc242aeba09a82fea94e5442f3da9
         5. series-cumsum-take-last-9ebf1cce482a441d819d8199eac0f721
         6. series-cumsum-d51d7003e20bd5d2f767cd554bdd5299
         7. sub-fed3e4af52ad0bd9c3cc3bf800544f57

         >>> result.visualize()

      .. image:: images/10_minutes_dataframe_graph.png
         :alt: Dask task graph for the Dask dataframe computation. The task graph shows a "loc" and "getitem" operations selecting a small section of the dataframe values, before applying a cumulative sum "cumsum" operation, then finally subtracting a value from the result.

   .. group-tab:: Array

      .. code-block:: python

         >>> a_max.dask
         HighLevelGraph with 6 layers.
         <dask.highlevelgraph.HighLevelGraph object at 0x7fd33a4aa400>
         1. array-ef3148ecc2e8957c6abe629e08306680
         2. amax-b9b637c165d9bf139f7b93458cd68ec3
         3. amax-partial-aaf8028d4a4785f579b8d03ffc1ec615
         4. amax-aggregate-07b2f92aee59691afaf1680569ee4a63
         5. getitem-f9e225a2fd32b3d2f5681070d2c3d767
         6. add-f54f3a929c7efca76a23d6c42cdbbe84

         >>> a_max.visualize()

      .. image:: images/10_minutes_array_graph.png
         :alt: Dask task graph for the Dask array computation. The task graph shows many "amax" operations on each chunk of the Dask array, that are then aggregated to find "amax" along the first array axis, then reversing the order of the array values with a "getitem" slicing operation, before an "add" operation to get the final result.

   .. group-tab:: Bag

      .. code-block:: python

         >>> c.dask
         HighLevelGraph with 3 layers.
         <dask.highlevelgraph.HighLevelGraph object at 0x7f96d0814fd0>
         1. from_sequence-cca2a33ba6e12645a0c9bc0fd3fe6c88
         2. lambda-93a7a982c4231fea874e07f71b4bcd7d
         3. zip-474300792cc4f502f1c1f632d50e0272

         >>> c.visualize()

      .. image:: images/10_minutes_bag_graph.png
         :alt: Dask task graph for the Dask bag computation. The task graph shows a "lambda" operation, and then a "zip" operation is applied to the partitions of the Dask bag. There is no communication needed between the bag partitions, this is an embarrassingly parallel computation.

Low-Level Interfaces
--------------------
Often when parallelizing existing code bases or building custom algorithms, you
run into code that is parallelizable, but isn't just a big DataFrame or array.

.. tabs::

   .. group-tab:: Delayed: Lazy

      :doc:`delayed` lets you to wrap individual function calls into a lazily constructed task graph:

      .. code-block:: python

         import dask

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

   .. group-tab:: Futures: Immediate

      Unlike the interfaces described so far, Futures are eager. Computation starts as soon
      as the function is submitted (see :doc:`futures`).

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

      .. note::

         Futures can only be used with distributed cluster. See the section below for more
         information.


Scheduling
----------

After you have generated a task graph, it is the scheduler's job to execute it
(see :doc:`scheduling`).

By default when you call ``compute`` on a Dask object, Dask uses the thread
pool on your computer to run computations in parallel.

If you want more control, use the distributed scheduler instead. Despite having
"distributed" in it's name, the distributed scheduler works well
on both single and multiple machines. Think of it as the "advanced scheduler".

.. tabs::

   .. group-tab:: Local

      This is how you set up a cluster that uses only your own computer.

      .. code-block:: python

         >>> from dask.distributed import Client
         ...
         ... client = Client()
         ... client
         <Client: 'tcp://127.0.0.1:41703' processes=4 threads=12, memory=31.08 GiB>

   .. group-tab:: Remote

      This is how you connect to a cluster that is already running.

      .. code-block:: python

         >>> from dask.distributed import Client
         ...
         ... client = Client("<url-of-scheduler>")
         ... client
         <Client: 'tcp://127.0.0.1:41703' processes=4 threads=12, memory=31.08 GiB>

      There are a variety of ways to set up a remote cluster. Refer to
      :doc:`how to deploy dask clusters <deploying>` for more
      information.

Once you create a client, any computation will run on the cluster that it points to.


Diagnostics
-----------

When using a distributed cluster, Dask provides a diagnostics dashboard where you can
see your tasks as they are processed.

.. code-block:: python

   >>> client.dashboard_link
   'http://127.0.0.1:8787/status'

To learn more about those graphs take a look at :doc:`diagnostics-distributed`.
