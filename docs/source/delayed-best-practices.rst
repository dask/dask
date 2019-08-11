Best Practices
==============

It is easy to get started with Dask delayed, but using it *well* does require
some experience.  This page contains suggestions for best practices, and
includes solutions to common problems.


Call delayed on the function, not the result
--------------------------------------------

Dask delayed operates on functions like ``dask.delayed(f)(x, y)``, not on their results like ``dask.delayed(f(x, y))``.  When you do the latter, Python first calculates ``f(x, y)`` before Dask has a chance to step in.

+------------------------------------------------+--------------------------------------------------------------+
| **Don't**                                      | **Do**                                                       |
+------------------------------------------------+--------------------------------------------------------------+
| .. code-block:: python                         | .. code-block:: python                                       |
|                                                |                                                              |
|    # This executes immediately                 |    # This makes a delayed function, acting lazily            |
|                                                |                                                              |
|    dask.delayed(f(x, y))                       |    dask.delayed(f)(x, y)                                     |
|                                                |                                                              |
+------------------------------------------------+--------------------------------------------------------------+


Compute on lots of computation at once
--------------------------------------

To improve parallelism, you want to include lots of computation in each compute call.
Ideally, you want to make many ``dask.delayed`` calls to define your computation and
then call ``dask.compute`` only at the end.  It is ok to call ``dask.compute``
in the middle of your computation as well, but everything will stop there as
Dask computes those results before moving forward with your code.

+--------------------------------------------------------+-------------------------------------------+
| **Don't**                                              | **Do**                                    |
+--------------------------------------------------------+-------------------------------------------+
| .. code-block:: python                                 | .. code-block:: python                    |
|                                                        |                                           |
|    # Avoid calling compute repeatedly                  |    # Collect many calls for one compute   |
|                                                        |                                           |
|    results = []                                        |    results = []                           |
|    for x in L:                                         |    for x in L:                            |
|        y = dask.delayed(f)(x)                          |        y = dask.delayed(f)(x)             |
|        results.append(y.compute())                     |        results.append(y)                  |
|                                                        |                                           |
|    results                                             |    results = dask.compute(*results)       |
+--------------------------------------------------------+-------------------------------------------+

Calling `y.compute()` within the loop would await the result of the computation every time, and
so inhibit parallelism.

Don't mutate inputs
-------------------

Your functions should not change the inputs directly.

+-----------------------------------------+--------------------------------------+
| **Don't**                               | **Do**                               |
+-----------------------------------------+--------------------------------------+
| .. code-block:: python                  | .. code-block:: python               |
|                                         |                                      |
|    # Mutate inputs in functions         |    # Return new values or copies     |
|                                         |                                      |
|    @dask.delayed                        |    @dask.delayed                     |
|    def f(x):                            |    def f(x):                         |
|        x += 1                           |        x = x + 1                     |
|        return x                         |        return x                      |
+-----------------------------------------+--------------------------------------+

If you need to use a mutable operation, then make a copy within your function first:

.. code-block:: python

   @dask.delayed
   def f(x):
       x = copy(x)
       x += 1
       return x


Avoid global state
------------------

Ideally, your operations shouldn't rely on global state.  Using global state
*might* work if you only use threads, but when you move to multiprocessing or
distributed computing then you will likely encounter confusing errors.

+-------------------------------------------+
| **Don't**                                 |
+-------------------------------------------+
| .. code-block:: python                    |
|                                           |
|   L = []                                  |
|                                           |
|   # This references global variable L     |
|                                           |
|   @dask.delayed                           |
|   def f(x):                               |
|       L.append(x)                         |
|                                           |
+-------------------------------------------+



Don't rely on side effects
--------------------------

Delayed functions only do something if they are computed.  You will always need
to pass the output to something that eventually calls compute.

+--------------------------------+-----------------------------------------+
| **Don't**                      | **Do**                                  |
+--------------------------------+-----------------------------------------+
| .. code-block:: python         | .. code-block:: python                  |
|                                |                                         |
|    # Forget to call compute    |    # Ensure delayed tasks are computed  |
|                                |                                         |
|    dask.delayed(f)(1, 2, 3)    |    x = dask.delayed(f)(1, 2, 3)         |
|                                |    ...                                  |
|    ...                         |    dask.compute(x, ...)                 |
+--------------------------------+-----------------------------------------+

In the first case here, nothing happens, because ``compute()`` is never called.

Break up computations into many pieces
--------------------------------------

Every ``dask.delayed`` function call is a single operation from Dask's perspective.
You achieve parallelism by having many delayed calls, not by using only a
single one: Dask will not look inside a function decorated with ``@dask.delayed``
and parallelize that code internally.  To accomplish that, it needs your help to
find good places to break up a computation.

+------------------------------------+--------------------------------------+
| **Don't**                          | **Do**                               |
+------------------------------------+--------------------------------------+
| .. code-block:: python             | .. code-block:: python               |
|                                    |                                      |
|    # One giant task                |    # Break up into many tasks        |
|                                    |                                      |
|                                    |    @dask.delayed                     |
|    def load(filename):             |    def load(filename):               |
|        ...                         |        ...                           |
|                                    |                                      |
|                                    |    @dask.delayed                     |
|    def process(filename):          |    def process(filename):            |
|        ...                         |        ...                           |
|                                    |                                      |
|                                    |    @dask.delayed                     |
|    def save(filename):             |    def save(filename):               |
|        ...                         |        ...                           |
|                                    |                                      |
|    @dask.delayed                   |                                      |
|    def f(filenames):               |    def f(filenames):                 |
|        results = []                |        results = []                  |
|        for filename in filenames:  |        for filename in filenames:    |
|            data = load(filename)   |            data = load(filename)     |
|            data = process(data)    |            data = process(data)      |
|            result = save(data)     |            result = save(data)       |
|                                    |                                      |
|        return results              |        return results                |
|                                    |                                      |
|    dask.compute(f(filenames))      |    dask.compute(f(filenames))        |
+------------------------------------+--------------------------------------+

The first version only has one delayed task, and so cannot parallelize.

Avoid too many tasks
--------------------

Every delayed task has an overhead of a few hundred microseconds.  Usually this
is ok, but it can become a problem if you apply ``dask.delayed`` too finely.  In
this case, it's often best to break up your many tasks into batches or use one
of the Dask collections to help you.

+------------------------------------+-------------------------------------------------------------+
| **Don't**                          | **Do**                                                      |
+------------------------------------+-------------------------------------------------------------+
| .. code-block:: python             | .. code-block:: python                                      |
|                                    |                                                             |
|    # Too mamy tasks                |    # Use collections                                        |
|                                    |                                                             |
|    results = []                    |    import dask.bag as db                                    |
|    for x in range(10000000):       |    b = db.from_sequence(range(10000000), npartitions=1000)  |
|        y = dask.delayed(f)(x)      |    b = b.map(f)                                             |
|        results.append(y)           |    ...                                                      |
|                                    |                                                             |
+------------------------------------+-------------------------------------------------------------+

Here we use ``dask.bag`` to automatically batch applying our function. We could also have constructed
our own batching as follows

.. code-block:: python

   def batch(seq):
       sub_results = []
       for x in seq:
           sub_results.append(f(x))
       return sub_results

    batches = []
    for i in range(0, 10000000, 10000):
        result_batch = dask.delayed(batch, range(i, i + 10000))
        batches.append(result_batch)


Here we construct batches where each delayed function call computes for many data points from
the original input.

Avoid calling delayed within delayed functions
----------------------------------------------

Often, if you are new to using Dask delayed, you place ``dask.delayed`` calls
everywhere and hope for the best.  While this may actually work, it's usually
slow and results in hard-to-understand solutions.

Usually you never call ``dask.delayed`` within ``dask.delayed`` functions.

+----------------------------------------+--------------------------------------+
| **Don't**                              | **Do**                               |
+----------------------------------------+--------------------------------------+
| .. code-block:: python                 | .. code-block:: python               |
|                                        |                                      |
|    # Delayed function calls delayed    |    # Normal function calls delayed   |
|                                        |                                      |
|    @dask.delayed                       |                                      |
|    def process_all(L):                 |    def process_all(L):               |
|        result = []                     |        result = []                   |
|        for x in L:                     |        for x in L:                   |
|            y = dask.delayed(f)(x)      |            y = dask.delayed(f)(x)    |
|            result.append(y)            |            result.append(y)          |
|        return result                   |        return result                 |
+----------------------------------------+--------------------------------------+

Because the normal function only does delayed work it is very fast and so
there is no reason to delay it.

Don't call dask.delayed on other Dask collections
-------------------------------------------------

When you place a Dask array or Dask DataFrame into a delayed call, that function
will receive the NumPy or Pandas equivalent.  Beware that if your array is
large, then this might crash your workers.

Instead, it's more common to use methods like ``da.map_blocks``

+--------------------------------------------------+---------------------------------------------+
| **Don't**                                        | **Do**                                      |
+--------------------------------------------------+---------------------------------------------+
| .. code-block:: python                           | .. code-block:: python                      |
|                                                  |                                             |
|    # Call delayed functions on Dask collections  |    # Use mapping methods if applicable      |
|                                                  |                                             |
|    import dask.dataframe as dd                   |    import dask.dataframe as dd              |
|    df = dd.read_csv('/path/to/*.csv')            |    df = dd.read_csv('/path/to/*.csv')       |
|                                                  |                                             |
|    dask.delayed(train)(df)                       |    df.map_partitions(train)                 |
+--------------------------------------------------+---------------------------------------------+

Alternatively, if the procedure doesn't fit into a mapping, you can always
turn your arrays or dataframes into *many* delayed
objects, for example

.. code-block:: python

    partitions = df.to_delayed()
    delayed_values = [dask.delayed(train)(part)
                      for part in partitions]

However, if you don't mind turning your Dask array/DataFrame into a single
chunk, then this is ok.

.. code-block:: python

   dask.delayed(train)(..., y=df.sum())


Avoid repeatedly putting large inputs into delayed calls
--------------------------------------------------------

Every time you pass a concrete result (anything that isn't delayed) Dask will
hash it by default to give it a name.  This is fairly fast (around 500 MB/s)
but can be slow if you do it over and over again.  Instead, it is better to
delay your data as well.

This is especially important when using a distributed cluster to avoid sending
your data separately for each function call.

+------------------------------------------+---------------------------------------------------------+
| **Don't**                                | **Do**                                                  |
+------------------------------------------+---------------------------------------------------------+
| .. code-block:: python                   | .. code-block:: python                                  |
|                                          |                                                         |
|    x = np.array(...)  # some large array |    x = np.array(...)    # some large array              |
|                                          |    x = dask.delayed(x)  # delay the data once           |
|    results = [dask.delayed(train)(x, i)  |    results = [dask.delayed(train)(x, i)                 |
|               for i in range(1000)]      |               for i in range(1000)]                     |
+------------------------------------------+---------------------------------------------------------+


Every call to ``dask.delayed(train)(x, ...)`` has to hash the NumPy array ``x``, which slows things down.


**Do**

.. code-block:: python

   x = np.array(...)  # some large array
   x = dask.delayed(x)  # delay the data, hashing once

   results = [dask.delayed(train)(x, i) for i in range(1000)]
