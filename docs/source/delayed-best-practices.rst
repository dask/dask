Best Practices
==============

It is easy to get started with Dask delayed, but using it *well* does require
some experience.  This page contains suggestions for best practices, and
includes solutions to common problems.


Call delayed on the function, not the result
--------------------------------------------

Dask delayed operates on functions like ``dask.delayed(f)(x, y)``, not on their results like ``dask.delayed(f(x, y))``.  When you do the latter, Python first calculates ``f(x, y)`` before Dask has a chance to step in.

**Don't**

.. code-block:: python

   dask.delayed(f(x, y))

**Do**

.. code-block:: python

   dask.delayed(f)(x, y)


Compute on lots of computation at once
--------------------------------------

To improve parallelism, you want to include lots of computation in each compute call.
Ideally, you want to make many ``dask.delayed`` calls to define your computation and
then call ``dask.compute`` only at the end.  It is ok to call ``dask.compute``
in the middle of your computation as well, but everything will stop there as
Dask computes those results before moving forward with your code.

**Don't**

.. code-block:: python

   for x in L:
       y = dask.delayed(f)(x)
       y.compute()  # calling compute after every delayed call stops parallelism

**Do**

.. code-block:: python

   results = []
   for x in L:
       y = dask.delayed(f)(x)
       results.append(y)

   results = dask.compute(*results)  # call compute after you have collected many delayed calls


Don't mutate inputs
-------------------

Your functions should not change the inputs directly.

**Don't**

.. code-block:: python

   @dask.delayed
   def f(x):
       x += 1
       return x

**Do**

.. code-block:: python

   @dask.delayed
   def f(x):
       return x + 1

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

**Don't**

.. code-block:: python

   L = []

   @dask.delayed
   def f(x):
       L.append(x)


Don't rely on side effects
--------------------------

Delayed functions only do something if they are computed.  You will always need
to pass the output to something that eventually calls compute.

**Don't**

.. code-block:: python

   dask.delayed(f)(1, 2, 3)  # this has no effect

**Do**

.. code-block:: python

   x = dask.delayed(f)(1, 2, 3)
   ...
   dask.compute(x, ...)  # need to call compute for something to happen


Break up computations into many pieces
--------------------------------------

Every ``dask.delayed`` function call is a single operation from Dask's perspective.
You achieve parallelism by having many delayed calls, not by using only a
single one: Dask will not look inside a function decorated with ``@dask.delayed``
and parallelize that code internally.  To accomplish that, it needs your help to 
find good places to break up a computation.

**Don't**

.. code-block:: python

   def load(filename):
       ...

   def process(data):
       ...

   def save(data):
       ...

   @dask.delayed
   def f(filenames):
       results = []
       for filename in filenames:
           data = load(filename)
           data = process(data)
           results.append(save(data))
       
       return results

   dask.compute(f(filenames))  # this is only a single task

**Do**

.. code-block:: python

   @dask.delayed
   def load(filename):
       ...

   @dask.delayed
   def process(data):
       ...

   @dask.delayed
   def save(data):
       ...

   def f(filenames):
       results = []
       for filename in filenames:
           data = load(filename)
           data = process(data)
           results.append(save(data))

       return results

   dask.compute(f(filenames))  # this has many tasks and so will parallelize


Avoid too many tasks
--------------------

Every delayed task has an overhead of a few hundred microseconds.  Usually this
is ok, but it can become a problem if you apply ``dask.delayed`` too finely.  In
this case, it's often best to break up your many tasks into batches or use one
of the Dask collections to help you.

**Don't**

.. code-block:: python

   results = []
   for x in range(1000000000):  # Too many dask.delayed calls
       y = dask.delayed(f)(x)
       results.append(y)

**Do**

.. code-block:: python

   # Use collections

   import dask.bag as db
   b = db.from_sequence(1000000000, npartitions=1000)
   b = b.map(f)

.. code-block:: python

   # Or batch manually

   def batch(seq):
       sub_results = []
       for x in seq:
           sub_results.append(f(x))
       return sub_results

   batches = []
   for i in range(0, 1000000000, 1000000):  # in steps of 1000000
       result_batch = dask.delayed(batch, range(i, i + 1000000))
       batches.append(result_batch)


Avoid calling delayed within delayed functions
----------------------------------------------

Often, if you are new to using Dask delayed, you place ``dask.delayed`` calls
everywhere and hope for the best.  While this may actually work, it's usually
slow and results in hard-to-understand solutions.

Usually you never call ``dask.delayed`` within ``dask.delayed`` functions.

**Don't**

.. code-block:: python

   @dask.delayed
   def process_all(L):
       result = []
       for x in L:
           y = dask.delayed(f)(x)
           result.append(y)
        return result

**Do**

Instead, because this function only does delayed work, it is very fast and so
there is no reason to delay it.

.. code-block:: python

   def process_all(L):
       result = []
       for x in L:
           y = dask.delayed(f)(x)
           result.append(y)
        return result


Don't call dask.delayed on other Dask collections
-------------------------------------------------

When you place a Dask array or Dask DataFrame into a delayed call, that function
will receive the NumPy or Pandas equivalent.  Beware that if your array is
large, then this might crash your workers.

Instead, it's more common to use methods like ``da.map_blocks`` or
``df.map_partitions``, or to turn your arrays or DataFrames into *many* delayed
objects.

**Don't**

.. code-block:: python

   import dask.dataframe as dd
   df = dd.read_csv('/path/to/*.csv')

   dask.delayed(train)(df)  # might as well have used Pandas instead

**Do**

.. code-block:: python

   import dask.dataframe as dd
   df = dd.read_csv('/path/to/*.csv')

   df.map_partitions(train)
   # or
   partitions = df.to_delayed()

   delayed_values = [dask.delayed(train)(part) for part in partitions]

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

**Don't**

.. code-block:: python

   x = np.array(...)  # some large array

   results = [dask.delayed(train)(x, i) for i in range(1000)]


Every call to ``dask.delayed(train)(x, ...)`` has to hash the NumPy array ``x``, which slows things down.


**Do**

.. code-block:: python

   x = np.array(...)  # some large array
   x = dask.delayed(x)  # delay the data, hashing once

   results = [dask.delayed(train)(x, i) for i in range(1000)]
