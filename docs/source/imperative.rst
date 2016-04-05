Imperative
==========

As discussed in the :ref:`custom graphs <custom-graph-example>` section,
sometimes a problem doesn't fit into one of the collections like ``dask.bag`` or
``dask.array``. Instead of creating a dask directly using a dictionary, one can
use the ``dask.imperative`` interface. This allows one to create graphs
directly with a light annotation of normal python code.


Examples
--------

`Example notebook <http://nbviewer.ipython.org/github/dask/dask-examples/blob/master/do-and-profiler.ipynb>`_.

Now, rebuilding the example from :ref:`custom graphs <custom-graph-example>`:

.. code-block:: python

    from dask.imperative import delayed, value

    @delayed
    def load(filename):
        ...

    @delayed
    def clean(data):
        ...

    @delayed
    def analyze(sequence_of_data):
        ...

    @delayed
    def store(result):
        with open(..., 'w') as f:
            f.write(result)

    files = ['myfile.a.data', 'myfile.b.data', 'myfile.c.data']
    loaded = [load(i) for i in files]
    cleaned = [clean(i) for i in loaded]
    analyzed = analyze(cleaned)
    stored = store(analyzed)

    stored.compute()

This builds the same graph as seen before, but using normal python syntax. In
fact, the only difference between python code that would do this in serial, and
the parallel version with dask is the ``delayed`` decorators on the functions, and
the call to ``compute`` at the end.


How it works
------------

The ``dask.imperative`` interface consists of two functions:

1. ``delayed``

   Wraps functions. Can be used as a decorator, or around function calls
   directly (i.e. ``delayed(foo)(a, b, c)``). Outputs from functions wrapped in
   ``delayed`` are proxy objects of type ``Value`` that contain a graph of all
   operations done to get to this result.

2. ``value``

   Wraps objects. Used to create ``Value`` proxies directly.

``Value`` objects can be thought of as representing a key in the dask. A
``Value`` supports *most* python operations, each of which creates another
``Value`` representing the result:

- Most operators (``*``, ``-``, and so on)
- Item access and slicing (``a[0]``)
- Attribute access (``a.size``)
- Method calls (``a.index(0)``)

Operations that aren't supported include:

- Mutating operators (``a += 1``)
- Mutating magics such as ``__setitem__``/``__setattr__`` (``a[0] = 1``, ``a.foo = 1``)
- Iteration. (``for i in a: ...``)
- Use as a predicate (``if a: ...``)

The last two in particular mean that ``Value`` objects cannot be used for
control flow, meaning that no ``Value`` can appear in a loop or if statement.
Even with this limitation, many workflows can easily be parallelized.


Example
-------

Here we have a serial blocked computation for computing the mean of all
positive elements in a large, on disk array:

.. code-block:: python

    x = h5py.File('myfile.hdf5')['/x']          # Trillion element array on disk

    sums = []
    counts = []
    for i in range(1000000):                    # One million times
        chunk = x[1000000*i:1000000*(i + 1)]    # Pull out chunk
        positive = chunk[chunk > 0]             # Filter out negative elements
        sums.append(positive.sum())             # Sum chunk
        counts.append(positive.size)            # Count chunk

    result = sum(sums) / sum(counts)            # Aggregate results


Below is the same code, parallelized using ``dask.imperative``:

.. code-block:: python

    x = value(h5py.File('myfile.hdf5')['/x'])   # Trillion element array on disk

    sums = []
    counts = []
    for i in range(1000000):                    # One million times
        chunk = x[1000000*i:1000000*(i + 1)]    # Pull out chunk
        positive = chunk[chunk > 0]             # Filter out negative elements
        sums.append(positive.sum())             # Sum chunk
        counts.append(positive.size)            # Count chunk

    result = delayed(sum)(sums) / delayed(sum)(counts)    # Aggregate results

    result.compute()                            # Perform the computation


Only 3 lines had to change to make this computation parallel instead of serial.

- Wrap the original array in ``value``. This makes all the slices on it return
  ``Value`` objects.
- Wrap both calls to ``sum`` with ``delayed``.
- Call the ``compute`` method on the result.

While the for loop above still iterates fully, it's just building up a graph of
the computation that needs to happen, without actually doing any computing.


Working with Collections
------------------------

Often we want to do a bit of custom work with ``dask.imperative`` (for example for
complex data ingest), then leverage the algorithms in ``dask.array`` or
``dask.dataframe``, and then switch back to custom work.  To this end, all
collections support ``from_imperative`` functions and ``to_imperative``
methods.

As an example, consider the case where we store tabular data in a custom format
not known by ``dask.dataframe``.  This format is naturally broken apart into
pieces and we have a function that reads one piece into a Pandas DataFrame.
We use ``dask.imperrative`` to lazily read these files into Pandas DataFrames,
use ``dd.from_imperative`` to wrap these pieces up into a single
``dask.dataframe``, use the complex algorithms within ``dask.dataframe``
(groupby, join, etc..) and then switch back to imperative to save our results
back to the custom format.

.. code-block:: python

   import dask.dataframe as dd
   from dask.imperative import delayed

   from my_custom_library import load, save

   filenames = ...
   dfs = [delayed(load)(fn) for fn in filenames]

   df = dd.from_imperative(dfs)
   df = ... # do work with dask.dataframe

   dfs = df.to_imperative()
   writes = [delayed(save)(df, fn) for df, fn in zip(dfs, filenames)]

   dd.compute(writes)

Data science is often complex, ``dask.imperative`` provides a release valve for
users to manage this complexity on their own, and solve the last mile problem
for custom formats and complex situations.


Definitions
-----------

.. currentmodule:: dask.imperative

.. autofunction:: value
.. autofunction:: delayed
.. autofunction:: compute
