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

    from dask.imperative import do, value

    @do
    def load(filename):
        ...

    @do
    def clean(data):
        ...

    @do
    def analyze(sequence_of_data):
        ...

    @do
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
the parallel version with dask is the ``do`` decorators on the functions, and
the call to ``compute`` at the end.


How it works
------------

The ``dask.imperative`` interface consists of two functions:

1. ``do``

   Wraps functions. Can be used as a decorator, or around function calls
   directly (i.e. ``do(foo)(a, b, c)``). Outputs from functions wrapped in
   ``do`` are proxy objects of type ``Value`` that contain a graph of all
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

    result = do(sum)(sums) / do(sum)(counts)    # Aggregate results

    result.compute()                            # Perform the computation


Only 3 lines had to change to make this computation parallel instead of serial.

- Wrap the original array in ``value``. This makes all the slices on it return
  ``Value`` objects.
- Wrap both calls to ``sum`` with ``do``.
- Call the ``compute`` method on the result.

While the for loop above still iterates fully, it's just building up a graph of
the computation that needs to happen, without actually doing any computing.


Definitions
-----------

.. currentmodule:: dask.imperative

.. autofunction:: value
.. autofunction:: do
.. autofunction:: compute
