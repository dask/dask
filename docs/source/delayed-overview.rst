Overview
========

Motivation and Example
----------------------

Dask.delayed lets you parallelize custom code.  It is useful whenever your
problem doesn't quite fit a high-level parallel object like dask.array or
dask.dataframe but could still benefit from parallelism.  Dask.delayed works by
delaying your function evaluations and putting them into a dask graph.
Dask.delayed is useful when wrapping existing code or when handling
non-standard problems.

Consider the following example:

.. code-block:: python

    def inc(x):
        return x + 1

    def double(x):
        return x + 2

    def add(x, y):
        return x + y

    data = [1, 2, 3, 4, 5]

    output = []
    for x in data:
        a = inc(x)
        b = double(x)
        c = add(a, b)
        output.append(c)

    total = sum(output)

As written this code runs sequentially in a single thread.  However we see that
a lot of this could be executed in parallel.  We use the ``delayed`` function
to parallelize this code by turning it into a dask graph.  We slightly modify
our code by wrapping functions in ``delayed``.  This delays the execution of
the function and generates a dask graph instead.

.. code-block:: python

    from dask import delayed

    output = []
    for x in data:
        a = delayed(inc)(x)
        b = delayed(double)(x)
        c = delayed(add)(a, b)
        output.append(c)

    total = delayed(sum)(output)

We used the ``delayed`` function to wrap the function calls that we want
to turn into tasks.  None of the ``inc``, ``double``, ``add`` or ``sum`` calls
have happened yet, instead the object ``total`` is a ``Delayed`` result that
contains a task graph of the entire computation.  Looking at the graph we see
clear opportunities for parallel execution.  The dask schedulers will exploit
this parallelism, generally improving performance.  (although not in this
example, because these functions are already very small and fast.)

.. code-block:: python

   total.visualize()  # see image to the right

.. image:: images/delayed-inc-double-add.svg
   :align: right
   :alt: simple task graph created with dask.delayed

We can now compute this lazy result to execute the graph in parallel:

.. code-block:: python

   >>> total.compute()
   45


Delayed Function
----------------

The ``dask.delayed`` interface consists of one function, ``delayed``:

- ``delayed`` wraps functions

   Wraps functions. Can be used as a decorator, or around function calls
   directly (i.e. ``delayed(foo)(a, b, c)``). Outputs from functions wrapped in
   ``delayed`` are proxy objects of type ``Delayed`` that contain a graph of
   all operations done to get to this result.

- ``delayed`` wraps objects

   Wraps objects. Used to create ``Delayed`` proxies directly.

``Delayed`` objects can be thought of as representing a key in the dask. A
``Delayed`` supports *most* python operations, each of which creates another
``Delayed`` representing the result:

- Most operators (``*``, ``-``, and so on)
- Item access and slicing (``a[0]``)
- Attribute access (``a.size``)
- Method calls (``a.index(0)``)

Operations that aren't supported include:

- Mutating operators (``a += 1``)
- Mutating magics such as ``__setitem__``/``__setattr__`` (``a[0] = 1``, ``a.foo = 1``)
- Iteration. (``for i in a: ...``)
- Use as a predicate (``if a: ...``)

The last two points in particular mean that ``Delayed`` objects cannot be used for 
control flow, meaning that no ``Delayed`` can appear in a loop or if statement. 
In other words you can't iterate over a ``Delayed`` object, or use it as part of
a condition in an if statement, but ``Delayed`` object can be used in a body of a loop 
or if statement (i.e. the example above is fine, but if ``data`` was a ``Delayed`` 
object it wouldn't be).
Even with this limitation, many workflows can easily be parallelized.
