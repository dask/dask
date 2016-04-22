Overview
========

The ``dask.delayed`` interface consists of two functions:

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
