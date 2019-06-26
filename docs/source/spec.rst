Specification
=============

Dask is a specification to encode a graph -- specifically, a directed 
acyclic graph of tasks with data dependencies -- using ordinary Python data 
structures, namely dicts, tuples, functions, and arbitrary Python
values. 


Definitions
-----------

A **Dask graph** is a dictionary mapping **keys** to **computations**:

.. code-block:: python

   {'x': 1,
    'y': 2,
    'z': (add, 'x', 'y'),
    'w': (sum, ['x', 'y', 'z']),
    'v': [(sum, ['w', 'z']), 2]}

A **key** is any hashable value that is not a **task**:

.. code-block:: python

   'x'
   ('x', 2, 3)

A **task** is a tuple with a callable first element.  Tasks represent atomic
units of work meant to be run by a single worker.  Example: 

.. code-block:: python

   (add, 'x', 'y')

We represent a task as a tuple such that the *first element is a callable
function* (like ``add``), and the succeeding elements are *arguments* for that
function. An *argument* may be any valid **computation**.

A **computation** may be one of the following:

1.  Any **key** present in the Dask graph like ``'x'``
2.  Any other value like ``1``, to be interpreted literally
3.  A **task** like ``(inc, 'x')`` (see below)
4.  A list of **computations**, like ``[1, 'x', (inc, 'x')]``

So all of the following are valid **computations**:

.. code-block:: python

   np.array([...])
   (add, 1, 2)
   (add, 'x', 2)
   (add, (inc, 'x'), 2)
   (sum, [1, 2])
   (sum, ['x', (inc, 'x')])
   (np.dot, np.array([...]), np.array([...]))
   [(sum, ['x', 'y']), 'z']

To encode keyword arguments, we recommend the use of ``functools.partial`` or
``toolz.curry``.


What functions should expect
----------------------------

In cases like ``(add, 'x', 'y')``, functions like ``add`` receive concrete
values instead of keys.  A Dask scheduler replaces keys (like ``'x'`` and ``'y'``) with
their computed values (like ``1``, and ``2``) *before* calling the ``add`` function.


Entry Point - The ``get`` function
----------------------------------

The ``get`` function serves as entry point to computation for all
:doc:`schedulers <scheduler-overview>`.  This function gets the value
associated to the given key.  That key may refer to stored data, as is the case
with ``'x'``, or to a task, as is the case with ``'z'``.  In the latter case,
``get`` should perform all necessary computation to retrieve the computed
value.

.. _scheduler: scheduler-overview.rst

.. code-block:: python

   >>> from dask.threaded import get

   >>> from operator import add

   >>> dsk = {'x': 1,
   ...        'y': 2,
   ...        'z': (add, 'x', 'y'),
   ...        'w': (sum, ['x', 'y', 'z'])}

.. code-block:: python

   >>> get(dsk, 'x')
   1

   >>> get(dsk, 'z')
   3

   >>> get(dsk, 'w')
   6

Additionally, if given a ``list``, get should simultaneously acquire values for
multiple keys:

.. code-block:: python

   >>> get(dsk, ['x', 'y', 'z'])
   [1, 2, 3]

Because we accept lists of keys as keys, we support nested lists:

.. code-block:: python

   >>> get(dsk, [['x', 'y'], ['z', 'w']])
   [[1, 2], [3, 6]]

Internally ``get`` can be arbitrarily complex, calling out to distributed
computing, using caches, and so on.


Why use tuples
--------------

With ``(add, 'x', 'y')``, we wish to encode the result of calling ``add`` on
the values corresponding to the keys ``'x'`` and ``'y'``.

We intend the following meaning:

.. code-block:: python

   add('x', 'y')  # after x and y have been replaced

But this will err because Python executes the function immediately
before we know values for ``'x'`` and ``'y'``.

We delay the execution by moving the opening parenthesis one term to the left,
creating a tuple:

.. code::

    Before: add( 'x', 'y')
    After: (add, 'x', 'y')

This lets us store the desired computation as data that we can analyze using
other Python code, rather than cause immediate execution.

LISP users will identify this as an s-expression, or as a rudimentary form of
quoting.
