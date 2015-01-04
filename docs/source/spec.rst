
Dask Specification
==================

We represent a computation as a directed acyclic graph of tasks with data
dependencies.  Dask is a specification to encode such a graph using ordinary
Python data structures, namely ``dict``s, ``tuple``s, ``function``s, and
arbitrary Python values.


Definitions
-----------

A **dask** is a dictionary mapping data-keys to values or tasks.

.. code-block:: python

   {'x': 1,
    'y': 2,
    'z': (add, 'x', 'y'),
    'w': (sum, ['x', 'y', 'z'])}

A **key** is any hashable value.

.. code-block:: python

   'x'
   (1, 2, 3)

A **task** is an atomic unit of work to be run by a single worker.

.. code-block:: python

   (add, 'x', 'y')

We represent a task as a tuple such that the *first element is a callable
function* (like ``add``), and the succeeding elements are *arguments* for that
function.

An **argument** may be one of the following:

1.  Any key present in the dask like ``'x'``
2.  Any other value like ``1``, to be interpreted literally
3.  Other tasks like ``(inc, 'x')``
4.  List of arguments, like ``[1, 'x', (inc, 'x')]``

So all of the following are valid tasks

.. code-block:: python

   (add, 1, 2)
   (add, 'x', 2)
   (add, (inc, 'x'), 2)
   (sum, [1, 2])
   (sum, ['x', (inc, 'x')])


What functions should expect
----------------------------

In cases like ``(add, 'x', 'y')`` functions like ``add`` receive concrete
values instead of keys.  A dask scheduler replaces keys (like ``'x'`` and ``'y'``) with
their computed values (like ``1``, and ``2``) *before* calling the ``add`` function.

If the argument is a list then a function should expect an ``Iterator`` of
concrete values.


Why use tuples
--------------

With ``(add, 'x', 'y')`` we wish to encode "the result of calling ``add`` on
the values corresponding to the keys ``'x'`` and ``'y'``.

We intend the following meaning:

.. code-block:: python

   add('x', 'y')  # after x and y have been replaced

But this will err because Python executes the function immediately
before we know values for ``'x'`` and ``'y'``.

We delay the execution by moving the opening parenthesis one term to the left,
creating a tuple.

.. code::

    Before: add( 'x', 'y')
    After: (add, 'x', 'y')

This lets us store the desired computation as data that we can analyze using
other Python code rather than cause immediate execution.

LISP users will identify this as an s-expression or as a rudimentary form of
quoting.
