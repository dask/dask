Specification
=============

Dask is a specification to encode a graph -- specifically, a directed
acyclic graph of tasks with data dependencies -- using ordinary Python data
structures, namely dicts, tuples, functions, and arbitrary Python
values.


Definitions
-----------

A **Dask graph** is a dictionary mapping **keys** to **computation**:

.. tab-set::

    .. tab-item:: Tasks

      .. code-block:: python

         {'x': (x := DataNode(None, 1)),
         'y': (y := DataNode(None, 2)),
         'z': (z := Task("z", add, x.ref(), y.ref())),
         'w': (w := Task("w", sum, List(x.ref(), y.ref(), z.ref()))),
         'v': List(Task(None, sum, List(w.ref(), z.ref())), 2)}

    .. tab-item:: Legacy

      .. code-block:: python

         {'x': 1,
         'y': 2,
         'z': (add, 'y', 'x'),
         'w': (sum, ['x', 'y', 'z']),
         'v': [(sum, ['w', 'z']), 2]}

A **key** is a str, int, float, or tuple thereof:

.. code-block:: python

   'x'
   ('x', 2, 3)

A **task** is a computation that Dask can perform and is expressed using the :class:`dask.Task` class.  Tasks represent atomic
units of work meant to be run by a single worker.  Example:

.. code-block:: python

   def add(x, y):
       return x + y

   t = Task("t", add, 1, 2)
   assert t() == 3

   t2 = Task("t2", add, t.ref(), 2)
   assert t2({"t": 3}) == 5


.. note::

   The legacy representation of a task is a tuple with the first element being a
   callable function.  The rest of the elements are arguments to that function.
   The legacy representation is deprecated and will be removed in a future
   version of Dask.  Use the :class:`dask.Task` class instead.


A **computation** may be one of the following:

1.  An :class:`~dask._task_spec.Alias` pointing to another key in the Dask graph like ``Alias('new', 'x')``
2.  A literal value as a :class:`~dask._task_spec.DataNode` like ``DataNode(None, 1)``
3.  A :class:`~dask._task_spec.Task` like ``Task("t", add, 1, 2)``
4.  A :class:`~dask._task_spec.List` of **computations**, like ``List(1, TaskRef('x'), Task(None, inc, TaskRef("x"))]``

So all of the following are valid **computations**:

.. tab-set::

    .. tab-item:: Computations

      .. code-block:: python

         DataNode(None, np.array([...]))
         Task("t", add, 1, 2)
         Task("t", add, TaskRef('x'), 2)
         Task("t", add, Task(None, inc, TaskRef('x')), 2)
         Task("t", sum, [1, 2])
         Task("t", sum, [TaskRef('x'), Task(None, inc, TaskRef('x'))])
         Task("t", np.dot, np.array([...]), np.array([...]))
         Task("t", sum, List(TaskRef('x'), TaskRef('y')), 'z')

    .. tab-item:: Legacy representation

      .. code-block:: python

         np.array([...])
         (add, 1, 2)
         (add, 'x', 2)
         (add, (inc, 'x'), 2)
         (sum, [1, 2])
         (sum, ['x', (inc, 'x')])
         (np.dot, np.array([...]), np.array([...]))
         [(sum, ['x', 'y']), 'z']



What functions should expect
----------------------------

In cases like ``Task("t", add, TaskRef('x'), 2)``, functions like ``add`` receive concrete values instead of keys.  A Dask scheduler replaces task references (like ``x``) with their computed values (like ``1``) *before* calling the ``add`` function.
These references can be provided as either literal key references using
:class:`~dask._task_spec.TaskRef` or if a reference to the task is available, by
calling :meth:`~dask._task_spec.Task.ref` on the task itself.


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

   >>> dsk = {'x': (x := DataNode(None, 1)),
   ...       'y': (y := DataNode(None, 2)),
   ...       'z': (z := Task("z", add, x.ref(), y.ref())),
   ...       'w': (w := Task("w", sum, List(x.ref(), y.ref(), z.ref())))}

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


Why not tuples?
---------------

The tuples are objectively a more compact representation than the ``Task`` class so why did we choose to introduce this new representation?

As a tuple, the task is not self-describing and heavily context dependent. The
meaning of a tuple like ``(func, "x", "y")`` is depending on the graph it is
embedded in. The literals ``x`` and ``y`` could be either actual literals that
should be passed to the function or they could be references to other tasks.
Therefore, the _interpretation_ of this task has to walk the tuple recursively
and compare every single encountered element with known keys in the graph.
Especially for large graphs or deeply nested tuple arguments, this can be a
performance bottleneck. For APIs that allow users to define their own key names
this can further cause false positives where intended literals are replaced by
pre-computed task results.

For the time being, both representation are supported. Legacy style tasks will
be automatically converted to new style tasks whenever dask encounters them. New
projects and algorithms are encouraged to use the new style tasks.
