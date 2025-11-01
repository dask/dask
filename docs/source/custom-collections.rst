Custom Collections
==================

For many problems, the built-in Dask collections (``dask.array``,
``dask.dataframe``, ``dask.bag``, and ``dask.delayed``) are sufficient. For
cases where they aren't, it's possible to create your own Dask collections. Here
we describe the required methods to fulfill the Dask collection interface.

.. note:: This is considered an advanced feature. For most cases the built-in
          collections are probably sufficient.

Before reading this you should read and understand:

- :doc:`overview <graphs>`
- :doc:`graph specification <spec>`
- :doc:`custom graphs <custom-graphs>`

**Contents**

- :ref:`Description of the Dask collection interface <collection-interface>`
- :ref:`How this interface is used to implement the core Dask
  methods <core-method-internals>`
- :ref:`How to add the core methods to your class <adding-methods-to-class>`
- :ref:`example-dask-collection`
- :ref:`How to check if something is a Dask collection <is-dask-collection>`
- :ref:`How to make tokenize work with your collection <deterministic-hashing>`


.. _collection-interface:

The Dask Collection Interface
-----------------------------

To create your own Dask collection, you need to fulfill the interface
defined by the :py:class:`dask.typing.DaskCollection` protocol. Note
that there is no required base class.

It is recommended to also read :ref:`core-method-internals` to see how this
interface is used inside Dask.

Collection Protocol
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: dask.typing.DaskCollection
   :members: __dask_graph__, __dask_keys__, __dask_postcompute__,
             __dask_postpersist__, __dask_tokenize__,
             __dask_optimize__, __dask_scheduler__, compute, persist,
             visualize

HLG Collection Protocol
~~~~~~~~~~~~~~~~~~~~~~~

.. note::
    HighLevelGraphs are being deprecated in favor of expressions. New projects are encouraged to not implement their own HLG Layers.

Collections backed by Dask's :ref:`high-level-graphs` must implement
an additional method, defined by this protocol:

.. autoclass:: dask.typing.HLGDaskCollection
   :members: __dask_layers__

Scheduler ``get`` Protocol
~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``SchedulerGetProtocol`` defines the signature that a Dask
collection's ``__dask_scheduler__`` definition must adhere to.

.. autoclass:: dask.typing.SchedulerGetCallable
   :members: __call__

Post-persist Callable Protocol
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Collections must define a ``__dask_postpersist__`` method which
returns a callable that adheres to the ``PostPersistCallable``
interface.

.. autoclass:: dask.typing.PostPersistCallable
   :members: __call__

.. _core-method-internals:

Internals of the Core Dask Methods
----------------------------------

Dask has a few *core* functions (and corresponding methods) that implement
common operations:

- ``compute``: Convert one or more Dask collections into their in-memory
  counterparts
- ``persist``: Convert one or more Dask collections into equivalent Dask
  collections with their results already computed and cached in memory
- ``optimize``: Convert one or more Dask collections into equivalent Dask
  collections sharing one large optimized graph
- ``visualize``: Given one or more Dask collections, draw out the graph that
  would be passed to the scheduler during a call to ``compute`` or ``persist``

Here we briefly describe the internals of these functions to illustrate how
they relate to the above interface.

Compute
~~~~~~~

The operation of ``compute`` can be broken into three stages:

1. **Graph Merging, finalization**

   First, the individual collections are converted to a single large expression
   and nested list of keys. This is done by
   :func:`~dask.base.collections_to_expr` and ensures that all collections are
   optimized together to eliminate common sub-expressions.

   .. note::

       At this stage, legacy HLG graphs are wrapped into a ``HLGExpr`` that
       encodes __dask_postcompute__ and the low level optimizer as determined by `__dask_optimize__` into the expression.

       The ``optimize_graph`` argument is only relevant for HLG graphs and
       controls whether low level optimizations are considered.

       - If ``optimize_graph`` is ``True`` (default), then the collections are
         first grouped by their ``__dask_optimize__`` methods.  All collections with the same ``__dask_optimize__`` method have their graphs merged and keys concatenated, and then a single call to each respective ``__dask_optimize__`` is made with the merged graphs and keys.  The resulting graphs are then merged.

       - If ``optimize_graph`` is ``False``, then all the graphs are merged and
         all the keys concatenated.

   The combined graph is _finalized_ with a ``FinalizeCompute`` expression
   which instructs the expression / graph to reduce to a single partition,
   suitable to be returned to the user after compute. This is either done by
   implemengint the ``__dask_postcompute__`` method of the collection or by
   implementing an optimization path of the expression.

   For the example of a DataFrame, the ``FinalizeCompute`` simplifies to a ``RepartitionToFewer(..., npartition=1)`` which simply concatenates all results to one ordinary DataFrame.

2. **(Expression) Optimization**

   The merged expression is optimized. This step should not be confused with
   the low level optimization that is defined by `__dask_optimize__` for legacy
   graphs. This is a step that is always performed and is a required step to
   simplify and lower expressions to their final form that can be used to
   actually generate the executable task graph. See also, :doc:`/dataframe-optimizer`.

   For legacy HLG graphs, the low level optimization step is embedded in the
   graph materialization which typically only happens after the graph has been
   passed to the scheduler (see below).


3. **Computation**

   After the graphs are merged and any optimizations performed, the resulting
   large graph and nested list of keys are passed on to the scheduler.  The
   scheduler to use is chosen as follows:

   - If a ``get`` function is specified directly as a keyword, use that
   - Otherwise, if a global scheduler is set, use that
   - Otherwise fall back to the default scheduler for the given collections.
     Note that if all collections don't share the same ``__dask_scheduler__``
     then an error will be raised.

   Once the appropriate scheduler ``get`` function is determined, it is called
   with the merged graph, keys, and extra keyword arguments.  After this stage,
   ``results`` is a nested list of values. The structure of this list mirrors
   that of ``keys``, with each key substituted with its corresponding result.


Persist
~~~~~~~

Persist is very similar to ``compute``, except for how the return values are
created. It too has three stages:

1. **Graph Merging, *no* finalization**

   Same as in ``compute`` but without a finalization. In the case of persist we
   do not want to concatenate all output partitions but instead want to return a
   future for every partition.

2. **(Expression) Optimization**

   Same as in ``compute``.

3. **Computation**

   Same as in ``compute`` with the difference that the returned results are a list of Futures.

4. **Postpersist**

   The futures returned by the scheduler are used with ``__dask_postpersist__`` to rebuild a collection that is pointing to the remote data.

   ``__dask_postpersist__`` returns two things:

   - A ``rebuild`` function, which takes in a persisted graph.  The keys of
     this graph are the same as ``__dask_keys__`` for the corresponding
     collection, and the values are computed results (for the single-machine
     scheduler) or futures (for the distributed scheduler).
   - A tuple of extra arguments to pass to ``rebuild`` after the graph

   To build the outputs of ``persist``, the list of collections and results is
   iterated over, and the rebuilder for each collection is called on the graph
   for its respective results.


Optimize
~~~~~~~~

The operation of ``optimize`` can be broken into two stages:

1. **Graph Merging, *no* finalization**

   Same as in ``persist``.

2. **(Expression) Optimization**

   Same as in ``compute`` and ``persist``.

3. **Materialization and Rebuilding**

   The entire graph is materialized (which also performs low level optimization).
   Similar to ``persist``, the ``rebuild`` function and arguments from
   ``__dask_postpersist__`` are used to reconstruct equivalent collections from
   the optimized graph.


Visualize
~~~~~~~~~

Visualize is the simplest of the 4 core functions. It only has two stages:

1. **Graph Merging & Optimization**

   Same as in ``compute``.

2. **Graph Drawing**

   The resulting merged graph is drawn using ``graphviz`` and outputs to the
   specified file.


.. _adding-methods-to-class:

Adding the Core Dask Methods to Your Class
------------------------------------------

Defining the above interface will allow your object to used by the core Dask
functions (``dask.compute``, ``dask.persist``, ``dask.visualize``, etc.). To
add corresponding method versions of these, you can subclass from
``dask.base.DaskMethodsMixin`` which adds implementations of ``compute``,
``persist``, and ``visualize`` based on the interface above.


Expressions to define computation
---------------------------------

It is recommended to define dask graphs using the :class:`dask.expr.Expr` class.
To get started, a minimal set of methods have to be implemented.

.. autoclass:: dask.Expr
   :members: _task, _layer, __dask_keys__, __dask_graph__


.. _example-dask-collection:

Example Dask Collection
-----------------------

Here we create a Dask collection representing a tuple.  Every element in the
tuple is represented as a task in the graph.  Note that this is for illustration
purposes only - the same user experience could be done using normal tuples with
elements of ``dask.delayed``:

.. code:: python

    import dask
    from dask.base import DaskMethodsMixin, replace_name_in_key
    from dask.expr import Expr, LLGExpr
    from dask.typing import Key
    from dask.task_spec import Task, DataNode, Alias


    # We subclass from DaskMethodsMixin to add common dask methods to
    # our class (compute, persist, and visualize). This is nice but not
    # necessary for creating a Dask collection (you can define them
    # yourself).
    class Tuple(DaskMethodsMixin):
        def __init__(self, expr):
            self._expr = expr

        def __dask_graph__(self):
            return self._expr.__dask_graph__()

        def __dask_keys__(self):
            return self._expr.__dask_keys__()

        # Use the threaded scheduler by default.
        __dask_scheduler__ = staticmethod(dask.threaded.get)

        def __dask_postcompute__(self):
            # We want to return the results as a tuple, so our finalize
            # function is `tuple`. There are no extra arguments, so we also
            # return an empty tuple.
            return tuple, ()

        def __dask_postpersist__(self):
            return Tuple._rebuild, ("mysuffix",)

        @staticmethod
        def _rebuild(futures: dict, name: str):
            expr = LLGExpr({
                (name, i): DataNode((name, i), val)
                for i, val in  enumerate(futures.values())
            })
            return Tuple(expr)

        def __dask_tokenize__(self):
            # For tokenize to work we want to return a value that fully
            # represents this object. In this case this is done by a type
            identifier plus the (also tokenized) name of the expression
            return (type(self), self._expr._name)

    class RemoteTuple(Expr):
        @property
        def npartitions(self):
            return len(self.operands)

        def __dask_keys__(self):
            return [(self._name, i) for i in range(self.npartitions)]

        def _task(self, name: Key, index: int) -> Task:
            return DataNode(name, self.operands[index])



Demonstrating this class:

.. code:: python

    >>> from dask_tuple import Tuple

    def from_pytuple(pytup: tuple) -> Tuple:
        return Tuple(RemoteTuple(*pytup))

    >>> dask_tup = from_pytuple(tuple(range(5)))

    >>> dask_tup.__dask_keys__()
    [('remotetuple-b7ea9a26c3ab8287c78d11fd45f26793', 0),
    ('remotetuple-b7ea9a26c3ab8287c78d11fd45f26793', 1),
    ('remotetuple-b7ea9a26c3ab8287c78d11fd45f26793', 2)]

    # Compute turns Tuple into a tuple
    >>> dask_tup.compute()
    (0, 1, 2)

    # Persist turns Tuple into a Tuple, with each task already computed
    >>> dask_tup2 = dask_tup.persist()
    >>> isinstance(dask_tup2, Tuple)
    True

    >>> dask_tup2.__dask_graph__()
    {('newname', 0): DataNode(0),
    ('newname', 1): DataNode(1),
    ('newname', 2): DataNode(2)}

    >>> x2.compute()
    (0, 1, 2)

    # Run-time typechecking
    >>> from dask.typing import DaskCollection
    >>> isinstance(x, DaskCollection)
    True


.. _is-dask-collection:

Checking if an object is a Dask collection
------------------------------------------

To check if an object is a Dask collection, use
``dask.base.is_dask_collection``:

.. code:: python

    >>> from dask.base import is_dask_collection
    >>> from dask import delayed

    >>> x = delayed(sum)([1, 2, 3])
    >>> is_dask_collection(x)
    True
    >>> is_dask_collection(1)
    False


.. _deterministic-hashing:

Implementing Deterministic Hashing
----------------------------------

Dask implements its own deterministic hash function to generate keys based on
the value of arguments.  This function is available as ``dask.base.tokenize``.
Many common types already have implementations of ``tokenize``, which can be
found in ``dask/base.py``.

When creating your own custom classes, you may need to register a ``tokenize``
implementation. There are two ways to do this:

1. The ``__dask_tokenize__`` method

   Where possible, it is recommended to define the ``__dask_tokenize__`` method.
   This method takes no arguments and should return a value fully
   representative of the object. It is a good idea to call ``dask.base.normalize_token``
   from it before returning any non-trivial objects.

2. Register a function with ``dask.base.normalize_token``

   If defining a method on the class isn't possible or you need to
   customize the tokenize function for a class whose super-class is
   already registered (for example if you need to sub-class built-ins),
   you can register a tokenize function with the ``normalize_token``
   dispatch.  The function should have the same signature as described
   above.

In both cases the implementation should be the same, where only the location of the
definition is different.

.. note:: Both Dask collections and normal Python objects can have
          implementations of ``tokenize`` using either of the methods
          described above.

Example
~~~~~~~

.. code:: python

    >>> from dask.base import tokenize, normalize_token

    # Define a tokenize implementation using a method.
    >>> class Point:
    ...     def __init__(self, x, y):
    ...         self.x = x
    ...         self.y = y
    ...
    ...     def __dask_tokenize__(self):
    ...         # This tuple fully represents self
    ...         # Wrap non-trivial objects with normalize_token before returning them
    ...         return normalize_token(Point), self.x, self.y

    >>> x = Point(1, 2)
    >>> tokenize(x)
    '5988362b6e07087db2bc8e7c1c8cc560'
    >>> tokenize(x) == tokenize(x)  # token is idempotent
    True
    >>> tokenize(Point(1, 2)) == tokenize(Point(1, 2))  # token is deterministic
    True
    >>> tokenize(Point(1, 2)) == tokenize(Point(2, 1))  # tokens are unique
    False


    # Register an implementation with normalize_token
    >>> class Point3D:
    ...     def __init__(self, x, y, z):
    ...         self.x = x
    ...         self.y = y
    ...         self.z = z

    >>> @normalize_token.register(Point3D)
    ... def normalize_point3d(x):
    ...     return normalize_token(Point3D), x.x, x.y, x.z

    >>> y = Point3D(1, 2, 3)
    >>> tokenize(y)
    '5a7e9c3645aa44cf13d021c14452152e'


For more examples, see ``dask/base.py`` or any of the built-in Dask collections.
