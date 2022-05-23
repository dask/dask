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

1. **Graph Merging & Optimization**

   First, the individual collections are converted to a single large graph and
   nested list of keys. How this happens depends on the value of the
   ``optimize_graph`` keyword, which each function takes:

   - If ``optimize_graph`` is ``True`` (default), then the collections are first
     grouped by their ``__dask_optimize__`` methods.  All collections with the
     same ``__dask_optimize__`` method have their graphs merged and keys
     concatenated, and then a single call to each respective
     ``__dask_optimize__`` is made with the merged graphs and keys.  The
     resulting graphs are then merged.

   - If ``optimize_graph`` is ``False``, then all the graphs are merged and all
     the keys concatenated.

   After this stage there is a single large graph and nested list of keys which
   represents all the collections.

2. **Computation**

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

3. **Postcompute**

   After the results are generated, the output values of ``compute`` need to be
   built. This is what the ``__dask_postcompute__`` method is for.
   ``__dask_postcompute__`` returns two things:

   - A ``finalize`` function, which takes in the results for the corresponding
     keys
   - A tuple of extra arguments to pass to ``finalize`` after the results

   To build the outputs, the list of collections and results is iterated over,
   and the finalizer for each collection is called on its respective results.

In pseudocode, this process looks like the following:

.. code:: python

    def compute(*collections, **kwargs):
        # 1. Graph Merging & Optimization
        # -------------------------------
        if kwargs.pop('optimize_graph', True):
            # If optimization is turned on, group the collections by
            # optimization method, and apply each method only once to the merged
            # sub-graphs.
            optimization_groups = groupby_optimization_methods(collections)
            graphs = []
            for optimize_method, cols in optimization_groups:
                # Merge the graphs and keys for the subset of collections that
                # share this optimization method
                sub_graph = merge_graphs([x.__dask_graph__() for x in cols])
                sub_keys = [x.__dask_keys__() for x in cols]
                # kwargs are forwarded to ``__dask_optimize__`` from compute
                optimized_graph = optimize_method(sub_graph, sub_keys, **kwargs)
                graphs.append(optimized_graph)
            graph = merge_graphs(graphs)
        else:
            graph = merge_graphs([x.__dask_graph__() for x in collections])
        # Keys are always the same
        keys = [x.__dask_keys__() for x in collections]

        # 2. Computation
        # --------------
        # Determine appropriate get function based on collections, global
        # settings, and keyword arguments
        get = determine_get_function(collections, **kwargs)
        # Pass the merged graph, keys, and kwargs to ``get``
        results = get(graph, keys, **kwargs)

        # 3. Postcompute
        # --------------
        output = []
        # Iterate over the results and collections
        for res, collection in zip(results, collections):
            finalize, extra_args = collection.__dask_postcompute__()
            out = finalize(res, **extra_args)
            output.append(out)

        # `dask.compute` always returns tuples
        return tuple(output)


Persist
~~~~~~~

Persist is very similar to ``compute``, except for how the return values are
created. It too has three stages:

1. **Graph Merging & Optimization**

   Same as in ``compute``.

2. **Computation**

   Same as in ``compute``, except in the case of the distributed scheduler,
   where the values in ``results`` are futures instead of values.

3. **Postpersist**

   Similar to ``__dask_postcompute__``, ``__dask_postpersist__`` is used to
   rebuild values in a call to ``persist``. ``__dask_postpersist__`` returns
   two things:

   - A ``rebuild`` function, which takes in a persisted graph.  The keys of
     this graph are the same as ``__dask_keys__`` for the corresponding
     collection, and the values are computed results (for the single-machine
     scheduler) or futures (for the distributed scheduler).
   - A tuple of extra arguments to pass to ``rebuild`` after the graph

   To build the outputs of ``persist``, the list of collections and results is
   iterated over, and the rebuilder for each collection is called on the graph
   for its respective results.

In pseudocode, this looks like the following:

.. code:: python

    def persist(*collections, **kwargs):
        # 1. Graph Merging & Optimization
        # -------------------------------
        # **Same as in compute**
        graph = ...
        keys = ...

        # 2. Computation
        # --------------
        # **Same as in compute**
        results = ...

        # 3. Postpersist
        # --------------
        output = []
        # Iterate over the results and collections
        for res, collection in zip(results, collections):
            # res has the same structure as keys
            keys = collection.__dask_keys__()
            # Get the computed graph for this collection.
            # Here flatten converts a nested list into a single list
            subgraph = {k: r for (k, r) in zip(flatten(keys), flatten(res))}

            # Rebuild the output dask collection with the computed graph
            rebuild, extra_args = collection.__dask_postpersist__()
            out = rebuild(subgraph, *extra_args)

            output.append(out)

        # dask.persist always returns tuples
        return tuple(output)


Optimize
~~~~~~~~

The operation of ``optimize`` can be broken into two stages:

1. **Graph Merging & Optimization**

   Same as in ``compute``.

2. **Rebuilding**

   Similar to ``persist``, the ``rebuild`` function and arguments from
   ``__dask_postpersist__`` are used to reconstruct equivalent collections from
   the optimized graph.

In pseudocode, this looks like the following:

.. code:: python

    def optimize(*collections, **kwargs):
        # 1. Graph Merging & Optimization
        # -------------------------------
        # **Same as in compute**
        graph = ...

        # 2. Rebuilding
        # -------------
        # Rebuild each dask collection using the same large optimized graph
        output = []
        for collection in collections:
            rebuild, extra_args = collection.__dask_postpersist__()
            out = rebuild(graph, *extra_args)
            output.append(out)

        # dask.optimize always returns tuples
        return tuple(output)


Visualize
~~~~~~~~~

Visualize is the simplest of the 4 core functions. It only has two stages:

1. **Graph Merging & Optimization**

   Same as in ``compute``.

2. **Graph Drawing**

   The resulting merged graph is drawn using ``graphviz`` and outputs to the
   specified file.

In pseudocode, this looks like the following:

.. code:: python

    def visualize(*collections, **kwargs):
        # 1. Graph Merging & Optimization
        # -------------------------------
        # **Same as in compute**
        graph = ...

        # 2. Graph Drawing
        # ----------------
        # Draw the graph with graphviz's `dot` tool and return the result.
        return dot_graph(graph, **kwargs)


.. _adding-methods-to-class:

Adding the Core Dask Methods to Your Class
------------------------------------------

Defining the above interface will allow your object to used by the core Dask
functions (``dask.compute``, ``dask.persist``, ``dask.visualize``, etc.). To
add corresponding method versions of these, you can subclass from
``dask.base.DaskMethodsMixin`` which adds implementations of ``compute``,
``persist``, and ``visualize`` based on the interface above.

.. _example-dask-collection:

Example Dask Collection
-----------------------

Here we create a Dask collection representing a tuple.  Every element in the
tuple is represented as a task in the graph.  Note that this is for illustration
purposes only - the same user experience could be done using normal tuples with
elements of ``dask.delayed``:

.. code:: python

    # Saved as dask_tuple.py
    import dask
    from dask.base import DaskMethodsMixin, replace_name_in_key
    from dask.optimization import cull

    def tuple_optimize(dsk, keys, **kwargs):
        # We cull unnecessary tasks here. See
        # https://docs.dask.org/en/stable/optimize.html for more
        # information on optimizations in Dask.
        dsk2, _ = cull(dsk, keys)
        return dsk2

    # We subclass from DaskMethodsMixin to add common dask methods to
    # our class (compute, persist, and visualize). This is nice but not
    # necessary for creating a Dask collection (you can define them
    # yourself).
    class Tuple(DaskMethodsMixin):
        def __init__(self, dsk, keys):
            # The init method takes in a dask graph and a set of keys to use
            # as outputs.
            self._dsk = dsk
            self._keys = keys

        def __dask_graph__(self):
            return self._dsk

        def __dask_keys__(self):
            return self._keys

        # use the `tuple_optimize` function defined above
        __dask_optimize__ = staticmethod(tuple_optimize)

        # Use the threaded scheduler by default.
        __dask_scheduler__ = staticmethod(dask.threaded.get)

        def __dask_postcompute__(self):
            # We want to return the results as a tuple, so our finalize
            # function is `tuple`. There are no extra arguments, so we also
            # return an empty tuple.
            return tuple, ()

        def __dask_postpersist__(self):
            # We need to return a callable with the signature
            # rebuild(dsk, *extra_args, rename: Mapping[str, str] = None)
            return Tuple._rebuild, (self._keys,)

        @staticmethod
        def _rebuild(dsk, keys, *, rename=None):
            if rename is not None:
                keys = [replace_name_in_key(key, rename) for key in keys]
            return Tuple(dsk, keys)

        def __dask_tokenize__(self):
            # For tokenize to work we want to return a value that fully
            # represents this object. In this case it's the list of keys
            # to be computed.
            return self._keys

Demonstrating this class:

.. code:: python

    >>> from dask_tuple import Tuple
    >>> from operator import add, mul

    # Define a dask graph
    >>> dsk = {"k0": 1,
    ...        ("x", "k1"): 2,
    ...        ("x", 1): (add, "k0", ("x", "k1")),
    ...        ("x", 2): (mul, ("x", "k1"), 2),
    ...        ("x", 3): (add, ("x", "k1"), ("x", 1))}

    # The output keys for this graph.
    # The first element of each tuple must be the same across the whole collection;
    # the remainder are arbitrary, unique hashables
    >>> keys = [("x", "k1"), ("x", 1), ("x", 2), ("x", 3)]

    >>> x = Tuple(dsk, keys)

    # Compute turns Tuple into a tuple
    >>> x.compute()
    (2, 3, 4, 5)

    # Persist turns Tuple into a Tuple, with each task already computed
    >>> x2 = x.persist()
    >>> isinstance(x2, Tuple)
    True
    >>> x2.__dask_graph__()
    {('x', 'k1'): 2, ('x', 1): 3, ('x', 2): 4, ('x', 3): 5}
    >>> x2.compute()
    (2, 3, 4, 5)

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
   representative of the object.

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
    >>> class Foo:
    ...     def __init__(self, a, b):
    ...         self.a = a
    ...         self.b = b
    ...
    ...     def __dask_tokenize__(self):
    ...         # This tuple fully represents self
    ...         return (Foo, self.a, self.b)

    >>> x = Foo(1, 2)
    >>> tokenize(x)
    '5988362b6e07087db2bc8e7c1c8cc560'
    >>> tokenize(x) == tokenize(x)  # token is deterministic
    True

    # Register an implementation with normalize_token
    >>> class Bar:
    ...     def __init__(self, x, y):
    ...         self.x = x
    ...         self.y = y

    >>> @normalize_token.register(Bar)
    ... def tokenize_bar(x):
    ...     return (Bar, x.x, x.x)

    >>> y = Bar(1, 2)
    >>> tokenize(y)
    '5a7e9c3645aa44cf13d021c14452152e'
    >>> tokenize(y) == tokenize(y)
    True
    >>> tokenize(y) == tokenize(x)  # tokens for different objects aren't equal
    False


For more examples, see ``dask/base.py`` or any of the built-in Dask collections.
