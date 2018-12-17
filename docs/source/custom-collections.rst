Custom Collections
==================

For many problems, the built-in Dask collections (``dask.array``,
``dask.dataframe``, ``dask.bag``, and ``dask.delayed``) are sufficient. For
cases where they aren't, it's possible to create your own Dask collections. Here
we describe the required methods to fullfill the Dask collection interface.

.. warning:: The custom collection API is experimental and subject to change
             without going through a deprecation cycle.

.. note:: This is considered an advanced feature. For most cases the built-in
          collections are probably sufficient.

Before reading this you should read and underestand:

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

To create your own Dask collection, you need to fullfill the following
interface. Note that there is no required base class.

It is recommended to also read :ref:`core-method-internals` to see how this
interface is used inside Dask.


.. method:: __dask_graph__(self)

    The Dask graph.

    Returns
    -------
    dsk : MutableMapping, None
        The Dask graph.  If ``None``, this instance will not be interpreted as a
        Dask collection, and none of the remaining interface methods will be
        called.


.. method:: __dask_keys__(self)

    The output keys for the Dask graph.

    Returns
    -------
    keys : list
        A possibly nested list of keys that represent the outputs of the graph.
        After computation, the results will be returned in the same layout,
        with the keys replaced with their corresponding outputs.


.. staticmethod:: __dask_optimize__(dsk, keys, \*\*kwargs)

    Given a graph and keys, return a new optimized graph.

    This method can be either a ``staticmethod`` or a ``classmethod``, but not
    an ``instancemethod``.

    Note that graphs and keys are merged before calling ``__dask_optimize__``;
    as such, the graph and keys passed to this method may represent more than
    one collection sharing the same optimize method.

    If not implemented, defaults to returning the graph unchanged.

    Parameters
    ----------
    dsk : MutableMapping
        The merged graphs from all collections sharing the same
        ``__dask_optimize__`` method.
    keys : list
        A list of the outputs from ``__dask_keys__`` from all collections
        sharing the same ``__dask_optimize__`` method.
    \*\*kwargs
        Extra keyword arguments forwarded from the call to ``compute`` or
        ``persist``. Can be used or ignored as needed.

    Returns
    -------
    optimized_dsk : MutableMapping
        The optimized Dask graph.


.. staticmethod:: __dask_scheduler__(dsk, keys, \*\*kwargs)

    The default scheduler ``get`` to use for this object.

    Usually attached to the class as a staticmethod, e.g.:

    >>> import dask.threaded
    >>> class MyCollection(object):
    ...     # Use the threaded scheduler by default
    ...     __dask_scheduler__ = staticmethod(dask.threaded.get)


.. method:: __dask_postcompute__(self)

    Return the finalizer and (optional) extra arguments to convert the computed
    results into their in-memory representation.

    Used to implement ``dask.compute``.

    Returns
    -------
    finalize : callable
        A function with the signature ``finalize(results, *extra_args)``.
        Called with the computed results in the same structure as the
        corresponding keys from ``__dask_keys__``, as well as any extra
        arguments as specified in ``extra_args``. Should perform any necessary
        finalization before returning the corresponding in-memory collection
        from ``compute``. For example, the ``finalize`` function for
        ``dask.array.Array`` concatenates all the individual array chunks into
        one large numpy array, which is then the result of ``compute``.
    extra_args : tuple
        Any extra arguments to pass to ``finalize`` after ``results``. If no
        extra arguments should be an empty tuple.


.. method:: __dask_postpersist__(self)

    Return the rebuilder and (optional) extra arguments to rebuild an equivalent
    Dask collection from a persisted graph.

    Used to implement ``dask.persist``.

    Returns
    -------
    rebuild : callable
        A function with the signature ``rebuild(dsk, *extra_args)``. Called
        with a persisted graph containing only the keys and results from
        ``__dask_keys__``, as well as any extra arguments as specified in
        ``extra_args``. Should return an equivalent Dask collection with the
        same keys as ``self``, but with the results already computed. For
        example, the ``rebuild`` function for ``dask.array.Array`` is just the
        ``__init__`` method called with the new graph but the same metadata.
    extra_args : tuple
        Any extra arguments to pass to ``rebuild`` after ``dsk``. If no extra
        arguments should be an empty tuple.


.. note:: It's also recommended to define ``__dask_tokenize__``,
          see :ref:`deterministic-hashing`.


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
     collection, and the values are computed results (for the single machine
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
    from dask.base import DaskMethodsMixin
    from dask.optimization import cull

    # We subclass from DaskMethodsMixin to add common dask methods to our
    # class. This is nice but not necessary for creating a dask collection.
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

        @staticmethod
        def __dask_optimize__(dsk, keys, **kwargs):
            # We cull unnecessary tasks here. Note that this isn't necessary,
            # dask will do this automatically, this just shows one optimization
            # you could do.
            dsk2, _ = cull(dsk, keys)
            return dsk2

        # Use the threaded scheduler by default.
        __dask_scheduler__ = staticmethod(dask.threaded.get)

        def __dask_postcompute__(self):
            # We want to return the results as a tuple, so our finalize
            # function is `tuple`. There are no extra arguments, so we also
            # return an empty tuple.
            return tuple, ()

        def __dask_postpersist__(self):
            # Since our __init__ takes a graph as its first argument, our
            # rebuild function can just be the class itself. For extra
            # arguments we also return a tuple containing just the keys.
            return Tuple, (self._keys,)

        def __dask_tokenize__(self):
            # For tokenize to work we want to return a value that fully
            # represents this object. In this case it's the list of keys
            # to be computed.
            return tuple(self._keys)

Demonstrating this class:

.. code:: python

    >>> from dask_tuple import Tuple
    >>> from operator import add, mul

    # Define a dask graph
    >>> dsk = {'a': 1,
    ...        'b': 2,
    ...        'c': (add, 'a', 'b'),
    ...        'd': (mul, 'b', 2),
    ...        'e': (add, 'b', 'c')}

    # The output keys for this graph
    >>> keys = ['b', 'c', 'd', 'e']

    >>> x = Tuple(dsk, keys)

    # Compute turns Tuple into a tuple
    >>> x.compute()
    (2, 3, 4, 5)

    # Persist turns Tuple into a Tuple, with each task already computed
    >>> x2 = x.persist()
    >>> isinstance(x2, Tuple)
    True
    >>> x2.__dask_graph__()
    {'b': 2,
     'c': 3,
     'd': 4,
     'e': 5}
    >>> x2.compute()
    (2, 3, 4, 5)


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

   If defining a method on the class isn't possible, you can register a tokenize
   function with the ``normalize_token`` dispatch.  The function should have the
   same signature as described above.

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
    >>> class Foo(object):
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
    >>> class Bar(object):
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
