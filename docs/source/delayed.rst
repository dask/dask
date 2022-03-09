Delayed
=======

.. toctree::
   :maxdepth: 1
   :hidden:

   delayed-collections.rst
   delayed-best-practices.rst

Sometimes problems don't fit into one of the collections like ``dask.array`` or
``dask.dataframe``. In these cases, users can parallelize custom algorithms
using the simpler ``dask.delayed`` interface. This allows one to create graphs
directly with a light annotation of normal python code:

.. code-block:: python

   >>> x = dask.delayed(inc)(1)
   >>> y = dask.delayed(inc)(2)
   >>> z = dask.delayed(add)(x, y)
   >>> z.compute()
   5
   >>> z.visualize()

.. image:: images/inc-add.svg
   :alt: A task graph with two "inc" functions combined using an "add" function resulting in an output node.

.. raw:: html

   <iframe width="560"
           height="315"
           src="https://www.youtube.com/embed/SHqFmynRxVU"
           style="margin: 0 auto 20px auto; display: block;"
           frameborder="0"
           allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
           allowfullscreen></iframe>

Example
-------

Visit https://examples.dask.org/delayed.html to see and run examples using Dask
Delayed.

Sometimes we face problems that are parallelizable, but don't fit into high-level
abstractions like Dask Array or Dask DataFrame.  Consider the following example:

.. code-block:: python

    def inc(x):
        return x + 1

    def double(x):
        return x * 2

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

There is clearly parallelism in this problem (many of the ``inc``,
``double``, and ``add`` functions can evaluate independently), but it's not
clear how to convert this to a big array or big DataFrame computation.

As written, this code runs sequentially in a single thread.  However, we see that
a lot of this could be executed in parallel.

The Dask ``delayed`` function decorates your functions so that they operate
*lazily*.  Rather than executing your function immediately, it will defer
execution, placing the function and its arguments into a task graph.

.. currentmodule:: dask.delayed

.. autosummary::
    delayed

We slightly modify our code by wrapping functions in ``delayed``.
This delays the execution of the function and generates a Dask graph instead:

.. code-block:: python

    import dask

    output = []
    for x in data:
        a = dask.delayed(inc)(x)
        b = dask.delayed(double)(x)
        c = dask.delayed(add)(a, b)
        output.append(c)

    total = dask.delayed(sum)(output)

We used the ``dask.delayed`` function to wrap the function calls that we want
to turn into tasks.  None of the ``inc``, ``double``, ``add``, or ``sum`` calls
have happened yet. Instead, the object ``total`` is a ``Delayed`` result that
contains a task graph of the entire computation.  Looking at the graph we see
clear opportunities for parallel execution.  The Dask schedulers will exploit
this parallelism, generally improving performance (although not in this
example, because these functions are already very small and fast.)

.. code-block:: python

   total.visualize()  # see image to the right

.. image:: images/delayed-inc-double-add.svg
   :align: right
   :alt: A task graph with many nodes for "inc" and "double" that combine with "add" nodes. The output of the "add" nodes finally aggregate with a "sum" node.

We can now compute this lazy result to execute the graph in parallel:

.. code-block:: python

   >>> total.compute()
   45

Decorator
---------

It is also common to see the delayed function used as a decorator.  Here is a
reproduction of our original problem as a parallel code:

.. code-block:: python

    import dask

    @dask.delayed
    def inc(x):
        return x + 1

    @dask.delayed
    def double(x):
        return x * 2

    @dask.delayed
    def add(x, y):
        return x + y

    data = [1, 2, 3, 4, 5]

    output = []
    for x in data:
        a = inc(x)
        b = double(x)
        c = add(a, b)
        output.append(c)

    total = dask.delayed(sum)(output)


Real time
---------

Sometimes you want to create and destroy work during execution, launch tasks
from other tasks, etc.  For this, see the :doc:`Futures <futures>` interface.


Best Practices
--------------

For a list of common problems and recommendations see :doc:`Delayed Best
Practices <delayed-best-practices>`.


Indirect Dependencies
---------------------

Sometimes you might find yourself wanting to add a dependency to a task that does
not take the result of that dependency as an input. For example when a task depends
on the side-effect of another task. In these cases you can use
``dask.graph_manipulation.bind``.

.. code-block:: python

    import dask
    from dask.graph_manipulation import bind

    DATA = []

    @dask.delayed
    def inc(x):
        return x + 1

    @dask.delayed
    def add_data(x):
        DATA.append(x)

    @dask.delayed
    def sum_data(x):
        return sum(DATA) + x

    a = inc(1)
    b = add_data(a)
    c = inc(3)
    d = add_data(c)
    e = inc(5)
    f = bind(sum_data, [b, d])(e)
    f.compute()

``sum_data`` will operate on DATA only after both the expected items have
been appended to it. ``bind`` can also be used along with direct dependencies
passed through the function arguments.
