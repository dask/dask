Task Graphs
===========

Internally, Dask encodes algorithms in a simple format involving Python dicts,
tuples, and functions. This graph format can be used in isolation from the
dask collections. Working directly with dask graphs is rare, unless you intend
to develop new modules with Dask.  Even then, :doc:`dask.delayed <delayed>` is
often a better choice. If you are a *core developer*, then you should start here.

.. toctree::
   :maxdepth: 1

   spec.rst
   custom-graphs.rst
   optimize.rst
   custom-collections.rst
   high-level-graphs.rst


Motivation
----------

Normally, humans write programs and then compilers/interpreters interpret them
(for example, ``python``, ``javac``, ``clang``).  Sometimes humans disagree with how
these compilers/interpreters choose to interpret and execute their programs.
In these cases, humans often bring the analysis, optimization, and execution of
code into the code itself.

Commonly a desire for parallel execution causes this shift of responsibility
from compiler to human developer.  In these cases, we often represent the
structure of our program explicitly as data within the program itself.

A common approach to parallel execution in user-space is *task scheduling*.  In
task scheduling we break our program into many medium-sized tasks or units of
computation, often a function call on a non-trivial amount of data.  We
represent these tasks as nodes in a graph with edges between nodes if one task
depends on data produced by another.  We call upon a *task scheduler* to
execute this graph in a way that respects these data dependencies and leverages
parallelism where possible, multiple independent tasks can be run
simultaneously.

Many solutions exist.  This is a common approach in parallel execution
frameworks.  Often task scheduling logic hides within other larger frameworks
(Luigi, Storm, Spark, IPython Parallel, and so on) and so is often reinvented.

Dask is a specification that encodes task schedules with minimal incidental
complexity using terms common to all Python projects, namely dicts, tuples,
and callables.  Ideally this minimum solution is easy to adopt and understand
by a broad community.

Example
-------

.. image:: _static/dask-simple.png
   :height: 400px
   :alt: A simple dask dictionary
   :align: right


Consider the following simple program:

.. code-block:: python

   def inc(i):
       return i + 1

   def add(a, b):
       return a + b

   x = 1
   y = inc(x)
   z = add(y, 10)

We encode this as a dictionary in the following way:

.. code-block:: python

   d = {'x': 1,
        'y': (inc, 'x'),
        'z': (add, 'y', 10)}

While less pleasant than our original code, this representation can be analyzed
and executed by other Python code, not just the CPython interpreter.  We don't
recommend that users write code in this way, but rather that it is an
appropriate target for automated systems.  Also, in non-toy examples, the
execution times are likely much larger than for ``inc`` and ``add``, warranting
the extra complexity.


Schedulers
----------

The Dask library currently contains a few schedulers to execute these
graphs.  Each scheduler works differently, providing different performance
guarantees and operating in different contexts.  These implementations are not
special and others can write different schedulers better suited to other
applications or architectures easily.  Systems that emit dask graphs (like
Dask Array, Dask Bag, and so on) may leverage the appropriate scheduler for
the application and hardware.


Task Expectations
-----------------

When a task is submitted to Dask for execution, there are a number of assumptions
that are made about that task. In general, tasks with side-effects that alter the
state of a future in-place are not recommended. Dask Futures are mutable, and can
be updated in-place. For example, consider a workflow involving a
``pandas.DataFrame``:

.. code-block:: python

   from distributed import Client
   import pandas 

   c = Client()  # LocalCluster
   df = pandas.DataFrame({"c":[1,2,3,4], "d":[4,5,6,76]})  
   a_future = c.scatter(df)
   assert a_future.result().index.equals(df.index) 
   b_future = c.submit(
      lambda df: df.set_axis(
         df.index.map(str), # Convert index to strings
         axis="index",
         inplace=True, # `inplace=True` will alter `a_future`
      ),
      a_future
   )
   assert a_future.result().index.equals(df.index)  # False, `a_future` is modified inplace

In the example above Dask will update the field (``index``) of the Future in-place.
This behavior holds for any object with mutable underlying data or fields. Completed 
Futures in Dask contain pointers to Python objects. If a field or attribute of that
underlying Python object is updated in-place, e.g. with ``setattr``, the Future
is effectively modified in-place. Subsequent accesses of that Future will return the
updated object.
