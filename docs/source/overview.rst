Dask Overview
=============

An explanation of dask task graphs.


Motivation
----------

Normally humans write programs and then compilers/interpreters interpret them
(e.g.  `python`, `javac`, `clang`).  Sometimes humans disagree with how these
compilers/interpreters choose to interpret and execute their programs.  In
these cases humans often bring the analysis, optimization, and execution of code
into the code itself.

Commonly a desire for parallel execution causes this shift of responsibility
from compiler to human developer.  In these cases we often represent the
structure of our program explicitly as data within the program itself.

A common approach to parallel execution in user-space is *task scheduling*.  In
task scheduling we break our program into many medium-sized tasks or units of
computation (often a function call on a non-trivial amount of data).  We
represent these tasks as nodes in a graph with edges between nodes if one task
depends on data produced by another.  We call upon a *task scheduler* to
execute this graph in a way that respects these data dependencies and leverages
parallelism where possible (e.g. multiple independent tasks can be run
simultaneously.)

Many solutions exist.  This is a common approach in parallel execution
frameworks.  Often task scheduling logic hides within other larger frameworks
(Luigi, Storm, Spark, IPython Parallel, etc.) and so is often reinvented.

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


Consider the following simple program

.. code-block:: python

   def inc(i):
       return i + 1

   def add(a, b):
       return a + b

   x = 1
   y = inc(x)
   z = add(y, 10)

We encode this as a dictionary in the following way

.. code-block:: python

   d = {'x': 1,
        'y': (inc, 'x'),
        'z': (add, 'y', 10)}

While less pleasant than our original code this representation can be analyzed
and executed by other Python code, not just the cPython interpreter.  We don't
recommend that users write code in this way, but rather that it is an
appropriate target for automated systems.  Also, in non-toy examples the
execution times are likely much larger than for ``inc`` and ``add``, warranting
the extra complexity.


Schedulers
----------

The ``dask`` library currently contains two schedulers, a simple inefficient
reference implementation and a more complex multi-threaded and cached
implementation.  These two implementations are not special.  Others can write
different schedulers better suited to other applications or architectures
easily.  They don't even need to import the ``dask`` library which merely
serves as reference.  Programs that emit dasks may leverage the appropriate
scheduler for their application and hardware.
