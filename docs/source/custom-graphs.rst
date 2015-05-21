Custom Graphs
=============

Sometimes you want parallel computing but your application doesn't fit neatly
into something like dask.array or dask.bag.  In these cases you can interact
with the dask schedulers directly which operate well as standalone modules.

This provides a release valve for complex situations and allows advanced
projects with their own internal representation additional opportunities for
parallel execution.  As dask schedulers advance to compute in a wider variety
of contexts, code written to use any dask scheduler will advance as well.

Example
-------

As discussed in graphs_ the schedulers take a task graph (a dict of tuples
of functions) and a list of desired keys from that graph

.. code-block:: python

   def load(filename):
       ...

   def clean(data):
       ...

   def analyze(sequence_of_data):
       ...

   def store(result):
       with open(..., 'w') as f:
           f.write(result)

   dsk = {'load-1': (load, 'myfile.a.data'),
          'load-2': (load, 'myfile.b.data'),
          'load-3': (load, 'myfile.c.data'),
          'preprocess-1': (clean, 'load-1'),
          'preprocess-2': (clean, 'load-2'),
          'preprocess-3': (clean, 'load-3'),
          'analyze': (analyze, ['preprocess-%d' % i for i in [1, 2, 3]]),
          'store': (dump, 'analyze')}

   from dask.multiprocessing import get
   get(dsk, 'store')  # executes in parallel


.. _graphs: graphs.html
