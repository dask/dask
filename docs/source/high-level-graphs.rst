High Level Graphs
=================

Dask graphs produced by collections like Arrays, Bags, and DataFrames have
high-level structure that can be useful for visualization and high-level
optimization.  The task graphs produced by these collections encode this
structure explicitly as ``HighLevelGraph`` objects.  This document describes
how to work with these in more detail.


Motivation and Example
----------------------

In full generality, Dask schedulers expect arbitrary task graphs where each
node is a single Python function call and each edge is a dependency between
two function calls.  These are usually stored in flat dictionaries.  Here is
some simple Dask DataFrame code and the task graph that it might generate:

.. code-block:: python

    import dask.dataframe as dd

    df = dd.read_csv('myfile.*.csv')
    df = df + 100
    df = df[df.name == 'Alice']

.. code-block:: python

   {
    ('read-csv', 0): (pandas.read_csv, 'myfile.0.csv'),
    ('read-csv', 1): (pandas.read_csv, 'myfile.1.csv'),
    ('read-csv', 2): (pandas.read_csv, 'myfile.2.csv'),
    ('read-csv', 3): (pandas.read_csv, 'myfile.3.csv'),
    ('add', 0): (operator.add, 'myfile.0.csv', 100),
    ('add', 1): (operator.add, 'myfile.1.csv', 100),
    ('add', 2): (operator.add, 'myfile.2.csv', 100),
    ('add', 3): (operator.add, 'myfile.3.csv', 100),
    ('filter', 0): (lambda part: part[part.name == 'Alice'], ('add', 0)),
    ('filter', 1): (lambda part: part[part.name == 'Alice'], ('add', 1)),
    ('filter', 2): (lambda part: part[part.name == 'Alice'], ('add', 2)),
    ('filter', 3): (lambda part: part[part.name == 'Alice'], ('add', 3)),
   }

The task graph is a dictionary that stores every Pandas-level function call
necessary to compute the final result.  We can see that there is some structure
to this dictionary if we separate out the tasks that were associated to each
high-level Dask DataFrame operation:

.. code-block:: python

   {
    # From the dask.dataframe.read_csv call
    ('read-csv', 0): (pandas.read_csv, 'myfile.0.csv'),
    ('read-csv', 1): (pandas.read_csv, 'myfile.1.csv'),
    ('read-csv', 2): (pandas.read_csv, 'myfile.2.csv'),
    ('read-csv', 3): (pandas.read_csv, 'myfile.3.csv'),

    # From the df + 100 call
    ('add', 0): (operator.add, 'myfile.0.csv', 100),
    ('add', 1): (operator.add, 'myfile.1.csv', 100),
    ('add', 2): (operator.add, 'myfile.2.csv', 100),
    ('add', 3): (operator.add, 'myfile.3.csv', 100),

    # From the df[df.name == 'Alice'] call
    ('filter', 0): (lambda part: part[part.name == 'Alice'], ('add', 0)),
    ('filter', 1): (lambda part: part[part.name == 'Alice'], ('add', 1)),
    ('filter', 2): (lambda part: part[part.name == 'Alice'], ('add', 2)),
    ('filter', 3): (lambda part: part[part.name == 'Alice'], ('add', 3)),
   }

By understanding this high-level structure we are able to understand our task
graphs more easily (this is more important for larger datasets when there are
thousands of tasks per layer) and how to perform high-level optimizations.  For
example, in the case above we may want to automatically rewrite our code to
filter our datasets before adding 100:

.. code-block:: python

    # Before
    df = dd.read_csv('myfile.*.csv')
    df = df + 100
    df = df[df.name == 'Alice']

    # After
    df = dd.read_csv('myfile.*.csv')
    df = df[df.name == 'Alice']
    df = df + 100

Dask's high level graphs help us to explicitly encode this structure by storing
our task graphs in layers with dependencies between layers:

.. code-block:: python

   >>> import dask.dataframe as dd

   >>> df = dd.read_csv('myfile.*.csv')
   >>> df = df + 100
   >>> df = df[df.name == 'Alice']

   >>> graph = df.__dask_graph__()
   >>> graph.layers
   {
    'read-csv': {('read-csv', 0): (pandas.read_csv, 'myfile.0.csv'),
                 ('read-csv', 1): (pandas.read_csv, 'myfile.1.csv'),
                 ('read-csv', 2): (pandas.read_csv, 'myfile.2.csv'),
                 ('read-csv', 3): (pandas.read_csv, 'myfile.3.csv')},

    'add': {('add', 0): (operator.add, 'myfile.0.csv', 100),
            ('add', 1): (operator.add, 'myfile.1.csv', 100),
            ('add', 2): (operator.add, 'myfile.2.csv', 100),
            ('add', 3): (operator.add, 'myfile.3.csv', 100)}

    'filter': {('filter', 0): (lambda part: part[part.name == 'Alice'], ('add', 0)),
               ('filter', 1): (lambda part: part[part.name == 'Alice'], ('add', 1)),
               ('filter', 2): (lambda part: part[part.name == 'Alice'], ('add', 2)),
               ('filter', 3): (lambda part: part[part.name == 'Alice'], ('add', 3))}
   }

   >>> graph.dependencies
   {
    'read-csv': set(),
    'add': {'read-csv'},
    'filter': {'add'}
   }

While the DataFrame points to the output layers on which it depends directly:

.. code-block:: python

   >>> df.__dask_layers__()
   {'filter'}


HighLevelGraphs
---------------

The :obj:`HighLevelGraph` object is a ``Mapping`` object composed of other
sub-``Mappings``, along with a high-level dependency mapping between them:

.. code-block:: python

   class HighLevelGraph(Mapping):
       layers: Dict[str, Mapping]
       dependencies: Dict[str, Set[str]]

You can construct a HighLevelGraph explicitly by providing both to the
constructor:

.. code-block:: python

   layers = {
      'read-csv': {('read-csv', 0): (pandas.read_csv, 'myfile.0.csv'),
                   ('read-csv', 1): (pandas.read_csv, 'myfile.1.csv'),
                   ('read-csv', 2): (pandas.read_csv, 'myfile.2.csv'),
                   ('read-csv', 3): (pandas.read_csv, 'myfile.3.csv')},

      'add': {('add', 0): (operator.add, 'myfile.0.csv', 100),
              ('add', 1): (operator.add, 'myfile.1.csv', 100),
              ('add', 2): (operator.add, 'myfile.2.csv', 100),
              ('add', 3): (operator.add, 'myfile.3.csv', 100)}

      'filter': {('filter', 0): (lambda part: part[part.name == 'Alice'], ('add', 0)),
                 ('filter', 1): (lambda part: part[part.name == 'Alice'], ('add', 1)),
                 ('filter', 2): (lambda part: part[part.name == 'Alice'], ('add', 2)),
                 ('filter', 3): (lambda part: part[part.name == 'Alice'], ('add', 3))}
   }

   dependencies = {'read-csv': set(),
                   'add': {'read-csv'},
                   'filter': {'add'}}

   graph = HighLevelGraph(layers, dependencies)

This object satisfies the ``Mapping`` interface, and so operates as a normal
Python dictionary that is the semantic merger of the underlying layers:

.. code-block:: python

   >>> len(graph)
   12
   >>> graph[('read-csv', 0)]
   ('read-csv', 0): (pandas.read_csv, 'myfile.0.csv'),


API
---

.. currentmodule:: dask.highlevelgraph

.. autoclass:: HighLevelGraph
   :members:
   :inherited-members:
