Data Processing Pipelines
=========================

`Example notebook <http://nbviewer.ipython.org/github/dask/dask-examples/blob/master/do-and-profiler.ipynb>`_.

Now, rebuilding the example from :ref:`custom graphs <custom-graph-example>`:

.. code-block:: python

    from dask import delayed, value

    @delayed
    def load(filename):
        ...

    @delayed
    def clean(data):
        ...

    @delayed
    def analyze(sequence_of_data):
        ...

    @delayed
    def store(result):
        with open(..., 'w') as f:
            f.write(result)

    files = ['myfile.a.data', 'myfile.b.data', 'myfile.c.data']
    loaded = [load(i) for i in files]
    cleaned = [clean(i) for i in loaded]
    analyzed = analyze(cleaned)
    stored = store(analyzed)

    stored.compute()

This builds the same graph as seen before, but using normal Python syntax. In
fact, the only difference between Python code that would do this in serial, and
the parallel version with dask is the ``delayed`` decorators on the functions, and
the call to ``compute`` at the end.
