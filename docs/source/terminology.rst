Terminology
======================

Dask is a large project with many terms that might be unfamiliar to new users. This glossary is to provide a way
for new and returning users alike to find the definitions of various Dask terms.

..
    _ the following terms should be in alphabetical order

.. glossary::
    Chunk
        Dask Arrays are structures that divide a large array into smaller parts, enabling parallel computation
        across the parts. These parts are called *chunks*. Each chunk is a NumPy (or NumPy-like) array. Chunks in
        Dask Arrays serve a similar purpose as partitions in Dask DataFrames. Throughout the Dask Array API,
        ``chunks`` stands for “chunk shape” rather than “number of chunks”.
        See :doc:`Chunks <array-chunks>` for more.

    Partition
        Dask DataFrames are structures that divide a large DataFrame into smaller parts, enabling parallel computation
        across the parts. These parts are called *partitions*. Each partition is a pandas (or pandas-like) DataFrame.
        Partitions in Dask DataFrame serve a similar purpose as chunks in Dask Arrays.

    Scheduler
        (also called *task scheduler*)

        Orchestrates parallel execution for Dask objects and operations.
        The scheduler does not do any "work", but designates parts of the task graph to execute on other processes or
        hardware. The scheduler can be run in a thread or process, or as a separate machine with a distributed cluster.
        See :doc:`Scheduling <scheduling>` for more information.

    Task Graph
        Dask operations are executed via a *task graph*, where each node in the graph is a normal Python function
        and edges between nodes are normal Python objects that are created by one task as outputs and used as
        inputs in another task.


For Dask Developers
--------------------------

Additional terms are used within the Dask development community. For users of Dask that are not contributing to the
actual Dask project, this section can be skipped.

.. glossary::
    Blockdims
        *blockdims* is used through the Dask codebase and is the same as :term:`Chunks<Chunk>`.