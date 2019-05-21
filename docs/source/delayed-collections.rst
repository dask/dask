Working with Collections
========================

Often we want to do a bit of custom work with ``dask.delayed`` (for example,
for complex data ingest), then leverage the algorithms in ``dask.array`` or
``dask.dataframe``, and then switch back to custom work.  To this end, all
collections support ``from_delayed`` functions and ``to_delayed``
methods.

As an example, consider the case where we store tabular data in a custom format
not known by Dask DataFrame.  This format is naturally broken apart into
pieces and we have a function that reads one piece into a Pandas DataFrame.
We use ``dask.delayed`` to lazily read these files into Pandas DataFrames,
use ``dd.from_delayed`` to wrap these pieces up into a single
Dask DataFrame, use the complex algorithms within the DataFrame
(groupby, join, etc.), and then switch back to ``dask.delayed`` to save our results
back to the custom format:

.. code-block:: python

   import dask.dataframe as dd
   from dask.delayed import delayed

   from my_custom_library import load, save

   filenames = ...
   dfs = [delayed(load)(fn) for fn in filenames]

   df = dd.from_delayed(dfs)
   df = ... # do work with dask.dataframe

   dfs = df.to_delayed()
   writes = [delayed(save)(df, fn) for df, fn in zip(dfs, filenames)]

   dd.compute(*writes)

Data science is often complex, and ``dask.delayed`` provides a release valve for
users to manage this complexity on their own, and solve the last mile problem
for custom formats and complex situations.
