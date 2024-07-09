Dask DataFrame
==============

.. meta::
    :description: A Dask DataFrame is a large parallel DataFrame composed of many smaller pandas DataFrames, split along the index. These pandas DataFrames may live on disk for larger-than-memory computing on a single machine, or on many different machines in a cluster.

.. toctree::
   :maxdepth: 1
   :hidden:

   Load and Save Data <dataframe-create.rst>
   Internal Design <dataframe-design.rst>
   Optimizer <dataframe-optimizer.rst>
   Best Practices <dataframe-best-practices.rst>
   API <dataframe-api.rst>
   dataframe-extra.rst

.. grid:: 1 1 2 2

   .. grid-item::
      :columns: 12 12 8 8

      Dask DataFrame helps you process large tabular data by parallelizing pandas,
      either on your laptop for larger-than-memory computing, or on a distributed
      cluster of computers.

      -  **Just pandas:** Dask DataFrames are a collection of many pandas DataFrames.

         The API is the same.  The execution is the same.
      -  **Large scale:** Works on 100 GiB on a laptop, or 100 TiB on a cluster.
      -  **Easy to use:** Pure Python, easy to set up and debug.

   .. grid-item::
      :columns: 12 12 4 4

      .. image:: images/dask-dataframe.svg
         :alt: Column of four squares collectively labeled as a Dask DataFrame with a single constituent square labeled as a pandas DataFrame.

Dask DataFrames coordinate many pandas DataFrames/Series arranged along the
index.  A Dask DataFrame is partitioned *row-wise*, grouping rows by index value
for efficiency.  These pandas objects may live on disk or on other machines.

From pandas to Dask
-------------------

Dask DataFrame copies pandas, and so should be familiar to most users

.. tab-set::

   .. tab-item:: Load Data

      Pandas and Dask have the same API, and so switching from one to the other
      is straightforward.

      .. grid:: 1 1 2 2

          .. grid-item::

              .. code-block:: python

                  >>> import pandas as pd

                  >>> df = pd.read_parquet('s3://mybucket/myfile.parquet')
                  >>> df.head()
                  0  1  a
                  1  2  b
                  2  3  c

          .. grid-item::

              .. code-block:: python

                  >>> import dask.dataframe as dd

                  >>> df = dd.read_parquet('s3://mybucket/myfile.*.parquet')
                  >>> df.head()
                  0  1  a
                  1  2  b
                  2  3  c

   .. tab-item:: Data Processing

      Dask does pandas in parallel.
      Dask is lazy; when you want an in-memory result add ``.compute()``.

      .. grid:: 1 1 2 2

          .. grid-item::

              .. code-block:: python

                  >>> import pandas as pd

                  >>> df = df[df.value >= 0]
                  >>> joined = df.merge(other, on="account")
                  >>> result = joined.groupby("account").value.mean()

                  >>> result
                  alice 123
                  bob   456


          .. grid-item::

              .. code-block:: python

                  >>> import dask.dataframe as dd

                  >>> df = df[df.value >= 0]
                  >>> joined = df.merge(other, on="account")
                  >>> result = joined.groupby("account").value.mean()

                  >>> result.compute()
                  alice 123
                  bob   456

   .. tab-item:: Machine Learning

      Machine learning libraries often have Dask submodules that
      expect Dask DataFrames and operate in parallel.

      .. grid:: 1 1 2 2

          .. grid-item::

              .. code-block:: python

                  >>> import pandas as pd
                  >>> import xgboost
                  >>> from sklearn.cross_validation import train_test_split

                  >>> X_train, X_test, y_train, y_test = train_test_split(
                  ...    X, y, test_size=0.2,
                  )
                  >>> dtrain = xgboost.DMatrix(X_train, label=y_train)

                  >>> xgboost.train(params, dtrain, 100)
                  <xgboost.Booster ...>

          .. grid-item::

              .. code-block:: python

                  >>> import dask.dataframe as dd
                  >>> import xgboost.dask
                  >>> from dask_ml.model_selection import train_test_split

                  >>> X_train, X_test, y_train, y_test = train_test_split(
                  ...    X, y, test_size=0.2,
                  )
                  >>> dtrain = xgboost.dask.DaskDMatrix(client, X, y)

                  >>> xgboost.dask.train(params, dtrain, 100)
                  <xgboost.Booster ...>

As with all Dask collections, you trigger computation by calling the
``.compute()`` method or persist data in distributed memory with the
``.persist()`` method.

When not to use Dask DataFrames
-------------------------------

Dask DataFrames are often used either when ...

1.  Your data is too big
2.  Your computation is too slow and other techniques don't work

You should probably stick to just using pandas if ...

1.  Your data is small
2.  Your computation is fast (subsecond)
3.  There are simpler ways to accelerate your computation, like avoiding
    ``.apply`` or Python for loops and using a built-in pandas method instead.

Examples
--------

Dask DataFrame is used across a wide variety of applications — anywhere where working
with large tabular dataset. Here are a few large-scale examples:

- `Parquet ETL with Dask DataFrame <https://docs.coiled.io/user_guide/uber-lyft.html?utm_source=dask-docs&utm_medium=dataframe>`_
- `XGBoost model training with Dask DataFrame <https://docs.coiled.io/user_guide/xgboost.html?utm_source=dask-docs&utm_medium=dataframe>`_
- `Visualize 1,000,000,000 points <https://docs.coiled.io/user_guide/datashader.html?utm_source=dask-docs&utm_medium=dataframe>`_

These examples all process larger-than-memory datasets on Dask clusters deployed with
`Coiled <https://coiled.io/?utm_source=dask-docs&utm_medium=dataframe>`_,
but there are many options for managing and deploying Dask.
See our :doc:`deploying` documentation for more information on deployment options.

You can also visit https://examples.dask.org/dataframe.html for a collection of additional examples.

.. raw:: html

  <iframe width="560"
          height="315"
          src="https://www.youtube.com/embed/AT2XtFehFSQ"
          style="margin: 0 auto 20px auto; display: block;"
          frameborder="0"
          allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
          allowfullscreen></iframe>
