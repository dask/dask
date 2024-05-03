Quickstart
==========

.. meta::
    :description: Dask Quickstart | Getting Started with Dask


.. currentmodule:: dask.dataframe

This is a short introduction to Dask, geared mainly towards new users.

Installation
------------

You can install Dask with ``conda``, with ``pip``.

.. tab-set::

   .. tab-item:: Conda

      You can  install or upgrade Dask using the
      `conda install <https://docs.conda.io/projects/conda/en/latest/commands/install.html>`_ command::

         conda install dask

      This installs Dask and **all** common dependencies, including pandas and NumPy.

   .. tab-item:: Pip

      To install Dask with ``pip`` run the following::

         python -m pip install "dask[complete]"    # Install everything

      This installs Dask and **all** common dependencies, including pandas and NumPy.

Intro to Dask
-------------

Dask offers a set of
API that give access to disitributed DataFrames build on pandas, distributed Arrays build on NumPy
and access to low level parallelism with a Futures API.

We will first create a Dask Cluster to run our computations on before we do a few
exemplary DataFrame computations.

.. tab-set::

   .. tab-item:: Local

     We can create a fully-featured Dask Cluster that is running on our local machine.
     This gives us access to multi-process computations and the diagnostic Dashboard.

     .. code-block:: python

        from dask.distributed import LocalCluster
        cluster = LocalCluster()          # Fully-featured local Dask cluster
        client = cluster.get_client()

     The :class:`~distributed.LocalCluster` follows the same interface as all other Dask Clusters, so
     you can easily switch to a distributed cluster.

     The Dask DataFrame API is used for scaling out pandas computations that can run
     on all cores of our machine. Dask uses pandas under the hood to perform the actual
     computations, so the API has a very pandas-like feel.

     .. code-block:: python
        :emphasize-lines: 7

        import dask.dataframe as dd
        import pandas as pd

        index = pd.date_range("2021-09-01", periods=2400, freq="1h")
        pdf = pd.DataFrame({"a": np.arange(2400), "b": list("abcaddbe" * 300)}, index=index)

        df = dd.from_pandas(df, npartitions=10)
        df

        Dask DataFrame Structure:
                                 a       b
        npartitions=10
        2021-09-01 00:00:00  int64  string
        2021-09-11 00:00:00    ...     ...
        ...                    ...     ...
        2021-11-30 00:00:00    ...     ...
        2021-12-09 23:00:00    ...     ...
        Dask Name: frompandas, 1 expression
        Expr=df

     :func:`from_pandas` can be used to create a Dask DataFrame from the
     pandas version. Dask also offers :ref:`IO-connectors <label_dataframe-io-api>` to read and write data from most
     common data sources.

     The DataFrame API is almost identical to the pandas API. We can use normal pandas
     methodology to execute operations on the Dask DataFrame.

     The main difference is that Dask is lazy and doesn't actually execute the computation
     before we ask for it. We can trigger the computation with :func:`~dask.compute` or
     :meth:`DataFrame.compute`.

     .. code-block:: python

        df.groupby("b").a.mean().compute()

        b
        a    1197.5
        b    1199.5
        c    1198.0
        d    1200.5
        e    1203.0
        Name: a, dtype: float64

     Triggering a computation will execute the query on the Dask Cluster and materialize the
     result as a pandas DataFrame or Series.

   .. tab-item:: Cloud

     |Coiled|_ is a commercial SaaS product handles the deployment of Dask Clusters for us.
     The free tier is large enough for getting started and exploring Dask, even for those
     who don't want to engage with a commercial company.  The API looks like the following.

     .. code-block:: python

        import coiled
        cluster = coiled.Cluster(n_workers=15, region="us-east-2")
        client = cluster.get_client()

     The Dask DataFrame API is used for scaling out pandas computations that can run
     on all cores of our machine. Dask uses pandas under the hood to perform the actual
     computations, so the API has a very pandas-like feel.

     .. code-block:: python
        :emphasize-lines: 3

        import dask.dataframe as dd

        df = dd.read_parquet("s3://coiled-data/uber/")
        df

        Dask DataFrame Structure:
                        hvfhs_license_num       tips    ...
        npartitions=720
                                   string    float64
                                      ...        ...
        ...                           ...        ...
                                      ...        ...
                                      ...        ...
        Dask Name: read_parquet, 1 expression
        Expr=ReadParquetFSSpec(8e22969)

     Dask offers :ref:`IO-connectors <label_dataframe-io-api>` to read and write data from most
     common data sources. The interface of these connectors is very similar to the pandas
     equivalent. :func:`read_parquet` has the same behavior as the pandas function.

     The DataFrame API is almost identical to the pandas API. We can use normal pandas
     methodology to execute operations on the Dask DataFrame.

     The main difference is that Dask is lazy and doesn't actually execute the computation
     before we ask for it. We can trigger the computation with :func:`~dask.compute` or
     :meth:`DataFrame.compute`.

     .. code-block:: python

        df.groupby("hvfhs_license_num").tips.mean().compute()

        hvfhs_license_num
        HV0005    0.946925
        HV0002    0.338458
        HV0004    0.217237
        HV0003    0.736362
        Name: tips, dtype: float64

     Triggering a computation will execute the query on the Coiled Cluster and materialize the
     result as a pandas DataFrame on our local machine.

.. _Coiled: https://docs.coiled.io/user_guide/index.html?utm_source=dask-docs&utm_medium=quickstart
.. |Coiled| replace:: **Coiled**
