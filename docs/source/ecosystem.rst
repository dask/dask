:orphan:

.. this page is referenced from the topbar which comes from the theme

Ecosystem
=========

There are a number of open source projects that extend the Dask interface and provide different
mechanisms for deploying Dask clusters. This is likely an incomplete list so if you spot something
missing - please `suggest a fix <https://github.com/dask/dask/edit/main/docs/source/ecosystem.rst>`_!

Building on Dask
----------------
Many packages include built-in support for Dask collections or wrap Dask collections internally
to enable parallelization.

Array
~~~~~
- `xarray <https://xarray.pydata.org>`_:  Wraps Dask
  Array, offering the same scalability, but with axis labels which add convenience when
  dealing with complex datasets.
- `cupy <https://docs.cupy.dev/en/stable>`_: Part of the Rapids project, GPU-enabled arrays
  can be used as the blocks of Dask Arrays. See the section :doc:`gpu` for more information.
- `sparse <https://github.com/pydata/sparse>`_: Implements sparse arrays of arbitrary dimension
  on top of ``numpy`` and ``scipy.sparse``.
- `pint <https://pint.readthedocs.io>`_: Allows arithmetic operations between them and conversions
  from and to different units.

DataFrame
~~~~~~~~~
- `cudf <https://docs.rapids.ai/api/cudf/stable/>`_: Part of the Rapids project, implements
  GPU-enabled dataframes which can be used as partitions in Dask Dataframes.
- `dask-geopandas <https://github.com/geopandas/dask-geopandas>`_: Early-stage subproject of
  geopandas, enabling parallelization of geopandas dataframes.

SQL
~~~
- `blazingSQL`_: Part of the Rapids project, implements SQL queries using ``cuDF``
  and Dask, for execution on CUDA/GPU-enabled hardware, including referencing
  externally-stored data.
- `dask-sql`_: Adds a SQL query layer on top of Dask.
  The API matches blazingSQL but it uses CPU instead of GPU. It still under development
  and not ready for a production use-case.
- `fugue-sql`_: Adds an abstract layer that makes code portable between across differing
  computing frameworks such as Pandas, Spark and Dask.

.. _blazingSQL: https://docs.blazingsql.com/
.. _dask-sql: https://dask-sql.readthedocs.io/en/latest/
.. _fugue-sql: https://fugue-tutorials.readthedocs.io/en/latest/tutorials/fugue_sql/index.html

Machine Learning
~~~~~~~~~~~~~~~~
- `dask-ml <https://ml.dask.org>`_: Implements distributed versions of common machine learning algorithms.
- `scikit-learn <https://scikit-learn.org/stable/>`_: Provide 'dask' to the joblib backend to parallelize
  scikit-learn algorithms with dask as the processor.
- `xgboost <https://xgboost.readthedocs.io>`_: Powerful and popular library for gradient boosted trees;
  includes native support for distributed training using dask.
- `lightgbm <https://lightgbm.readthedocs.io>`_: Similar to XGBoost; lightgmb also natively supplies native
  distributed training for decision trees.

Deploying Dask
--------------
There are many different implementations of the Dask distributed cluster.

- `dask-jobqueue <https://jobqueue.dask.org>`_: Deploy Dask on job queuing systems like PBS, Slurm, MOAB, SGE, LSF, and HTCondor.
- `dask-kubernetes <https://kubernetes.dask.org>`_: Deploy Dask workers on Kubernetes from within a Python script or interactive session.
- `dask-helm <https://helm.dask.org>`_: Deploy Dask and (optionally) Jupyter or JupyterHub on Kubernetes easily using Helm.
- `dask-yarn / Hadoop <https://yarn.dask.org>`_: Deploy Dask on YARN clusters, such as are found in traditional Hadoop
  installations.
- `dask-cloudprovider <https://cloudprovider.dask.org>`_: Deploy Dask on various cloud platforms such as AWS, Azure, and GCP
  leveraging cloud native APIs.
- `dask-gateway <https://gateway.dask.org>`_: Secure, multi-tenant server for managing Dask clusters. Launch and use Dask
  clusters in a shared, centrally managed cluster environment, without requiring users to have direct access to the underlying
  cluster backend.
- `dask-cuda <https://github.com/rapidsai/dask-cuda>`_: Construct a Dask cluster which resembles ``LocalCluster``  and is specifically
  optimized for GPUs.
