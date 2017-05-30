External Packages
=================

Dask work occurs in several different projects, each for different domains.
This page lists some of those projects.

Motivation
----------

Dask is used to parallelize a variety of other projects and domains.  By
creating a culture of many subprojects we enable smaller communities to more
easily build, package, and maintain functionality useful for their domain
with minimal coordination with the core project.

However, the lack of a central all-inclusive package also means that peripheral
domain-specific projects don't get as much exposure.  This page attempts to fix
this by providing some exposure in documentation.

Machine Learning
----------------

-  Dask GLM: `docs <http://dask-glm.readthedocs.io/en/latest/>`_ and `source <https://github.com/dask/dask-glm>`_.  Generalized linear models like Logistic Regression on large datasets.
-  Dask-SearchCV: `docs <http://dask-searchcv.readthedocs.io/en/latest/>`_ and `source <https://github.com/dask/dask-searchcv>`_.  Hyperparameter optimization of Scikit-Learn models
-  Dask PatternSearch: `source <https://github.com/eriknw/dask-patternsearch>`_.  Gradient free search optimization


Image Analysis
--------------

The `dask-image github organization <https://dask-image.github.io/>`_ collects
many common subprojects together based off of dask.array.


File Formats
------------

-  Fastparquet: `docs <http://fastparquet.readthedocs.io/en/latest/>`_ and `source <https://github.com/dask/fastparquet>`_.  Parquet format with NumPy, and Numba
-  Parquet-Cpp with Arrow: `docs <https://arrow.apache.org/docs/python/parquet.html>`_ and `parquet source <https://github.com/apache/parquet-cpp>`_ and `arrow source <https://github.com/apache/arrow>`_


Data Sources
------------

-  S3: `docs <http://s3fs.readthedocs.io/en/latest/>`_ and `source <https://github.com/dask/s3fs>`_.  Amazon's S3
-  GCS: `docs <http://gcsfs.readthedocs.io/en/latest/>`_ and `source <https://github.com/martindurant/gcsfs>`_.  Google's cloud storage
-  HDFS: `docs <http://hdfs3.readthedocs.io/en/latest/>`_ and `source <https://github.com/dask/hdfs3>`_.  Hadoop File System wrappers for Python


Cluster Managers
----------------

-  Kubernetes: `source <https://github.com/martindurant/dask-kubernetes>`_
-  YARN: `source <https://github.com/dask/dask-yarn>`_
-  Marathon: `source <https://github.com/mrocklin/dask-marathon>`_
-  DRMAA: `source <https://github.com/dask/dask-drmaa>`_
-  EC2: `source <https://github.com/dask/dask-ec2>`_


Other
-----

-  XArray: `docs <http://xarray.pydata.org/en/stable/>`_ and `source <https://github.com/pydata/xarray>`_ for labeled and indexed arrays
