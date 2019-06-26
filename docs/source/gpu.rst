GPUs
====

Dask works with GPUs in a few ways.


Custom Computations
-------------------

Many people use Dask alongside GPU-accelerated libraries like PyTorch and
TensorFlow to manage workloads across several machines.  They typically use
Dask's custom APIs, notably :doc:`Delayed <delayed>` and :doc:`Futures
<futures>`.

Dask doesn't need to know that these functions use GPUs.  It just runs Python
functions.  Whether or not those Python functions use a GPU is orthogonal to
Dask.  It will work regardless.

As a worked example, you may want to view this talk:

.. raw:: html

    <video width="560" height="315" controls>
        <source src="https://developer.download.nvidia.com/video/gputechconf/gtc/2019/video/S9198/s9198-dask-and-v100s-for-fast-distributed-batch-scoring-of-computer-vision-workloads.mp4"
                type="video/mp4">
    </video>

High Level Collections
----------------------

Dask can also help to scale out large array and dataframe computations by
combining the Dask Array and DataFrame collections with a GPU-accelerated
array or dataframe library.

Recall that :doc:`Dask Array <array>` creates a large array out of many NumPy
arrays and :doc:`Dask DataFrame <dataframe>` creates a large dataframe out of
many Pandas dataframes.  We can use these same systems with GPUs if we swap out
the NumPy/Pandas components with GPU-accelerated versions of those same
libraries, as long as the GPU accelerated version looks enough like
NumPy/Pandas in order to interoperate with Dask.

Fortunately, libraries that mimic NumPy, Pandas, and Scikit-Learn on the GPU do
exist.


DataFrames
~~~~~~~~~~

The `RAPIDS <https://rapids.ai>`_ libraries provide a GPU accelerated
Pandas-like library,
`cuDF <https://rapidsai.github.io/projects/cudf/en/latest/>`_,
which interoperates well and is tested against Dask DataFrame.

If you have cudf installed then you should be able to convert a Pandas-backed
Dask DataFrame to a cuDF-backed Dask DataFrame as follows:

.. code-block:: python

   import cudf

   df = df.map_partitions(cudf.from_pandas)  # convert pandas partitions into cudf partitions

However, cuDF does not support the entire Pandas interface, and so a variety of
Dask DataFrame operations will not function properly. Check the
`cudf API Reference <https://rapidsai.github.io/projects/cudf/en/latest/api.html>`_
for currently supported interface.


Arrays
~~~~~~

.. note:: Dask's integration with CuPy relies on features recently added to
   NumPy and CuPy, particularly in version ``numpy>=1.17`` and ``cupy>=6``

`Chainer's CuPy <https://cupy.chainer.org/>`_ library provides a GPU
accelerated NumPy-like library that interoperates nicely with Dask Array.

If you have CuPy installed then you should be able to convert a NumPy-backed
Dask Array into a CuPy backed Dask Array as follows:

.. code-block:: python

   import cupy

   x = x.map_blocks(cupy.asarray)

CuPy is fairly mature and adheres closely to the NumPy API.  However, small
differences do exist and these can cause Dask Array operations to function
improperly. Check the
`CuPy Reference Manual <https://docs-cupy.chainer.org/en/stable/reference/index.html>`_
for API compatibility.


Scikit-Learn
~~~~~~~~~~~~

There are a variety of GPU accelerated machine learning libraries that follow
the Scikit-Learn Estimator API of fit, transform, and predict.  These can
generally be used within `Dask-ML's <https://ml.dask.org>`_ meta estimators,
such as `hyper parameter optimization <https://ml.dask.org/hyper-parameter-search.html>`_.

Some of these include:

-  `Skorch <https://skorch.readthedocs.io/>`_
-  `cuML <https://rapidsai.github.io/projects/cuml/en/latest/>`_
-  `LightGBM <https://github.com/Microsoft/LightGBM>`_
-  `XGBoost <https://xgboost.readthedocs.io/en/latest/>`_
-  `Thunder SVM <https://github.com/Xtra-Computing/thundersvm>`_
-  `Thunder GBM <https://github.com/Xtra-Computing/thundergbm>`_


Setup
-----

From the examples above we can see that the user experience of using Dask with
GPU-backed libraries isn't very different from using it with CPU-backed
libraries.  However, there are some changes you might consider making when
setting up your cluster.

Restricting Work
~~~~~~~~~~~~~~~~

By default Dask allows as many tasks as you have CPU cores to run concurrently.
However if your tasks primarily use a GPU then you probably want far fewer
tasks running at once.  There are a few ways to limit parallelism here:

-   Limit the number of threads explicitly on your workers using the
    ``--nthreads`` keyword in the CLI or the ``ncores=`` keyword the
    Cluster constructor.
-   Use `worker resources <https://distributed.dask.org/en/latest/resources.html>`_ and tag certain
    tasks as GPU tasks so that the scheduler will limit them, while leaving the
    rest of your CPU cores for other work

Specifying GPUs per Machine
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some configurations may have many GPU devices per node.  Dask is often used to
balance and coordinate work between these devices.

In these situations it is common to start one Dask worker per device, and use
the CUDA environment varible ``CUDA_VISIBLE_DEVICES`` to pin each worker to
prefer one device.

.. code-block:: bash

   # If we have four GPUs on one machine
   CUDA_VISIBLE_DEVICES=0 dask-worker ...
   CUDA_VISIBLE_DEVICES=1 dask-worker ...
   CUDA_VISIBLE_DEVICES=2 dask-worker ...
   CUDA_VISIBLE_DEVICES=3 dask-worker ...

The `Dask CUDA <https://github.com/rapidsai/dask-cuda>`_ project contains some
convenience CLI and Python utilities to automate this process.

Work in Progress
----------------

GPU computing is a quickly moving field today and as a result the information
in this page is likely to go out of date quickly.  We encourage interested
readers to check out `Dask's Blog <https://blog.dask.org>`_ which has more
timely updates on ongoing work.
