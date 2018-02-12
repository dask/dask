Kubernetes-Native
=================

If you are already running Python code on a Kubernetes cluster
then you can easily start a Dask cluster by using Daskernetes_.

.. code-block:: bash

   pip install daskernetes

.. code-block:: python

   from daskernetes import KubeCluster
   cluster = KubeCluster.from_yaml('worker-template.yaml')
   cluster.scale(20)  # create a fixed set of twenty workers
   cluser.adapt()  # or create and destroy workers dynamically based on workload

   from dask.distributed import Client
   client = Client(cluster)

.. code-block:: yaml

    # worker-template.yaml
    metadata:
    spec:
      restartPolicy: Never
      containers:
      - args: [dask-worker, --nthreads, '2', --no-bokeh, --memory-limit, 7GB, --death-timeout, '60']
        image: daskdev/dask:latest
        name: dask-worker
        env:
          - name: EXTRA_PIP_PACKAGES
            value: fastparquet s3fs
        resources:
          limits:
            cpu: "1.75"
            memory: 6G
          requests:
            cpu: "1.75"
            memory: 6G

This cluster will expand and contract within the lifetime of the Python
process.  If you are looking for something longer-lived, or need help also
starting a Jupyter server to run your Python code, then you should consider
using :doc:`Kubernetes and Helm <kubernetes-helm>`.

For more information on this approach with Daskernetes, please see `external
documentation <https://daskernetes.readthedocs.io>`_.

.. _Daskernetes: https://daskernetes.readthedocs.io
