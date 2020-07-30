Configuration Reference
=======================

.. contents:: :local:

.. note::
   It is possible to configure Dask inline with dot notation, with YAML or via environment variables.
   See the `conversion utility <configuration.html#conversion-utility>`_ for converting the following dot notation to other forms.

Dask
----

.. dask-config-block::
    :location: dask
    :config: https://raw.githubusercontent.com/dask/dask/master/dask/dask.yaml
    :schema: https://raw.githubusercontent.com/dask/dask/master/dask/dask-schema.yaml


Distributed
-----------

Client
~~~~~~

.. dask-config-block::
    :location: distributed.client
    :config: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed-schema.yaml

Comm
~~~~

.. dask-config-block::
    :location: distributed.comm
    :config: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed-schema.yaml


Dashboard
~~~~~~~~~

.. dask-config-block::
    :location: distributed.dashboard
    :config: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed-schema.yaml


Deploy
~~~~~~

.. dask-config-block::
    :location: distributed.deploy
    :config: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed-schema.yaml



Scheduler
~~~~~~~~~

.. dask-config-block::
    :location: distributed.scheduler
    :config: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed-schema.yaml


Worker
~~~~~~

.. dask-config-block::
    :location: distributed.worker
    :config: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed-schema.yaml


Admin
~~~~~

.. dask-config-block::
    :location: distributed.admin
    :config: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed-schema.yaml


UCX
~~~

.. dask-config-block::
   :location: ucx
   :config: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed.yaml
   :schema: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed-schema.yaml


RMM
~~~

.. dask-config-block::
    :location: rmm
    :config: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/master/distributed/distributed-schema.yaml
