Client Interaction
==================

As discussed in the quickstart_ users can interact with the `worker-center`_
network with the Executor abstraction.

This is built with lower level functions described below.

.. _`quickstart`: quickstart.html
.. _`worker-center`: worker-center.html


RemoteData
----------

The data within worker nodes forms a poorman's distributed key-value store.  We
can refer to an item in this key-value store with a ``RemoteData`` object.

.. autoclass:: distributed.client.RemoteData
   :members: get


Scatter/Gather
--------------

Users rarely create RemoteData objects by hand.  They are created by other
client libraries or functions like ``gather`` and ``scatter``.

.. autofunction:: distributed.client.scatter
.. autofunction:: distributed.client.gather
.. autofunction:: distributed.client.delete
.. autofunction:: distributed.client.clear

Executor
--------

As described in the `executor example in the quickstart`_, the Executor mimics the
``concurrent.futures.Executor`` object, providing the functions ``map`` and
``submit``.  These functions produce ``Future`` objects so that repeated
applications of ``map`` and ``submit`` leave data on the remote network.

Additionally, Executor attempts to run computations on nodes that already have
the data, further avoiding unnecessary communication.

.. autoclass:: distributed.executor.Executor
   :members: map, submit, gather, get

.. _`executor example in the quickstart`: quickstart.html#executor
