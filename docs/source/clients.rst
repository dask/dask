Client Interaction
==================

As discussed in the :doc:`quickstart <quickstart>` users can interact with the :doc:`worker-center <worker-center>`
network with the Executor abstraction.

This is built with lower level functions described below.

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
