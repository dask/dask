User Interaction
================

You should read through the quickstart_ before reading this document.

We build multiple of user modules on top of the foundations_ of
``distributed``.

* ``Pool`` mimics ``multiprocessing.Pool``
*  ``get`` mimics ``dask.get``

The adventurous reader might consider what other systems might be built on top
of the `worker-center`_ network.

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
client libraries, like Pool, which themselves use other client functions like
``gather``and ``scatter``.

.. autofunction:: distributed.client.scatter
.. autofunction:: distributed.client.gather

Pool
----

As described in the `pool example in the quickstart`_, the Pool mimics the
``multiprocessing.Pool`` object, providing functions like ``map`` and
``apply``.  These functions produce ``RemoteData`` objects so that repeated
applications of ``map`` or ``apply`` leave data on the remote network.

Additionally, the Pool attempts to run computations on nodes that already have
the data, further avoiding unnecessary communication.

.. autoclass:: distributed.pool.Pool
   :members: map
             apply
             apply_async
             scatter
             gather

.. _`pool example in the quickstart`: quickstart.html#pool


get
---

As described in the `dask example in the quickstart`_, the get function mimics
the ``dask.get`` function, allowing computation of arbitrary directed acyclic
graphs of tasks.

.. _`dask example in the quickstart`: quickstart.html#get

Like the Pool, the get function also attempts to run computations on nodes that
already have the relevant data.  It does this in a greedy (but
cheap-to-schedule) fashion and so will make mistakes.

.. autofunction:: distributed.dask.get
