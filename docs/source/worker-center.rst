Worker-Center
=============

We build a distributed network from two kinds of nodes

*  A single Center node
*  Several Worker nodes


Worker
------

Serve Data
~~~~~~~~~~

Workers serve data from a local dictionary of data::

   {'x': np.array(...),
    'y': np.array(...)}

Operations include normal dictionary operations, like get, set, and delete
key-value pairs.  In the following example we connect to two workers, collect
data from one worker and send it to another.

.. code-block:: python

   alice = rpc(ip='192.168.0.101', port=8788)
   d = yield alice.get_data(keys=['x', 'y'])

   bob = rpc(ip='192.168.0.102', port=8788)
   yield alice.update_data(data=d)

However, this is only an example, typically one does not manually manage data
transfer between workers.  They handle that as necessary on their own.

Compute
~~~~~~~

Workers evaluate functions provided by the user on their data.  They evaluate
functions either on their data or can automatically collect data from peers (as
shown above) if they don't have the necessary data but their peers do::

    z <- add(x, y)  # can be done with only local data
    z <- add(x, a)  # need to find out where we can get 'a'

The result of such a computation on our end is just a response ``b'OK'``.  The
actual result stays on the remote worker.

.. code-block:: python

   response = yield alice.compute(function=add, keys=['x', 'a'])
   assert response == b'OK'

The worker also reports back to the center whenever it completes a computation.
Metadata storage is centralized but all data transfer is peer-to-peer.  Here is
a quick example of what happens during a call to ``compute``::

   client:  Hey Alice!   Compute ``z <- add(x, a)``

   Alice:   Hey Center!  Who has a?
   Center:  Hey Alice!   Bob has a.
   Alice:   Hey Bob!     Send me a!
   Bob:     Hey Alice!   Here's a!

   Alice:   Hey Client!  I've computed z and am holding on to it!
   Alice:   Hey Center!  I have z!


.. autoclass:: distributed.worker.Worker


Center
------

Centers hold metadata about what data resides on which workers.

.. code-block:: python

   has_what = {'alice':   {'x', 'y'}
               'bob':     {'a', 'b', 'c'},
               'charlie': {'w', 'x', 'b'}}

   who_has  = {'x': {'alice', 'charlie'},
               'y': {'alice'},
               'a': {'bob'},
               'b': {'bob', 'charlie'},
               'c': {'bob'},
               'w': {'charlie'}}

All worker nodes in the same network have the same center node.  They update
and query this center node to share and learn what nodes have what data.  The
center node could conceptually be replaced by a Redis server.

.. autoclass:: distributed.center.Center
