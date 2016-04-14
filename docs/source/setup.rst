Setup Network
=============

A ``distributed`` network consists of one ``Scheduler`` node and several
``Worker`` nodes.  One can set these up in a variety of ways


Using the Command Line
----------------------

We launch the ``dscheduler`` executable in one process and the ``dworker``
executable in several processes, possibly on different machines.

Launch ``dscheduler`` on one node::

   $ dscheduler
   Start scheduler at 192.168.0.1:8786

Then launch ``dworker`` on the rest of the nodes, providing the address to the
node that hosts ``dscheduler``::

   $ dworker 192.168.0.1:8786
   Start worker at:            192.168.0.2:12345
   Registered with center at:  192.168.0.1:8786

   $ dworker 192.168.0.1:8786
   Start worker at:            192.168.0.3:12346
   Registered with center at:  192.168.0.1:8786

   $ dworker 192.168.0.1:8786
   Start worker at:            192.168.0.4:12347
   Registered with center at:  192.168.0.1:8786

There are various mechanisms to deploy these executables on a cluster, ranging
from manualy SSH-ing into all of the nodes to more automated systems like
SGE/SLURM/Torque or Yarn/Mesos.


Using SSH
---------

The convenience script ``dcluster`` opens several SSH connections to your
target computers and initializes the network accordingly.  You can give it a
list of hostnames or IP addresses::

   $ dcluster 192.168.0.1 192.168.0.2 192.168.0.3 192.168.0.4

Or you can use normal UNIX grouping::

   $ dcluster 192.168.0.{1,2,3,4}

Or you can specify a hostfile that includes a list of hosts::

   $ cat hostfile.txt
   192.168.0.1
   192.168.0.2
   192.168.0.3
   192.168.0.4

   $ dcluster --hostfile hostfile.txt


Using the Python API
--------------------

Alternatively you can start up the ``distributed.scheduler.Scheduler`` and
``distributed.worker.Worker`` objects within a Python session manually.  Both
are ``torando.tcpserver.TCPServer`` objects.

Start the Scheduler, provide the listening port (defaults to 8786) and Tornado
IOLoop (defaults to ``IOLoop.current()``)

.. code-block:: python

   from distributed import Scheduler
   s = Scheduler(loop=loop)
   s.start(port)

On other nodes start worker processes that point to the IP address and port of
the scheduler.

.. code-block:: python

   from distributed import Worker
   w = Worker('192.168.0.1', 8786, loop=loop)
   w.start(0)  # choose randomly assigned port

Alternatively, replace ``Worker`` with ``Nanny`` if you want your workers to be
managed in a separate process by a local nanny process.

If you do not already have a Tornado event loop running you will need to create
and start one, possibly in a separate thread.

.. code-block:: python

   from tornado.ioloop import IOLoop
   loop = IOLoop()

   from threading import Thread
   t = Thread(target=loop.start)
   t.start()


Using Amazon EC2
----------------

See the :doc:`EC2 quickstart <dec2>` for information on the ``dec2`` easy setup
script to launch a canned cluster on EC2.


Cleanup
-------

It is common and safe to terminate the cluster by just killing the processes.
The workers and scheduler have no persistent state.

Programmatically you can use the client interface (``rpc``) to call the
``terminate`` methods on the workers and schedulers.
