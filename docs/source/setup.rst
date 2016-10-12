Setup Network
=============

A ``dask.distributed`` network consists of one ``Scheduler`` node and several
``Worker`` nodes.  One can set these up in a variety of ways


Using the Command Line
----------------------

We launch the ``dask-scheduler`` executable in one process and the
``dask-worker`` executable in several processes, possibly on different
machines.

Launch ``dask-scheduler`` on one node::

   $ dask-scheduler
   Start scheduler at 192.168.0.1:8786

Then launch ``dask-worker`` on the rest of the nodes, providing the address to the
node that hosts ``dask-scheduler``::

   $ dask-worker 192.168.0.1:8786
   Start worker at:            192.168.0.2:12345
   Registered with center at:  192.168.0.1:8786

   $ dask-worker 192.168.0.1:8786
   Start worker at:            192.168.0.3:12346
   Registered with center at:  192.168.0.1:8786

   $ dask-worker 192.168.0.1:8786
   Start worker at:            192.168.0.4:12347
   Registered with center at:  192.168.0.1:8786

There are various mechanisms to deploy these executables on a cluster, ranging
from manualy SSH-ing into all of the nodes to more automated systems like
SGE/SLURM/Torque or Yarn/Mesos. Additionally, cluster SSH tools exist to
send the same commands to many machines. One example is `tmux-cssh`__.

__ https://github.com/dennishafemann/tmux-cssh


Using SSH
---------

The convenience script ``dask-ssh`` opens several SSH connections to your
target computers and initializes the network accordingly. You can
give it a list of hostnames or IP addresses::

   $ dask-ssh 192.168.0.1 192.168.0.2 192.168.0.3 192.168.0.4

Or you can use normal UNIX grouping::

   $ dask-ssh 192.168.0.{1,2,3,4}

Or you can specify a hostfile that includes a list of hosts::

   $ cat hostfile.txt
   192.168.0.1
   192.168.0.2
   192.168.0.3
   192.168.0.4

   $ dask-ssh --hostfile hostfile.txt

The ``dask-ssh`` utility depends on the ``paramiko``::

    pip install paramiko


Using the Python API
--------------------

Alternatively you can start up the ``distributed.scheduler.Scheduler`` and
``distributed.worker.Worker`` objects within a Python session manually.  Both
are ``torando.tcpserver.TCPServer`` objects.

Start the Scheduler, provide the listening port (defaults to 8786) and Tornado
IOLoop (defaults to ``IOLoop.current()``)

.. code-block:: python

   from distributed import Scheduler
   from tornado.ioloop import IOLoop
   from threading import Thread

   loop = IOLoop.current()
   t = Thread(target=loop.start, daemon=True)
   t.start()

   s = Scheduler(loop=loop)
   s.start(8786)

On other nodes start worker processes that point to the IP address and port of
the scheduler.

.. code-block:: python

   from distributed import Worker
   from tornado.ioloop import IOLoop
   from threading import Thread

   loop = IOLoop.current()
   t = Thread(target=loop.start, daemon=True)
   t.start()

   w = Worker('127.0.0.1', 8786, loop=loop)
   w.start(0)  # choose randomly assigned port

Alternatively, replace ``Worker`` with ``Nanny`` if you want your workers to be
managed in a separate process by a local nanny process.  This allows workers to
restart themselves in case of failure, provides some additional monitoring, and
is useful when coordinating many workers that should live in different
processes to avoid the GIL_.

.. _GIL: https://docs.python.org/3/glossary.html#term-gil


Using LocalCluster
------------------

You can do the work above easily using :doc:`LocalCluster<local-cluster>`.

.. code-block:: python

   from distributed import LocalCluster
   c = LocalCluster(nanny=False)

A scheduler will be available under ``c.scheduler`` and a list of workers under
``c.workers``.  There is an IOLoop running in a background thread.


Using Amazon EC2
----------------

See the :doc:`EC2 quickstart <ec2>` for information on the ``dask-ec2`` easy
setup script to launch a canned cluster on EC2.


Cluster Resource Managers
-------------------------

Dask.distributed has been deployed on dozens of different cluster resource
managers.  This section contains links to some external projects, scripts, and
instructions that may serve as useful starting points.

Kubernetes
~~~~~~~~~~

*  https://github.com/ogrisel/docker-distributed
*  https://github.com/hammerlab/dask-distributed-on-kubernetes/

Marathon
~~~~~~~~

*  https://github.com/mrocklin/dask-marathon

DRMAA (SGE, SLURM, Torque, etc..)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*  https://github.com/mfouesneau/dasksge

YARN
~~~~

*   https://knit.readthedocs.io/en/latest/


Software Environment
--------------------

The workers and clients should all share the same software environment.  That
means that they should all have access to the same libraries and that those
libraries should be the same version.  Dask generally assumes that it can call
a function on any worker with the same outcome (unless explicitly told
otherwise.)

This is typically enforced through external means, such as by having a network
file system (NFS) mount for libraries, by starting the ``dask-worker``
processes in equivalent Docker_ containers, using Conda_ environments, or
through any of the other means typically employed by cluster administrators.

.. _Docker: https://www.docker.com/
.. _Conda: http://conda.pydata.org/docs/


Windows
~~~~~~~

.. note::

  - Running a ``dask-scheduler`` on Windows architectures is supported for only a
    limited number of workers (roughly 100). This is a detail of the underlying tcp server
    implementation and is discussed `here`__.

  - Running ``dask-worker`` processes on Windows is well supported, performant, and without limit.

If you wish to run in a primarily Windows environment, it is recommneded
to run a ``dask-scheduler`` on a linux or MacOSX environment, with ``dask-worker`` workers
on the Windows boxes. This works because the scheduler environment is de-coupled from that of
the workers.

__ https://github.com/jfisteus/ztreamy/issues/26

