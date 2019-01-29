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
   Start worker at:               192.168.0.2:12345
   Registered with Scheduler at:  192.168.0.1:8786

   $ dask-worker 192.168.0.1:8786
   Start worker at:               192.168.0.3:12346
   Registered with Scheduler at:  192.168.0.1:8786

   $ dask-worker 192.168.0.1:8786
   Start worker at:               192.168.0.4:12347
   Registered with Scheduler at:  192.168.0.1:8786

There are various mechanisms to deploy these executables on a cluster, ranging
from manualy SSH-ing into all of the nodes to more automated systems like
SGE/SLURM/Torque or Yarn/Mesos. Additionally, cluster SSH tools exist to
send the same commands to many machines. One example is `tmux-cssh`__.

.. note::

  - The scheduler and worker both need to accept TCP connections.  By default
    the scheduler uses port 8786 and the worker binds to a random open port.
    If you are behind a firewall then you may have to open particular ports or
    tell Dask to use particular ports with the ``--port`` and ``-worker-port``
    keywords.    Other ports like 8787, 8788, and 8789 are also useful to keep
    open for the diagnostic web interfaces.
  - More information about relevant ports is available by looking at the help
    pages with ``dask-scheduler --help`` and ``dask-worker --help``

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


Using a Shared Network File System and a Job Scheduler
------------------------------------------------------

Some clusters benefit from a shared network file system (NFS) and can use this
to communicate the scheduler location to the workers::

   dask-scheduler --scheduler-file /path/to/scheduler.json

   dask-worker --scheduler-file /path/to/scheduler.json
   dask-worker --scheduler-file /path/to/scheduler.json

.. code-block:: python

   >>> client = Client(scheduler_file='/path/to/scheduler.json')

This can be particularly useful when deploying ``dask-scheduler`` and
``dask-worker`` processes using a job scheduler like
``SGE/SLURM/Torque/etc..``  Here is an example using SGE's ``qsub`` command::

    # Start a dask-scheduler somewhere and write connection information to file
    qsub -b y /path/to/dask-scheduler --scheduler-file /path/to/scheduler.json

    # Start 100 dask-worker processes in an array job pointing to the same file
    qsub -b y -t 1-100 /path/to/dask-worker --scheduler-file /path/to/scheduler.json

Note, the ``--scheduler-file`` option is *only* valuable if your scheduler and
workers share a standard POSIX file system.


Using MPI
---------

You can launch a Dask network using ``mpirun`` or ``mpiexec`` and the
``dask-mpi`` command line executable.

.. code-block:: bash

   mpirun --np 4 dask-mpi --scheduler-file /path/to/scheduler.json

.. code-block:: python

   from dask.distributed import Client
   client = Client(scheduler_file='/path/to/scheduler.json')

This depends on the `mpi4py <http://mpi4py.readthedocs.io/>`_ library.  It only
uses MPI to start the Dask cluster, and not for inter-node communication.  You
may want to specify a high-bandwidth network interface like infiniband using
the ``--interface`` keyword

.. code-block:: bash

   mpirun --np 4 dask-mpi --nthreads 1 \
                          --interface ib0 \
                          --scheduler-file /path/to/scheduler.json

Using the Python API
--------------------

Alternatively you can start up the ``distributed.scheduler.Scheduler`` and
``distributed.worker.Worker`` objects within a Python session manually.

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
   s.start('tcp://:8786')   # Listen on TCP port 8786

On other nodes start worker processes that point to the URL of the scheduler.

.. code-block:: python

   from distributed import Worker
   from tornado.ioloop import IOLoop
   from threading import Thread

   loop = IOLoop.current()
   t = Thread(target=loop.start, daemon=True)
   t.start()

   w = Worker('tcp://127.0.0.1:8786', loop=loop)
   w.start()  # choose randomly assigned port

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
   c = LocalCluster(processes=False)

A scheduler will be available under ``c.scheduler`` and a list of workers under
``c.workers``.  There is an IOLoop running in a background thread.


Using AWS
---------

See `Cloud Deployments`_ for the latest information on deploying to Amazon
cloud.

.. _`Cloud Deployments`: https://docs.dask.org/en/latest/setup/cloud.html


Using Google Cloud
------------------

See the dask-kubernetes_ project to easily launch clusters on `Google Kubernetes
Engine`_.

.. _dask-kubernetes: https://github.com/dask/dask-kubernetes
.. _`Google Kubernetes Engine`: https://cloud.google.com/kubernetes-engine/

Cluster Resource Managers
-------------------------

Dask.distributed has been deployed on dozens of different cluster resource
managers.  This section contains links to some external projects, scripts, and
instructions that may serve as useful starting points.

Kubernetes
~~~~~~~~~~

*  https://github.com/martindurant/dask-kubernetes
*  https://github.com/ogrisel/docker-distributed
*  https://github.com/hammerlab/dask-distributed-on-kubernetes/

Marathon
~~~~~~~~

*  https://github.com/mrocklin/dask-marathon

DRMAA (SGE, SLURM, Torque, etc..)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*  https://github.com/dask/dask-drmaa
*  https://github.com/mfouesneau/dasksge

YARN
~~~~

*   https://github.com/dask/dask-yarn
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

If you wish to run in a primarily Windows environment, it is recommended
to run a ``dask-scheduler`` on a linux or MacOSX environment, with ``dask-worker`` workers
on the Windows boxes. This works because the scheduler environment is de-coupled from that of
the workers.

__ https://github.com/jfisteus/ztreamy/issues/26


Customizing initialization
--------------------------

Both ``dask-scheduler`` and ``dask-worker`` support a ``--preload`` option that
allows custom initialization of each scheduler/worker respectively. A module or
python file passed as a ``--preload`` value is guaranteed to be imported before
establishing any connection. A ``dask_setup(service)`` function is called if
found, with a ``Scheduler`` or ``Worker`` instance as the argument. As the
service stops, ``dask_teardown(service)`` is called if present.

To support additional configuration a single ``--preload`` module may register
additional command-line arguments by exposing ``dask_setup`` as a  Click_
command.  This command will be used to parse additional arguments provided to
``dask-worker`` or ``dask-scheduler`` and will be called before service
initialization.

.. _Click: http://click.pocoo.org/


As an example, consider the following file that creates a
:doc:`scheduler plugin <plugins>` and registers it with the scheduler

.. code-block:: python

   # scheduler-setup.py
   import click

   from distributed.diagnostics.plugin import SchedulerPlugin

   class MyPlugin(SchedulerPlugin):
       def __init__(self, print_count):
         self.print_count = print_count
         SchedulerPlugin.__init__(self)

       def add_worker(self, scheduler=None, worker=None, **kwargs):
           print("Added a new worker at:", worker)
           if self.print_count and scheduler is not None:
               print("Total workers:", len(scheduler.workers))

   @click.command()
   @click.option("--print-count/--no-print-count", default=False)
   def dask_setup(scheduler, print_count):
       plugin = MyPlugin(print_count)
       scheduler.add_plugin(plugin)

We can then run this preload script by referring to its filename (or module name
if it is on the path) when we start the scheduler::

   dask-scheduler --preload scheduler-setup.py --print-count
