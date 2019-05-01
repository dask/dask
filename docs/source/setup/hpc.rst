High Performance Computers
==========================

Relevant Machines
-----------------

This page includes instructions and guidelines when deploying Dask on high
performance supercomputers commonly found in scientific and industry research
labs.  These systems commonly have the following attributes:

1.  Some mechanism to launch MPI applications or use job schedulers like
    SLURM, SGE, TORQUE, LSF, DRMAA, PBS, or others
2.  A shared network file system visible to all machines in the cluster
3.  A high performance network interconnect, such as Infiniband
4.  Little or no node-local storage


Where to start
--------------

Most of this page documents various ways and best practices to use Dask on an
HPC cluster.  This is technical and aimed both at users with some experience
deploying Dask and also system administrators.

The preferred and simplest way to run Dask on HPC systems today both for new,
experienced users or administrator is to use 
`dask-jobqueue <https://jobqueue.dask.org>`_.

However, dask-jobqueue is slightly oriented toward interactive analysis usage,
and it might be better to use tools like dask-mpi in some routine batch
production workloads.


Dask-jobqueue and Dask-drmaa
----------------------------

The following projects provide easy high-level access to Dask using resource
managers that are commonly deployed on HPC systems:

1.  `dask-jobqueue <https://jobqueue.dask.org>`_ for use with PBS,
    SLURM, LSF, SGE and other resource managers
2.  `dask-drmaa <https://github.com/dask/dask-drmaa>`_ for use with any DRMAA
    compliant resource manager

They provide interfaces that look like the following:

.. code-block:: python

   from dask_jobqueue import PBSCluster

   cluster = PBSCluster(cores=36,
                        memory="100GB",
                        project='P48500028',
                        queue='premium',
                        interface='ib0',
                        walltime='02:00:00')

   cluster.scale(100)  # Start 100 workers in 100 jobs that match the description above

   from dask.distributed import Client
   client = Client(cluster)    # Connect to that cluster

Dask-jobqueue provides a lot of possibilities like adaptive dynamic scaling
of workers, we recommend reading the `dask-jobqueue documentation 
<https://jobqueue.dask.org>`_ first to get a basic system running and then
returning to this documentation for fine-tuning if necessary.


Using MPI
---------

.. note:: This section may not be necessary if you use a tool like 
   dask-jobqueue.

You can launch a Dask network using ``mpirun`` or ``mpiexec`` and the
``dask-mpi`` command line executable.

.. code-block:: bash

   mpirun --np 4 dask-mpi --scheduler-file /home/$USER/scheduler.json

.. code-block:: python

   from dask.distributed import Client
   client = Client(scheduler_file='/path/to/scheduler.json')

This depends on the `mpi4py <https://mpi4py.readthedocs.io/>`_ library.  It only
uses MPI to start the Dask cluster and not for inter-node communication. MPI 
implementations differ: the use of ``mpirun --np 4`` is specific to the 
``mpich`` or ``open-mpi`` MPI implementation installed through conda and linked
to mpi4py.

.. code-block:: bash

   conda install mpi4py

It is not necessary to use exactly this implementation, but you may want to
verify that your ``mpi4py`` Python library is linked against the proper
``mpirun/mpiexec`` executable and that the flags used (like ``--np 4``) are
correct for your system.  The system administrator of your cluster should be
very familiar with these concerns and able to help.

In some setups, MPI processes are not allowed to fork other processes. In this
case, we recommend using ``--no-nanny`` option in order to prevent dask from
using an additional nanny process to manage workers. 

Run ``dask-mpi --help`` to see more options for the ``dask-mpi`` command.


Using a Shared Network File System and a Job Scheduler
------------------------------------------------------

.. note:: This section is not necessary if you use a tool like dask-jobqueue.

Some clusters benefit from a shared File System (NFS, GPFS, Lustre or alike),
and can use this to communicate the scheduler location to the workers::

   dask-scheduler --scheduler-file /path/to/scheduler.json  # writes address to file

   dask-worker --scheduler-file /path/to/scheduler.json  # reads file for address
   dask-worker --scheduler-file /path/to/scheduler.json  # reads file for address

.. code-block:: python

   >>> client = Client(scheduler_file='/path/to/scheduler.json')

This can be particularly useful when deploying ``dask-scheduler`` and
``dask-worker`` processes using a job scheduler like
SGE/SLURM/Torque/etc.  Here is an example using SGE's ``qsub`` command::

    # Start a dask-scheduler somewhere and write the connection information to a file
    qsub -b y /path/to/dask-scheduler --scheduler-file /home/$USER/scheduler.json

    # Start 100 dask-worker processes in an array job pointing to the same file
    qsub -b y -t 1-100 /path/to/dask-worker --scheduler-file /home/$USER/scheduler.json

Note, the ``--scheduler-file`` option is *only* valuable if your scheduler and
workers share a network file system.


High Performance Network
------------------------

Many HPC systems have both standard Ethernet networks as well as
high-performance networks capable of increased bandwidth.  You can instruct
Dask to use the high-performance network interface by using the ``--interface``
keyword with the ``dask-worker``, ``dask-scheduler``, or ``dask-mpi`` commands or
the ``interface=`` keyword with the dask-jobqueue ``Cluster`` objects:

.. code-block:: bash

   mpirun --np 4 dask-mpi --scheduler-file /home/$USER/scheduler.json --interface ib0

In the code example above, we have assumed that your cluster has an Infiniband
network interface called ``ib0``. You can check this by asking your system
administrator or by inspecting the output of ``ifconfig``

.. code-block:: bash

	$ ifconfig
	lo          Link encap:Local Loopback                       # Localhost
				inet addr:127.0.0.1  Mask:255.0.0.0
				inet6 addr: ::1/128 Scope:Host
	eth0        Link encap:Ethernet  HWaddr XX:XX:XX:XX:XX:XX   # Ethernet
				inet addr:192.168.0.101
				...
	ib0         Link encap:Infiniband                           # Fast InfiniBand
				inet addr:172.42.0.101

https://stackoverflow.com/questions/43881157/how-do-i-use-an-infiniband-network-with-dask


No Local Storage
----------------

Users often exceed memory limits available to a specific Dask deployment.  In
normal operation, Dask spills excess data to disk.  However, in HPC systems, the
individual compute nodes often lack locally attached storage, preferring
instead to store data in a robust high performance network storage solution.
As a result, when a Dask cluster starts to exceed memory limits, its workers can
start making many small writes to the remote network file system.  This is both
inefficient (small writes to a network file system are *much* slower than local
storage for this use case) and potentially dangerous to the file system itself.

See `this page
<https://distributed.dask.org/en/latest/worker.html#memory-management>`_
for more information on Dask's memory policies.  Consider changing the
following values in your ``~/.config/dask/distributed.yaml`` file:

.. code-block:: yaml

   distributed:
     worker:
       memory:
         target: false  # don't spill to disk
         spill: false  # don't spill to disk
         pause: 0.80  # pause execution at 80% memory use
         terminate: 0.95  # restart the worker at 95% use

This stops Dask workers from spilling to disk, and instead relies entirely on
mechanisms to stop them from processing when they reach memory limits.

As a reminder, you can set the memory limit for a worker using the
``--memory-limit`` keyword::

   dask-mpi ... --memory-limit 10GB

Alternatively, if you *do* have local storage mounted on your compute nodes, you
can point Dask workers to use a particular location in your filesystem using
the ``--local-directory`` keyword::

   dask-mpi ... --local-directory /scratch


Launch Many Small Jobs
----------------------

.. note:: This section is not necessary if you use a tool like dask-jobqueue.

HPC job schedulers are optimized for large monolithic jobs with many nodes that
all need to run as a group at the same time.  Dask jobs can be quite a bit more
flexible: workers can come and go without strongly affecting the job.  If we
split our job into many smaller jobs, we can often get through the job
scheduling queue much more quickly than a typical job.  This is particularly
valuable when we want to get started right away and interact with a Jupyter
notebook session rather than waiting for hours for a suitable allocation block
to become free.

So, to get a large cluster quickly, we recommend allocating a dask-scheduler
process on one node with a modest wall time (the intended time of your session)
and then allocating many small single-node dask-worker jobs with shorter wall
times (perhaps 30 minutes) that can easily squeeze into extra space in the job
scheduler.  As you need more computation, you can add more of these single-node
jobs or let them expire.


Use Dask to co-launch a Jupyter server
--------------------------------------

Dask can help you by launching other services alongside it.  For example, you
can run a Jupyter notebook server on the machine running the ``dask-scheduler``
process with the following commands

.. code-block:: python

   from dask.distributed import Client
   client = Client(scheduler_file='scheduler.json')

   import socket
   host = client.run_on_scheduler(socket.gethostname)

   def start_jlab(dask_scheduler):
       import subprocess
       proc = subprocess.Popen(['/path/to/jupyter', 'lab', '--ip', host, '--no-browser'])
       dask_scheduler.jlab_proc = proc

   client.run_on_scheduler(start_jlab)
