High Performance Computers
==========================

Relevant Machines
-----------------

This page includes instructions and guidelines when deploying Dask on high
performance compute clusters and supercomputers commonly found in scientific 
and industry research labs and large enterprises.  

These systems commonly have the following attributes:

1.  Some mechanism to launch MPI applications or use job schedulers like
    SLURM, SGE, TORQUE, LSF, DRMAA, PBS, or others
2.  A shared network file system visible to all machines in the cluster
3.  A high performance network interconnect, such as Infiniband
4.  Little or no node-local storage


Where to start
--------------

Most of this page documents various ways and best practices to use Dask on an
HPC cluster.

There are three different models of Dask deployment that we commonly see from HPC users.

1. Submit many small jobs via `dask-jobqueue <https://jobqueue.dask.org>`_ with a
   Dask scheduler running outside the cluster on a laptop, workstation or login node.
2. Submit a large job with many nodes and then use `dask-mpi <http://mpi.dask.org/en/latest/>`_
   and/or custom scripts to coordinate those nodes and populate them with a Dask cluster.
3. Delegate job submission to a central controller like `dask-gateway <https://gateway.dask.org>`_
   which submits Dask components as jobs on the users behalf.

There are pros/cons to each model and we recommend you check out each section below
and decide which best suits you.

Dask Jobqueue
^^^^^^^^^^^^^

`dask-jobqueue <https://jobqueue.dask.org>`_ provides cluster managers for PBS,
SLURM, LSF, SGE and other resource managers. You can launch a Dask cluster on
these systems like this.

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

By submitting many small jobs into a cluster it is possible to get allocations
fairly quickly compared to submitting a single large job. You can also scale your
cluster up and down at any time by adding/removing the individual jobs.

This approach can feel like pouring sand (our small jobs) into a bucket of rocks
(an HPC full of large jobs) and allow us to get resource that would otherwise be unused.

However large jobs are typically placed on nodes that are physically close to each 
other and benefit from better network connectivity as a result.

Submitting many small jobs with high network traffic between them can have poor network
performance as HPCs are usually optimised for intra-job traffic and the inter-job traffic
created by dask-jobqueue can be slower and impact other users. In rare cases some 
HPC systems will also prioritize larger jobs or will limit the number of running jobs each
user can have which can make using ``dask-jobqueue`` problematic.
Check with your cluster admin before using this model.

Dask-jobqueue provides a lot of possibilities like fast allocations and adaptive dynamic scaling
of workers, we recommend reading the `dask-jobqueue documentation
<https://jobqueue.dask.org>`_ first to get a basic system running and then
returning to this documentation for fine-tuning if necessary.


Dask MPI
^^^^^^^^

You can launch a Dask cluster using ``mpirun`` or ``mpiexec`` and the
`dask-mpi <http://mpi.dask.org/en/latest/>`_ command line tool.

.. code-block:: bash

   # Launch a Dask cluster
   mpirun --np 4 dask-mpi --scheduler-file /home/$USER/scheduler.json

.. code-block:: python

   # Connect to the cluster from a node with access to the shared filesystem
   from dask.distributed import Client
   client = Client(scheduler_file='/path/to/scheduler.json')

This depends on the `mpi4py <https://mpi4py.readthedocs.io/>`_ library.  It only
uses MPI to start the Dask cluster and not for inter-node communication. MPI
implementations differ: the use of ``mpirun --np 4`` is specific to the
``mpich`` or ``open-mpi`` MPI implementation installed through conda and linked
to mpi4py.

.. code-block:: bash

   conda install mpi4py

You can also submit your workload in a batch style by calling ``dask_mpi.initialize()``
inside your script.

.. code-block:: python

   # myscript.py
   from dask_mpi import initialize
   # MPI Ranks 1-n will be used for the Dask scheduler and workers 
   # and will not progress beyond this initialization call
   initialize()

   # MPI Rank 0 will continue executing the script once the scheduler has started
   from dask.distributed import Client
   client = Client()  # The scheduler address is found automatically via MPI


.. code-block:: bash

   mpirun -np 4 python myscript.py

This approach submits a single large allocation to the HPC and then populates it
with a Dask cluster. This feels like inflating a balloon (the Dask cluster) 
inside a box (your allocation) so that it fills all available space.

It is not necessary to use exactly this implementation, but you may want to
verify that your ``mpi4py`` Python library is linked against the proper
``mpirun/mpiexec`` executable and that the flags used (like ``--np 4``) are
correct for your system.  The system administrator of your cluster should be
very familiar with these concerns and able to help.

In some setups, MPI processes are not allowed to fork other processes. In this
case, we recommend using ``--no-nanny`` option in order to prevent dask from
using an additional nanny process to manage workers.

Dask-MPI fits with a more traditional HPC job workflow and can provide benefits
such as lower latency between workers due to locality, we recommend reading the 
`dask-mpi documentation <https://mpi.dask.org>`_ first to get a basic system running 
and then returning to this documentation for fine-tuning if necessary.

Dask Gateway
^^^^^^^^^^^^

Dask Gateway provides a secure, multi-tenant server for managing Dask clusters. 
It allows users to launch and use Dask clusters in a shared, centrally managed 
cluster environment, without requiring users to have direct access to the 
underlying cluster backend.

This requires some setup from HPC cluster admins to allow Dask gateway to run
as a user that can launch jobs as the user IDs of the HPC users.

`Dask Gateway <https://gateway.dask.org>`_ is commonly installed as a component 
of the HPC itself, similar to
`Jupyter Hub <https://jupyter.org/hub>`_. This gives cluster admins more control
and visibility of the Dask workloads that users are submitting. They can provide
`preconfigured environments and cluster configurations <https://gateway.dask.org/cluster-options.html>`_
as well as setting `cluster resource limits <https://gateway.dask.org/resource-limits.html>`_.

DIY Approach
^^^^^^^^^^^^

Alternatively you may prefer to brew your own scripts to launch a Dask cluster.
See the General Tips section on how to leverage common HPC features like shared 
filesystems to launch you cluster.

General Tips
------------

Here is some general advice to help you run and optimize Dask cluster on HPC.

Using a Shared Network File System and a Job Scheduler
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^^^^^^^

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


Local Storage
^^^^^^^^^^^^^

Users often exceed memory limits available to a specific Dask deployment.  In
normal operation, Dask spills excess data to disk, often to the default
temporary directory.

However, in HPC systems this default temporary directory may point to an
network file system (NFS) mount which can cause problems as Dask tries to read
and write many small files.  

.. warning::
   Beware, reading and writing many tiny files from
   many distributed processes is a good way to shut down a national
   supercomputer.

If available, it's good practice to point Dask workers to local storage, or
hard drives that are physically on each node.  Your IT administrators will be
able to point you to these locations.  You can do this with the
``--local-directory`` or ``local_directory=`` keyword in the ``dask-worker``
command::

   dask-mpi ... --local-directory /path/to/local/storage

or any of the other Dask Setup utilities, or by specifying the
following :doc:`configuration value <../../configuration>`:

.. code-block:: yaml

   temporary-directory: /path/to/local/storage

However, not all HPC systems have local storage.  If this is the case then you
may want to turn off Dask's ability to spill to disk altogether.
See :doc:`this page <worker-memory>` for more information on Dask's memory policies.
Consider changing the following values in your ``~/.config/dask/distributed.yaml`` file
to disable spilling data to disk:

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


Launch Many Small Jobs
^^^^^^^^^^^^^^^^^^^^^^

HPC job schedulers are optimized for large monolithic jobs with many nodes that
all need to run as a group at the same time.  Dask jobs can be quite a bit more
flexible: workers can come and go without strongly affecting the job.  If we
split our job into many smaller jobs, we can often get through the job
scheduling queue much more quickly than a typical job.  This is particularly
valuable when we want to get started right away and interact with a Jupyter
notebook session rather than waiting for hours for a suitable allocation block
to become free.

So, to get a large cluster quickly,you could allocate a dask-scheduler
process on one node with a modest wall time (the intended time of your session)
and then allocating many small single-node dask-worker jobs with shorter wall
times (perhaps 30 minutes) that can easily squeeze into extra space in the job
scheduler.  As you need more computation, you can add more of these single-node
jobs or let them expire.


Use Dask to co-launch a Jupyter server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Dask can help you by launching other services alongside it.  For example, you
can run a Jupyter notebook server on the machine running the ``dask-scheduler``
process and setting the ``--jupyter`` flag.

This will start Jupyter running on the same web server as the Dask Dashboard
(which is typically found on port ``8787``).

.. code-block:: bash

   dask-scheduler --jupyter
