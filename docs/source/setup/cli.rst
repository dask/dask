Command Line
============

This is the most fundamental way to deploy Dask on multiple machines.  In
production environments, this process is often automated by some other resource
manager. Hence, it is rare that people need to follow these instructions
explicitly.  Instead, these instructions are useful for IT professionals who
may want to set up automated services to deploy Dask within their institution.

A ``dask.distributed`` network consists of one ``dask-scheduler`` process and
several ``dask-worker`` processes that connect to that scheduler.  These are
normal Python processes that can be executed from the command line.  We launch
the ``dask-scheduler`` executable in one process and the ``dask-worker``
executable in several processes, possibly on different machines.

To accomplish this, launch ``dask-scheduler`` on one node::

   $ dask-scheduler
   Scheduler at:   tcp://192.0.0.100:8786

Then, launch ``dask-worker`` on the rest of the nodes, providing the address to
the node that hosts ``dask-scheduler``::

   $ dask-worker tcp://192.0.0.100:8786
   Start worker at:  tcp://192.0.0.1:12345
   Registered to:    tcp://192.0.0.100:8786

   $ dask-worker tcp://192.0.0.100:8786
   Start worker at:  tcp://192.0.0.2:40483
   Registered to:    tcp://192.0.0.100:8786

   $ dask-worker tcp://192.0.0.100:8786
   Start worker at:  tcp://192.0.0.3:27372
   Registered to:    tcp://192.0.0.100:8786

The workers connect to the scheduler, which then sets up a long-running network
connection back to the worker.  The workers will learn the location of other
workers from the scheduler.


Handling Ports
--------------

The scheduler and workers both need to accept TCP connections on an open port.
By default, the scheduler binds to port ``8786`` and the worker binds to a
random open port.  If you are behind a firewall then you may have to open
particular ports or tell Dask to listen on particular ports with the ``--port``
and ``--worker-port`` keywords.::

   dask-scheduler --port 8000
   dask-worker --bokeh-port 8000 --nanny-port 8001


Nanny Processes
---------------

Dask workers are run within a nanny process that monitors the worker process
and restarts it if necessary.


Diagnostic Web Servers
----------------------

Additionally, Dask schedulers and workers host interactive diagnostic web
servers using `Bokeh <https://bokeh.pydata.org>`_.  These are optional, but
generally useful to users.  The diagnostic server on the scheduler is
particularly valuable, and is served on port ``8787`` by default (configurable
with the ``--bokeh-port`` keyword).

For more information about relevant ports, please take a look at the available
:ref:`command line options <worker-scheduler-cli-options>`.

Automated Tools
---------------

There are various mechanisms to deploy these executables on a cluster, ranging
from manually SSH-ing into all of the machines to more automated systems like
SGE/SLURM/Torque or Yarn/Mesos.  Additionally, cluster SSH tools exist to send
the same commands to many machines.  We recommend searching online for "cluster
ssh" or "cssh".


.. _worker-scheduler-cli-options:

CLI Options
-----------

.. note::

   The command line documentation here may differ depending on your installed
   version. We recommend referring to the output of ``dask-scheduler --help``
   and ``dask-worker --help``.

.. click:: distributed.cli.dask_scheduler:main
   :prog: dask-scheduler
   :show-nested:

.. click:: distributed.cli.dask_worker:main
   :prog: dask-worker
   :show-nested:
