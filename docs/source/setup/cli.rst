Command Line
============

This is the most fundamental way to deploy Dask on multiple machines.  In
production environments this process is often automated by some other resource
manager and so it is rare that people need to follow these instructions
explicitly.  Instead, these instructions are useful for IT professionals who
may want to set up automated services to deploy Dask within their institution.

A ``dask.distributed`` network consists of one ``Scheduler`` node and several
``Worker`` nodes.  These are normal Python processes that can be executed from
the command line.  We launch the ``dask-scheduler`` executable in one process
and the ``dask-worker`` executable in several processes, possibly on different
machines.

Launch ``dask-scheduler`` on one node::

   $ dask-scheduler
   Scheduler at:   tcp://172.27.6.208:8786

Then launch ``dask-worker`` on the rest of the nodes, providing the address to
the node that hosts ``dask-scheduler``::

   $ dask-worker tcp://172.27.6.100:8786
   Start worker at:  tcp://172.27.6.208:40483
   Registered to:    tcp://172.27.6.208:8786

   $ dask-worker tcp://172.27.6.101:8786
   Start worker at:  tcp://172.27.6.208:40483
   Registered to:    tcp://172.27.6.208:8786

   $ dask-worker tcp://172.27.6.102:8786
   Start worker at:  tcp://172.27.6.208:40483
   Registered to:    tcp://172.27.6.208:8786

The workers connect to the scheduler, which then sets up a long-running network
connection back to the worker.  The workers will learn the location of other
workers from the scheduler.

Handling Ports
--------------

The scheduler and worker both need to accept TCP connections.  By default the
scheduler uses port ``8786`` and the worker binds to a random open port.  If
you are behind a firewall then you may have to open particular ports or tell
Dask to listen on particular ports with the ``--port`` and ``-worker-port``
keywords.

Diagnostic Web Servers
----------------------

Additionally Dask schedulers and workers host interactive diagnostic web
servers using `Bokeh <https://bokeh.pydata.org>`_.  These are optional, but
generally useful to users.  The diagnostic server on the scheduler is
particularly valuable, and is served on port ``8787`` by default (configurable
with the ``-bokeh-port`` keyword).

  - More information about relevant ports is available by looking at the help
    pages with ``dask-scheduler --help`` and ``dask-worker --help``

Automated Tools
---------------

There are various mechanisms to deploy these executables on a cluster, ranging
from manually SSH-ing into all of the machines to more automated systems like
SGE/SLURM/Torque or Yarn/Mesos. Additionally, cluster SSH tools exist to send
the same commands to many machines.  We recommend searching online for "cluster
ssh" or "cssh"..
