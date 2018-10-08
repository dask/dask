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

.. note::

    For more information about relevant ports, please take a look at the help
    pages with ``dask-scheduler --help`` and ``dask-worker --help``


Automated Tools
---------------

There are various mechanisms to deploy these executables on a cluster, ranging
from manually SSH-ing into all of the machines to more automated systems like
SGE/SLURM/Torque or Yarn/Mesos.  Additionally, cluster SSH tools exist to send
the same commands to many machines.  We recommend searching online for "cluster
ssh" or "cssh".


API
---

.. warning::

   These may be out-dated.  We recommend referring to the ``--help`` text of your
   installed version.

dask-scheduler
~~~~~~~~~~~~~~

.. code-block:: bash

   $ dask-scheduler --help
   Usage: dask-scheduler [OPTIONS]

   Options:
     --host TEXT             URI, IP or hostname of this server
     --port INTEGER          Serving port
     --interface TEXT        Preferred network interface like 'eth0' or 'ib0'
     --tls-ca-file PATH      CA cert(s) file for TLS (in PEM format)
     --tls-cert PATH         certificate file for TLS (in PEM format)
     --tls-key PATH          private key file for TLS (in PEM format)
     --bokeh-port INTEGER    Bokeh port for visual diagnostics
     --bokeh / --no-bokeh    Launch Bokeh Web UI  [default: True]
     --show / --no-show      Show web UI
     --bokeh-whitelist TEXT  IP addresses to whitelist for bokeh
     --bokeh-prefix TEXT     Prefix for the bokeh app
     --use-xheaders BOOLEAN  User xheaders in bokeh app for ssl termination in
                             header  [default: False]
     --pid-file TEXT         File to write the process PID
     --scheduler-file TEXT   File to write connection information. This may be a
                             good way to share connection information if your
                             cluster is on a shared network file system
     --local-directory TEXT  Directory to place scheduler files
     --preload TEXT          Module that should be loaded by each worker process
                             like "foo.bar" or "/path/to/foo.py"
     --help                  Show this message and exit


dask-worker
~~~~~~~~~~~

.. code-block:: bash

   $ dask-worker --help
   Usage: dask-worker [OPTIONS] [SCHEDULER]

   Options:
     --tls-ca-file PATH            CA cert(s) file for TLS (in PEM format)
     --tls-cert PATH               certificate file for TLS (in PEM format)
     --tls-key PATH                private key file for TLS (in PEM format)
     --worker-port INTEGER         Serving computation port, defaults to random
     --nanny-port INTEGER          Serving nanny port, defaults to random
     --bokeh-port INTEGER          Bokeh port, defaults to 8789
     --bokeh / --no-bokeh          Launch Bokeh Web UI  [default: True]
     --listen-address TEXT         The address to which the worker binds.
                                   Example: tcp://0.0.0.0:9000
     --contact-address TEXT        The address the worker advertises to the
                                   scheduler for communication with it and other
                                   workers. Example: tcp://127.0.0.1:9000
     --host TEXT                   Serving host. Should be an ip address that is
                                   visible to the scheduler and other workers.
                                   See --listen-address and --contact-address if
                                   you need different listen and contact
                                   addresses. See --interface
     --interface TEXT              Network interface like 'eth0' or 'ib0'
     --nthreads INTEGER            Number of threads per process
     --nprocs INTEGER              Number of worker processes.  Defaults to one
     --name TEXT                   A unique name for this worker like 'worker-1'
     --memory-limit TEXT           Bytes of memory that the worker can use. This
                                   can be an integer (bytes), float (fraction of
                                   total system memory), string (like 5GB or
                                   5000M), 'auto', or zero for no memory
                                   management
     --reconnect / --no-reconnect  Reconnect to scheduler if disconnected
     --nanny / --no-nanny          Start workers in nanny process for management
     --pid-file TEXT               File to write the process PID
     --local-directory TEXT        Directory to place worker files
     --resources TEXT              Resources for task constraints like "GPU=2
                                   MEM=10e9"
     --scheduler-file TEXT         Filename to JSON encoded scheduler information. 
                                   Use with dask-scheduler --scheduler-file
     --death-timeout FLOAT         Seconds to wait for a scheduler before closing
     --bokeh-prefix TEXT           Prefix for the bokeh app
     --preload TEXT                Module that should be loaded by each worker
                                   process like "foo.bar" or "/path/to/foo.py"
     --help                        Show this message and exit

