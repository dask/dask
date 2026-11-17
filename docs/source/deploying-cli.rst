.. _deploying-cli:

Command Line
============

This is the most fundamental way to deploy Dask on multiple machines.  In
production environments this process is often automated by some other resource
manager. Hence, it is rare that people need to follow these instructions
explicitly.  Instead, these instructions are useful to help understand what
*cluster managers* and other automated tooling is doing under the hood and to
help users deploy onto platforms that have no automated tools today.

A ``dask.distributed`` network consists of one ``dask scheduler`` process and
several ``dask worker`` processes that connect to that scheduler.  These are
normal Python processes that can be executed from the command line.  We launch
the ``dask scheduler`` executable in one process and the ``dask worker``
executable in several processes, possibly on different machines.

To accomplish this, launch ``dask scheduler`` on one node::

   $ dask scheduler
   Scheduler at:   tcp://192.0.0.100:8786

Then, launch ``dask worker`` on the rest of the nodes, providing the address to
the node that hosts ``dask scheduler``::

   $ dask worker tcp://192.0.0.100:8786
   Start worker at:  tcp://192.0.0.1:12345
   Registered to:    tcp://192.0.0.100:8786

   $ dask worker tcp://192.0.0.100:8786
   Start worker at:  tcp://192.0.0.2:40483
   Registered to:    tcp://192.0.0.100:8786

   $ dask worker tcp://192.0.0.100:8786
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

   dask scheduler --port 8000
   dask worker --dashboard-address 8000 --nanny-port 8001


Nanny Processes
---------------

Dask workers are run within a nanny process that monitors the worker process
and restarts it if necessary.


Diagnostic Web Servers
----------------------

Additionally, Dask schedulers and workers host interactive diagnostic web
servers using `Bokeh <https://docs.bokeh.org>`_.  These are optional, but
generally useful to users.  The diagnostic server on the scheduler is
particularly valuable, and is served on port ``8787`` by default (configurable
with the ``--dashboard-address`` keyword).

For more information about relevant ports, please take a look at the available
:ref:`command line options <worker-scheduler-cli-options>`.

Automated Tools
---------------

There are various mechanisms to deploy these executables on a cluster, ranging
from manually SSH-ing into all of the machines to more automated systems like
SGE/SLURM/Torque or Yarn/Mesos.  Additionally, cluster SSH tools exist to send
the same commands to many machines.  We recommend searching online for "cluster
ssh" or "cssh".


.. _deploying-cli.spec:

Dask Spec
---------

The ``dask spec`` command allows you to launch a Dask cluster using a
declarative specification. This is useful for defining complex cluster setups
that involve multiple services.

There are two ways to provide the specification:

``--spec-file``
   Path to a YAML or JSON file containing the specification.

``--spec``
   An inline JSON string containing the specification.

You must provide exactly one of these options. For example:

.. code-block:: bash

   # From a YAML file
   dask spec --spec-file my-cluster.yaml

   # From an inline JSON string
   dask spec --spec '{"my-worker": {"cls": "distributed.Nanny", "opts": {"nthreads": 2}}}'

Specification Format
~~~~~~~~~~~~~~~~~~~~

The specification is a mapping of names to process definitions. Each entry
must include a ``cls`` key and may include an ``opts`` key:

``cls``
   A fully qualified Python class name as a string. Common choices include
   ``"distributed.Scheduler"``, ``"distributed.Worker"``, and
   ``"distributed.Nanny"``.

``opts``
   A mapping of keyword arguments to pass to the class constructor.

.. important::

   In the Python API of :class:`~distributed.SpecCluster`, ``cls`` is a
   Python class object (e.g., ``Nanny``) and keyword arguments are provided
   under ``options``. In YAML/JSON specifications used by ``dask spec``,
   ``cls`` must be a **dotted import path string** (e.g.,
   ``"distributed.Nanny"``) and keyword arguments go under ``opts``.

Example
~~~~~~~

The following example adapts the
:class:`~distributed.SpecCluster` Python examples into a YAML specification
file. Save this as ``my-cluster.yaml``:

.. code-block:: yaml

   my-scheduler:
     cls: distributed.Scheduler
     opts:
       dashboard_address: ':8787'

   my-worker:
     cls: distributed.Worker
     opts:
       nthreads: 1

   my-nanny:
     cls: distributed.Nanny
     opts:
       nthreads: 2

Then launch the cluster:

.. code-block:: bash

   dask spec --spec-file my-cluster.yaml

This is equivalent to the following Python code using
:class:`~distributed.SpecCluster`:

.. code-block:: python

   from distributed import Scheduler, Worker, Nanny, SpecCluster

   scheduler = {"cls": Scheduler, "options": {"dashboard_address": ":8787"}}
   workers = {
       "my-worker": {"cls": Worker, "options": {"nthreads": 1}},
       "my-nanny": {"cls": Nanny, "options": {"nthreads": 2}},
   }
   cluster = SpecCluster(scheduler=scheduler, workers=workers)

Note the two key differences between the Python API and the YAML format:

- ``cls`` uses a dotted import string (``"distributed.Nanny"``) instead of a
  class reference (``Nanny``).
- Keyword arguments use the key ``opts`` instead of ``options``.


.. _worker-scheduler-cli-options:

CLI Options
-----------

.. note::

   The command line documentation here may differ depending on your installed
   version. We recommend referring to the output of ``dask scheduler --help``,
   ``dask worker --help`` and ``dask spec --help``.

.. click:: distributed.cli.dask_scheduler:main
   :prog: dask scheduler
   :show-nested:

.. click:: distributed.cli.dask_worker:main
   :prog: dask worker
   :show-nested:

.. click:: distributed.cli.dask_spec:main
   :prog: dask spec
   :show-nested:
