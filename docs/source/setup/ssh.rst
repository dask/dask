SSH
===

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


CLI Options
----------- 

Launch a distributed cluster over SSH. A ``dask-scheduler`` process will run
on the first host specified in [HOSTNAMES] or in the hostfile (unless
``--scheduler`` is specified explicitly). One or more ``dask-worker`` processes
will be run on each host in [HOSTNAMES] or in the hostfile. Use command line
flags to adjust how many dask-worker process are run on each host
(``--nprocs``) and how many cpus are used by each dask-worker process
(``--nthreads``).

Options:

.. note::

   This table may grow out of date, you should check ``dask-ssh --help`` to get an up-to-date listing of all options.

+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| Option             | TYPE    | Description                                                                                                    |
+====================+=========+================================================================================================================+
| --scheduler        | TEXT    | Specify scheduler node.  Defaults to first address                                                             |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --scheduler-port   | INTEGER | Specify scheduler port number.  Defaults to port 8786                                                          |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --nthreads         | INTEGER | Number of threads per worker process. Defaults to number of cores divided by the number of processes per host  |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --nprocs           | INTEGER | Number of worker processes per host. Defaults to one                                                           |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --hostfile         | PATH    | Textfile with hostnames/IP addresses                                                                           |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --ssh-username     | TEXT    | Username to use when establishing SSH connections                                                              |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --ssh-port         | INTEGER | Port to use for SSH connections                                                                                |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --ssh-private-key  | TEXT    | Private key file to use for SSH connections                                                                    |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --log-directory    | PATH    | Directory to use on all cluster nodes for the output of dask-scheduler and dask-worker commands                |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --remote-python    | TEXT    | Path to Python on remote nodes                                                                                 |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --memory-limit     | TEXT    | Bytes of memory that the worker can use. This can be an integer (bytes), float(fraction of total system memory)|
|                    |         | string (like 5GB or 5000M), 'auto', or zero for no memory management                                           |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --worker-port      | INTEGER | Serving computation port, defaults to random                                                                   |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --nanny-port       | INTEGER | Serving nanny port, defaults to random                                                                         |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
| --nohost           |         | Do not pass the hostname to the worker                                                                         |
+--------------------+---------+----------------------------------------------------------------------------------------------------------------+
