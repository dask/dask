.. _configuration:

=============
Configuration
=============

As with any distributed computation system, taking full advantage of
Dask distributed sometimes requires configuration.  Some options can be
passed as :ref:`API <api>` parameters and/or command line options to the
various Dask executables.  However, some options can also be entered in
the Dask configuration file.


User-wide configuration
=======================

Dask accepts some configuration options in a configuration file, which by
default is a ``.dask/config.yaml`` file located in your home directory.
The file path can be overriden using the ``DASK_CONFIG`` environment variable.

The file is written in the YAML format, which allows for a human-readable
hierarchical key-value configuration.  All keys in the configuration file
are optional, though Dask will create a default configuration file for you
on its first launch.

Here is a synopsis of the configuration file:

.. code-block:: yaml

   logging:
       distributed: info
       distributed.client: warning
       bokeh: critical

   # Scheduler options
   bandwidth: 100000000    # 100 MB/s estimated worker-worker bandwidth
   allowed-failures: 3     # number of retries before a task is considered bad
   pdb-on-err: False       # enter debug mode on scheduling error
   transition-log-length: 100000

   # Worker options
   multiprocessing-method: forkserver

   # Communication options
   compression: auto
   tcp-timeout: 30         # seconds delay before calling an unresponsive connection dead
   default-scheme: tcp
   require-encryption: False   # whether to require encryption on non-local comms
   tls:
       ca-file: myca.pem
       scheduler:
           cert: mycert.pem
           key: mykey.pem
       worker:
           cert: mycert.pem
           key: mykey.pem
       client:
           cert: mycert.pem
           key: mykey.pem
       #ciphers:
           #ECDHE-ECDSA-AES128-GCM-SHA256

   # Bokeh web dashboard
   bokeh-export-tool: False


We will review some of those options hereafter.


Communication options
---------------------

``compression``
"""""""""""""""

This key configures the desired compression scheme when transferring data
over the network.  The default value, "auto", applies heuristics to try and
select the best compression scheme for each piece of data.


``default-scheme``
""""""""""""""""""

The :ref:`communication <communications>` scheme used by default.  You can
override the default ("tcp") here, but it is recommended to use explicit URIs
for the various endpoints instead (for example ``tls://`` if you want to
enable :ref:`TLS <tls>` communications).


``require-encryption``
""""""""""""""""""""""

Whether to require that all non-local communications be encrypted.  If true,
then Dask will refuse establishing any clear-text communications (for example
over TCP without TLS), forcing you to use a secure transport such as
:ref:`TLS <tls>`.


``tcp-timeout``
"""""""""""""""

The default "timeout" on TCP sockets.  If a remote endpoint is unresponsive
(at the TCP layer, not at the distributed layer) for at least the specified
number of seconds, the communication is considered closed.  This helps detect
endpoints that have been killed or have disconnected abruptly.


``tls``
"""""""

This key configures :ref:`TLS <tls>` communications.  Several sub-keys are
recognized:

* ``ca-file`` configures the CA certificate file used to authenticate
  and authorize all endpoints.
* ``ciphers`` restricts allowed ciphers on TLS communications.

Each kind of endpoint has a dedicated endpoint sub-key: ``scheduler``,
``worker`` and ``client``.  Each endpoint sub-key also supports several
sub-keys:

* ``cert`` configures the certificate file for the endpoint.
* ``key`` configures the private key file for the endpoint.


Scheduler options
-----------------

``allowed-failures``
""""""""""""""""""""

The number of retries before a "suspicious" task is considered bad.
A task is considered "suspicious" if the worker died while executing it.


``bandwidth``
"""""""""""""

The estimated network bandwidth, in bytes per second, from worker to worker.
This value is used to estimate the time it takes to ship data from one node
to another, and balance tasks and data accordingly.


Misc options
------------

``logging``
"""""""""""

This key configures the logging settings.  There are two possible formats.
The simple, recommended format configures the desired verbosity level
for each logger.  It also sets default values for several loggers such
as ``distributed`` unless explicitly configured.

A more extended format is possible following the :mod:`logging` module's
`Configuration dictionary schema <https://docs.python.org/2/library/logging.config.html#logging-config-dictschema>`_.
To enable this extended format, there must be a ``version`` sub-key as
mandated by the schema.  The extended format does not set any default values.

.. note::
   Python's :mod:`logging` module uses a hierarchical logger tree.
   For example, configuring the logging level for the ``distributed``
   logger will also affect its children such as ``distributed.scheduler``,
   unless explicitly overriden.


``logging-file-config``
"""""""""""""""""""""""

As an alternative to the two logging settings formats discussed above,
you can specify a logging config file.
Its format adheres to the :mod:`logging` module's
`Configuration file format <https://docs.python.org/2/library/logging.config.html#configuration-file-format>`_.

.. note::
   The configuration options `logging-file-config` and `logging` are mutually exclusive.