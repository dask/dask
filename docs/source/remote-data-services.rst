Remote Data Services
====================

As described in Section :doc:`Internal Data Ingestion <bytes>`, various
user-facing functions such as ``dataframe.read_parquet`` and lower level
byte-manipulating functions may point to data
that lives not on the local storage of the workers, but on a remote system
such as Amazon S3.

In this section we describe how to use the various known back-end storage
systems. Below we give some details for interested developers on how further
storage back-ends may de provided for dask's use.


Known Storage Implementations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When specifying a storage location, a URL should be provided using the geneal
form ``protocol://path/to/data``.
If no protocol is provided, the local file-system is assumed (same as
``file://``). Two methods exist for passing parameters to the backend
file-system driver: extending the URL to include username, password, server,
port, etc.; and providing ``storage_options``, a dictionary of parameters
to pass on.
Further details on how to provide configuration for each backend
is listed next.

Each back-end has additional installation requirements and may not be
available at runtime. The dictionary ``dask.bytes.core._filesystems``
contains the currently available file-systems. Some require
appropriate imports before use.

The following list gives the protocol shorthands and the back-ends they refer
to:

    - ``file:`` - the local file system, default in the absence of any protocol

    - ``hdfs:`` - Hadoop Distributed File System, a for resilient, replicated
      files within a cluster, using the library hdfs3_

    - ``s3:`` - Amazon S3 remote binary store, often used with Amazon EC2,
      using the library s3fs_

    .. - # ``adlfs:`` - Azure Data-lake cloud storage, for use with the Microsoft
      Azure platform, using azure-data-lake-store-python_

    .. - ``gcsfs:`` - Google Cloud Storage, typically used with Google Compute
      resource, using gcsfs_

.. _hdfs3: http://hdfs3.readthedocs.io/
.. _s3fs: http://s3fs.readthedocs.io/
.. .. _azure-data-lake-store-python: https://github.com/Azure/azure-data-lake-store-python
.. .. _gcsfs: http://gcsfs.readthedocs.io/

Local File System
-----------------

Local files are always accessible, and all parameters passed as part of the URL
(beyond the path itself) or with the ``storage_options``
dictionary will be ignored.

This is the default back-end, and the one used if no protocol is passed at all.

Locations specified relative to the current working directory will, in
general, be respected (as they would be with the built-in python ``open``),
but this may fail in the case that the client and worker processes do not
necessarily have the same working sirectory.

HDFS
----

The Hadoop File System (HDFS) is a widely deployed, distributed, data-local file
system written in Java. This file system backs many clusters running Hadoop and
Spark.

Within dask, HDFS is only available when the module ``distributed.hdfs`` is
explicitly imported, since the usage of HDFS only makes sense in a cluster
setting. The distributed scheduler will intelligently make sure that tasks
which read from HDFS are performed on machines which have local copies of the
blocks required for their work.

By default, hdfs3 attempts to read the default server and port from local
Hadoop configuration files on each node, so it may be that no configuration is
required. However, the server, port and user can be passed as part of the
url: ``hdfs://user:pass@server:port/path/to/data``.

The following parameters may be passed to hdfs3 using ``storage_options``:

    - host, port, user: basic authentication
    - ticket_cache, token: kerberos authentication
    - pars: dictionary of further parameters (e.g., for `high availability`_)

.. _high availability: http://hdfs3.readthedocs.io/en/latest/hdfs.html#high-availability-mode

Important environment variables:

    - HADOOP_CONF_DIR or HADOOP_INSTALL: directory containing ``core-site.xml``
      and/or ``hdfs-site.xml``, containing configuration information

    - LIBHDFS3_CONF: location of a specific xml file with configuration for
      the client; this may be the same as one of the files above. `Short
      circuit` reads should be defined in this file (`see here`_)

.. _see here: http://hdfs3.readthedocs.io/en/latest/hdfs.html#short-circuit-reads-in-hdfs


S3
--

Amazon S3 (Simple Storage Service) is a web service offered by Amazon Web
Services.

The S3 back-end will be available to dask is s3fs is importable when dask is
imported.

Authentication for S3 is provided by the underlying library boto3. As described
in the `auth docs`_ this could be achieved by placing credentials files in one
of several locations on each node: ``~/.aws/credentials``, ``~/.aws/config``,
``/etc/boto.cfg`` and ``~/.boto``. Alternatively, for nodes located
within Amazon EC2, IAM roles can be set up for each node, and then no further
configuration is required. The final authentication option, is for user
credentials can be passed directly in the URL
(``s3://keyID:keySecret/bucket/key/name``) or using ``storage_options``. In
this case, however, the key/secret will be passed to all workers in-the-clear,
so this method is only recommended on well-secured networks.

.. _auth docs: http://boto3.readthedocs.io/en/latest/guide/configuration.html

The following parameters may be passed to s3fs using ``storage_options``:

    - anon: whether access should be anonymous (default False)

    - key, secret: for user authentication

    - token: if authentication has been done with some other S3 client

    - use_ssl: whether connections are encryted and secure (default True)

    - client_kwargs: dict passed to the `boto3 client`_, with keys such
      as `region_name`, `endpoint_url`

    - requester_pays: set True if the authenticated user will assume transfer
      costs, which is required by some providers of bulk daya

    - default_block_size, default_fill_cache: these are not of particular
      interest to dask users, as they concern the behaviour of the buffer
      between successive reads

    - kwargs: other parameters are passed to the `boto3 Session`_ object,
      such as `profile_name`, to pick one of the authentication sections from
      the configuration files referred to above (see `here`_)

.. _boto3 client: http://boto3.readthedocs.io/en/latest/reference/core/session.html#boto3.session.Session.client
.. _boto3 Session: http://boto3.readthedocs.io/en/latest/reference/core/session.html
.. _here: http://boto3.readthedocs.io/en/latest/guide/configuration.html#shared-credentials-file
