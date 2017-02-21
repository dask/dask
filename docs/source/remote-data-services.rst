Remote Data Services
====================

As described in Section :doc:`Internal Data Ingestion <bytes>`, various
user-facing functions (such as ``dataframe.read_csv``,
``dataframe.read_parquet``, ``bag.read_text``) and lower level
byte-manipulating functions may point to data
that lives not on the local storage of the workers, but on a remote system
such as Amazon S3.

In this section we describe how to use the various known back-end storage
systems. Below we give some details for interested developers on how further
storage back-ends may be provided for dask's use.


Known Storage Implementations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When specifying a storage location, a URL should be provided using the general
form ``protocol://path/to/data``.
If no protocol is provided, the local file-system is assumed (same as
``file://``). Two methods exist for passing parameters to the backend
file-system driver: extending the URL to include username, password, server,
port, etc.; and providing ``storage_options``, a dictionary of parameters
to pass on. Examples:

.. code-block:: python

   df = dd.read_csv('hdfs://user@server:port/path/*.csv')

   df = dd.read_parquet('s3://bucket/path',
        storage_options={'anon': True, 'use_ssl': False})

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

    - ``gcs:`` or ``gs:`` - Google Cloud Storage, typically used with Google Compute
      resource, using gcsfs_ (in development)

    .. - # ``adlfs:`` - Azure Data-lake cloud storage, for use with the Microsoft
      Azure platform, using azure-data-lake-store-python_


.. _hdfs3: http://hdfs3.readthedocs.io/
.. _s3fs: http://s3fs.readthedocs.io/
.. .. _azure-data-lake-store-python: https://github.com/Azure/azure-data-lake-store-python
.. _gcsfs: https://github.com/martindurant/gcsfs/

Local File System
-----------------

Local files are always accessible, and all parameters passed as part of the URL
(beyond the path itself) or with the ``storage_options``
dictionary will be ignored.

This is the default back-end, and the one used if no protocol is passed at all.

We assume here that each worker has access to the same file-system - either
the workers are co-located on the same machine, or a network file system
is mounted and referenced at the same path location for every worker node.

Locations specified relative to the current working directory will, in
general, be respected (as they would be with the built-in python ``open``),
but this may fail in the case that the client and worker processes do not
necessarily have the same working directory.

HDFS
----

The Hadoop File System (HDFS) is a widely deployed, distributed, data-local file
system written in Java. This file system backs many clusters running Hadoop and
Spark.

Within dask, HDFS is only available when the module ``distributed.hdfs`` is
explicitly imported, since the usage of HDFS usually only makes sense in a cluster
setting. The distributed scheduler will prefer to allocate tasks
which read from HDFS  to machines which have local copies of the
blocks required for their work, where possible.

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
      the `libhdfs3 client`_; this may be the same as one of the files above. `Short
      circuit` reads should be defined in this file (`see here`_)

.. _see here: http://hdfs3.readthedocs.io/en/latest/hdfs.html#short-circuit-reads-in-hdfs
.. _libhdfs3 client: https://github.com/Pivotal-Data-Attic/pivotalrd-libhdfs3/wiki/Configure-Parameters

S3
-----

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
      costs, which is required by some providers of bulk data

    - default_block_size, default_fill_cache: these are not of particular
      interest to dask users, as they concern the behaviour of the buffer
      between successive reads

    - kwargs: other parameters are passed to the `boto3 Session`_ object,
      such as `profile_name`, to pick one of the authentication sections from
      the configuration files referred to above (see `here`_)

.. _boto3 client: http://boto3.readthedocs.io/en/latest/reference/core/session.html#boto3.session.Session.client
.. _boto3 Session: http://boto3.readthedocs.io/en/latest/reference/core/session.html
.. _here: http://boto3.readthedocs.io/en/latest/guide/configuration.html#shared-credentials-file


Google Cloud Storage
--------------------

(gcsfs is in early development, expect the details here to change)

Google Cloud Storage is a RESTful online file storage web service for storing
and accessing data on Google's infrastructure.

The GCS backend will be available only after importing gcsfs_. The
protocol identifiers ``gcs`` and ``gs`` are identical in their effect.

Authentication for GCS is based on OAuth2, and designed for user verification.
Interactive authentication is available when ``token==None`` using the local
browser, or by using gcloud_ to produce a JSON token file and passing that.
In either case, gcsfs stores a cache of tokens in a local file, so subsequent
authentication will not be necessary.

.. _gcsfs: https://github.com/martindurant/gcsfs/
.. _gcloud: https://cloud.google.com/sdk/docs/

At the time of writing, ``gcsfs.GCSFileSystem`` instances pickle including the auth token, so sensitive
information is passed between nodes of a dask distributed cluster. This will
be changed to allow the use of either local JSON or pickle files for storing
tokens and authenticating on each node automatically, instead of
passing around an authentication token, similar to S3, above.

Every use of GCS requires the specification of a project to run within - if the
project is left empty, the user will not be able to perform any bucket-level
operations. The project can be defined using the  variable
GCSFS_DEFAULT_PROJECT in the environment of every worker, or by passing
something like the following

.. code-block:: python

   dd.read_parquet('gs://bucket/path', storage_options={'project': 'myproject'}

Possible additional storage options:

   -  access : 'read_only', 'read_write', 'full_control', access privilege
      level (note that the token cache uses a separate token for each level)

   -  token: either an actual dictionary of a google token, or location of
      a JSON file created by gcloud.


Developer API
~~~~~~~~~~~~~

The prototype for any file-system back-end can be found in
``bytes.local.LocalFileSystem``. Any new implementation should provide the
same API, and make itself available as a protocol to dask. For example, the
following would register the protocol "myproto", described by the implementation
class ``MyProtoFileSystem``. URLs of the form ``myproto://`` would thereafter
be dispatched to the methods of this class.

.. code-block:: python

   dask.bytes.core._filesystems['myproto'] = MyProtoFileSystem

For a more complicated example, users may wish to also see
``dask.bytes.s3.DaskS3FileSystem``.

.. currentmodule:: dask.bytes.local

.. autoclass:: LocalFileSystem
   :members:
