Internal Data Ingestion
=======================

Dask contains internal tools for extensible data ingestion in the
``dask.bytes`` package.  *These functions are developer-focused rather than for
direct consumption by users.  These functions power user facing functions like
``dd.read_csv`` and ``db.read_text`` which are probably more useful for most
users.*

.. currentmodule:: dask.bytes

.. autosummary::
   read_bytes
   open_files
   open_text_files

These functions are extensible in their output formats (bytes, file objects),
their input locations (file system, S3, HDFS), line delimiters, and compression
formats.

These functions provide data as ``dask.delayed`` objects.  These objects either
point to blocks of bytes (``read_bytes``) or open file objects (``open_files``,
``open_text_files``).  They can handle different compression formats by
prepending protocols like ``s3://`` or ``hdfs://``.  They handle compression
formats listed in the ``dask.bytes.compression`` module.

These functions are not used for all data sources.  Some data sources like HDF5
are quite particular and receive custom treatment.


Delimiters
----------

The ``read_bytes`` function takes a path (or globstring of paths) and produces
a sample of the first file and a list of delayed objects for each of the other
files.  If passed a delimiter such as ``delimiter=b'\n'`` it will ensure that
the blocks of bytes start directly after a delimiter and end directly before a
delimiter.  This allows other functions, like ``pd.read_csv``, to operate on
these delayed values with expected behavior.

These delimiters are useful both for typical line-based formats (log files,
CSV, JSON) as well as other delimited formats like Avro, which may separate
logical chunks by a complex sentinel string.

Locations
---------

These functions dispatch to other functions that handle different storage
backends, like S3 and HDFS.  These storage backends register themselves with
protocols and so are called whenever the path is prepended with a string like
the following::

   s3://bucket/keys-*.csv

Compression
-----------

These functions support widely available compression technologies like ``gzip``,
``bz2``, ``xz``, ``snappy``, and ``lz4``.  More compressions can be easily
added by inserting functions into dictionaries available in the
``dask.bytes.compression`` module.  This can be done at runtime and need not be
added directly to the codebase.

However, not all compression technologies are available for all functions.  In
particular, compression technologies like ``gzip`` do not support efficient
random access and so are useful for streaming ``open_files`` but not useful for
``read_bytes`` which splits files at various points.

Functions
---------

.. autofunction:: read_bytes
.. autofunction:: open_files
.. autofunction:: open_text_files
