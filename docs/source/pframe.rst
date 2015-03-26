Partitioned Frames
==================

Dask frame partitions data along the index by appending a sequence of in-memory
DataFrames on to the on-disk Partition ``pframe`` data structure.  This data
structure splits incoming DataFrames and stores the shards efficiently in
growing BColz_ carrays.

.. image:: images/pframe-design.png
   :alt: Partitioned frame design

.. _BColz: http://bcolz.blosc.org/
