API
===

.. currentmodule:: dask.bag

Top level user functions:

.. autosummary::
    Bag
    Bag.all
    Bag.any
    Bag.compute
    Bag.count
    Bag.distinct
    Bag.filter
    Bag.flatten
    Bag.fold
    Bag.foldby
    Bag.frequencies
    Bag.groupby
    Bag.join
    Bag.map
    Bag.map_partitions
    Bag.max
    Bag.mean
    Bag.min
    Bag.pluck
    Bag.product
    Bag.reduction
    Bag.random_sample
    Bag.remove
    Bag.repartition
    Bag.starmap
    Bag.std
    Bag.sum
    Bag.take
    Bag.to_dataframe
    Bag.to_delayed
    Bag.to_textfiles
    Bag.to_avro
    Bag.topk
    Bag.var
    Bag.visualize

Create Bags
-----------

.. autosummary::
   from_sequence
   from_delayed
   read_text
   from_url
   read_avro
   range

Top-level functions
-------------------

.. autosummary::
   concat
   map
   map_partitions
   zip

Turn Bags into other things
---------------------------

.. autosummary::
   Bag.to_textfiles
   Bag.to_dataframe
   Bag.to_delayed
   Bag.to_avro

Bag methods
-----------

.. autoclass:: Bag
   :members:

Other functions
---------------

.. autofunction:: from_sequence
.. autofunction:: from_delayed
.. autofunction:: read_text
.. autofunction:: from_url
.. autofunction:: read_avro
.. autofunction:: range
.. autofunction:: concat
.. autofunction:: map_partitions
.. autofunction:: map
.. autofunction:: zip
