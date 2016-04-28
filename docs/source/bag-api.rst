API
===

.. currentmodule:: dask.bag

Top level user functions:

.. autosummary::
    Bag
    Bag.all
    Bag.any
    Bag.compute
    Bag.concat
    Bag.count
    Bag.distinct
    Bag.filter
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
    Bag.remove
    Bag.repartition
    Bag.std
    Bag.sum
    Bag.take
    Bag.to_dataframe
    Bag.to_delayed
    Bag.to_textfiles
    Bag.topk
    Bag.var
    Bag.visualize

Create Bags
-----------

.. autosummary::
   concat
   from_sequence
   read_text

Turn Bags into other things
---------------------------

.. autosummary::
   Bag.to_textfiles
   Bag.to_dataframe

Bag methods
-----------

.. autoclass:: Bag
   :members:

Other functions
---------------

.. autofunction:: from_sequence
.. autofunction:: read_text
.. autofunction:: concat
