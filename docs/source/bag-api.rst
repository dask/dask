API
===

.. currentmodule:: dask.bag

Create Bags
-----------

.. autosummary::
   :toctree: generated/

   from_sequence
   from_delayed
   from_url
   range
   read_text
   read_avro

From dataframe
~~~~~~~~~~~~~~

.. currentmodule:: dask.dataframe

.. autosummary::
   :toctree: generated/

   DataFrame.to_bag
   Series.to_bag

Top-level functions
-------------------

.. currentmodule:: dask.bag

.. autosummary::
   :toctree: generated/

   concat
   map
   map_partitions
   to_textfiles
   zip

Random Sampling
---------------

.. autosummary::
   :toctree: generated/

   random.choices
   random.sample


Turn Bags into other things
---------------------------

.. autosummary::
   :toctree: generated/

   Bag.to_textfiles
   Bag.to_dataframe
   Bag.to_delayed
   Bag.to_avro


Bag Methods
-----------

.. autosummary::
   :toctree: generated/

   Bag
   Bag.accumulate
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
   Bag.persist
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
   Bag.to_avro
   Bag.to_dataframe
   Bag.to_delayed
   Bag.to_textfiles
   Bag.topk
   Bag.var
   Bag.visualize


Item Methods
------------

.. autosummary::
   :toctree: generated/

   Item
   Item.apply
   Item.compute
   Item.from_delayed
   Item.persist
   Item.to_delayed
   Item.visualize
