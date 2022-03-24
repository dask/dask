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
   :toctree: generated
   :template: custom-module-template.rst

   random

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
   :toctree: generated
   :recursive:
   :template: custom-class-template.rst

   Bag

Item Methods
------------

.. autosummary::
   :toctree: generated
   :recursive:
   :template: custom-class-template.rst

   Item