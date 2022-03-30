.. _order:

``sizeof`` Plugins
========

.. note::

   This is an advanced topic that most users won't need to worry about.

When Dask needs to compute the memory footprint of an object, e.g. to determine which objects to spill to disk, it uses the `sizeof` API. Libraries which need to define a `sizeof` implementation for their own objects can use the `sizeof.register` API:

.. code-block:: python

   >>> import numpy as np
   >>> from dask.sizeof import sizeof
   >>> @sizeof.register(np.ndarray)
   >>> def sizeof_numpy_like(array):
   ...     return array.nbytes

This code can be executed in order to register the implementation with Dask by placing it in one of the library's modules e.g. ``__init__.py``. However, this introduces a maintenance burden on the developers of these libraries, and must be manually imported on all workers in the event that these libraries do not accept the patch. 

Dask exposes an `entrypoint <https://packaging.python.org/specifications/entry-points/>` under the group ``dask.sizeof`` to enable third-party libraries to develop and maintain these ``sizeof`` implementations for other libraries. 

For a fictitious library ``numpy_sizeof_dask.py``, the neccessary ``setup.cfg`` configuration would be as follows:

.. code-block:: ini

   [options.entry_points]
   dask.sizeof = 
      numpy = numpy_sizeof_lib:sizeof_plugin

whilst ``numpy_sizeof_dask.py`` would contain

.. code-block:: python

   >>> import numpy as np
   >>> def sizeof_plugin(sizeof):
   ...    @sizeof.register(np.ndarray)
   ...    def sizeof_numpy_like(array):
   ...        return array.nbytes 

Dask calls the entrypoint (``sizeof_plugin``) with the ``dask.sizeof.sizeof`` object, which can then be used as usual.
