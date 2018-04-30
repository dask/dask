Configuration
=============

Taking full advantage of Dask sometimes requires user configuration.
This might be to control logging, specify cluster configuration, provide
credentials for security, or any of several other options that arise in
production.

Configuration is specified in one of the following ways:

1.  YAML files in ``~/.config/dask/`` or ``/etc/dask/``
2.  Environment variables like ``DASK_WORK_STEALING=True``
3.  Default settings within sub-libraries

This combination makes it easy to specify configuration in a variety of
settings ranging from personal workstations, to IT-mandated configuration, to
docker images.


Access Configuration
--------------------

.. currentmodule:: dask.config

.. autosummary::
   get

Configuration is usually read by using the ``dask.config`` module, either with
the ``config`` dictionary or the ``get`` function:

.. code-block:: python

   >>> import dask
   >>> dask.config.config
   {
    'logging': {
      'distributed': 'info',
      'bokeh': 'critical',
      'tornado': 'critical',
      },
    'work-stealing': True,
    'log-format': '%(name)s - %(levelname)s - %(message)s'
   }

   >>> dask.config.get('work-stealing')
   True

   >>> dask.config.get('logging.bokeh')  # use `.` for nested access
   'critical'

You may wish to inspect the ``dask.config.config`` dictionary to get a sense
for what configuration is being used by your current system.


Specify with YAML files
-----------------------

Dask checks the following locations for configuration files:

1.  ``~/.config/dask``
3.  the ``{sys.executable}/etc/dask`` directory local to the Python executable
4.  The root ``/etc/dask/`` directory

Within each of these directories it collects *all* YAML files and merges them
together, preferring values according to the ordering in the list above (top
takes precedence over bottom).  Additionally you can specify a path with the
``DASK_CONFIG`` environment variable, that takes precedence at the top of the
list above.

The contents of these YAML files are merged together, allowing different
dask subprojects like ``dask-kubernetes`` or ``dask-ml`` to manage configuration
files separately, but have them merge into the same global configuration.

*Note: for historical reasons we also look in the ``~/.dask`` directory for
config files.  This is deprecated and will soon be removed.*


Specify with Environment Variables
----------------------------------

You can also specify configuration values with environment variables like
the following:

.. code-block:: bash

   export DASK_FOO_BAR=True

resulting in configuration values like the following:

.. code-block:: python

   {'foo-bar': True}

Dask searches for all environment variables that start with ``DASK_``, then
transforms keys by converting to lower case and changing underscores to
hyphens.  Dask tries to parse all values with ``ast.literal_eval``, letting
users pass numeric and boolean values (such as ``True`` in the example above)
as well as lists, dictionaries, and so on with normal Python syntax.

Environment variables take precedence over configuration values found in YAML
files.


Specify within Python
---------------------

.. autosummary::
   set_config

Configuration is stored within a normal Python dictionary in
``dask.config.config`` and can be modified using normal Python operations.

Additionally, you can temporarily set a configuration value using the
``set_config`` context manager.

.. code-block:: python

   with dask.config.set_config({'work-stealing': False}):
       ...

API
---

.. autofunction:: get
.. autofunction:: set_config
