Configuration
=============

Taking full advantage of Dask sometimes requires user configuration.
This might be to control logging verbosity, specify cluster configuration,
provide credentials for security, or any of several other options that arise in
production.

Configuration is specified in one of the following ways:

1.  YAML files in ``~/.config/dask/`` or ``/etc/dask/``
2.  Environment variables like ``DASK_SCHEDULER__WORK_STEALING=True``
3.  Default settings within sub-libraries

This combination makes it easy to specify configuration in a variety of
settings ranging from personal workstations, to IT-mandated configuration, to
docker images.


Access Configuration
--------------------

.. currentmodule:: dask

.. autosummary::
   dask.config.get

Configuration is usually read by using the ``dask.config`` module, either with
the ``config`` dictionary or the ``get`` function:

.. code-block:: python

   >>> import dask
   >>> import dask.distributed  # populate config with distributed defaults
   >>> dask.config.config
   {
     'logging': {
       'distributed': 'info',
       'bokeh': 'critical',
       'tornado': 'critical',
     }
     'admin': {
       'log-format': '%(name)s - %(levelname)s - %(message)s'
     }
   }

   >>> dask.config.get('logging')
   {'distributed': 'info',
    'bokeh': 'critical',
    'tornado': 'critical'}

   >>> dask.config.get('logging.bokeh')  # use `.` for nested access
   'critical'

You may wish to inspect the ``dask.config.config`` dictionary to get a sense
for what configuration is being used by your current system.


Specify Configuration
---------------------

YAML files
~~~~~~~~~~

You can specify configuration values in YAML files like the following:

.. code-block:: yaml

   logging:
     distributed: info
     bokeh: critical
     tornado: critical

   scheduler:
     work-stealing: True
     allowed-failures: 5

    admin:
      log-format: '%(name)s - %(levelname)s - %(message)s'

These files can live in any of the following locations:

1.  The ``~/.config/dask`` directory in the user's home directory
2.  The ``{sys.prefix}/etc/dask`` directory local to Python
3.  The root ``/etc/dask/`` directory

Dask searches for *all* YAML files within each of these directories and merges
them together, preferring configuration files closer to the user over system
configuration files (preference follows the order in the list above).
Additionally users can specify a path with the ``DASK_CONFIG`` environment
variable, that takes precedence at the top of the list above.

The contents of these YAML files are merged together, allowing different
dask subprojects like ``dask-kubernetes`` or ``dask-ml`` to manage configuration
files separately, but have them merge into the same global configuration.

*Note: for historical reasons we also look in the ``~/.dask`` directory for
config files.  This is deprecated and will soon be removed.*


Environment Variables
~~~~~~~~~~~~~~~~~~~~~

You can also specify configuration values with environment variables like
the following:

.. code-block:: bash

   export DASK_SCHEDULER__WORK_STEALING=True
   export DASK_SCHEDULER__ALLOWED_FAILURES=5

resulting in configuration values like the following:

.. code-block:: python

   {'scheduler': {'work-stealing': True, 'allowed-failures': 5}}

Dask searches for all environment variables that start with ``DASK_``, then
transforms keys by converting to lower case, changing double-underscores to
nested structures, and changing single underscores to hyphens.

Dask tries to parse all values with `ast.literal_eval
<https://docs.python.org/3/library/ast.html#ast.literal_eval>`_, letting users
pass numeric and boolean values (such as ``True`` in the example above) as well
as lists, dictionaries, and so on with normal Python syntax.

Environment variables take precedence over configuration values found in YAML
files.


Defaults
~~~~~~~~

Additionally, individual subprojects may add their own default values when they
are imported.  These are always added with lower priority than the YAML files
or environment variables mentioned above

.. code-block:: python

   >>> import dask.config
   >>> dask.config.config  # no configuration by default
   {}

   >>> import dask.distributed
   >>> dask.config.config  # New values have been added
   {'scheduler': ...,
    'worker': ...,
    'tls': ...}


Directly within Python
~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   dask.config.set

Configuration is stored within a normal Python dictionary in
``dask.config.config`` and can be modified using normal Python operations.

Additionally, you can temporarily set a configuration value using the
``dask.config.set`` function.  This function accepts a dictionary as an input
and interprets ``"."`` as nested access

.. code-block:: python

   >>> dask.config.set({'scheduler.work-stealing': True})

This function can also be used as a context manager for consistent cleanup.

.. code-block:: python

   with dask.config.set({'scheduler.work-stealing': True}):
       ...


Updating and Merging Configuration
----------------------------------

.. autosummary::
   dask.config.merge
   dask.config.update

As described above, configuration can come from many places, including several
YAML files, environment variables, and project defaults.  Each of these
provides a configuration that is possibly nested like the following:

.. code-block:: python

   x = {'a': 0, 'c': {'d': 4}}
   y = {'a': 1, 'b': 2, 'c': {'e': 5}}

Dask will merge these configurations respecting nested data structures, and
respecting order.

.. code-block:: python

   >>> dask.config.merge(x, y)
   {'a': 1, 'b': 2, 'c': {'d': 4, 'e': 5}}

You can also use the ``update`` function to update the existing configuration
in place with a new configuration.  This can be done with priority being given
to either config.  This is often used to update the global configuration in
``dask.config.config``

.. code-block:: python

   dask.config.update(dask.config, new, priority='new')  # Give priority to new values
   dask.config.update(dask.config, new, priority='old')  # Give priority to old values

API
---

.. autofunction:: dask.config.get
.. autofunction:: dask.config.set
.. autofunction:: dask.config.merge
.. autofunction:: dask.config.update
