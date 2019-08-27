Configuration
=============

Taking full advantage of Dask sometimes requires user configuration.
This might be to control logging verbosity, specify cluster configuration,
provide credentials for security, or any of several other options that arise in
production.

Configuration is specified in one of the following ways:

1.  YAML files in ``~/.config/dask/`` or ``/etc/dask/``
2.  Environment variables like ``DASK_DISTRIBUTED__SCHEDULER__WORK_STEALING=True``
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

Note that the ``get`` function treats underscores and hyphens identically.
For example, ``dask.config.get('num_workers')`` is equivalent to
``dask.config.get('num-workers')``.


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
3.  The root directory (specified by the ``DASK_ROOT_CONFIG`` environment
    variable or ``/etc/dask/`` by default)

Dask searches for *all* YAML files within each of these directories and merges
them together, preferring configuration files closer to the user over system
configuration files (preference follows the order in the list above).
Additionally, users can specify a path with the ``DASK_CONFIG`` environment
variable, which takes precedence at the top of the list above.

The contents of these YAML files are merged together, allowing different
Dask subprojects like ``dask-kubernetes`` or ``dask-ml`` to manage configuration
files separately, but have them merge into the same global configuration.

*Note: for historical reasons we also look in the ``~/.dask`` directory for
config files.  This is deprecated and will soon be removed.*


Environment Variables
~~~~~~~~~~~~~~~~~~~~~

You can also specify configuration values with environment variables like
the following:

.. code-block:: bash

   export DASK_DISTRIBUTED__SCHEDULER__WORK_STEALING=True
   export DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES=5

resulting in configuration values like the following:

.. code-block:: python

   {'distributed':
     {'scheduler':
       {'work-stealing': True,
        'allowed-failures': 5}
     }
   }

Dask searches for all environment variables that start with ``DASK_``, then
transforms keys by converting to lower case and changing double-underscores to
nested structures.

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
or environment variables mentioned above:

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
and interprets ``"."`` as nested access:

.. code-block:: python

   >>> dask.config.set({'scheduler.work-stealing': True})

This function can also be used as a context manager for consistent cleanup:

.. code-block:: python

   with dask.config.set({'scheduler.work-stealing': True}):
       ...

Note that the ``set`` function treats underscores and hyphens identically.
For example, ``dask.config.set({'scheduler.work-stealing': True})`` is
equivalent to ``dask.config.set({'scheduler.work_stealing': True})``.


Updating Configuration
----------------------

Manipulating configuration dictionaries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   dask.config.merge
   dask.config.update
   dask.config.expand_environment_variables

As described above, configuration can come from many places, including several
YAML files, environment variables, and project defaults.  Each of these
provides a configuration that is possibly nested like the following:

.. code-block:: python

   x = {'a': 0, 'c': {'d': 4}}
   y = {'a': 1, 'b': 2, 'c': {'e': 5}}

Dask will merge these configurations respecting nested data structures, and
respecting order:

.. code-block:: python

   >>> dask.config.merge(x, y)
   {'a': 1, 'b': 2, 'c': {'d': 4, 'e': 5}}

You can also use the ``update`` function to update the existing configuration
in place with a new configuration.  This can be done with priority being given
to either config.  This is often used to update the global configuration in
``dask.config.config``:

.. code-block:: python

   dask.config.update(dask.config, new, priority='new')  # Give priority to new values
   dask.config.update(dask.config, new, priority='old')  # Give priority to old values

Sometimes it is useful to expand environment variables stored within a
configuration.  This can be done with the ``expand_environment_variables``
function:

.. code-block:: python

    dask.config.config = dask.config.expand_environment_variables(dask.config.config)

Refreshing Configuration
~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   dask.config.collect
   dask.config.refresh

If you change your environment variables or YAML files, Dask will not
immediately see the changes.  Instead, you can call ``refresh`` to go through
the configuration collection process and update the default configuration:

.. code-block:: python

   >>> dask.config.config
   {}

   >>> # make some changes to yaml files

   >>> dask.config.refresh()
   >>> dask.config.config
   {...}

This function uses ``dask.config.collect``, which returns the configuration
without modifying the global configuration.  You might use this to determine
the configuration of particular paths not yet on the config path:

.. code-block:: python

   >>> dask.config.collect(paths=[...])
   {...}

Downstream Libraries
--------------------

.. autosummary::
   dask.config.ensure_file
   dask.config.update
   dask.config.update_defaults

Downstream Dask libraries often follow a standard convention to use the central
Dask configuration.  This section provides recommendations for integration
using a fictional project, ``dask-foo``, as an example.

Downstream projects typically follow the following convention:

1.  Maintain default configuration in a YAML file within their source
    directory::

       setup.py
       dask_foo/__init__.py
       dask_foo/config.py
       dask_foo/core.py
       dask_foo/foo.yaml  # <---

2.  Place configuration in that file within a namespace for the project:

    .. code-block:: yaml

       # dask_foo/foo.yaml

       foo:
         color: red
         admin:
           a: 1
           b: 2

3.  Within a config.py file (or anywhere) load that default config file and
    update it into the global configuration:

    .. code-block:: python

       # dask_foo/config.py
       import os
       import yaml

       import dask.config

       fn = os.path.join(os.path.dirname(__file__), 'foo.yaml')

       with open(fn) as f:
           defaults = yaml.load(f)

       dask.config.update_defaults(defaults)

4.  Within that same config.py file, copy the ``'foo.yaml'`` file to the user's
    configuration directory if it doesn't already exist.

    We also comment the file to make it easier for us to change defaults in the
    future.

    .. code-block:: python

       # ... continued from above

       dask.config.ensure_file(source=fn, comment=True)

    The user can investigate ``~/.config/dask/*.yaml`` to see all of the
    commented out configuration files to which they have access.

5.  Ensure that this file is run on import by including it in ``__init__.py``:

    .. code-block:: python

       # dask_foo/__init__.py

       from . import config

6.  Within ``dask_foo`` code, use the ``dask.config.get`` function to access
    configuration values:

    .. code-block:: python

       # dask_foo/core.py

       def process(fn, color=dask.config.get('foo.color')):
           ...

7.  You may also want to ensure that your yaml configuration files are included
    in your package.  This can be accomplished by including the following line
    in your MANIFEST.in::

       recursive-include <PACKAGE_NAME> *.yaml

    and the following in your setup.py ``setup`` call:

    .. code-block:: python

        from setuptools import setup

        setup(...,
              include_package_data=True,
              ...)

This process keeps configuration in a central place, but also keeps it safe
within namespaces.  It places config files in an easy to access location
by default (``~/.config/dask/\*.yaml``), so that users can easily discover what
they can change, but maintains the actual defaults within the source code, so
that they more closely track changes in the library.

However, downstream libraries may choose alternative solutions, such as
isolating their configuration within their library, rather than using the
global dask.config system.  All functions in the ``dask.config`` module also
work with parameters, and do not need to mutate global state.


API
---

.. autofunction:: dask.config.get
.. autofunction:: dask.config.set
.. autofunction:: dask.config.merge
.. autofunction:: dask.config.update
.. autofunction:: dask.config.collect
.. autofunction:: dask.config.refresh
.. autofunction:: dask.config.ensure_file
.. autofunction:: dask.config.expand_environment_variables
