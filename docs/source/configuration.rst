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

Dask's configuration system is usually accessed using the ``dask.config.get`` function.
You can use ``.`` for nested access, for example:

.. code-block:: python

   >>> import dask
   >>> import dask.distributed  # populate config with distributed defaults

   >>> dask.config.get("distributed.client") # use `.` for nested access
   {'heartbeat': '5s', 'scheduler-info-interval': '2s'}

   >>> dask.config.get("distributed.scheduler.unknown-task-duration")
   '500ms'

You may wish to inspect the ``dask.config.config`` dictionary to get a sense
for what configuration is being used by your current system.

Note that the ``get`` function treats underscores and hyphens identically.
For example, ``dask.config.get("temporary-directory")`` is equivalent to
``dask.config.get("temporary_directory")``.

Values like ``"128 MiB"`` and ``"10s"`` are parsed using the functions in
:ref:`api.utilities`.

Specify Configuration
---------------------

YAML files
~~~~~~~~~~

You can specify configuration values in YAML files. For example:

.. code-block:: yaml

   array:
     chunk-size: 128 MiB

   distributed:
     worker:
       memory:
         spill: 0.85  # default: 0.7
         target: 0.75  # default: 0.6
         terminate: 0.98  # default: 0.95
            
     dashboard:
       # Locate the dashboard if working on a Jupyter Hub server
       link: /user/<user>/proxy/8787/status
        

These files can live in any of the following locations:

1.  The ``~/.config/dask`` directory in the user's home directory
2.  The ``{sys.prefix}/etc/dask`` directory local to Python
3.  The ``{prefix}/etc/dask`` directories with ``{prefix}`` in `site.PREFIXES
    <https://docs.python.org/3/library/site.html#site.PREFIXES>`_
4.  The root directory (specified by the ``DASK_ROOT_CONFIG`` environment
    variable or ``/etc/dask/`` by default)

Dask searches for *all* YAML files within each of these directories and merges
them together, preferring configuration files closer to the user over system
configuration files (preference follows the order in the list above).
Additionally, users can specify a path with the ``DASK_CONFIG`` environment
variable, which takes precedence at the top of the list above.

The contents of these YAML files are merged together, allowing different
Dask subprojects like ``dask-kubernetes`` or ``dask-ml`` to manage configuration
files separately, but have them merge into the same global configuration.


Environment Variables
~~~~~~~~~~~~~~~~~~~~~

You can also specify configuration values with environment variables like
the following:

.. code-block:: bash

   export DASK_DISTRIBUTED__SCHEDULER__WORK_STEALING=True
   export DASK_DISTRIBUTED__SCHEDULER__ALLOWED_FAILURES=5
   export DASK_DISTRIBUTED__DASHBOARD__LINK="/user/<user>/proxy/8787/status"

resulting in configuration values like the following:

.. code-block:: python

   {
       'distributed': {
           'scheduler': {
               'work-stealing': True,
               'allowed-failures': 5
           }
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
   {
       'scheduler': ...,
       'worker': ...,
       'tls': ...
   }


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

   >>> dask.config.set({'optimization.fuse.ave-width': 4})

This function can also be used as a context manager for consistent cleanup:

.. code-block:: python

   >>> with dask.config.set({'optimization.fuse.ave-width': 4}):
   ...     arr2, = dask.optimize(arr)

Note that the ``set`` function treats underscores and hyphens identically.
For example, ``dask.config.set({'optimization.fuse.ave_width': 4})`` is
equivalent to ``dask.config.set({'optimization.fuse.ave-width': 4})``.

Finally, note that persistent objects may acquire configuration settings when
they are initialized. These settings may also be cached for performance reasons.
This is particularly true for ``dask.distributed`` objects such as Client, Scheduler,
Worker, and Nanny.


Distributing configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~

It may also be desirable to package up your whole Dask configuration for use on
another machine. This is used in some Dask Distributed libraries to ensure remote components
have the same configuration as your local system.

This is typically handled by the downstream libraries which use base64 encoding to pass
config via the ``DASK_INTERNAL_INHERIT_CONFIG`` environment variable.

.. autosummary::
   dask.config.serialize
   dask.config.deserialize

Conversion Utility
~~~~~~~~~~~~~~~~~~

It is possible to configure Dask inline with dot notation, with YAML or via environment variables. You can enter
your own configuration items below to convert back and forth.

.. warning::
   This utility is designed to improve understanding of converting between different notations
   and does not claim to be a perfect implementation. Please use for reference only.

**YAML**

.. raw:: html

   <textarea id="configConvertUtilYAML" name="configConvertUtilYAML" rows="10" cols="50" class="configTextArea" wrap="off">
   array:
      chunk-size: 128 MiB

   distributed:
      workers:
         memory:
            spill: 0.85
            target: 0.75
            terminate: 0.98
   </textarea>

**Environment variable**

.. raw:: html

   <textarea id="configConvertUtilEnv" name="configConvertUtilEnv" rows="10" cols="50" class="configTextArea" wrap="off">
   export DASK_ARRAY__CHUNK_SIZE="128 MiB"
   export DASK_DISTRIBUTED__WORKERS__MEMORY__SPILL=0.85
   export DASK_DISTRIBUTED__WORKERS__MEMORY__TARGET=0.75
   export DASK_DISTRIBUTED__WORKERS__MEMORY__TERMINATE=0.98
   </textarea>

**Inline with dot notation**

.. raw:: html

   <textarea id="configConvertUtilCode" name="configConvertUtilCode" rows="10" cols="50" class="configTextArea" wrap="off">
   >>> dask.config.set({"array.chunk-size": "128 MiB"})
   >>> dask.config.set({"distributed.workers.memory.spill": 0.85})
   >>> dask.config.set({"distributed.workers.memory.target": 0.75})
   >>> dask.config.set({"distributed.workers.memory.terminate": 0.98})
   </textarea>

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
           defaults = yaml.safe_load(f)

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


Configuration Reference
-----------------------

.. contents:: :local:

.. note::
   It is possible to configure Dask inline with dot notation, with YAML or via environment variables.
   See the `conversion utility <#conversion-utility>`_ for converting the following dot notation to other forms.

Dask
~~~~

.. dask-config-block::
    :location: dask
    :config: https://raw.githubusercontent.com/dask/dask/main/dask/dask.yaml
    :schema: https://raw.githubusercontent.com/dask/dask/main/dask/dask-schema.yaml


Distributed Client
~~~~~~~~~~~~~~~~~~

.. dask-config-block::
    :location: distributed.client
    :config: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed-schema.yaml

Distributed Comm
~~~~~~~~~~~~~~~~

.. dask-config-block::
    :location: distributed.comm
    :config: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed-schema.yaml


Distributed Dashboard
~~~~~~~~~~~~~~~~~~~~~

.. dask-config-block::
    :location: distributed.dashboard
    :config: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed-schema.yaml


Distributed Deploy
~~~~~~~~~~~~~~~~~~

.. dask-config-block::
    :location: distributed.deploy
    :config: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed-schema.yaml


Distributed Scheduler
~~~~~~~~~~~~~~~~~~~~~

.. dask-config-block::
    :location: distributed.scheduler
    :config: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed-schema.yaml


Distributed Worker
~~~~~~~~~~~~~~~~~~

.. dask-config-block::
    :location: distributed.worker
    :config: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed-schema.yaml


Distributed Nanny
~~~~~~~~~~~~~~~~~

.. dask-config-block::
    :location: distributed.nanny
    :config: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed-schema.yaml


Distributed Admin
~~~~~~~~~~~~~~~~~

.. dask-config-block::
    :location: distributed.admin
    :config: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed-schema.yaml


Distributed RMM
~~~~~~~~~~~~~~~

.. dask-config-block::
    :location: distributed.rmm
    :config: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed.yaml
    :schema: https://raw.githubusercontent.com/dask/distributed/main/distributed/distributed-schema.yaml
