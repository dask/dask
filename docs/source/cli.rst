Command Line Interface
======================

Dask provides the ``dask`` executable to accomplish tasks directly
from the command line. Projects in the Dask ecosystem leverage the
command line tool by adding subcommands.

Built in Commands
-----------------

dask docs
~~~~~~~~~

Command to open the Dask documentation website.

dask info
~~~~~~~~~

Command to inspect information about a Dask installation.

Extending the Dask CLI
----------------------

Third party packages can augment the ``dask`` command line tool via
entry points and Click_. Dask will discover :obj:`click.Command` and
:obj:`click.Group` objects registered as entry points under the
``dask_cli`` namespace.

More documentation on entry points can be found at:

- `The python packaging documentation
  <https://setuptools.pypa.io/en/latest/userguide/entry_point.html>`_.
- `The setuptools user guide
  <https://setuptools.pypa.io/en/latest/userguide/entry_point.html>`_.
- `The poetry plugins documentation
  <https://python-poetry.org/docs/pyproject/#plugins>`_.

Example: PEP 621
~~~~~~~~~~~~~~~~

According to `PEP-621 <https://peps.python.org/pep-0621/>`_, if
starting a new project, the canonical way to add an entry point to
your Python project is to use the ``[project.entry-points]`` table in
the ``pyproject.toml`` file. This method should be picked up by any
Python build system that is compatible with ``PEP-621``'s ``project``
configuration.

If your project is called ``mypackage``, and it contains a ``cli.py``
module under the ``mypackage`` namespace with the following contents:

.. code-block::

   # in the file mypackage/cli.py
   import click

   @click.command(name="mycommand")
   @click.option("-c", "--count", default=1)
   def main(count):
       for _ in range(count):
           click.echo("hello from mycommand!")

You can create an entry point that will be discovered by Dask by
adding to ``pyproject.toml``:

.. code-block:: toml

    [project.entry-points."dask_cli"]
    mycommand = "mypackage.cli:main"

After installing ``mypackage``, the ``mycommand`` subcommand should be
available to the ``dask`` CLI:

.. code-block:: shell

   $ dask mycommand
   hello from mycommand!

   $ dask mycommand -c 3
   hello from mycommand!
   hello from mycommand!
   hello from mycommand!

Example: setuptools
~~~~~~~~~~~~~~~~~~~

If your project already uses ``setuptools`` as the build system with a
``setup.cfg`` file and/or a ``setup.py`` file, we can create an entry
point for the same ``mycommand.cli:main`` function introduced in the
previous section. If using ``setup.cfg``, the entry point can be
registered by adding the following block to the file:

.. code-block:: ini

   [options.entry_points]
   dask_cli =
       mycommand = mypackage.cli:main

Or the entry point can be registered directly in ``setup.py`` with:

.. code-block:: python

   from setuptools import setup

   setup(
       ...
       entry_points="""
           [dask_cli]
           mycommand=mypackage.cli:main
       """,
   )

.. _Click: https://click.palletsprojects.com/
