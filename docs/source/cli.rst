Command Line Interface
======================

Dask provides a ``dask`` executable for a command line interface.
Dask's CLI is :ref:`designed to be extensible <extending-cli>` allowing
other projects in the Dask ecosystem (such as ``distributed``) to
add subcommands.

Built-in commands
-----------------

``dask`` comes with the following commands.

.. click:: dask.cli:info
   :prog: dask info
   :show-nested:

.. click:: dask.cli:docs
   :prog: dask docs
   :show-nested:


.. _extending-cli:

Extending the Dask CLI
----------------------

.. note::

    This section is intended for library authors who want to
    integrate their library with the ``dask`` CLI.

Third party packages can extend the ``dask`` command line tool via
entry points and Click_. Dask will discover :obj:`click.Command` and
:obj:`click.Group` objects registered as entry points under the
``dask_cli`` namespace. Below you'll find two examples which augment
the ``dask`` CLI by adding a ``dask_cli`` entry point to a project.

Click provides great documentation for writing commands; more
documentation on entry points can be found at:

- `The python packaging documentation
  <https://setuptools.pypa.io/en/latest/userguide/entry_point.html>`_.
- `The setuptools user guide
  <https://setuptools.pypa.io/en/latest/userguide/entry_point.html>`_.
- `The poetry plugins documentation
  <https://python-poetry.org/docs/pyproject/#plugins>`_.

Example: PEP-621
~~~~~~~~~~~~~~~~

Since `PEP-621 <https://peps.python.org/pep-0621/>`_, if starting a
new project, the canonical way to add an entry point to your Python
project is to use the ``[project.entry-points]`` table in the
``pyproject.toml`` file. This method should be picked up by any Python
build system that is compatible with ``PEP-621``'s ``project``
configuration. Hatch_, Flit_, and setuptools_ (version 61.0.0 or
later) are three example build systems which are PEP-621 compatible
and use ``[project.entry-points]``.

For example, if your project is called ``mypackage``, and it contains
a ``cli.py`` module under the ``mypackage`` namespace with the
following contents:

.. code-block::

   # in the file mypackage/cli.py
   import click

   @click.command(name="mycommand")
   @click.argument("name", type=str)
   @click.option("-c", "--count", default=1)
   def main(name, count):
       for _ in range(count):
           click.echo(f"hello {name} from mycommand!")

You can create an entry point that will be discovered by Dask by
adding to ``pyproject.toml``:

.. code-block:: toml

    [project.entry-points."dask_cli"]
    mycommand = "mypackage.cli:main"

After installing ``mypackage``, the ``mycommand`` subcommand should be
available to the ``dask`` CLI:

.. code-block:: shell

   $ dask mycommand world
   hello world from mycommand!

   $ dask mycommand user -c 3
   hello user from mycommand!
   hello user from mycommand!
   hello user from mycommand!

Example: setup.cfg and setup.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   If you are starting a new project the recommendation from the
   Python Packaging Authority (PyPA_) is to use PEP-621, these
   setuptools instructions are provided for existing projects.

If your project already uses ``setuptools`` with a ``setup.cfg`` file
and/or a ``setup.py`` file, we can create an entry point for the same
``mycommand.cli:main`` function introduced in the previous section. If
using ``setup.cfg``, the entry point can be registered by adding the
following block to the file:

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
.. _Hatch: https://github.com/pypa/hatch
.. _setuptools: https://setuptools.pypa.io/en/latest/index.html
.. _PyPA: https://pypa.io/
.. _Flit: https://flit.pypa.io/
