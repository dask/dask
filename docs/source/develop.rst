Development Guidelines
======================

Dask is a community maintained project.  We welcome contributions in the form
of bug reports, documentation, code, design proposals, and more.
This page provides resources on how best to contribute.

Where to ask for help
---------------------

Dask conversation happens in the following places:

1.  `Stack Overflow #dask tag`_: for usage questions
2.  `GitHub Issue Tracker`_: for discussions around new features or established bugs
3.  `Gitter chat`_: for real-time discussion

For usage questions and bug reports we strongly prefer the use of Stack Overflow
and GitHub issues over gitter chat.  GitHub and Stack Overflow are more easily
searchable by future users and so is more efficient for everyone's time.
Gitter chat is generally reserved for community discussion.

.. _`Stack Overflow  #dask tag`: https://stackoverflow.com/questions/tagged/dask
.. _`GitHub Issue Tracker`: https://github.com/dask/dask/issues/
.. _`Gitter chat`: https://gitter.im/dask/dask


Separate Code Repositories
--------------------------

Dask maintains code and documentation in a few git repositories hosted on the
GitHub ``dask`` organization, https://github.com/dask.  This includes the primary
repository and several other repositories for different components.  A
non-exhaustive list follows:

*  https://github.com/dask/dask: The main code repository holding parallel
   algorithms, the single-machine scheduler, and most documentation
*  https://github.com/dask/distributed: The distributed memory scheduler
*  https://github.com/dask/dask-ml: Machine learning algorithms
*  https://github.com/dask/s3fs: S3 Filesystem interface
*  https://github.com/dask/gcsfs: GCS Filesystem interface
*  https://github.com/dask/hdfs3: Hadoop Filesystem interface
*  ...

Git and GitHub can be challenging at first.  Fortunately good materials exist
on the internet.  Rather than repeat these materials here, we refer you to
Pandas' documentation and links on this subject at
https://pandas.pydata.org/pandas-docs/stable/contributing.html


Issues
------

The community discusses and tracks known bugs and potential features in the
`GitHub Issue Tracker`_.  If you have a new idea or have identified a bug, then
you should raise it there to start public discussion.

If you are looking for an introductory issue to get started with development,
then check out the `"good first issue" label`_, which contains issues that are good
for starting developers.  Generally, familiarity with Python, NumPy, Pandas, and
some parallel computing are assumed.

.. _`"good first issue" label`: https://github.com/dask/dask/labels/good%20first%20issue


Development Environment
-----------------------

Download code
~~~~~~~~~~~~~

Make a fork of the main `Dask repository <https://github.com/dask/dask>`_ and
clone the fork::

   git clone https://github.com/<your-github-username>/dask

Contributions to Dask can then be made by submitting pull requests on GitHub.


Install
~~~~~~~

You may want to install larger dependencies like NumPy and Pandas using a
binary package manager like conda_.  You can skip this step if you already
have these libraries, don't care to use them, or have sufficient build
environment on your computer to compile them when installing with ``pip``::

   conda install -y numpy pandas scipy bokeh

.. _conda: https://conda.io/

Install Dask and dependencies::

   cd dask
   pip install -e ".[complete]"

For development, Dask uses the following additional dependencies::

   pip install pytest moto mock


Run Tests
~~~~~~~~~

Dask uses py.test_ for testing.  You can run tests from the main dask directory
as follows::

   py.test dask --verbose --doctest-modules

.. _py.test: https://docs.pytest.org/en/latest/


Contributing to Code
--------------------

Dask maintains development standards that are similar to most PyData projects.  These standards include
language support, testing, documentation, and style.

Python Versions
~~~~~~~~~~~~~~~

Dask supports Python versions 3.5, 3.6, and 3.7.
Name changes are handled by the :file:`dask/compatibility.py` file.

Test
~~~~

Dask employs extensive unit tests to ensure correctness of code both for today
and for the future.  Test coverage is expected for all code contributions.

Tests are written in a py.test style with bare functions:

.. code-block:: python

   def test_fibonacci():
       assert fib(0) == 0
       assert fib(1) == 0
       assert fib(10) == 55
       assert fib(8) == fib(7) + fib(6)

       for x in [-3, 'cat', 1.5]:
           with pytest.raises(ValueError):
               fib(x)

These tests should compromise well between covering all branches and fail cases
and running quickly (slow test suites get run less often).

You can run tests locally by running ``py.test`` in the local dask directory::

   py.test dask --verbose

You can also test certain modules or individual tests for faster response::

   py.test dask/dataframe --verbose

   py.test dask/dataframe/tests/test_dataframe_core.py::test_set_index

Tests run automatically on the Travis.ci and Appveyor continuous testing
frameworks on every push to every pull request on GitHub.

Tests are organized within the various modules' subdirectories::

    dask/array/tests/test_*.py
    dask/bag/tests/test_*.py
    dask/dataframe/tests/test_*.py
    dask/diagnostics/tests/test_*.py

For the Dask collections like Dask Array and Dask DataFrame, behavior is
typically tested directly against the NumPy or Pandas libraries using the
``assert_eq`` functions:

.. code-block:: python

   import numpy as np
   import dask.array as da
   from dask.array.utils import assert_eq

   def test_aggregations():
       nx = np.random.random(100)
       dx = da.from_array(x, chunks=(10,))

       assert_eq(nx.sum(), dx.sum())
       assert_eq(nx.min(), dx.min())
       assert_eq(nx.max(), dx.max())
       ...

This technique helps to ensure compatibility with upstream libraries and tends
to be simpler than testing correctness directly.  Additionally, by passing Dask
collections directly to the ``assert_eq`` function rather than call compute
manually, the testing suite is able to run a number of checks on the lazy
collections themselves.


Docstrings
~~~~~~~~~~

User facing functions should roughly follow the numpydoc_ standard, including
sections for ``Parameters``, ``Examples``, and general explanatory prose.

By default, examples will be doc-tested.  Reproducible examples in documentation
is valuable both for testing and, more importantly, for communication of common
usage to the user.  Documentation trumps testing in this case and clear
examples should take precedence over using the docstring as testing space.
To skip a test in the examples add the comment ``# doctest: +SKIP`` directly
after the line.

.. code-block:: python

   def fib(i):
       """ A single line with a brief explanation

       A more thorough description of the function, consisting of multiple
       lines or paragraphs.

       Parameters
       ----------
       i: int
            A short description of the argument if not immediately clear

       Examples
       --------
       >>> fib(4)
       3
       >>> fib(5)
       5
       >>> fib(6)
       8
       >>> fib(-1)  # Robust to bad inputs
       ValueError(...)
       """

.. _numpydoc: https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt

Docstrings are currently tested under Python 3.6 on Travis.ci.  You can test
docstrings with pytest as follows::

   py.test dask --doctest-modules

Docstring testing requires ``graphviz`` to be installed. This can be done via::

   conda install -y graphviz


Code Formatting
~~~~~~~~~~~~~~~

Dask uses `Black <https://black.readthedocs.io/en/stable/>`_ and
`Flake8 <http://flake8.pycqa.org/en/latest/>`_ to ensure a consistent code
format throughout the project. ``black`` and ``flake8`` can be installed with
``pip``::

   pip install black flake8

and then run from the root of the Dask repository::

   black dask
   flake8 dask

to auto-format your code. Additionally, many editors have plugins that will
apply ``black`` as you edit files.

Optionally, you may wish to setup `pre-commit hooks <https://pre-commit.com/>`_
to automatically run ``black`` and ``flake8`` when you make a git commit. This
can be done by installing ``pre-commit``::

   pip install pre-commit

and then running::

   pre-commit install

from the root of the Dask repository. Now ``black`` and ``flake8`` will be run
each time you commit changes. You can skip these checks with
``git commit --no-verify``.


Contributing to Documentation
-----------------------------

Dask uses Sphinx_ for documentation, hosted on https://readthedocs.org .
Documentation is maintained in the RestructuredText markup language (``.rst``
files) in ``dask/docs/source``.  The documentation consists both of prose
and API documentation.

To build the documentation locally, first install the necessary requirements::

   cd docs/
   pip install -r requirements-docs.txt

Then build the documentation with ``make``::

   make html

The resulting HTML files end up in the ``build/html`` directory.

You can now make edits to rst files and run ``make html`` again to update
the affected pages.

.. _Sphinx: https://www.sphinx-doc.org/
