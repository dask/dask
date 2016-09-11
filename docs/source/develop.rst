Development Guidelines
======================

Dask is a community maintained project.  We welcome contributions in the form
of bug reports, documentation, code, design proposals, and more.
This page provides resources on how best to contribute.

Where to ask for help
---------------------

Dask conversation happens in the following places:

1.  `StackOverflow #dask tag`_: for usage questions
2.  `Github Issue Tracker`_: for discussions around new features or established bugs
3.  `Gitter chat`_: for real-time discussion

For usage questions and bug reports we strongly prefer the use of StackOverflow
and Github issues over gitter chat.  Github and StackOverflow are more easily
searchable by future users and so is more efficient for everyone's time.
Gitter chat is generally reserved for community discussion.

.. _`StackOverflow #dask tag`: http://stackoverflow.com/questions/tagged/dask
.. _`Github Issue Tracker`: https://github.com/dask/dask/issues/
.. _`Gitter chat`: https://gitter.im/dask/dask


Separate Code Repositories
--------------------------

Dask maintains code and documentation in a few git repositories hosted on the
Github ``dask`` organization, http://github.com/dask.  This includes the primary
repository and several other repositories for different components.  A
non-exhaustive list follows:

*  http://github.com/dask/dask: The main code repository holding parallel
   algorithms, the single-machine scheduler, and most documentation.
*  http://github.com/dask/distributed: The distributed memory scheduler
*  http://github.com/dask/hdfs3: Hadoop Filesystem interface
*  http://github.com/dask/s3fs: S3 Filesystem interface
*  http://github.com/dask/dask-ec2: AWS launching
*  ...

Git and Github can be challenging at first.  Fortunately good materials exist
on the internet.  Rather than repeat these materials here we refer you to
Pandas' documentation and links on this subject at
http://pandas.pydata.org/pandas-docs/stable/contributing.html


Issues
------

The community discusses and tracks known bugs and potential features in the
`Github Issue Tracker`_.  If you have a new idea or have identified a bug then
you should raise it there to start public discussion.

If you are looking for an introductory issue to get started with development
then check out the `introductory label`_, which contains issues that are good
for starting developers.  Generally familiarity with Python, NumPy, Pandas, and
some parallel computing are assumed.

.. _`introductory label`: https://github.com/dask/dask/issues?q=is%3Aissue+is%3Aopen+label%3Aintroductory


Development Environment
-----------------------

Download code
~~~~~~~~~~~~~

Clone the main dask git repository (or whatever repository you're working on.)::

   git clone git@github.com:dask/dask.git


Install
~~~~~~~

You may want to install larger dependencies like NumPy and Pandas using a
binary package manager, like conda_.  You can skip this step if you already
have these libraries, don't care to use them, or have sufficient build
environment on your computer to compile them when installing with ``pip``::

   conda install -y numpy pandas scipy bokeh cytoolz pytables h5py

.. _conda: http://conda.pydata.org/docs/

Install dask and dependencies::

   cd dask
   pip install -e .[complete]

For development dask uses the following additional dependencies::

   pip install pytest moto mock


Run Tests
~~~~~~~~~

Dask uses py.test_ for testing.  You can run tests from the main dask directory
as follows::

   py.test dask --verbose

.. _py.test: http://pytest.org/latest/


Contributing to Code
--------------------

Dask maintains development standards that are similar to most PyData projects.  These standards include
language support, testing, documentation, and style.

Python Versions
~~~~~~~~~~~~~~~

Dask supports Python versions 2.7, 3.3, 3.4, and 3.5 in a single codebase.
Name changes are handled by the :file:`dask/compatibility.py` file.

Test
~~~~

Dask employs extensive unit tests to ensure correctness of code both for today
and for the future.  Test coverage is expected for all code contributions.

Tests are written in a py.test style with bare functions.

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
and running quickly (slow test suites get run less often.)

You can run tests locally by running ``py.test`` in the local dask directory::

   py.test dask --verbose

You can also test certain modules or individual tests for faster response::

   py.test dask/dataframe --verbose

   py.test dask/dataframe/tests/test_dataframe_core.py::test_set_index

Tests run automatically on the Travis.ci continuous testing framework on every
push to every pull request on GitHub.


Docstrings
~~~~~~~~~~

User facing functions should roughly follow the numpydoc_ standard, including
sections for ``Parameters``, ``Examples`` and general explanatory prose.

By default examples will be doc-tested.  Reproducible examples in documentation
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

Docstrings are currently tested under Python 2.7 on travis.ci.  You can test
docstrings with pytest as follows::

   py.test dask --doctest-modules


Style
~~~~~

Dask adheres loosely to PEP8 with rule-breaking allowed.


Changelog
~~~~~~~~~

Every significative code contribution should be listed in the
:doc:`changelog` under the corresponding version. When submitting a Pull
Request in Github please add to that file explaining what was added/modified.


Contributing to Documentation
-----------------------------

Dask uses Sphinx_ for documentation, hosted on http://readthedocs.org .
Documentation is maintained in the RestructuredText markup language (``.rst``
files) in ``dask/docs/source``.  The documentation consists both of prose
and API documentation.

To build the documentation locally, first install requirements::

   cd docs/
   pip install -r requirements-docs.txt

Then build documentation with ``make``::

   make html

The resulting HTML files end up in the ``build/html`` directory.

You can now make edits to rst files and run ``make html`` again to update
the affected pages.

.. _Sphinx: http://www.sphinx-doc.org/
