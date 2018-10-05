Development Guidelines
======================

This repository is part of the Dask_ projects.  General development guidelines
including where to ask for help, a layout of repositories, testing practices,
and documentation and style standards are available at the `Dask developer
guidelines`_ in the main documentation.

.. _Dask: http://dask.org
.. _`Dask developer guidelines`: http://docs.dask.org/en/latest/develop.html

Install
-------

After setting up an environment as described in the `Dask developer
guidelines`_ you can clone this repository with git::

   git clone git@github.com:dask/distributed.git

and install it from source::

   cd distributed
   python setup.py install

Test
----

Test using ``py.test``::

   py.test distributed --verbose

Tornado
-------

Dask.distributed is a Tornado TCP application.  Tornado provides us with both a
communication layer on top of sockets, as well as a syntax for writing
asynchronous coroutines, similar to asyncio.  You can make modest changes to
the policies within this library without understanding much about Tornado,
however moderate changes will probably require you to understand Tornado
IOLoops, coroutines, and a little about non-blocking communication..  The
Tornado API documentation is quite good and we recommend that you read the
following resources:

*  http://www.tornadoweb.org/en/stable/gen.html
*  http://www.tornadoweb.org/en/stable/ioloop.html

Additionally, if you want to interact at a low level with the communication
between workers and scheduler then you should understand the Tornado
``TCPServer`` and ``IOStream`` available here:

*  http://www.tornadoweb.org/en/stable/networking.html

Dask.distributed wraps a bit of logic around Tornado.  See
:doc:`Foundations<foundations>` for more information.

Writing Tests
-------------

Testing distributed systems is normally quite difficult because it is difficult
to inspect the state of all components when something goes wrong.  Fortunately,
the non-blocking asynchronous model within Tornado allows us to run a
scheduler, multiple workers, and multiple clients all within a single thread.
This gives us predictable performance, clean shutdowns, and the ability to drop
into any point of the code during execution.
At the same time, sometimes we want everything to run in different processes in
order to simulate a more realistic setting.

The test suite contains three kinds of tests

1.  ``@gen_cluster``: Fully asynchronous tests where all components live in the
    same event loop in the main thread.  These are good for testing complex
    logic and inspecting the state of the system directly.  They are also
    easier to debug and cause the fewest problems with shutdowns.
2.  ``def test_foo(client)``: Tests with multiple processes forked from the master
    process.  These are good for testing the synchronous (normal user) API and
    when triggering hard failures for resilience tests.
3.  ``popen``: Tests that call out to the command line to start the system.
    These are rare and mostly for testing the command line interface.

If you are comfortable with the Tornado interface then you will be happiest
using the ``@gen_cluster`` style of test

.. code-block:: python

   from distributed.utils_test import gen_cluster

   @gen_cluster(client=True)
   def test_submit(c, s, a, b):
       assert isinstance(c, Client)
       assert isinstance(s, Scheduler)
       assert isinstance(a, Worker)
       assert isinstance(b, Worker)

       future = c.submit(inc, 1)
       assert future.key in c.futures

       # result = future.result()  # This synchronous API call would block
       result = yield future
       assert result == 2

       assert future.key in s.tasks
       assert future.key in a.data or future.key in b.data

The ``@gen_cluster`` decorator sets up a scheduler, client, and workers for
you and cleans them up after the test.  It also allows you to directly inspect
the state of every element of the cluster directly.  However, you can not use
the normal synchronous API (doing so will cause the test to wait forever) and
instead you need to use the coroutine API, where all blocking functions are
prepended with an underscore (``_``).  Beware, it is a common mistake to use
the blocking interface within these tests.

If you want to test the normal synchronous API you can use the ``client``
pytest fixture style test, which sets up a scheduler and workers for you in
different forked processes:

.. code-block:: python

   from distributed.utils_test import client

   def test_submit(client):
       future = client.submit(inc, 10)
       assert future.result() == 11

Additionally, if you want access to the scheduler and worker processes you can
also add the ``s, a, b`` fixtures as well.


.. code-block:: python

   from distributed.utils_test import client

   def test_submit(client, s, a, b):
       future = client.submit(inc, 10)
       assert future.result() == 11  # use the synchronous/blocking API here

       a['proc'].terminate()  # kill one of the workers

       result = future.result()  # test that future remains valid
       assert result == 2

In this style of test you do not have access to the scheduler or workers.  The
variables ``s, a, b`` are now dictionaries holding a
``multiprocessing.Process`` object and a port integer.  However, you can now
use the normal synchronous API (never use yield in this style of test) and you
can close processes easily by terminating them.

Typically for most user-facing functions you will find both kinds of tests.
The ``@gen_cluster`` tests test particular logic while the ``client`` pytest
fixture tests test basic interface and resilience.

You should avoid ``popen`` style tests unless absolutely necessary, such as if
you need to test the command line interface.
