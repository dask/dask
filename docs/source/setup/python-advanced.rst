Python API (advanced)
=====================

In some rare cases, experts may want to create ``Scheduler`` and ``Worker``
objects explicitly in Python manually.  This is often necessary when making
tools to automatically deploy Dask in custom settings.

However, often it is sufficient to rely on the :doc:`Dask command line interface
<cli>`.

Scheduler
---------

To start the Scheduler, provide the listening port (defaults to 8786) and Tornado
IOLoop (defaults to ``IOLoop.current()``)

.. code-block:: python

   from distributed import Scheduler
   from tornado.ioloop import IOLoop
   from threading import Thread

   s = Scheduler()
   s.start('tcp://:8786')   # Listen on TCP port 8786

   loop = IOLoop.current()
   loop.start()

Alternatively, you may want the IOLoop and scheduler to run in a separate
thread.  In this case, you would replace the ``loop.start()`` call with the
following:

.. code-block:: python

   t = Thread(target=loop.start, daemon=True)
   t.start()


Worker
------

On other nodes, start worker processes that point to the URL of the scheduler.

.. code-block:: python

   from distributed import Worker
   from tornado.ioloop import IOLoop
   from threading import Thread

   w = Worker('tcp://127.0.0.1:8786')
   w.start()  # choose randomly assigned port

   loop = IOLoop.current()
   loop.start()

Alternatively, replace ``Worker`` with ``Nanny`` if you want your workers to be
managed in a separate process by a local nanny process.  This allows workers to
restart themselves in case of failure. Also, it provides some additional monitoring, 
and is useful when coordinating many workers that should live in different
processes in order to avoid the GIL_.

.. _GIL: https://docs.python.org/3/glossary.html#term-gil
