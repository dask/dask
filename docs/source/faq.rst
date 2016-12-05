Frequently Asked Questions
==========================

More questions can be found on StackOverflow at http://stackoverflow.com/search?tab=votes&q=dask%20distributed

How do I use external modules?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``client.upload_file``. For more detail, see the `API docs`_ and a
StackOverflow question
`"Can I use functions imported from .py files in Dask/Distributed?"`__
This function supports both standalone file and setuptools's ``.egg`` files
for larger modules.

__ http://stackoverflow.com/questions/39295200/can-i-use-functions-imported-from-py-files-in-dask-distributed
.. _API docs: https://distributed.readthedocs.io/en/latest/api.html#distributed.executor.Executor.upload_file

Too many open file descriptors?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Your operating system imposes a limit to how many open files or open network
connections any user can have at once.  Depending on the scale of your
cluster the ``dask-scheduler`` may run into this limit.

By default most Linux distributions set this limit at 1024 open
files/connections and OS-X at 128 or 256.  Each worker adds a few open
connections to a running scheduler (somewhere between one and ten, depending on
how contentious things get.)

If you are on a managed cluster you can usually ask whoever manages your
cluster to increase this limit.  If you have root access and know what you are
doing you can change the limits on Linux by editing
``/etc/security/limits.conf``.  Instructions are here under the heading "User
Level FD Limits":
http://www.cyberciti.biz/faq/linux-increase-the-maximum-number-of-open-files/

Error when running dask-worker about ``OMP_NUM_THREADS``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For more problems with OMP_NUM_THREADS, see
http://stackoverflow.com/questions/39422092/error-with-omp-num-threads-when-using-dask-distributed


Does Dask handle Data Locality?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, both data locality in memory and data locality on disk.

Often it's *much* cheaper to move computations to where data lives.  If one of
your tasks creates a large array and a future task computes the sum of that
array, you want to be sure that the sum runs on the same worker that has the
array in the first place, otherwise you'll wait for a long while as the data
moves between workers.  Needless communication can easily dominate costs if
we're sloppy.

The Dask Scheduler tracks the location and size of every intermediate value
produced by every worker and uses this information when assigning future tasks
to workers.  Dask tries to make computations more efficient by minimizing data
movement.

Sometimes your data is on a hard drive or other remote storage that isn't
controlled by Dask.  In this case the scheduler is unaware of exactly where your
data lives, so you have to do a bit more work.  You can tell Dask to
preferentially run a task on a particular worker or set of workers.

For example Dask developers use this ability to build in data locality when we
communicate to data-local storage systems like the Hadoop File System.  When
users use high-level functions like
``dask.dataframe.read_csv('hdfs:///path/to/files.*.csv'`` Dask talks to the
HDFS name node, finds the locations of all of the blocks of data, and sends
that information to the scheduler so that it can make smarter decisions and
improve load times for users.
