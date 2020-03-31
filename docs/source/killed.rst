.. _killed:

Why did my worker die?
----------------------

A Dask worker can cease functioning for a number of reasons. These fall into the
following categories:

- the worker chooses to exit
- an unrecoverable exception happens within the worker
- the worker process is shut down by some external action

Each of these cases will be described in more detail below. The *symptoms* you will
experience when these things happen range from simply work not getting done anymore,
to various exceptions appearing when you interact with your local client, such as
``KilledWorker``, ``TimeoutError`` and ``CommClosedError``.

Note the special case of ``KilledWorker``: this means that a particular task was
tried on a worker, and it died, and then the same task was sent to another worker,
which also died. After a configurable number of deaths (config key "
``distributed.scheduler.allowed-failures``), Dask decides to blame the
task itself, and returns this exception. Note, that it is possible for a task to be
unfairly blamed - the worker happened to die while the task was active, perhaps
due to another thread - complicating diagnosis.

In every case, the first place to look for further information is the logs of
the given worker, which may well give a complete description of what happened. These
logs are printed by the worker to its "standard error", which may appear in the text
console from which you launched the worker, or some logging system maintained by
the cluster infrastructure. It is also helpful to watch the diagnostic dashboard to
look for memory spikes, but of course this is only possible while the worker is still
alive.

In all cases, the scheduler will notice that the worker has gone, either because
of an explicit de-registration, or because the worker no longer produces heartbeats,
and it should be possible to reroute tasks to other workers and have the system
keep running.

Scenarios
~~~~~~~~~

Worker chose to exit
''''''''''''''''''''

Workers may exit in normal functioning because they have been asked to, e.g.,
they received a keyboard interrupt (^C), or the scheduler scaled down the cluster.
In such cases, the work that was being done by the worker will be redirected to
other workers, if there are any left.

You should expect to see the following message at the end of the worker's log:

::

   distributed.dask_worker - INFO - End worker

In these cases, there is not normally anything which you need to do, since the
behaviour is expected.

Unrecoverable Exception
'''''''''''''''''''''''

The worker is a python process, and like any other code, an exception may occur
which causes the process to exit. One typical example of this might be a
version mismatch between the packages of the client and worker, so that
a message sent to the worker errors while being unpacked. There are a number of
packages that need to match, not only ``dask`` and ``distributed``.

In this case, you should expect to see the full python traceback in the worker's
log. In the event of a version mismatch, this might be complaining about a bad
import or missing attribute. However, other fatal exceptions are also possible,
such as trying to allocate more memory than the system has available, or writing
temporary files without appropriate permissions.

To assure that you have matching versions, you should run (more recent versions
of distributed may do this automatically)

.. code-block::

   client.get_versions(check=True)

For other errors, you might want to run the computation in your local client, if
possible, or try grabbing just the task that errored and using
:func:`recreate_error_locally <distributed.recreate_exceptions.ReplayExceptionClient.recreate_error_locally>`,
as you would for ordinary exceptions happening during task execution.

Specifically for connectivity problems (e.g., timeout exceptions in the worker
logs), you will need to diagnose your networking infrastructure, which is more
complicated than can be described here. Commonly, it may involve logging into
the machine running the affected worker
(although you can :ref:`ipylaunch`).

Killed by Nanny
'''''''''''''''

The Dask "nanny" is a process which watches the worker, and restarts it if
necessary. It also tracks the worker's memory usage, and if it should cross
a given fraction of total memory, then also the worker will be restarted,
interrupting any work in progress. The log will show a message like

::

    Worker exceeded X memory budget. Restarting

Where X is the memory fraction. You can set this critical fraction using
the configuration, see :ref:`memman`. If you have an external system for
watching memory usage provided by your cluster infrastructure (HPC,
kubernetes, etc.), then it may be reasonable to turn off this memory
limit. Indeed, in these cases, restarts might be handled for you too, so
you could do without the nanny at all (``--no-nanny`` CLI option or
configuration equivalent).

Sudden Exit
'''''''''''

The worker process may stop working without notice. This can happen due to
something internal to the worker, e.g., a memory violation (common if interfacing
with compiled code), or due to something external, e.g., the ``kill`` command, or
stopping of the container or machine on which the worker is running.

In the best case, you may have a line in the logs from the OS saying that the
worker was shut down, such as the single word "killed"  or something more descriptive.
In these cases, the fault may well be in your code, and you might be able to use the
same debugging tools as in the previous section.

However, if the action was initiated by some outside framework, then the worker will
have no time to leave a logging message, and the death *may* have nothing to do with
what the worker was doing at the time. For example, if kubernetes decides to evict a
pod, or your ec2 instance goes down for maintenance, the worker is not at fault.
Hopefully, the system provides a reasonable message of what happened in the process
output.
However, if the memory allocation (or other resource) exceeds toleration, then it
*is* the code's fault - although you may be able to fix with better configuration
of Dask's own limits, or simply with a bigger cluster. In any case, your deployment
framework has its own logging system, and you should look there for the reason that
the dask worker was taken down.

Specifically for memory issues, refer to the memory section of `best practices`_.

.. _best practices: https://docs.dask.org/en/latest/best-practices.html#avoid-very-large-partitions
